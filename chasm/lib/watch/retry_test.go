package watch

import (
	"fmt"
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	watchpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/grpc/codes"
)

func TestControlRetryResponseLossDoesNotReauthorize(t *testing.T) {
	t.Parallel()
	env, h, create := newHandlerTestEnvironment(t)
	_, err := env.accept(Batch{Revision: 1, Incarnation: "query-incarnation", Initial: true, Complete: true})
	require.NoError(t, err)
	receipt, err := env.accept(testBatch())
	require.NoError(t, err)
	id := receipt.ActionIDs[0]
	require.NoError(t, env.update(func(c *Component, ctx chasm.MutableContext) error {
		c.Ambiguous(ctx, id, 0)
		return nil
	}))
	request := &watchpb.RetryDeliveryRequest{
		NamespaceId: create.NamespaceId, ExecutionId: create.ExecutionId,
		Command: &contractpb.RetryDeliveryRequest{
			Namespace: env.create.Namespace, Id: create.WatchId, ExpectedRevision: 1,
			ActionId: id, RequestId: "retry-once", AcknowledgeAmbiguous: true,
		},
	}

	first, err := h.RetryDelivery(env.ctx, request)
	require.NoError(t, err)
	require.NotNil(t, first.GetDelivery())
	require.NoError(t, env.update(func(c *Component, ctx chasm.MutableContext) error {
		intent, err := c.BeginDispatch(ctx, id)
		if err != nil {
			return err
		}
		require.NotNil(t, intent)
		c.Ambiguous(ctx, id, intent.Attempts)
		return nil
	}))
	before := env.intent(t, id)
	retried, err := h.RetryDelivery(env.ctx, request)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, first, retried)
	protorequire.ProtoEqual(t, before, env.intent(t, id))
	require.False(t, env.intent(t, id).RetryAuthorized)
}

func TestControlRetryRetainsOriginalRejection(t *testing.T) {
	t.Parallel()
	env, h, create := newHandlerTestEnvironment(t)
	_, err := env.accept(Batch{Revision: 1, Incarnation: "query-incarnation", Initial: true, Complete: true})
	require.NoError(t, err)
	accepted, err := env.accept(testBatch())
	require.NoError(t, err)
	id := accepted.ActionIDs[0]
	request := &watchpb.RetryDeliveryRequest{
		NamespaceId: create.NamespaceId, ExecutionId: create.ExecutionId,
		Command: &contractpb.RetryDeliveryRequest{
			Namespace: env.create.Namespace, Id: create.WatchId, ExpectedRevision: 1, ActionId: id, RequestId: "original",
		},
	}
	_, err = h.RetryDelivery(env.ctx, request)
	require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())
	require.NoError(t, env.update(func(c *Component, ctx chasm.MutableContext) error {
		return c.Complete(ctx, id, 0, "", "PermissionDenied")
	}))
	before := env.intent(t, id)
	_, err = h.RetryDelivery(env.ctx, request)
	require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())
	protorequire.ProtoEqual(t, before, env.intent(t, id))
	request.Command.AcknowledgeAmbiguous = true
	_, err = h.RetryDelivery(env.ctx, request)
	require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	protorequire.ProtoEqual(t, before, env.intent(t, id))
	request.Command.RequestId = "new-authorization"
	first, err := h.RetryDelivery(env.ctx, request)
	require.NoError(t, err)
	require.NoError(t, env.update(func(c *Component, ctx chasm.MutableContext) error {
		if err := c.Complete(ctx, id, 0, "delivered-run", ""); err != nil {
			return err
		}
		return c.Pause(c.Revision, true)
	}))
	retried, err := h.RetryDelivery(env.ctx, request)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, first, retried)
	require.Equal(t, watchpb.DELIVERY_STATUS_DELIVERED, env.intent(t, id).Status)
}

func TestControlRetryCapacityDoesNotEvictOrAuthorize(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"count", "bytes"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			env, h, create := newHandlerTestEnvironment(t)
			_, err := env.accept(Batch{Revision: 1, Incarnation: "query-incarnation", Initial: true, Complete: true})
			require.NoError(t, err)
			receipt, err := env.accept(testBatch())
			require.NoError(t, err)
			id := receipt.ActionIDs[0]
			require.NoError(t, env.update(func(c *Component, ctx chasm.MutableContext) error {
				c.RetryReceipts = make(map[string]*watchpb.RetryReceipt)
				if mode == "count" {
					for i := range 1024 {
						c.RetryReceipts[fmt.Sprint(i)] = &watchpb.RetryReceipt{Fingerprint: []byte("retained")}
					}
				} else {
					c.RetryReceipts["large"] = &watchpb.RetryReceipt{Fingerprint: make([]byte, 1<<20)}
				}
				return c.Complete(ctx, id, 0, "", "PermissionDenied")
			}))
			before, intent := env.state(t), env.intent(t, id)
			_, err = h.RetryDelivery(env.ctx, &watchpb.RetryDeliveryRequest{
				NamespaceId: create.NamespaceId, ExecutionId: create.ExecutionId,
				Command: &contractpb.RetryDeliveryRequest{
					Namespace: env.create.Namespace, Id: create.WatchId, ExpectedRevision: 1, ActionId: id, RequestId: "new",
				},
			})
			require.Equal(t, codes.ResourceExhausted, serviceerror.ToStatus(err).Code())
			protorequire.ProtoEqual(t, before, env.state(t))
			protorequire.ProtoEqual(t, intent, env.intent(t, id))
		})
	}
}

func TestControlRetryReceiptSurvivesDetailEviction(t *testing.T) {
	t.Parallel()
	env, h, create := newHandlerTestEnvironment(t)
	_, err := env.accept(Batch{Revision: 1, Incarnation: "query-incarnation", Initial: true, Complete: true})
	require.NoError(t, err)
	accepted, err := env.accept(testBatch())
	require.NoError(t, err)
	id := accepted.ActionIDs[0]
	require.NoError(t, env.update(func(c *Component, ctx chasm.MutableContext) error {
		return c.Complete(ctx, id, 0, "", "PermissionDenied")
	}))
	request := &watchpb.RetryDeliveryRequest{
		NamespaceId: create.NamespaceId, ExecutionId: create.ExecutionId,
		Command: &contractpb.RetryDeliveryRequest{
			Namespace: env.create.Namespace, Id: create.WatchId, ExpectedRevision: 1, ActionId: id, RequestId: "retained-retry",
		},
	}
	original, err := h.RetryDelivery(env.ctx, request)
	require.NoError(t, err)
	require.NoError(t, env.update(func(c *Component, ctx chasm.MutableContext) error {
		c.MaxRecent = 0
		if err := c.Complete(ctx, id, 0, "delivered-run", ""); err != nil {
			return err
		}
		return c.Cancel(c.Revision)
	}))
	require.Empty(t, env.state(t).PendingIds)
	require.Empty(t, env.state(t).RecentIds)
	replayed, err := h.RetryDelivery(env.ctx, request)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, original, replayed)
}
