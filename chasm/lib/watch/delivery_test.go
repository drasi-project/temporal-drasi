package watch

import (
	"testing"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	watchpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAcceptedDeliveryRetainsOutputIdentity(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t)
	first, err := env.accept(testBatch())
	require.NoError(t, err)
	id := first.ActionIDs[0]
	expected := &contractpb.OutputIdentity{
		QueryRevision: 3, Incarnation: "query-incarnation", Sequence: 1, RowIdentity: "row",
	}
	protorequire.ProtoEqual(t, expected, env.intent(t, id).Output)
	protorequire.ProtoEqual(t, timestamppb.New(env.clock.Now()), env.intent(t, id).UpdatedAt)

	next := testBatch()
	next.Sequence, next.Actions[0].RowKey = 2, "another-row"
	_, err = env.accept(next)
	require.NoError(t, err)
	completedAt := env.clock.Now().Add(time.Second)
	env.clock.Update(completedAt)
	require.NoError(t, env.update(func(c *Component, ctx chasm.MutableContext) error {
		return c.Complete(ctx, id, 0, "delivered-run", "")
	}))
	intent := env.intent(t, id)
	require.Empty(t, intent.Request)
	protorequire.ProtoEqual(t, expected, intent.Output)
	protorequire.ProtoEqual(t, timestamppb.New(completedAt), intent.UpdatedAt)

	response, err := describeExecution(env.ctx, env.ref)
	require.NoError(t, err)
	require.Equal(t, uint64(2), response.State.Cursor.Sequence)
	require.Len(t, response.Deliveries, 2)
	protorequire.ProtoEqual(t, intent, response.Deliveries[1])
	for _, delivery := range response.Deliveries {
		require.Empty(t, delivery.Request)
	}
}

func TestDeliveryTransitionTimestamps(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		prepare func(*Component, chasm.MutableContext, string) error
		run     func(*Component, chasm.MutableContext, string) error
	}{
		{"dispatch", nil, func(c *Component, ctx chasm.MutableContext, id string) error {
			_, err := c.BeginDispatch(ctx, id)
			return err
		}},
		{"blocked", nil, func(c *Component, ctx chasm.MutableContext, id string) error {
			return c.Complete(ctx, id, 0, "", "PermissionDenied")
		}},
		{"ambiguous", nil, func(c *Component, ctx chasm.MutableContext, id string) error {
			c.Ambiguous(ctx, id, 0)
			return nil
		}},
		{"retry", func(c *Component, ctx chasm.MutableContext, id string) error {
			return c.Complete(ctx, id, 0, "", "PermissionDenied")
		}, func(c *Component, ctx chasm.MutableContext, id string) error {
			return c.retry(ctx, id, false)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newTestEnvironment(t)
			receipt, err := env.accept(testBatch())
			require.NoError(t, err)
			id := receipt.ActionIDs[0]
			if tc.prepare != nil {
				require.NoError(t, env.update(func(c *Component, ctx chasm.MutableContext) error {
					return tc.prepare(c, ctx, id)
				}))
			}
			before := env.intent(t, id)
			now := env.clock.Now().Add(time.Second)
			env.clock.Update(now)
			require.NoError(t, env.update(func(c *Component, ctx chasm.MutableContext) error {
				return tc.run(c, ctx, id)
			}))
			after := env.intent(t, id)
			protorequire.ProtoEqual(t, before.Output, after.Output)
			protorequire.ProtoEqual(t, timestamppb.New(now), after.UpdatedAt)
		})
	}
}

func TestDeliveryMetadataBudgetIsAtomic(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t)
	batch := testBatch()
	receipt, err := env.accept(batch)
	require.NoError(t, err)
	intent := env.intent(t, receipt.ActionIDs[0])
	require.Equal(t, uint64(len(intent.Request)+proto.Size(intent.Output)), env.state(t).PendingBytes)

	before := env.state(t)
	batch.Sequence = 2
	batch.Actions[0].RowKey = string(make([]byte, env.create.Limits.PendingBytes))
	_, err = env.accept(batch)
	require.Equal(t, codes.ResourceExhausted, serviceerror.ToStatus(err).Code())
	protorequire.ProtoEqual(t, before, env.state(t))
	protorequire.ProtoEqual(t, intent, env.intent(t, receipt.ActionIDs[0]))

	require.NoError(t, env.update(func(c *Component, ctx chasm.MutableContext) error {
		return c.Complete(ctx, receipt.ActionIDs[0], 0, "delivered-run", "")
	}))
	require.Zero(t, env.state(t).PendingBytes)
}

func TestAcceptanceRequiresAuthoritativeQueryIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		definition []byte
	}{
		{"malformed definition", []byte{0xff}},
		{"missing query", nil},
		{"missing revision", []byte{0x22, 0x02, 0x0a, 0x00}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newTestEnvironment(t)
			require.NoError(t, env.update(func(c *Component, _ chasm.MutableContext) error {
				c.Definition = tc.definition
				return nil
			}))
			before := env.state(t)
			_, err := env.accept(testBatch())
			require.Equal(t, codes.DataLoss, serviceerror.ToStatus(err).Code())
			protorequire.ProtoEqual(t, before, env.state(t))
		})
	}
}

func TestDeliveryDuplicatePreservesRecordedMetadata(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t)
	batch := testBatch()
	receipt, err := env.accept(batch)
	require.NoError(t, err)
	id := receipt.ActionIDs[0]
	before := env.intent(t, id)
	env.clock.Update(env.clock.Now().Add(time.Second))
	duplicate, err := env.accept(batch)
	require.NoError(t, err)
	require.True(t, duplicate.Duplicate)
	protorequire.ProtoEqual(t, before, env.intent(t, id))

	require.NoError(t, env.update(func(c *Component, ctx chasm.MutableContext) error {
		return c.Complete(ctx, id, 0, "delivered-run", "")
	}))
	before = env.intent(t, id)
	env.clock.Update(env.clock.Now().Add(time.Second))
	require.NoError(t, env.update(func(c *Component, ctx chasm.MutableContext) error {
		return c.Complete(ctx, id, 0, "other-run", "")
	}))
	protorequire.ProtoEqual(t, before, env.intent(t, id))

	encoded, err := proto.Marshal(before)
	require.NoError(t, err)
	var restored watchpb.Intent
	require.NoError(t, proto.Unmarshal(encoded, &restored))
	protorequire.ProtoEqual(t, before, &restored)
	cloned := common.CloneProto(before)
	cloned.Output.RowIdentity = "modified-copy"
	require.Equal(t, "row", env.intent(t, id).Output.RowIdentity)
}
