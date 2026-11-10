package watchregistry

import (
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/grpc/codes"
)

func TestRegistryDeliveryRetryAuthorizesOnceBeforeRevisionChanges(t *testing.T) {
	t.Parallel()
	env := newEnvironment(t, DefaultLimits())
	_, err := env.mutate(env.createSource("source", "source"))
	require.NoError(t, err)
	_, err = env.mutate(env.createQuery("query", "source", 1))
	require.NoError(t, err)
	_, err = env.mutate(env.createWatch("watch", watchSpec("query", 1)))
	require.NoError(t, err)
	command := &contractpb.RetryDeliveryRequest{
		Namespace: env.namespace, Id: "watch", ExpectedRevision: 1, ActionId: "action", RequestId: "retry",
	}
	request := &registrypb.MutateRequest{
		RetryIncarnation: "1", Operation: &registrypb.MutateRequest_RetryDelivery{RetryDelivery: command},
	}
	before := env.state(t)
	original, err := env.mutate(request)
	require.NoError(t, err)
	require.NotEmpty(t, original.RetryExecutionId)
	require.Equal(t, before.Generation, env.state(t).Generation)
	protorequire.ProtoEqual(t, before.Current["3/watch"], env.state(t).Current["3/watch"])
	require.Len(t, env.state(t).Revisions, len(before.Revisions))
	_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_DeleteWatch{
		DeleteWatch: env.watchOperation("watch", 1, "delete"),
	}})
	require.NoError(t, err)
	env.enabled = false
	request.RetryIncarnation = "obsolete-resolution"
	replayed, err := env.mutate(request)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, original, replayed)
	lookedUp, err := env.read(&registrypb.ReadRequest{Operation: &registrypb.ReadRequest_DeliveryRetry{DeliveryRetry: command}})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, original, lookedUp.DeliveryRetry)
	changed := common.CloneProto(command)
	changed.AcknowledgeAmbiguous = true
	_, err = env.read(&registrypb.ReadRequest{Operation: &registrypb.ReadRequest_DeliveryRetry{DeliveryRetry: changed}})
	require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	changed.RequestId = "stale"
	_, err = env.mutate(&registrypb.MutateRequest{
		RetryIncarnation: "1", Operation: &registrypb.MutateRequest_RetryDelivery{RetryDelivery: changed},
	})
	require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())
	changed.RequestId, changed.ExpectedRevision = "cleanup", 2
	cleanup, err := env.mutate(&registrypb.MutateRequest{
		RetryIncarnation: "1", Operation: &registrypb.MutateRequest_RetryDelivery{RetryDelivery: changed},
	})
	require.NoError(t, err)
	require.Equal(t, original.RetryExecutionId, cleanup.RetryExecutionId)
}

func TestRegistryDeliveryRetryRetainsMissingActionDecision(t *testing.T) {
	t.Parallel()
	env := newEnvironment(t, DefaultLimits())
	_, err := env.mutate(env.createSource("source", "source"))
	require.NoError(t, err)
	_, err = env.mutate(env.createQuery("query", "source", 1))
	require.NoError(t, err)
	_, err = env.mutate(env.createWatch("watch", watchSpec("query", 1)))
	require.NoError(t, err)
	command := &contractpb.RetryDeliveryRequest{
		Namespace: env.namespace, Id: "watch", ExpectedRevision: 1, ActionId: "not-yet-accepted", RequestId: "missing-action",
	}
	request := &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_RetryDelivery{RetryDelivery: command}}
	_, err = env.mutate(request)
	require.Equal(t, codes.NotFound, serviceerror.ToStatus(err).Code())
	request.RetryIncarnation = "1"
	_, err = env.mutate(request)
	require.Equal(t, codes.NotFound, serviceerror.ToStatus(err).Code())
	lookedUp, err := env.read(&registrypb.ReadRequest{Operation: &registrypb.ReadRequest_DeliveryRetry{DeliveryRetry: command}})
	require.NoError(t, err)
	require.Equal(t, registrypb.REJECTION_CODE_NOT_FOUND, lookedUp.DeliveryRetry.Rejection.Code)
	_, err = env.read(&registrypb.ReadRequest{Operation: &registrypb.ReadRequest_DeliveryRetry{DeliveryRetry: &contractpb.RetryDeliveryRequest{
		Namespace: env.namespace, Id: "watch", ExpectedRevision: 1, ActionId: "action", RequestId: "create-watch",
	}}})
	require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
}
