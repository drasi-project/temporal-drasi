package watchregistry

import (
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
)

func TestRegistryLifecycleAndExplicitReset(t *testing.T) {
	t.Parallel()
	env := newEnvironment(t, DefaultLimits())
	_, err := env.mutate(env.createSource("source", "source"))
	require.NoError(t, err)
	_, err = env.mutate(env.createQuery("query", "source", 1))
	require.NoError(t, err)
	started, err := env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_StartQuery{
		StartQuery: env.queryOperation("query", 1, "start-query"),
	}})
	require.NoError(t, err)
	require.Equal(t, registrypb.DESIRED_STATE_RUNNING, started.Record.Desired)
	stopped, err := env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_StopQuery{
		StopQuery: env.queryOperation("query", 2, "stop-query"),
	}})
	require.NoError(t, err)
	require.Equal(t, registrypb.DESIRED_STATE_STOPPED, stopped.Record.Desired)
	created, err := env.mutate(env.createWatch("watch", watchSpec("query", 1)))
	require.NoError(t, err)
	spec := common.CloneProto(created.Record.GetWatch().Spec)
	spec.Trigger = contractpb.Trigger_TRIGGER_UPDATE
	updated, err := env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_UpdateWatch{
		UpdateWatch: &contractpb.UpdateWatchRequest{
			Namespace: env.namespace, Id: "watch", ExpectedRevision: 1, RequestId: "update", Spec: spec,
		},
	}})
	require.NoError(t, err)
	require.Equal(t, "2", updated.Record.Incarnation)
	paused, err := env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_PauseWatch{
		PauseWatch: env.watchOperation("watch", 2, "pause"),
	}})
	require.NoError(t, err)
	require.Equal(t, registrypb.DESIRED_STATE_PAUSED, paused.Record.Desired)
	resumed, err := env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_ResumeWatch{
		ResumeWatch: env.watchOperation("watch", 3, "resume"),
	}})
	require.NoError(t, err)
	require.Equal(t, updated.Record.Incarnation, resumed.Record.Incarnation)
	resetRequest := &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_ResetWatch{
		ResetWatch: &contractpb.ResetWatchRequest{
			Namespace: env.namespace, Id: "watch", ExpectedRevision: 4, RequestId: "reset",
			InitialResults: contractpb.InitialResults_INITIAL_RESULTS_INCLUDE,
		},
	}}
	reset, err := env.mutate(resetRequest)
	require.NoError(t, err)
	require.Equal(t, "5", reset.Record.Incarnation)
	require.Equal(t, contractpb.InitialResults_INITIAL_RESULTS_INCLUDE, reset.Record.GetWatch().Spec.InitialResults)
	require.Nil(t, reset.Record.GetWatch().Status)
	retried, err := env.mutate(resetRequest)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, reset, retried)
	_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_CancelWatch{
		CancelWatch: env.watchOperation("watch", 5, "invalid-cancel"),
	}})
	require.ErrorContains(t, err, "only one-shot")
	_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_DeleteWatch{
		DeleteWatch: env.watchOperation("watch", 5, "delete-watch"),
	}})
	require.NoError(t, err)
	_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_DeleteQuery{
		DeleteQuery: env.queryOperation("query", 3, "delete-query"),
	}})
	require.NoError(t, err)
	recreateRequest := env.createWatch("watch", watchSpec("query", 1))
	recreateRequest.GetCreateWatch().RequestId = "recreate"
	recreated, err := env.mutate(recreateRequest)
	require.ErrorContains(t, err, "dependency does not exist or is deleted")
	require.Nil(t, recreated)
}
