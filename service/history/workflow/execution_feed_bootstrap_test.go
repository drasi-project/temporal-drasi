package workflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/history/executionfeed"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestExecutionFeedBootstrapSerializesWithUpdate(t *testing.T) {
	t.Parallel()
	ms := executionFeedTestState(t, true)
	// Enumeration yields only keys, not a stale baseline object.
	key := ms.GetWorkflowKey()
	ms.pendingActivityInfoIDs[5] = &persistencespb.ActivityInfo{
		ActivityId: "activity", ScheduledEventId: 5, Attempt: 1, StartedEventId: common.EmptyEventID, HasRetryPolicy: true,
	}
	require.NoError(t, ms.initializeExecutionFeed())
	controller := gomock.NewController(t)
	shard := historyi.NewMockShardContext(controller)
	store := persistence.NewMockExecutionManager(controller)
	registry := namespace.NewMockRegistry(controller)
	wf := historyi.NewMockWorkflowContext(controller)
	shard.EXPECT().GetNamespaceRegistry().Return(registry)
	registry.EXPECT().GetNamespaceByID(ms.namespaceEntry.ID()).Return(tests.LocalNamespaceEntry, nil)
	shard.EXPECT().GetConfig().Return(ms.config)
	shard.EXPECT().GetExecutionManager().Return(store)
	shard.EXPECT().GetShardID().Return(int32(1))
	store.EXPECT().ListConcreteExecutions(gomock.Any(), &persistence.ListConcreteExecutionsRequest{ShardID: 1, PageSize: 100, KeysOnly: true}).
		Return(&persistence.ListConcreteExecutionsResponse{Keys: []definition.WorkflowKey{key}}, nil)

	updateNow := make(chan struct{})
	updateDone := make(chan error, 1)
	go func() {
		select {
		case <-updateNow:
		case <-t.Context().Done():
			return
		}
		ai := ms.pendingActivityInfoIDs[5]
		_, err := ms.AddActivityTaskStartedEvent(ai, 5, "start", "worker", nil, nil, nil, "", nil)
		if err == nil {
			_, _, err = ms.CloseTransactionAsMutation(t.Context(), historyi.TransactionPolicyActive)
		}
		updateDone <- err
	}()
	shard.EXPECT().GetCachedWorkflowContext(gomock.Any(), ms.namespaceEntry.ID(),
		&commonpb.WorkflowExecution{WorkflowId: key.WorkflowID, RunId: key.RunID}, locks.PriorityLow).
		DoAndReturn(func(context.Context, namespace.ID, *commonpb.WorkflowExecution, locks.Priority) (historyi.WorkflowContext, historyi.ReleaseWorkflowContextFunc, error) {
			// The update wins the execution lock after enumeration but before baseline capture.
			close(updateNow)
			err := await.Rcv(t, updateDone)
			return wf, func(error) {}, err
		})
	wf.EXPECT().LoadMutableState(gomock.Any(), shard).Return(ms, nil)
	var baseline *tasks.ExecutionFeedTask
	wf.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), shard).
		DoAndReturn(func(ctx context.Context, _ historyi.ShardContext) error {
			mutation, _, err := ms.CloseTransactionAsMutation(ctx, historyi.TransactionPolicyActive)
			if err == nil {
				baseline = mutation.Tasks[tasks.CategoryExecutionFeed][0].(*tasks.ExecutionFeedTask)
			}
			return err
		})
	token, err := executionfeed.BootstrapPage(t.Context(), shard, key.NamespaceID, nil, 100)
	require.NoError(t, err)
	require.Empty(t, token)
	require.NotNil(t, baseline)
	require.True(t, baseline.Baseline)
	require.Equal(t, "STARTED", baseline.State.Activities[0].Status)
	require.Empty(t, baseline.Facts)
}

func TestExecutionFeedBootstrapNotFound(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"primary lookup", "locked load", "commit"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			ms := executionFeedTestState(t, true)
			key := ms.GetWorkflowKey()
			controller := gomock.NewController(t)
			shard := historyi.NewMockShardContext(controller)
			store := persistence.NewMockExecutionManager(controller)
			registry := namespace.NewMockRegistry(controller)
			wf := historyi.NewMockWorkflowContext(controller)
			missing := serviceerror.NewNotFound("missing execution or history")
			shard.EXPECT().GetNamespaceRegistry().Return(registry)
			registry.EXPECT().GetNamespaceByID(ms.namespaceEntry.ID()).Return(tests.LocalNamespaceEntry, nil)
			shard.EXPECT().GetConfig().Return(ms.config)
			shard.EXPECT().GetExecutionManager().Return(store)
			shard.EXPECT().GetShardID().Return(int32(1))
			store.EXPECT().ListConcreteExecutions(gomock.Any(), gomock.Any()).
				Return(&persistence.ListConcreteExecutionsResponse{Keys: []definition.WorkflowKey{key}}, nil)
			acquire := shard.EXPECT().GetCachedWorkflowContext(gomock.Any(), ms.namespaceEntry.ID(), gomock.Any(), locks.PriorityLow)
			if stage == "primary lookup" {
				acquire.Return(nil, nil, missing)
			} else {
				acquire.Return(wf, historyi.ReleaseWorkflowContextFunc(func(error) {}), nil)
				load := wf.EXPECT().LoadMutableState(gomock.Any(), shard)
				if stage == "locked load" {
					load.Return(nil, missing)
				} else {
					load.Return(ms, nil)
					wf.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), shard).Return(missing)
				}
			}
			_, err := executionfeed.BootstrapPage(t.Context(), shard, key.NamespaceID, nil, 100)
			if stage == "commit" {
				require.ErrorIs(t, err, missing)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
