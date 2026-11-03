package persistence

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

func TestExecutionFeedPersistenceMetrics(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		category  tasks.Category
		ack       bool
		operation string
	}{
		{name: "read ingestion queue", category: tasks.CategoryExecutionFeed, operation: "GetExecutionFeedTasks"},
		{name: "read retained journal", category: tasks.CategoryExecutionFeedJournal, operation: "GetExecutionFeedJournalTasks"},
		{name: "ack ingestion queue", category: tasks.CategoryExecutionFeed, ack: true, operation: "CompleteExecutionFeedTask"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			manager := NewMockExecutionManager(gomock.NewController(t))
			handler := metricstest.NewCaptureHandler()
			client := NewExecutionPersistenceMetricsClient(
				manager, handler, NoopHealthSignalAggregator, log.NewNoopLogger(), dynamicconfig.GetBoolPropertyFn(false),
			)
			capture := handler.StartCapture()
			if test.ack {
				request := &CompleteHistoryTaskRequest{ShardID: 1, TaskCategory: test.category, TaskKey: tasks.NewImmediateKey(10)}
				manager.EXPECT().CompleteHistoryTask(t.Context(), request).Return(nil)
				require.NoError(t, client.CompleteHistoryTask(t.Context(), request))
			} else {
				request := &GetHistoryTasksRequest{ShardID: 1, TaskCategory: test.category, BatchSize: 1}
				expected := &GetHistoryTasksResponse{}
				manager.EXPECT().GetHistoryTasks(t.Context(), request).Return(expected, nil)
				actual, err := client.GetHistoryTasks(t.Context(), request)
				require.NoError(t, err)
				require.Equal(t, expected, actual)
			}
			recorded := capture.Snapshot()[metrics.PersistenceRequests.Name()]
			require.Len(t, recorded, 1)
			require.Equal(t, test.operation, recorded[0].Tags[metrics.OperationTag("").Key])
		})
	}
}

func TestExecutionFeedJournalCleanupIsRejected(t *testing.T) {
	t.Parallel()
	manager := NewMockExecutionManager(gomock.NewController(t))
	client := NewExecutionPersistenceMetricsClient(
		manager, metrics.NoopMetricsHandler, NoopHealthSignalAggregator, log.NewNoopLogger(), dynamicconfig.GetBoolPropertyFn(false),
	)
	require.Error(t, client.CompleteHistoryTask(t.Context(), &CompleteHistoryTaskRequest{
		ShardID: 1, TaskCategory: tasks.CategoryExecutionFeedJournal,
	}))
	require.Error(t, client.RangeCompleteHistoryTasks(t.Context(), &RangeCompleteHistoryTasksRequest{
		ShardID: 1, TaskCategory: tasks.CategoryExecutionFeedJournal,
	}))
}
