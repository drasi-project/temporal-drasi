package workflow

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testvars"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func executionFeedTestState(t *testing.T, enabled bool) *MutableStateImpl {
	t.Helper()
	s := &mutableStateSuite{}
	s.SetT(t)
	s.SetupTest()
	t.Cleanup(s.TearDownTest)
	s.mockConfig.ExecutionFeedEnabled = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(enabled)
	s.mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, gomock.Any()).
		Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	tv := testvars.New(t)
	tv = tv.WithRunID("10000000-0000-0000-0000-000000000001")
	ms := TestLocalMutableState(s.mockShard, s.mockEventsCache, tests.LocalNamespaceEntry, tv.WorkflowID(), tv.RunID(), s.logger)
	ms.executionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	ms.executionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	return ms
}

func closeFeedMutation(t *testing.T, ms *MutableStateImpl) *persistence.WorkflowMutation {
	t.Helper()
	mutation, _, err := ms.CloseTransactionAsMutation(t.Context(), historyi.TransactionPolicyActive)
	require.NoError(t, err)
	return mutation
}

func feedTask(t *testing.T, mutation *persistence.WorkflowMutation) *tasks.ExecutionFeedTask {
	t.Helper()
	require.Len(t, mutation.Tasks[tasks.CategoryExecutionFeed], 1)
	return mutation.Tasks[tasks.CategoryExecutionFeed][0].(*tasks.ExecutionFeedTask)
}

type captureEpochShard struct {
	historyi.ShardContext
	epoch int64
}

func (s *captureEpochShard) ExecutionFeedCaptureEpoch(namespace.ID) int64 {
	return s.epoch
}

func TestExecutionFeedDisabledCaptureLeavesDurableGap(t *testing.T) {
	t.Parallel()
	for _, initiallyEnabled := range []bool{false, true} {
		name := "loaded while disabled"
		if initiallyEnabled {
			name = "disabled after loading"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ms := executionFeedTestState(t, initiallyEnabled)
			ms.shard = &captureEpochShard{ShardContext: ms.shard, epoch: 7}
			require.NoError(t, ms.initializeExecutionFeed())
			ms.config.ExecutionFeedEnabled = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
			ms.executionInfo.TaskQueue = testvars.New(t).TaskQueue().Name
			record := feedTask(t, closeFeedMutation(t, ms))
			data, err := json.Marshal(record)
			require.NoError(t, err)
			var actual map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(data, &actual))
			require.JSONEq(t, "7", string(actual["capture_epoch"]))
			require.JSONEq(t, "true", string(actual["capture_gap"]))
			require.Empty(t, record.State)
			require.Empty(t, record.Facts)
			require.Empty(t, closeFeedMutation(t, ms).Tasks[tasks.CategoryExecutionFeed])
		})
	}
}

func TestExecutionFeedPassiveCommitLeavesDurableGap(t *testing.T) {
	t.Parallel()
	ms := executionFeedTestState(t, true)
	ms.shard = &captureEpochShard{ShardContext: ms.shard, epoch: 7}
	require.NoError(t, ms.initializeExecutionFeed())
	ms.executionInfo.TaskQueue = testvars.New(t).TaskQueue().Name
	mutation, _, err := ms.CloseTransactionAsMutation(t.Context(), historyi.TransactionPolicyPassive)
	require.NoError(t, err)
	record := feedTask(t, mutation)
	require.Equal(t, int64(7), mutation.ExecutionFeedCaptureEpoch)
	require.True(t, record.CaptureGap)
	require.Empty(t, record.State)
}

func TestExecutionFeedDisabledProjectionFailureLeavesDurableGap(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name                string
		loadedBeforeDisable bool
	}{
		{name: "load while disabled"},
		{name: "disabled after loading", loadedBeforeDisable: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ms := executionFeedTestState(t, test.loadedBeforeDisable)
			ms.shard = &captureEpochShard{ShardContext: ms.shard, epoch: 7}
			if test.loadedBeforeDisable {
				require.NoError(t, ms.initializeExecutionFeed())
			}
			ms.config.ExecutionFeedEnabled = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
			ms.executionInfo.SearchAttributes = map[string]*commonpb.Payload{
				"broken": {Data: []byte("malformed")},
			}
			if !test.loadedBeforeDisable {
				require.NoError(t, ms.initializeExecutionFeed())
			}
			mutation := closeFeedMutation(t, ms)
			record := feedTask(t, mutation)
			require.Equal(t, int64(7), mutation.ExecutionFeedCaptureEpoch)
			require.True(t, record.ValidSchema())
			require.True(t, record.CaptureGap)
			require.Empty(t, record.State)
			require.Empty(t, record.Facts)
		})
	}
}

func TestExecutionFeedEnabledProjectionFailureRemainsAnError(t *testing.T) {
	t.Parallel()
	ms := executionFeedTestState(t, true)
	ms.shard = &captureEpochShard{ShardContext: ms.shard, epoch: 7}
	ms.executionInfo.SearchAttributes = map[string]*commonpb.Payload{
		"broken": {Data: []byte("malformed")},
	}
	require.Error(t, ms.initializeExecutionFeed())
	_, _, err := ms.CloseTransactionAsMutation(t.Context(), historyi.TransactionPolicyActive)
	require.Error(t, err)
}

func TestExecutionFeedBaselineRefreshesCachedCaptureEpoch(t *testing.T) {
	t.Parallel()
	ms := executionFeedTestState(t, true)
	ms.shard = &captureEpochShard{ShardContext: ms.shard, epoch: 7}
	require.NoError(t, ms.RequestExecutionFeedBaseline())
	mutation := closeFeedMutation(t, ms)
	record := feedTask(t, mutation)
	require.Equal(t, int64(7), mutation.ExecutionFeedCaptureEpoch)
	require.Equal(t, int64(7), record.CaptureEpoch)
	require.True(t, record.Baseline)
	require.False(t, record.CaptureGap)
	require.NotNil(t, record.State.Workflow)
}

func TestExecutionFeedStateOnlyActivityTransitions(t *testing.T) {
	t.Parallel()
	ms := executionFeedTestState(t, true)
	tv := testvars.New(t)
	_, ai, err := ms.AddActivityTaskScheduledEvent(4, &commandpb.ScheduleActivityTaskCommandAttributes{
		ActivityId: tv.ActivityID(), ActivityType: tv.ActivityType(), TaskQueue: tv.TaskQueue(),
		ScheduleToCloseTimeout: durationpb.New(time.Hour),
		RetryPolicy:            &commonpb.RetryPolicy{InitialInterval: durationpb.New(time.Second), BackoffCoefficient: 1},
	}, false)
	require.NoError(t, err)
	closeFeedMutation(t, ms)

	// Retry-policy starts do not append history.
	started, err := ms.AddActivityTaskStartedEvent(ai, ai.ScheduledEventId, tv.RequestID(), "worker", nil, nil, nil, "", nil)
	require.NoError(t, err)
	require.Nil(t, started)
	mutation, events, err := ms.CloseTransactionAsMutation(t.Context(), historyi.TransactionPolicyActive)
	require.NoError(t, err)
	require.Empty(t, events)
	start := feedTask(t, mutation)
	require.Equal(t, "STARTED", start.State.Activities[0].Status)
	require.Contains(t, start.Facts, tasks.FeedFact{
		Entity: "ACTIVITY", ID: tv.ActivityID(), ScheduledEventID: ai.ScheduledEventId,
		Attempt: 1, Transition: "STARTED", Time: start.State.Activities[0].StartedTime,
	})

	failure := &failurepb.Failure{
		Message: "SECRET_FAILURE_MESSAGE",
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
			TimeoutType:          enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			LastHeartbeatDetails: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("SECRET_HEARTBEAT")}}},
		}},
	}
	retry, err := ms.RetryActivity(ai, failure)
	require.NoError(t, err)
	require.Equal(t, enumspb.RETRY_STATE_IN_PROGRESS, retry)
	mutation, events, err = ms.CloseTransactionAsMutation(t.Context(), historyi.TransactionPolicyActive)
	require.NoError(t, err)
	require.Empty(t, events)
	failed := feedTask(t, mutation)
	require.Equal(t, int32(2), failed.State.Activities[0].Attempt)
	require.True(t, slices.ContainsFunc(failed.Facts, func(f tasks.FeedFact) bool {
		return f.Attempt == 1 && f.Transition == "TIMED_OUT" && f.TimeoutType == enumspb.TIMEOUT_TYPE_START_TO_CLOSE
	}))
	data, err := json.Marshal(failed)
	require.NoError(t, err)
	require.NotContains(t, string(data), "SECRET")

	_, err = ms.AddActivityTaskStartedEvent(ai, ai.ScheduledEventId, tv.RequestID()+"-retry", "worker", nil, nil, nil, "", nil)
	require.NoError(t, err)
	closeFeedMutation(t, ms)
	_, err = ms.AddActivityTaskCompletedEvent(ai.ScheduledEventId, common.TransientEventID,
		&workflowservice.RespondActivityTaskCompletedRequest{})
	require.NoError(t, err)
	completed := feedTask(t, closeFeedMutation(t, ms))
	require.Empty(t, completed.State.Activities)
	require.False(t, slices.ContainsFunc(completed.Facts, func(f tasks.FeedFact) bool { return f.Transition == "STARTED" }))
	require.True(t, slices.ContainsFunc(completed.Facts, func(f tasks.FeedFact) bool {
		return f.Transition == "COMPLETED" && f.Attempt == 2
	}))
}

func TestExecutionFeedPreservesTransientAttempt(t *testing.T) {
	t.Parallel()
	ms := executionFeedTestState(t, true)
	_, ai, err := ms.AddActivityTaskScheduledEvent(4, &commandpb.ScheduleActivityTaskCommandAttributes{
		ActivityId: "activity", ActivityType: &commonpb.ActivityType{Name: "type"}, TaskQueue: &taskqueuepb.TaskQueue{Name: "queue"},
		ScheduleToCloseTimeout: durationpb.New(time.Hour),
		RetryPolicy:            &commonpb.RetryPolicy{InitialInterval: durationpb.New(time.Second), BackoffCoefficient: 1},
	}, false)
	require.NoError(t, err)
	closeFeedMutation(t, ms)
	_, err = ms.AddActivityTaskStartedEvent(ai, ai.ScheduledEventId, "start", "worker", nil, nil, nil, "", nil)
	require.NoError(t, err)
	retry, err := ms.RetryActivity(ai, &failurepb.Failure{FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
		ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{},
	}})
	require.NoError(t, err)
	require.Equal(t, enumspb.RETRY_STATE_IN_PROGRESS, retry)
	task := feedTask(t, closeFeedMutation(t, ms))
	var transitions []string
	for _, fact := range task.Facts {
		transitions = append(transitions, fact.Transition)
	}
	require.Equal(t, []string{"STARTED", "FAILED", "SCHEDULED"}, transitions)
	require.Equal(t, int32(2), task.State.Activities[0].Attempt)
}

func TestExecutionFeedHeartbeatAndDisabledCapture(t *testing.T) {
	t.Parallel()
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "heartbeat-only"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ms := executionFeedTestState(t, enabled)
			ai := &persistencespb.ActivityInfo{
				ActivityId: "activity", ActivityType: &commonpb.ActivityType{Name: "type"},
				TaskQueue: "queue", ScheduledEventId: 5, StartedEventId: 6, Attempt: 1,
				StartedTime: timestamppb.New(time.Unix(100, 0)), ScheduledTime: timestamppb.New(time.Unix(90, 0)),
			}
			ms.pendingActivityInfoIDs[5] = ai
			require.NoError(t, ms.initializeExecutionFeed())
			require.NoError(t, ms.UpdateActivity(5, func(a *persistencespb.ActivityInfo, _ historyi.MutableState) error {
				a.LastHeartbeatUpdateTime = timestamppb.New(time.Unix(110, 0))
				a.LastHeartbeatDetails = &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("heartbeat")}}}
				if !enabled {
					a.Attempt++
				}
				return nil
			}))
			require.Empty(t, closeFeedMutation(t, ms).Tasks[tasks.CategoryExecutionFeed])
		})
	}
}

func TestExecutionFeedBufferedTimerCancellation(t *testing.T) {
	t.Parallel()
	ms := executionFeedTestState(t, true)
	_, _, err := ms.AddTimerStartedEvent(4, &commandpb.StartTimerCommandAttributes{TimerId: "timer", StartToFireTimeout: durationpb.New(time.Second)})
	require.NoError(t, err)
	closeFeedMutation(t, ms)
	// A running workflow task keeps TimerFired buffered and revocable.
	ms.executionInfo.WorkflowTaskScheduledEventId = 10
	ms.executionInfo.WorkflowTaskStartedEventId = 11
	ms.executionInfo.WorkflowTaskAttempt = 1
	ms.executionInfo.WorkflowTaskTimeout = durationpb.New(time.Minute)
	ms.executionInfo.TaskQueue = "queue"
	_, err = ms.AddTimerFiredEvent("timer")
	require.NoError(t, err)
	buffered := feedTask(t, closeFeedMutation(t, ms))
	require.Equal(t, "FIRE_BUFFERED", buffered.State.Timers[0].Status)
	require.False(t, slices.ContainsFunc(buffered.Facts, func(f tasks.FeedFact) bool { return f.Transition == "FIRED" }))
	_, err = ms.AddTimerCanceledEvent(12, &commandpb.CancelTimerCommandAttributes{TimerId: "timer"}, "worker")
	require.NoError(t, err)
	ms.executionInfo.WorkflowTaskStartedEventId = common.EmptyEventID
	canceled := feedTask(t, closeFeedMutation(t, ms))
	require.Empty(t, canceled.State.Timers)
	require.True(t, slices.ContainsFunc(canceled.Facts, func(f tasks.FeedFact) bool { return f.Transition == "CANCELED" }))
	require.False(t, slices.ContainsFunc(canceled.Facts, func(f tasks.FeedFact) bool { return f.Transition == "FIRED" }))
}
