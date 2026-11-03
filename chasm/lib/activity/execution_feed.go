package activity

import (
	"strings"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ExecutionFeedState excludes request data, results, failure payloads, and heartbeats.
func (a *Activity) ExecutionFeedState(ctx chasm.Context) (tasks.FeedActivity, error) {
	status, known := activitypb.ActivityExecutionStatus_name[int32(a.GetStatus())]
	if !known {
		return tasks.FeedActivity{}, serviceerror.NewFailedPrecondition("execution feed encountered an unknown standalone activity status")
	}
	attempt := a.LastAttempt.Get(ctx)
	state := tasks.FeedActivity{
		Standalone: true, Type: a.GetActivityType().GetName(), TaskQueue: a.GetTaskQueue().GetName(),
		Status: strings.TrimPrefix(status, "ACTIVITY_EXECUTION_STATUS_"), Attempt: attempt.GetCount(),
		ScheduledTime: feedTime(a.GetScheduleTime()), StartedTime: feedTime(attempt.GetStartedTime()),
		RetryTime:       feedTime(attempt.GetDispatchTime()),
		CancelRequested: a.GetCancelState() != nil, Paused: a.isPaused(),
		CompleteTime: feedTime(attempt.GetCompleteTime()),
	}
	if a.isTerminal() {
		if failure := a.terminalFailure(ctx); failure != nil {
			state.LastFailureKind = "FAILURE"
			if timeout := failure.GetTimeoutFailureInfo(); timeout != nil {
				state.LastFailureKind, state.LastTimeoutType = "TIMEOUT", timeout.TimeoutType
			}
		}
	}
	if last := attempt.GetLastFailureDetails(); last != nil {
		state.LastFailureTime = feedTime(last.Time)
		state.LastFailureKind = "FAILURE"
		if timeout := last.GetFailure().GetTimeoutFailureInfo(); timeout != nil {
			state.LastFailureKind, state.LastTimeoutType = "TIMEOUT", timeout.TimeoutType
		}
	}
	return state, nil
}

func feedTime(t *timestamppb.Timestamp) *time.Time {
	if t == nil {
		return nil
	}
	v := t.AsTime().UTC()
	return &v
}
