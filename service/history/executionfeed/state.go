// Package executionfeed exposes committed, payload-free execution state and replay.
package executionfeed

import (
	"encoding/json"
	"slices"
	"strconv"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// FeedRecord carries one source commit; (ShardID, TaskID, fact ordinal) is stable.
type FeedRecord struct {
	ShardID int32 `json:"shard_id"`
	*tasks.ExecutionFeedTask
}

func Time(t *timestamppb.Timestamp) *time.Time {
	if t == nil {
		return nil
	}
	v := t.AsTime().UTC()
	return &v
}

// WorkflowState only reads committed-state fields, not heartbeat telemetry or payloads.
func WorkflowState(info *persistencespb.WorkflowExecutionInfo, state *persistencespb.WorkflowExecutionState,
	activities map[int64]*persistencespb.ActivityInfo, timers map[string]*persistencespb.TimerInfo,
	buffered []*historypb.HistoryEvent,
) (tasks.FeedState, error) {
	w := &tasks.FeedWorkflow{
		Type: info.WorkflowTypeName, TaskQueue: info.TaskQueue, State: state.State, Status: state.Status,
		StartTime: Time(state.StartTime), ExecutionTime: Time(info.ExecutionTime), CloseTime: Time(info.CloseTime),
		FirstRunID: info.FirstExecutionRunId, NewRunID: info.NewExecutionRunId, ResetRunID: info.ResetRunId,
		BaseRunID:         info.GetBaseExecutionInfo().GetRunId(),
		ParentNamespaceID: info.ParentNamespaceId, ParentWorkflowID: info.ParentWorkflowId, ParentRunID: info.ParentRunId,
		CreateRequestID: state.CreateRequestId,
	}
	if w.StartTime == nil {
		w.StartTime = Time(info.StartTime)
	}
	var err error
	w.SearchAttributes, err = searchAttributes(info.SearchAttributes)
	if err != nil {
		return tasks.FeedState{}, err
	}
	result := tasks.FeedState{Workflow: w}
	for _, a := range activities {
		status := "SCHEDULED"
		if a.StartedEventId != common.EmptyEventID {
			status = "STARTED"
		}
		if a.Paused {
			status = "PAUSED"
		}
		if a.CancelRequested {
			status = "CANCEL_REQUESTED"
		}
		kind, timeout := FailureKind(a.RetryLastFailure)
		result.Activities = append(result.Activities, tasks.FeedActivity{
			ID: a.ActivityId, ScheduledEventID: a.ScheduledEventId, Type: a.GetActivityType().GetName(),
			TaskQueue: a.TaskQueue, Attempt: a.Attempt, Status: status,
			ScheduledTime: Time(a.FirstScheduledTime), StartedTime: Time(a.StartedTime),
			RetryTime: Time(a.ScheduledTime), Paused: a.Paused, CancelRequested: a.CancelRequested,
			LastFailureKind: kind, LastTimeoutType: timeout,
		})
	}
	slices.SortFunc(result.Activities, func(a, b tasks.FeedActivity) int {
		if a.ScheduledEventID < b.ScheduledEventID {
			return -1
		}
		if a.ScheduledEventID > b.ScheduledEventID {
			return 1
		}
		return 0
	})
	for _, t := range timers {
		result.Timers = append(result.Timers, tasks.FeedTimer{
			ID: t.TimerId, StartedEventID: t.StartedEventId, Deadline: Time(t.ExpiryTime), Status: "SCHEDULED",
		})
	}
	for _, e := range buffered {
		if a := e.GetTimerFiredEventAttributes(); a != nil {
			result.Timers = append(result.Timers, tasks.FeedTimer{
				ID: a.TimerId, StartedEventID: a.StartedEventId, Status: "FIRE_BUFFERED",
			})
		}
	}
	slices.SortFunc(result.Timers, func(a, b tasks.FeedTimer) int {
		if a.StartedEventID < b.StartedEventID {
			return -1
		}
		if a.StartedEventID > b.StartedEventID {
			return 1
		}
		return 0
	})
	return result, nil
}

func searchAttributes(attributes map[string]*commonpb.Payload) (map[string]tasks.FeedSearchAttribute, error) {
	if len(attributes) == 0 {
		return nil, nil
	}
	result := make(map[string]tasks.FeedSearchAttribute, len(attributes))
	for name, payload := range attributes {
		typ := sadefs.GetMetadataType(payload)
		value, err := sadefs.DecodeValue(payload, typ, true)
		if err != nil {
			return nil, err
		}
		data, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		result[name] = tasks.FeedSearchAttribute{Type: typ, Value: data}
	}
	return result, nil
}

// FailureKind deliberately excludes messages, stack traces, and failure payloads.
func FailureKind(f *failurepb.Failure) (string, enumspb.TimeoutType) {
	switch {
	case f == nil:
		return "", enumspb.TIMEOUT_TYPE_UNSPECIFIED
	case f.GetTimeoutFailureInfo() != nil:
		return "TIMEOUT", f.GetTimeoutFailureInfo().TimeoutType
	case f.GetCanceledFailureInfo() != nil:
		return "CANCELED", enumspb.TIMEOUT_TYPE_UNSPECIFIED
	default:
		return "FAILURE", enumspb.TIMEOUT_TYPE_UNSPECIFIED
	}
}

// Facts preserves finalized history transitions and mutable-state-only transitions.
// Materializing a previously captured transient Started event is not a second start.
func Facts(before, after tasks.FeedState, batches [][]*historypb.HistoryEvent) []tasks.FeedFact {
	var facts []tasks.FeedFact
	previous := make(map[int64]tasks.FeedActivity)
	attempts := make(map[int64]int32)
	for _, a := range before.Activities {
		previous[a.ScheduledEventID] = a
		attempts[a.ScheduledEventID] = a.Attempt
	}
	for _, a := range after.Activities {
		attempts[a.ScheduledEventID] = a.Attempt
	}
	for _, batch := range batches {
		for _, e := range batch {
			if fact, ok := historyFact(e, previous, attempts); ok {
				facts = append(facts, fact)
			}
		}
	}
	facts = append(facts, activityFacts(previous, after.Activities, facts)...)
	return facts
}

func historyFact(e *historypb.HistoryEvent, previous map[int64]tasks.FeedActivity, attempts map[int64]int32) (tasks.FeedFact, bool) {
	f := tasks.FeedFact{EventID: e.EventId, Time: Time(e.EventTime)}
	switch e.EventType {
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
		f.Entity, f.ScheduledEventID, f.Transition = "ACTIVITY", e.EventId, "SCHEDULED"
		f.ID, f.Attempt = e.GetActivityTaskScheduledEventAttributes().ActivityId, 1
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
		a := e.GetActivityTaskStartedEventAttributes()
		attempts[a.ScheduledEventId] = a.Attempt
		old := previous[a.ScheduledEventId]
		if old.Attempt == a.Attempt && old.StartedTime != nil {
			return tasks.FeedFact{}, false
		}
		f.Entity, f.ScheduledEventID, f.Attempt, f.Transition = "ACTIVITY", a.ScheduledEventId, a.Attempt, "STARTED"
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
		f.Entity, f.ScheduledEventID, f.Transition = "ACTIVITY", e.GetActivityTaskCompletedEventAttributes().ScheduledEventId, "COMPLETED"
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
		a := e.GetActivityTaskFailedEventAttributes()
		f.Entity, f.ScheduledEventID, f.Transition = "ACTIVITY", a.ScheduledEventId, "FAILED"
		f.FailureKind, f.TimeoutType = FailureKind(a.Failure)
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
		a := e.GetActivityTaskTimedOutEventAttributes()
		f.Entity, f.ScheduledEventID, f.Transition = "ACTIVITY", a.ScheduledEventId, "TIMED_OUT"
		f.FailureKind, f.TimeoutType = FailureKind(a.Failure)
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
		f.Entity, f.ScheduledEventID, f.Transition = "ACTIVITY", e.GetActivityTaskCanceledEventAttributes().ScheduledEventId, "CANCELED"
	case enumspb.EVENT_TYPE_TIMER_STARTED:
		a := e.GetTimerStartedEventAttributes()
		f.Entity, f.ID, f.ScheduledEventID, f.Transition = "TIMER", a.TimerId, e.EventId, "SCHEDULED"
	case enumspb.EVENT_TYPE_TIMER_FIRED:
		a := e.GetTimerFiredEventAttributes()
		f.Entity, f.ID, f.ScheduledEventID, f.Transition = "TIMER", a.TimerId, a.StartedEventId, "FIRED"
	case enumspb.EVENT_TYPE_TIMER_CANCELED:
		a := e.GetTimerCanceledEventAttributes()
		f.Entity, f.ID, f.ScheduledEventID, f.Transition = "TIMER", a.TimerId, a.StartedEventId, "CANCELED"
	default:
		return tasks.FeedFact{}, false
	}
	if f.Entity == "ACTIVITY" {
		if f.Attempt == 0 {
			f.Attempt = attempts[f.ScheduledEventID]
		}
		if f.ID == "" {
			f.ID = previous[f.ScheduledEventID].ID
		}
		if f.ID == "" {
			f.ID = strconv.FormatInt(f.ScheduledEventID, 10)
		}
	}
	return f, true
}

func activityFacts(previous map[int64]tasks.FeedActivity, after []tasks.FeedActivity, history []tasks.FeedFact) []tasks.FeedFact {
	var facts []tasks.FeedFact
	for _, a := range after {
		old, exists := previous[a.ScheduledEventID]
		if a.Standalone && a.LastFailureTime != nil &&
			(!exists || old.LastFailureTime == nil || !a.LastFailureTime.Equal(*old.LastFailureTime)) {
			transition := "FAILED"
			if a.LastTimeoutType != enumspb.TIMEOUT_TYPE_UNSPECIFIED {
				transition = "TIMED_OUT"
			}
			attempt := old.Attempt
			if !exists {
				attempt = a.Attempt
			}
			facts = append(facts, tasks.FeedFact{Entity: "ACTIVITY", ID: a.ID, Attempt: attempt,
				Transition: transition, Time: a.LastFailureTime, FailureKind: a.LastFailureKind, TimeoutType: a.LastTimeoutType})
		}
		if exists && old.Attempt == a.Attempt && old.Status == a.Status {
			continue
		}
		duplicate := slices.ContainsFunc(history, func(f tasks.FeedFact) bool {
			return f.Entity == "ACTIVITY" && f.ScheduledEventID == a.ScheduledEventID && f.Attempt == a.Attempt && f.Transition == a.Status
		})
		if !duplicate {
			at := a.StartedTime
			if a.Status == "SCHEDULED" {
				at = a.RetryTime
			}
			if a.CompleteTime != nil {
				at = a.CompleteTime
			}
			facts = append(facts, tasks.FeedFact{
				Entity: "ACTIVITY", ID: a.ID, ScheduledEventID: a.ScheduledEventID, Attempt: a.Attempt,
				Transition: a.Status, Time: at,
			})
		}
	}
	return facts
}
