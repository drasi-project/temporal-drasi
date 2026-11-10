package tasks

import (
	"encoding/json"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

// FeedState is a complete, payload-free replacement for one retained execution.
// Entity identities are scoped to NamespaceID/WorkflowID/RunID on the enclosing task.
type FeedState struct {
	Workflow   *FeedWorkflow  `json:"workflow,omitempty"`
	Activities []FeedActivity `json:"activities,omitempty"`
	Timers     []FeedTimer    `json:"timers,omitempty"`
}

type FeedWorkflow struct {
	Type              string                          `json:"type"`
	TaskQueue         string                          `json:"task_queue"`
	State             enumsspb.WorkflowExecutionState `json:"state"`
	Status            enumspb.WorkflowExecutionStatus `json:"status"`
	StartTime         *time.Time                      `json:"start_time,omitempty"`
	ExecutionTime     *time.Time                      `json:"execution_time,omitempty"`
	CloseTime         *time.Time                      `json:"close_time,omitempty"`
	FirstRunID        string                          `json:"first_run_id,omitempty"`
	PreviousRunID     string                          `json:"previous_run_id,omitempty"`
	NewRunID          string                          `json:"new_run_id,omitempty"`
	ResetRunID        string                          `json:"reset_run_id,omitempty"`
	BaseRunID         string                          `json:"base_run_id,omitempty"`
	ParentNamespaceID string                          `json:"parent_namespace_id,omitempty"`
	ParentWorkflowID  string                          `json:"parent_workflow_id,omitempty"`
	ParentRunID       string                          `json:"parent_run_id,omitempty"`
	CreateRequestID   string                          `json:"create_request_id,omitempty"`
	SearchAttributes  map[string]FeedSearchAttribute  `json:"search_attributes,omitempty"`
}

// FeedSearchAttribute retains the declared type and decoded JSON value, never a Payload.
type FeedSearchAttribute struct {
	Type  enumspb.IndexedValueType `json:"type"`
	Value json.RawMessage          `json:"value"`
}

type FeedActivity struct {
	ID               string              `json:"id"`
	ScheduledEventID int64               `json:"scheduled_event_id,omitempty"`
	Standalone       bool                `json:"standalone,omitempty"`
	Type             string              `json:"type"`
	TaskQueue        string              `json:"task_queue"`
	Attempt          int32               `json:"attempt"`
	Status           string              `json:"status"`
	ScheduledTime    *time.Time          `json:"scheduled_time,omitempty"`
	StartedTime      *time.Time          `json:"started_time,omitempty"`
	CompleteTime     *time.Time          `json:"complete_time,omitempty"`
	RetryTime        *time.Time          `json:"retry_time,omitempty"`
	LastFailureTime  *time.Time          `json:"last_failure_time,omitempty"`
	LastFailureKind  string              `json:"last_failure_kind,omitempty"`
	LastTimeoutType  enumspb.TimeoutType `json:"last_timeout_type,omitempty"`
	Paused           bool                `json:"paused,omitempty"`
	CancelRequested  bool                `json:"cancel_requested,omitempty"`
}

type FeedTimer struct {
	ID             string     `json:"id"`
	StartedEventID int64      `json:"started_event_id"`
	Deadline       *time.Time `json:"deadline,omitempty"`
	// Status is SCHEDULED or FIRE_BUFFERED. Only a finalized fact asserts FIRED.
	Status string `json:"status"`
}

// FeedFact describes a committed transition, separate from current retained state.
type FeedFact struct {
	Entity           string              `json:"entity"`
	ID               string              `json:"id"`
	ScheduledEventID int64               `json:"scheduled_event_id,omitempty"`
	Attempt          int32               `json:"attempt,omitempty"`
	Transition       string              `json:"transition"`
	EventID          int64               `json:"event_id,omitempty"`
	Time             *time.Time          `json:"time,omitempty"`
	FailureKind      string              `json:"failure_kind,omitempty"`
	TimeoutType      enumspb.TimeoutType `json:"timeout_type,omitempty"`
}

// ExecutionFeedTask is immutable after transaction close except for its queue key.
// Payload uses a versioned JSON schema independent of mutable-state protobufs.
type ExecutionFeedTask struct {
	definition.WorkflowKey
	TaskID              int64      `json:"task_id"`
	ArchetypeID         uint32     `json:"archetype_id"`
	VisibilityTimestamp time.Time  `json:"commit_time"`
	Version             int64      `json:"version"`
	SchemaVersion       int        `json:"schema_version"`
	CaptureEpoch        int64      `json:"capture_epoch,omitempty"`
	CaptureGap          bool       `json:"capture_gap,omitempty"`
	TransitionCount     int64      `json:"transition_count"`
	Baseline            bool       `json:"baseline,omitempty"`
	Deleted             bool       `json:"deleted,omitempty"`
	State               FeedState  `json:"state"`
	Facts               []FeedFact `json:"facts,omitempty"`
	Journal             bool       `json:"-"`
}

func (t *ExecutionFeedTask) GetKey() Key                   { return NewImmediateKey(t.TaskID) }
func (t *ExecutionFeedTask) GetVersion() int64             { return t.Version }
func (t *ExecutionFeedTask) SetVersion(v int64)            { t.Version = v }
func (t *ExecutionFeedTask) GetTaskID() int64              { return t.TaskID }
func (t *ExecutionFeedTask) SetTaskID(v int64)             { t.TaskID = v }
func (t *ExecutionFeedTask) GetVisibilityTime() time.Time  { return t.VisibilityTimestamp }
func (t *ExecutionFeedTask) SetVisibilityTime(v time.Time) { t.VisibilityTimestamp = v }
func (t *ExecutionFeedTask) GetCategory() Category {
	if t.Journal {
		return CategoryExecutionFeedJournal
	}
	return CategoryExecutionFeed
}
func (t *ExecutionFeedTask) GetType() enumsspb.TaskType { return enumsspb.TASK_TYPE_UNSPECIFIED }

var _ Task = (*ExecutionFeedTask)(nil)

func (t *ExecutionFeedTask) ValidSchema() bool {
	switch t.SchemaVersion {
	case 1:
		return t.CaptureEpoch == 0 && !t.CaptureGap
	case 2:
		return t.CaptureEpoch > 0 && (!t.CaptureGap ||
			(t.State.Workflow == nil && len(t.State.Activities) == 0 && len(t.State.Timers) == 0 &&
				len(t.Facts) == 0 && !t.Baseline && !t.Deleted))
	default:
		return false
	}
}
