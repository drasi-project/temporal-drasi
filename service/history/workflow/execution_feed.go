package workflow

import (
	"context"
	"encoding/json"
	"slices"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/executionfeed"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
)

func (ms *MutableStateImpl) executionFeedEnabled() bool {
	return ms.config.ExecutionFeedEnabled != nil && ms.config.ExecutionFeedEnabled(ms.namespaceEntry.Name().String())
}

func (ms *MutableStateImpl) initializeExecutionFeed() error {
	ms.executionFeedCaptureEpoch = ms.currentExecutionFeedCaptureEpoch()
	enabled := ms.executionFeedEnabled()
	if !enabled && ms.executionFeedCaptureEpoch == 0 {
		return nil
	}
	state, err := ms.executionFeedState(context.Background())
	ms.executionFeedPrevious = state
	if err != nil && !enabled {
		ms.logger.Warn("Cannot initialize execution feed projection while capture is disabled",
			tag.WorkflowNamespaceID(ms.executionInfo.NamespaceId),
			tag.WorkflowID(ms.executionInfo.WorkflowId),
			tag.WorkflowRunID(ms.executionState.RunId),
			tag.Error(err))
		return nil
	}
	return err
}

func (ms *MutableStateImpl) currentExecutionFeedCaptureEpoch() int64 {
	if shard, ok := ms.shard.(interface {
		ExecutionFeedCaptureEpoch(namespace.ID) int64
	}); ok {
		return shard.ExecutionFeedCaptureEpoch(ms.namespaceEntry.ID())
	}
	return 0
}

// RequestExecutionFeedBaseline must be called while holding this execution's lock.
// The baseline becomes visible only with the normal mutable-state commit.
func (ms *MutableStateImpl) RequestExecutionFeedBaseline() error {
	if !ms.executionFeedEnabled() {
		return serviceerror.NewFailedPrecondition("execution feed capture is disabled")
	}
	if ms.namespaceEntry.IsGlobalNamespace() {
		return serviceerror.NewFailedPrecondition("execution feed does not support global namespaces")
	}
	ms.executionFeedBaseline = true
	ms.executionFeedCaptureEpoch = ms.currentExecutionFeedCaptureEpoch()
	return nil
}

func (ms *MutableStateImpl) executionFeedState(ctx context.Context) (tasks.FeedState, error) {
	if ms.IsWorkflow() {
		return executionfeed.WorkflowState(ms.executionInfo, ms.executionState, ms.pendingActivityInfoIDs,
			ms.pendingTimerInfoIDs, ms.bufferEventsInDB)
	}
	node, ok := ms.chasmTree.(*chasm.Node)
	if !ok {
		return tasks.FeedState{}, serviceerror.NewFailedPrecondition("execution feed requires a CHASM tree")
	}
	cctx := chasm.NewContext(ctx, node)
	root, err := ms.chasmTree.ComponentByPath(cctx, nil)
	if err != nil {
		return tasks.FeedState{}, err
	}
	provider, ok := root.(interface {
		ExecutionFeedState(chasm.Context) (tasks.FeedActivity, error)
	})
	if !ok {
		// Other CHASM archetypes are not part of the execution graph.
		return tasks.FeedState{}, nil
	}
	activity, err := provider.ExecutionFeedState(cctx)
	if err != nil {
		return tasks.FeedState{}, err
	}
	activity.ID = ms.executionInfo.WorkflowId
	return tasks.FeedState{Activities: []tasks.FeedActivity{activity}}, nil
}

func (ms *MutableStateImpl) captureExecutionFeed(ctx context.Context, policy historyi.TransactionPolicy,
	batches [][]*historypb.HistoryEvent,
) error {
	enabled := ms.executionFeedEnabled() && policy == historyi.TransactionPolicyActive
	if !enabled && ms.executionFeedCaptureEpoch == 0 {
		return nil
	}
	if ms.namespaceEntry.IsGlobalNamespace() {
		return serviceerror.NewFailedPrecondition("execution feed does not support global namespaces")
	}
	state, err := ms.executionFeedState(ctx)
	if err != nil {
		if enabled {
			return err
		}
		ms.logger.Warn("Cannot project execution; recording execution feed gap",
			tag.WorkflowNamespaceID(ms.executionInfo.NamespaceId),
			tag.WorkflowID(ms.executionInfo.WorkflowId),
			tag.WorkflowRunID(ms.executionState.RunId),
			tag.Error(err))
		task := ms.newExecutionFeedTask()
		task.CaptureGap = true
		task.Baseline = false
		ms.AddTasks(task)
		return nil
	}
	if state.Workflow == nil && len(state.Activities) == 0 {
		return nil
	}
	derived := executionfeed.Facts(ms.executionFeedPrevious, state, batches)
	facts := combineExecutionFeedFacts(ms.executionFeedFacts, derived)
	if state.Workflow != nil {
		if err := ms.executionFeedLineage(ctx, state.Workflow, batches, enabled); err != nil {
			return err
		}
		if err := ms.executionFeedTimerDeadlines(ctx, &state, enabled); err != nil {
			return err
		}
	}
	previous := comparablePreviousFeedState(ms.executionFeedPrevious, state)
	previousBytes, err := json.Marshal(previous)
	if err != nil {
		return err
	}
	stateBytes, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if !ms.executionFeedBaseline && len(facts) == 0 && slices.Equal(previousBytes, stateBytes) {
		return nil
	}
	task := ms.newExecutionFeedTask()
	task.State, task.Facts = state, facts
	if ms.executionFeedCaptureEpoch > 0 && !enabled {
		task.CaptureGap = true
		task.State = tasks.FeedState{}
		task.Facts = nil
		task.Baseline = false
	}
	ms.AddTasks(task)
	ms.executionFeedPrevious = state
	return nil
}

func (ms *MutableStateImpl) newExecutionFeedTask() *tasks.ExecutionFeedTask {
	task := &tasks.ExecutionFeedTask{
		WorkflowKey: ms.GetWorkflowKey(), SchemaVersion: 1, Version: ms.currentVersion,
		ArchetypeID:         ms.chasmTree.ArchetypeID(),
		VisibilityTimestamp: ms.timeSource.Now(), TransitionCount: ms.executionInfo.StateTransitionCount,
		Baseline: ms.executionFeedBaseline,
	}
	if ms.executionFeedCaptureEpoch > 0 {
		task.SchemaVersion = 2
		task.CaptureEpoch = ms.executionFeedCaptureEpoch
	}
	return task
}

func combineExecutionFeedFacts(explicit, derived []tasks.FeedFact) []tasks.FeedFact {
	var facts []tasks.FeedFact
	for _, fact := range derived {
		if fact.EventID > 0 {
			facts = append(facts, fact)
		}
	}
	for _, fact := range slices.Concat(explicit, derived) {
		if fact.EventID > 0 {
			continue
		}
		if slices.ContainsFunc(facts, func(existing tasks.FeedFact) bool {
			if fact.Entity != existing.Entity || fact.Attempt != existing.Attempt || fact.Transition != existing.Transition {
				return false
			}
			if fact.ScheduledEventID != 0 {
				return fact.ScheduledEventID == existing.ScheduledEventID
			}
			return fact.ID == existing.ID
		}) {
			continue
		}
		facts = append(facts, fact)
	}
	return facts
}

func (ms *MutableStateImpl) executionFeedLineage(ctx context.Context, state *tasks.FeedWorkflow, batches [][]*historypb.HistoryEvent, loadMissing bool) error {
	for _, batch := range batches {
		for _, e := range batch {
			if a := e.GetWorkflowExecutionStartedEventAttributes(); a != nil {
				state.PreviousRunID = a.ContinuedExecutionRunId
			}
		}
	}
	if state.PreviousRunID == "" && ms.executionFeedPrevious.Workflow != nil {
		state.PreviousRunID = ms.executionFeedPrevious.Workflow.PreviousRunID
	}
	if loadMissing && state.PreviousRunID == "" && state.FirstRunID != "" &&
		state.FirstRunID != ms.executionState.RunId && ms.nextEventIDInDB > common.FirstEventID {
		start, err := ms.GetStartEvent(ctx)
		if err != nil {
			return err
		}
		state.PreviousRunID = start.GetWorkflowExecutionStartedEventAttributes().GetContinuedExecutionRunId()
	}
	return nil
}

func comparablePreviousFeedState(previous, state tasks.FeedState) tasks.FeedState {
	if previous.Workflow != nil {
		w := *previous.Workflow
		if w.PreviousRunID == "" && state.Workflow != nil {
			w.PreviousRunID = state.Workflow.PreviousRunID
		}
		previous.Workflow = &w
	}
	previous.Timers = slices.Clone(previous.Timers)
	for i := range previous.Timers {
		if previous.Timers[i].Deadline != nil {
			continue
		}
		for _, timer := range state.Timers {
			if timer.StartedEventID == previous.Timers[i].StartedEventID {
				previous.Timers[i].Deadline = timer.Deadline
				break
			}
		}
	}
	return previous
}

func (ms *MutableStateImpl) executionFeedTimerDeadlines(ctx context.Context, state *tasks.FeedState, loadMissing bool) error {
	for i := range state.Timers {
		timer := &state.Timers[i]
		if timer.Deadline != nil {
			continue
		}
		for _, old := range ms.executionFeedPrevious.Timers {
			if old.StartedEventID == timer.StartedEventID {
				timer.Deadline = old.Deadline
				break
			}
		}
		if timer.Deadline != nil || !loadMissing {
			continue
		}
		deadline, err := ms.executionFeedTimerStartDeadline(ctx, timer.StartedEventID)
		if err != nil {
			return err
		}
		timer.Deadline = deadline
	}
	return nil
}

func (ms *MutableStateImpl) executionFeedTimerStartDeadline(ctx context.Context, startedEventID int64) (*time.Time, error) {
	branch, err := ms.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}
	var token []byte
	for {
		page, err := ms.shard.GetExecutionManager().ReadHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
			ShardID: ms.shard.GetShardID(), BranchToken: branch, MinEventID: common.FirstEventID,
			MaxEventID: startedEventID + 1, PageSize: 1000, NextPageToken: token,
		})
		if err != nil {
			return nil, err
		}
		for _, event := range page.HistoryEvents {
			if event.EventId == startedEventID {
				start := event.GetTimerStartedEventAttributes()
				if start == nil {
					return nil, serviceerror.NewDataLoss("execution feed timer start has an unexpected event type")
				}
				deadline := event.EventTime.AsTime().Add(start.StartToFireTimeout.AsDuration()).UTC()
				return &deadline, nil
			}
		}
		if len(page.NextPageToken) == 0 {
			return nil, serviceerror.NewDataLoss("execution feed timer start is not retained")
		}
		token = page.NextPageToken
	}
}

func (ms *MutableStateImpl) recordExecutionFeedRetry(ai *persistencespb.ActivityInfo, attempt int32, failure *failurepb.Failure) {
	kind, timeout := executionfeed.FailureKind(failure)
	transition := "FAILED"
	if timeout != enumspb.TIMEOUT_TYPE_UNSPECIFIED {
		transition = "TIMED_OUT"
	}
	now := ms.timeSource.Now().UTC()
	ms.executionFeedFacts = append(ms.executionFeedFacts, tasks.FeedFact{
		Entity: "ACTIVITY", ID: ai.GetActivityId(), ScheduledEventID: ai.GetScheduledEventId(), Attempt: attempt,
		Transition: transition, FailureKind: kind, TimeoutType: timeout, Time: &now,
	})
}
