package shard

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
)

func (s *ContextImpl) ExecutionFeedCaptureEpoch(namespaceID namespace.ID) int64 {
	s.rLock()
	defer s.rUnlock()
	return s.shardInfo.ExecutionFeedCaptureEpochs[namespaceID.String()]
}

func (s *ContextImpl) BeginExecutionFeedCapture(ctx context.Context, namespaceID namespace.ID) (*historyi.ExecutionFeedCaptureScope, error) {
	if err := s.ioSemaphoreAcquire(ctx); err != nil {
		return nil, err
	}
	defer s.ioSemaphoreRelease()
	s.wLock()
	defer s.wUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.errorByState(); err != nil {
		return nil, err
	}
	if err := s.renewRangeLocked(false, namespaceID); err != nil {
		return nil, err
	}
	return &historyi.ExecutionFeedCaptureScope{
		Epoch:               s.shardInfo.ExecutionFeedCaptureEpochs[namespaceID.String()],
		ReconstructionStart: s.taskKeyManager.peekTaskKey(tasks.CategoryExecutionFeed).TaskID,
	}, nil
}

func (s *ContextImpl) validateExecutionFeedCaptureLocked(namespaceID namespace.ID, epochs ...int64) error {
	current := s.shardInfo.ExecutionFeedCaptureEpochs[namespaceID.String()]
	for _, epoch := range epochs {
		if epoch != current {
			return &persistence.WorkflowConditionFailedError{Msg: "execution feed capture changed while preparing transaction"}
		}
	}
	return nil
}

func (s *ContextImpl) deleteWorkflowExecutionWithFeed(ctx context.Context, request *persistence.DeleteWorkflowExecutionRequest,
	ns *namespace.Namespace, version int64,
) error {
	epoch := s.ExecutionFeedCaptureEpoch(namespace.ID(request.NamespaceID))
	enabled := s.executionFeedDeletionEnabled(ns)
	if ns != nil && ns.IsGlobalNamespace() && (enabled || epoch > 0) {
		return serviceerror.NewFailedPrecondition("execution feed does not support global namespaces")
	}
	task := &tasks.ExecutionFeedTask{
		WorkflowKey: definition.NewWorkflowKey(request.NamespaceID, request.WorkflowID, request.RunID),
		ArchetypeID: request.ArchetypeID, SchemaVersion: 1, Version: version,
		VisibilityTimestamp: s.timeSource.Now(), Deleted: true,
	}
	if epoch > 0 {
		task.SchemaVersion = 2
		task.CaptureEpoch = epoch
		task.CaptureGap = !enabled
		task.Deleted = enabled
	}
	taskMap := map[tasks.Category][]tasks.Task{tasks.CategoryExecutionFeed: {task}}
	if !enabled && epoch == 0 {
		taskMap = nil
		task = nil
	}
	s.wLock()
	if err := s.errorByState(); err != nil {
		s.wUnlock()
		return err
	}
	if err := s.validateExecutionFeedCaptureLocked(namespace.ID(request.NamespaceID), epoch); err != nil {
		s.wUnlock()
		return err
	}
	completed, err := s.taskKeyManager.setAndTrackTaskKeys(taskMap)
	if err != nil {
		s.wUnlock()
		return err
	}
	request.RangeID = s.getRangeIDLocked()
	request.ExecutionFeedTask = task
	s.wUnlock()
	err = s.GetExecutionManager().DeleteWorkflowExecution(ctx, request)
	completed(err)
	if task == nil {
		return err
	}
	if len(taskMap) > 0 && persistence.OperationPossiblySucceeded(err) {
		if engine, engineErr := s.GetEngine(ctx); engineErr == nil {
			engine.NotifyNewTasks(taskMap)
		}
	}
	return s.handleWriteError(request.RangeID, err)
}

var _ historyi.ExecutionFeedCapture = (*ContextImpl)(nil)

func (s *ContextImpl) executionFeedDeletionEnabled(ns *namespace.Namespace) bool {
	if ns == nil {
		// Once namespace metadata is gone, retain tombstones whenever the source
		// is registered rather than guessing its former namespace-filtered gate.
		_, registered := s.taskCategoryRegistry.GetCategoryByID(tasks.CategoryIDExecutionFeed)
		return registered
	}
	return s.config.ExecutionFeedEnabled != nil && s.config.ExecutionFeedEnabled(ns.Name().String())
}
