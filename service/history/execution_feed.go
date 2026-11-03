package history

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// GetCachedWorkflowContext acquires the execution's actual archetype cache lock.
// The primary read discovers only the immutable archetype; callers load fresh
// mutable state after acquisition rather than committing the unlocked read.
func (e *historyEngineImpl) GetCachedWorkflowContext(
	ctx context.Context,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	priority locks.Priority,
) (historyi.WorkflowContext, historyi.ReleaseWorkflowContextFunc, error) {
	stored, err := e.shardContext.GetExecutionManager().GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		ShardID: e.shardContext.GetShardID(), NamespaceID: namespaceID.String(),
		WorkflowID: execution.GetWorkflowId(), RunID: execution.GetRunId(), ArchetypeID: chasm.WorkflowArchetypeID,
	})
	if err != nil {
		return nil, nil, err
	}
	archetypeID, err := chasm.ExecutionArchetypeID(stored.State)
	if err != nil {
		return nil, nil, err
	}
	return e.workflowCache.GetOrCreateChasmExecution(ctx, e.shardContext, namespaceID, execution, archetypeID, priority)
}
