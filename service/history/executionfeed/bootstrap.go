package executionfeed

import (
	"context"
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// BootstrapPage captures baselines under the same workflow serialization as
// updates. Persist the returned enumeration token before advancing to another
// page. Repeating a page is safe: later baselines replace, rather than merge
// into, the preceding state. Capture must remain enabled throughout bootstrap.
//
// An empty token on return completes enumeration. Drain each shard to a safe
// cutoff afterwards, then pair Reader.Snapshot with that vector. New creates
// and deletes during enumeration are included through their committed tasks.
func BootstrapPage(ctx context.Context, shard historyi.ShardContext, namespaceID string, token []byte, pageSize int) ([]byte, error) {
	if pageSize <= 0 {
		return nil, serviceerror.NewInvalidArgument("execution feed bootstrap page size must be positive")
	}
	ns, err := shard.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(namespaceID))
	if err != nil {
		return nil, err
	}
	config := shard.GetConfig()
	if config.ExecutionFeedEnabled == nil || !config.ExecutionFeedEnabled(ns.Name().String()) {
		return nil, serviceerror.NewFailedPrecondition("execution feed capture is disabled")
	}
	if ns.IsGlobalNamespace() {
		return nil, serviceerror.NewFailedPrecondition("execution feed does not support global namespaces")
	}
	page, err := shard.GetExecutionManager().ListConcreteExecutions(ctx, &persistence.ListConcreteExecutionsRequest{
		ShardID: shard.GetShardID(), PageSize: pageSize, PageToken: token, KeysOnly: true,
	})
	if err != nil {
		return nil, err
	}
	for _, key := range page.Keys {
		if key.NamespaceID != namespaceID {
			continue
		}
		if err := captureBaseline(ctx, shard, key.NamespaceID, key.WorkflowID, key.RunID); err != nil {
			return nil, err
		}
	}
	return page.PageToken, nil
}

func captureBaseline(ctx context.Context, shard historyi.ShardContext, namespaceID, workflowID, runID string) (retErr error) {
	wf, release, err := shard.GetCachedWorkflowContext(ctx, namespace.ID(namespaceID),
		&commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID}, locks.PriorityLow)
	if err != nil {
		if _, gone := errors.AsType[*serviceerror.NotFound](err); gone {
			// A deletion racing primary lookup is represented by its atomic tombstone.
			return nil
		}
		return err
	}
	defer func() { release(retErr) }()
	state, err := wf.LoadMutableState(ctx, shard)
	if err != nil {
		if _, gone := errors.AsType[*serviceerror.NotFound](err); gone {
			return nil
		}
		return err
	}
	capturer, ok := state.(interface{ RequestExecutionFeedBaseline() error })
	if !ok {
		return serviceerror.NewFailedPrecondition("mutable state does not support execution feed baseline capture")
	}
	if err := capturer.RequestExecutionFeedBaseline(); err != nil {
		return err
	}
	if state.IsWorkflowExecutionRunning() {
		return wf.UpdateWorkflowExecutionAsActive(ctx, shard)
	}
	return wf.SubmitClosedWorkflowSnapshot(ctx, shard, historyi.TransactionPolicyActive)
}
