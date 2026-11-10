//go:build integration && temporal_with_drasi && cgo

package temporal_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/sdk/go/condition"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdktemporal "go.temporal.io/sdk/temporal"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testvars"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type conditionResponseLossClient struct {
	contractpb.WatchServiceClient
	mu        sync.Mutex
	dropped   bool
	confirmed int
	lost      chan *contractpb.CreateWatchRequest
}

func (c *conditionResponseLossClient) CreateWatch(ctx context.Context, req *contractpb.CreateWatchRequest, options ...grpc.CallOption) (*contractpb.Watch, error) {
	created, err := c.WatchServiceClient.CreateWatch(ctx, req, options...)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	drop := !c.dropped
	c.dropped = true
	if !drop {
		c.confirmed++
	}
	c.mu.Unlock()
	if drop {
		c.lost <- common.CloneProto(req)
		return nil, status.Error(codes.Unavailable, "condition create response lost after commit")
	}
	return created, nil
}

func watchConditionHelperWorkflow(ctx workflow.Context, c condition.Condition) (condition.Result, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		RetryPolicy:            &sdktemporal.RetryPolicy{InitialInterval: time.Minute},
		ScheduleToCloseTimeout: 3 * time.Minute,
	})
	return condition.Await(ctx, c)
}

func (pipeline *watchHistoryPipeline) verifyConditionHelper(t *testing.T, producerID string) {
	t.Helper()
	tv := testvars.New(t)
	run, err := pipeline.client.ExecuteWorkflow(t.Context(), sdkclient.StartWorkflowOptions{
		ID: tv.WorkflowID(), TaskQueue: pipeline.taskQueue,
	}, pipeline.helperType, condition.Condition{
		QueryID: pipeline.query.Id, QueryRevision: pipeline.query.Revision, Deadline: time.Now().Add(2 * time.Minute),
	})
	require.NoError(t, err)
	var result condition.Result
	require.NoError(t, run.Get(t.Context(), &result))
	require.Equal(t, condition.CleanupNotNeeded, result.Cleanup)
	require.Equal(t, map[string]any{"workflow_id": producerID}, result.Match.After)
	require.NotEmpty(t, result.Match.ActionID)
	pipeline.helperClient.mu.Lock()
	confirmed := pipeline.helperClient.confirmed
	pipeline.helperClient.mu.Unlock()
	require.Zero(t, confirmed, "the matching signal must complete the helper before registration confirmation")
	lost := await.Rcv(t, pipeline.helperClient.lost)
	require.Equal(t, run.GetID(), lost.Spec.GetOnce().Owner.WorkflowId)
	require.Equal(t, run.GetRunID(), lost.Spec.GetOnce().Owner.RunId)
	require.Equal(t, lost.Spec.GetOnce().Owner, lost.Spec.Action.GetSignal().GetRun())
	await.Require(t.Context(), t, func(t *await.T) {
		resource, err := pipeline.definitions.GetWatch(t.Context(), &contractpb.GetWatchRequest{
			Namespace: lost.Namespace, Id: lost.Id,
		})
		require.NoError(t, err)
		require.Equal(t, contractpb.LifecycleState_LIFECYCLE_STATE_COMPLETED, resource.GetStatus().GetState())
		deliveries, err := pipeline.definitions.ListDeliveries(t.Context(), &contractpb.ListDeliveriesRequest{
			Namespace: lost.Namespace, Id: lost.Id,
		})
		require.NoError(t, err)
		require.Len(t, deliveries.Deliveries, 1)
		require.Equal(t, result.Match.ActionID, deliveries.Deliveries[0].ActionId)
		require.Equal(t, lost.Spec.GetOnce().Owner, deliveries.Deliveries[0].Execution)
	}, 30*time.Second, 100*time.Millisecond)

	var history historypb.History
	events := pipeline.client.GetWorkflowHistory(t.Context(), run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for events.HasNext() {
		event, err := events.Next()
		require.NoError(t, err)
		history.Events = append(history.Events, event)
	}
	replayer := sdkworker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(watchConditionHelperWorkflow, workflow.RegisterOptions{Name: pipeline.helperType})
	require.NoError(t, replayer.ReplayWorkflowHistory(nil, &history))

	empty, err := pipeline.definitions.GetQuery(t.Context(), &contractpb.GetQueryRequest{
		Namespace: pipeline.query.Namespace, Id: "runtime.condition.empty",
	})
	require.NoError(t, err)
	waiting, err := pipeline.client.ExecuteWorkflow(t.Context(), sdkclient.StartWorkflowOptions{
		ID: tv.WithWorkflowIDNumber(2).WorkflowID(), TaskQueue: pipeline.taskQueue,
	}, pipeline.helperType, condition.Condition{
		QueryID: empty.Id, QueryRevision: empty.Revision,
		Deadline: time.Now().Add(2 * time.Minute), WaitTimeout: 250 * time.Millisecond,
	})
	require.NoError(t, err)
	err = waiting.Get(t.Context(), nil)
	app, ok := errors.AsType[*sdktemporal.ApplicationError](err)
	require.True(t, ok, "%v", err)
	require.Equal(t, "DrasiConditionWaitTimeout", app.Type())
	await.Require(t.Context(), t, func(t *await.T) {
		watches, err := pipeline.definitions.ListWatches(t.Context(), &contractpb.ListWatchesRequest{Namespace: empty.Namespace})
		require.NoError(t, err)
		var owned *contractpb.Watch
		for _, resource := range watches.Watches {
			if resource.Spec.GetOnce().GetOwner().GetRunId() == waiting.GetRunID() {
				owned = resource
				break
			}
		}
		require.NotNil(t, owned)
		require.Equal(t, uint64(2), owned.Revision)
		require.Equal(t, contractpb.DesiredState_DESIRED_STATE_CANCELED, owned.DesiredState)
		require.Equal(t, contractpb.LifecycleState_LIFECYCLE_STATE_CANCELED, owned.GetStatus().GetState())
		require.Zero(t, owned.GetStatus().GetPendingDeliveries())
	}, 30*time.Second, 100*time.Millisecond)
}
