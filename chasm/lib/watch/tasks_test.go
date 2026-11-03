package watch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	watchpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

func (e *testEnvironment) execute(id string) (bool, error) {
	return chasmtest.ExecuteSideEffectTask(
		e.ctx, e.engine, e.component, e.dispatch, chasm.TaskAttributes{},
		&watchpb.DispatchTask{ActionId: id},
	)
}

func TestSafeStartRetryUsesOriginalIdentity(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t)
	receipt, err := env.accept(testBatch())
	require.NoError(t, err)
	id := receipt.ActionIDs[0]
	var requestIDs []string
	gomock.InOrder(
		env.client.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, req *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
				requestIDs = append(requestIDs, req.RequestId)
				return nil, serviceerror.NewUnavailable("response lost after acceptance")
			}),
		env.client.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, req *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
				requestIDs = append(requestIDs, req.RequestId)
				return nil, serviceerror.NewWorkflowExecutionAlreadyStarted("already started", id, "accepted-run")
			}),
	)
	_, err = env.execute(id)
	require.ErrorContains(t, err, "response lost")
	dropped, err := env.execute(id)
	require.NoError(t, err)
	require.False(t, dropped)
	require.Equal(t, []string{id, id}, requestIDs)
	intent := env.intent(t, id)
	require.Equal(t, watchpb.DELIVERY_STATUS_DELIVERED, intent.Status)
	require.Equal(t, uint32(2), intent.Attempts)
	require.Equal(t, "accepted-run", intent.RunId)
	dropped, err = env.execute(id)
	require.NoError(t, err)
	require.True(t, dropped)
}

func TestUncertainStartDoesNotReplayOutsideDeduplicationScope(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t)
	batch := testBatch()
	batch.Actions[0].Request.(*workflowservice.StartWorkflowExecutionRequest).WorkflowIdReusePolicy =
		enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	receipt, err := env.accept(batch)
	require.NoError(t, err)
	id := receipt.ActionIDs[0]
	env.client.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewUnavailable("response lost"))
	_, err = env.execute(id)
	require.NoError(t, err)
	require.Equal(t, watchpb.DELIVERY_STATUS_AMBIGUOUS, env.intent(t, id).Status)
	dropped, err := env.execute(id)
	require.NoError(t, err)
	require.True(t, dropped)
	require.Zero(t, env.state(t).Delivered)
}

func TestSignalWithStartAmbiguityRequiresExplicitRetry(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t)
	batch := testBatch()
	batch.Actions[0].Request = &workflowservice.SignalWithStartWorkflowExecutionRequest{
		WorkflowId: "target-workflow", SignalName: "signal",
		WorkflowType: &commonpb.WorkflowType{Name: "test-workflow-type"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: "test-task-queue"},
	}
	receipt, err := env.accept(batch)
	require.NoError(t, err)
	id := receipt.ActionIDs[0]
	env.client.EXPECT().SignalWithStartWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewUnavailable("response lost"))
	_, err = env.execute(id)
	require.NoError(t, err)
	require.Equal(t, watchpb.DELIVERY_STATUS_AMBIGUOUS, env.intent(t, id).Status)
	require.ErrorContains(t, env.update(func(c *Component, ctx chasm.MutableContext) error {
		return c.retry(ctx, id, false)
	}), "explicit acknowledgment")
	dropped, err := env.execute(id)
	require.NoError(t, err)
	require.True(t, dropped)
	require.NoError(t, env.update(func(c *Component, ctx chasm.MutableContext) error {
		return c.retry(ctx, id, true)
	}))
	env.client.EXPECT().SignalWithStartWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&workflowservice.SignalWithStartWorkflowExecutionResponse{RunId: "target-run"}, nil)
	_, err = env.execute(id)
	require.NoError(t, err)
	require.Equal(t, watchpb.DELIVERY_STATUS_DELIVERED, env.intent(t, id).Status)
	require.Equal(t, uint32(2), env.intent(t, id).Attempts)
}

func TestSignalRunClosureAfterLostResponseIsAmbiguous(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t)
	batch := testBatch()
	batch.Actions[0].Request = &workflowservice.SignalWorkflowExecutionRequest{
		SignalName: "signal",
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: "target-workflow", RunId: "pinned-run",
		},
	}
	receipt, err := env.accept(batch)
	require.NoError(t, err)
	id := receipt.ActionIDs[0]
	var runs []string
	gomock.InOrder(
		env.client.EXPECT().SignalWorkflowExecution(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, req *workflowservice.SignalWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.SignalWorkflowExecutionResponse, error) {
				runs = append(runs, req.WorkflowExecution.RunId)
				return nil, serviceerror.NewUnavailable("response lost")
			}),
		env.client.EXPECT().SignalWorkflowExecution(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, req *workflowservice.SignalWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.SignalWorkflowExecutionResponse, error) {
				runs = append(runs, req.WorkflowExecution.RunId)
				return nil, serviceerror.NewNotFound("target run closed")
			}),
	)
	_, err = env.execute(id)
	require.ErrorContains(t, err, "response lost")
	_, err = env.execute(id)
	require.NoError(t, err)
	require.Equal(t, []string{"pinned-run", "pinned-run"}, runs)
	require.Equal(t, watchpb.DELIVERY_STATUS_AMBIGUOUS, env.intent(t, id).Status)
}

func TestInterruptedUncertainDispatchDoesNotRepeatEffect(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t)
	batch := testBatch()
	batch.Actions[0].Request.(*workflowservice.StartWorkflowExecutionRequest).WorkflowIdReusePolicy =
		enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	receipt, err := env.accept(batch)
	require.NoError(t, err)
	id := receipt.ActionIDs[0]
	require.NoError(t, env.update(func(c *Component, ctx chasm.MutableContext) error {
		_, err := c.BeginDispatch(ctx, id)
		return err
	}))
	_, err = env.execute(id)
	require.NoError(t, err)
	require.Equal(t, watchpb.DELIVERY_STATUS_AMBIGUOUS, env.intent(t, id).Status)
	require.Equal(t, uint32(1), env.intent(t, id).Attempts)
}

func TestBlockedActionRetainsPayloadAndPreventsReordering(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t)
	batch := testBatch()
	first, err := env.accept(batch)
	require.NoError(t, err)
	batch.Sequence = 2
	second, err := env.accept(batch)
	require.NoError(t, err)
	env.client.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewPermissionDenied("sensitive diagnostic", ""))
	_, err = env.execute(first.ActionIDs[0])
	require.NoError(t, err)
	intent := env.intent(t, first.ActionIDs[0])
	require.Equal(t, watchpb.DELIVERY_STATUS_FAILED, intent.Status)
	require.Equal(t, "PermissionDenied", intent.Error)
	require.NotEmpty(t, intent.Request)
	require.Equal(t, []string{first.ActionIDs[0], second.ActionIDs[0]}, env.state(t).PendingIds)
	dropped, err := env.execute(second.ActionIDs[0])
	require.NoError(t, err)
	require.True(t, dropped)
}
