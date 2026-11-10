package watch

import (
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGraphConditionOwnerStatus(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		status enumspb.WorkflowExecutionStatus
		closed bool
	}{
		{enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, false},
		{enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, true},
		{enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, true},
		{enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, true},
		{enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, true},
		{enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, true},
		{enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, true},
	} {
		t.Run(tc.status.String(), func(t *testing.T) {
			t.Parallel()
			owner := &commonpb.WorkflowExecution{WorkflowId: "owner", RunId: "original-run"}
			watch := &contractpb.Watch{Namespace: "ns", Spec: &contractpb.WatchSpec{
				Lifetime: &contractpb.WatchSpec_Once{Once: &contractpb.Once{Owner: owner}},
			}}
			client := workflowservicemock.NewMockWorkflowServiceClient(gomock.NewController(t))
			client.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Eq(&workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: watch.Namespace, Execution: owner,
			})).Return(&workflowservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Execution: owner, Status: tc.status},
			}, nil)
			graph := &Graph{options: GraphOptions{Workflows: client}}
			closed, err := graph.ownerClosed(t.Context(), watch)
			require.NoError(t, err)
			require.Equal(t, tc.closed, closed)
		})
	}
}

func TestGraphConditionOwnerDescriptionFailures(t *testing.T) {
	t.Parallel()
	owner := &commonpb.WorkflowExecution{WorkflowId: "owner", RunId: "original-run"}
	for _, tc := range []struct {
		name     string
		response *workflowservice.DescribeWorkflowExecutionResponse
		err      error
		closed   bool
		code     codes.Code
	}{
		{"missing owner", nil, serviceerror.NewNotFound("retained owner is gone"), true, codes.OK},
		{"gRPC missing owner", nil, status.Error(codes.NotFound, "retained owner is gone"), true, codes.OK},
		{"unavailable", nil, serviceerror.NewUnavailable("owner lookup unavailable"), false, codes.Unavailable},
		{"permission denied", nil, serviceerror.NewPermissionDenied("owner lookup denied", ""), false, codes.PermissionDenied},
		{"missing response", nil, nil, false, codes.DataLoss},
		{"missing info", &workflowservice.DescribeWorkflowExecutionResponse{}, nil, false, codes.DataLoss},
		{"successor run", &workflowservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
				Execution: &commonpb.WorkflowExecution{WorkflowId: owner.WorkflowId, RunId: "successor-run"},
				Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
		}, nil, false, codes.DataLoss},
		{"unspecified status", &workflowservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Execution: owner},
		}, nil, false, codes.DataLoss},
		{"unknown status", &workflowservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Execution: owner, Status: 999},
		}, nil, false, codes.DataLoss},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := workflowservicemock.NewMockWorkflowServiceClient(gomock.NewController(t))
			client.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(tc.response, tc.err)
			graph := &Graph{options: GraphOptions{Workflows: client}}
			watch := &contractpb.Watch{Namespace: "ns", Spec: &contractpb.WatchSpec{
				Lifetime: &contractpb.WatchSpec_Once{Once: &contractpb.Once{Owner: owner}},
			}}
			closed, err := graph.ownerClosed(t.Context(), watch)
			require.Equal(t, tc.closed, closed)
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
			if tc.err != nil && tc.code != codes.OK {
				require.ErrorIs(t, err, tc.err)
			}
		})
	}
}

func TestGraphPersistentWatchHasNoConditionOwner(t *testing.T) {
	t.Parallel()
	graph := &Graph{}
	closed, err := graph.ownerClosed(t.Context(), &contractpb.Watch{Spec: &contractpb.WatchSpec{}})
	require.NoError(t, err)
	require.False(t, closed)
}
