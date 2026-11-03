//go:build integration

package tests

import (
	"testing"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/watch"
	controlpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/common/testing/testvars"
	watchservice "go.temporal.io/server/service/watch"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestDrasiDeliveryRetryRPC(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t, testcore.WithDynamicConfig(dynamicconfig.EnableDrasiWatches, true))
	ctx := testcontext.For(t)
	tv := testvars.New(t)
	definitions, control := env.GetTestCluster().DrasiClient(), env.GetTestCluster().WatchControlClient()
	source, err := definitions.CreateSource(ctx, &contractpb.CreateSourceRequest{
		Namespace: env.Namespace().String(), Id: "delivery.source", RequestId: tv.RequestID() + "-source",
		Spec: &contractpb.SourceSpec{Kind: &contractpb.SourceSpec_Temporal{Temporal: &emptypb.Empty{}}},
	})
	require.NoError(t, err)
	query, err := definitions.CreateQuery(ctx, &contractpb.CreateQueryRequest{
		Namespace: source.Namespace, Id: "delivery.query", RequestId: tv.RequestID() + "-query",
		Spec: &contractpb.QuerySpec{
			Language: "cypher", Text: "MATCH (n) RETURN n", StateStore: "persistent",
			Sources: []*contractpb.ResourceRef{{Id: source.Id, Revision: source.Revision}},
		},
	})
	require.NoError(t, err)
	env.SdkWorker().RegisterWorkflowWithOptions(func(_ workflow.Context, value string) (string, error) {
		return value, nil
	}, workflow.RegisterOptions{Name: tv.WorkflowType().Name})
	resource, err := definitions.CreateWatch(ctx, &contractpb.CreateWatchRequest{
		Namespace: source.Namespace, Id: "delivery.watch", RequestId: tv.RequestID() + "-watch",
		Spec: &contractpb.WatchSpec{
			Query: &contractpb.ResourceRef{Id: query.Id, Revision: query.Revision}, Trigger: contractpb.Trigger_TRIGGER_ENTER,
			Lifetime: &contractpb.WatchSpec_Persistent{Persistent: &emptypb.Empty{}},
			Action: &contractpb.WorkflowAction{
				ResultPayload: contractpb.ResultPayload_RESULT_PAYLOAD_OMIT,
				Operation: &contractpb.WorkflowAction_Start{Start: &contractpb.WorkflowStart{
					WorkflowId: tv.WorkflowID(), WorkflowType: tv.WorkflowType().Name, TaskQueue: env.WorkerTaskQueue(),
					Input: payloads.EncodeString("delivered"), ReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
				}},
			},
		},
	})
	require.NoError(t, err)
	executionID, err := watch.ExecutionID(env.NamespaceID().String(), resource.Id, "1")
	require.NoError(t, err)
	definition, err := proto.Marshal(resource)
	require.NoError(t, err)
	_, err = control.CreateExecution(ctx, &controlpb.CreateExecutionRequest{
		NamespaceId: env.NamespaceID().String(), ExecutionId: executionID, WatchId: resource.Id,
		Revision: resource.Revision, Definition: definition, RequestId: tv.RequestID() + "-control",
	})
	require.NoError(t, err)
	_, err = control.MutateExecution(ctx, &controlpb.MutateExecutionRequest{
		NamespaceId: env.NamespaceID().String(), ExecutionId: executionID, ExpectedRevision: 1,
		Mutation: &controlpb.MutateExecutionRequest_Claim{Claim: &controlpb.ClaimMutation{NewEpoch: 1}},
	})
	require.NoError(t, err)
	_, err = control.AcceptOutput(ctx, &controlpb.AcceptOutputRequest{
		NamespaceId: env.NamespaceID().String(), ExecutionId: executionID, Revision: 1, OwnerEpoch: 1,
		Incarnation: "query-incarnation", Initial: true, Complete: true,
	})
	require.NoError(t, err)
	removeFailure := env.InjectRequestFault(func(request any) error {
		if request, ok := request.(*workflowservice.StartWorkflowExecutionRequest); ok && request.WorkflowId == tv.WorkflowID() {
			return serviceerror.NewPermissionDenied("private request diagnostic", "")
		}
		return nil
	})
	intake := &watchservice.Intake{Control: control, Workflows: env.GetTestCluster().FrontendClient()}
	_, err = intake.Accept(ctx, watchservice.IntakeTarget{
		NamespaceID: env.NamespaceID().String(), ExecutionID: executionID, OwnerEpoch: 1, Watch: resource,
	}, watchservice.QueryOutput{
		QueryID: query.Id, Revision: query.Revision, Incarnation: "query-incarnation", Sequence: 1,
		Changes: []watchservice.RowChange{{
			Identity: "row", Trigger: contractpb.Trigger_TRIGGER_ENTER,
			After: &structpb.Struct{Fields: map[string]*structpb.Value{"value": structpb.NewStringValue("match")}},
		}},
	})
	require.NoError(t, err)
	var blocked *contractpb.Delivery
	await.Require(ctx, t, func(t *await.T) {
		listed, err := definitions.ListDeliveries(t.Context(), &contractpb.ListDeliveriesRequest{
			Namespace: resource.Namespace, Id: resource.Id,
		})
		require.NoError(t, err)
		require.Len(t, listed.Deliveries, 1)
		blocked = listed.Deliveries[0]
		require.Equal(t, contractpb.DeliveryState_DELIVERY_STATE_BLOCKED, blocked.State)
		require.Equal(t, "PermissionDenied", blocked.Message)
		require.Equal(t, uint32(1), blocked.Attempts)
	}, 15*time.Second, 20*time.Millisecond)
	removeFailure()

	request := &contractpb.RetryDeliveryRequest{
		Namespace: resource.Namespace, Id: resource.Id, ActionId: blocked.ActionId,
		ExpectedRevision: resource.Revision, RequestId: tv.RequestID() + "-retry",
	}
	removeLostResponse := env.InjectResponseFault(func(request, _ any, _ error) error {
		if _, ok := request.(*contractpb.RetryDeliveryRequest); ok {
			return serviceerror.NewUnavailable("retry response lost")
		}
		return nil
	})
	_, err = definitions.RetryDelivery(ctx, request)
	require.Equal(t, codes.Unavailable, serviceerror.ToStatus(err).Code())
	removeLostResponse()
	var original *contractpb.Delivery
	await.Require(ctx, t, func(t *await.T) {
		response, err := control.DescribeExecution(t.Context(), &controlpb.DescribeExecutionRequest{
			NamespaceId: env.NamespaceID().String(), ExecutionId: executionID,
		})
		require.NoError(t, err)
		require.Equal(t, uint64(1), response.State.Delivered)
		require.Empty(t, response.State.PendingIds)
		require.Len(t, response.Deliveries, 1)
		require.Equal(t, uint32(2), response.Deliveries[0].Attempts)
		receipt := response.State.RetryReceipts[request.RequestId]
		require.NotNil(t, receipt)
		original = receipt.Delivery
	}, 15*time.Second, 20*time.Millisecond)
	paused, err := definitions.PauseWatch(ctx, &contractpb.WatchOperationRequest{
		Namespace: resource.Namespace, Id: resource.Id, ExpectedRevision: resource.Revision, RequestId: tv.RequestID() + "-pause",
	})
	require.NoError(t, err)
	replayed, err := definitions.RetryDelivery(ctx, request)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, original, replayed)
	stale := common.CloneProto(request)
	stale.RequestId += "-stale"
	_, err = definitions.RetryDelivery(ctx, stale)
	require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())
	changed := common.CloneProto(request)
	changed.AcknowledgeAmbiguous = true
	_, err = definitions.RetryDelivery(ctx, changed)
	require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	_, err = definitions.DeleteWatch(ctx, &contractpb.WatchOperationRequest{
		Namespace: resource.Namespace, Id: resource.Id, ExpectedRevision: paused.Revision, RequestId: tv.RequestID() + "-delete",
	})
	require.NoError(t, err)
	replayed, err = definitions.RetryDelivery(ctx, request)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, original, replayed)
	listed, err := definitions.ListDeliveries(ctx, &contractpb.ListDeliveriesRequest{Namespace: resource.Namespace, Id: resource.Id})
	require.NoError(t, err)
	require.Len(t, listed.Deliveries, 1)
	require.Equal(t, uint32(2), listed.Deliveries[0].Attempts)
	require.Equal(t, contractpb.DeliveryState_DELIVERY_STATE_DELIVERED, listed.Deliveries[0].State)
	_, err = control.MutateExecution(ctx, &controlpb.MutateExecutionRequest{
		NamespaceId: env.NamespaceID().String(), ExecutionId: executionID, ExpectedRevision: 1,
		Mutation: &controlpb.MutateExecutionRequest_Cancel{Cancel: &emptypb.Empty{}},
	})
	require.NoError(t, err)
	replayed, err = definitions.RetryDelivery(ctx, request)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, original, replayed)
	var result string
	require.NoError(t, env.SdkClient().GetWorkflow(ctx, tv.WorkflowID(), "").Get(ctx, &result))
	require.Equal(t, "delivered", result)
}
