package watch

import (
	"context"
	"strconv"
	"strings"
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	controlpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

type intakeControlClient struct {
	controlpb.ControlServiceClient
	state        *controlpb.State
	requests     []*controlpb.AcceptOutputRequest
	loseResponse bool
	once         bool
}

func (c *intakeControlClient) DescribeExecution(
	context.Context, *controlpb.DescribeExecutionRequest, ...grpc.CallOption,
) (*controlpb.ExecutionResponse, error) {
	return &controlpb.ExecutionResponse{State: common.CloneProto(c.state)}, nil
}

func (c *intakeControlClient) AcceptOutput(
	_ context.Context, request *controlpb.AcceptOutputRequest, _ ...grpc.CallOption,
) (*controlpb.AcceptOutputResponse, error) {
	if request.ItemCount == 0 && !request.Complete {
		return nil, serviceerror.NewInvalidArgument("empty output chunk must complete the output")
	}
	c.requests = append(c.requests, common.CloneProto(request))
	c.state.Cursor = &controlpb.Cursor{
		Sequence: request.Sequence, NextOrdinal: request.FirstOrdinal + request.ItemCount,
		Complete: request.Complete,
	}
	c.state.Incarnation = request.Incarnation
	c.state.ProgressPhase = controlpb.PROGRESS_PHASE_SNAPSHOTTING
	if request.Complete {
		c.state.ProgressPhase = controlpb.PROGRESS_PHASE_LIVE
	}
	if c.once && len(request.Actions) != 0 {
		c.state.Status = controlpb.STATUS_SATISFIED
	}
	if c.loseResponse {
		c.loseResponse = false
		return nil, serviceerror.NewUnavailable("acceptance response lost")
	}
	return &controlpb.AcceptOutputResponse{Cursor: common.CloneProto(c.state.Cursor)}, nil
}

func newIntake(t *testing.T) (*Intake, IntakeTarget, *intakeControlClient) {
	t.Helper()
	tv := testvars.New(t)
	target := IntakeTarget{
		NamespaceID: tv.NamespaceID().String(), ExecutionID: tv.WorkflowID(), OwnerEpoch: 1,
		Watch: &contractpb.Watch{
			Namespace: tv.NamespaceName().String(), Id: tv.WorkflowID(), Revision: 1,
			Spec: &contractpb.WatchSpec{
				Query: &contractpb.ResourceRef{Id: "query", Revision: 1}, Trigger: contractpb.Trigger_TRIGGER_ENTER,
				Lifetime: &contractpb.WatchSpec_Persistent{Persistent: &emptypb.Empty{}},
				Action: &contractpb.WorkflowAction{Operation: &contractpb.WorkflowAction_Start{
					Start: &contractpb.WorkflowStart{
						WorkflowId: "{action_id}", WorkflowType: tv.WorkflowType().Name, TaskQueue: tv.TaskQueue().Name,
						ReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
					},
				}},
			},
		},
	}
	control := &intakeControlClient{state: &controlpb.State{
		Namespace: target.Watch.Namespace, WatchId: target.Watch.Id,
		Revision: 1, CreationRevision: 1, OwnerEpoch: 1, Status: controlpb.STATUS_RUNNING,
		Cursor: &controlpb.Cursor{}, MaxBatch: 2,
	}}
	return &Intake{Control: control}, target, control
}

func TestIntakeResumedControlKeepsOriginalDefinition(t *testing.T) {
	t.Parallel()
	intake, target, client := newIntake(t)
	target.Watch.Spec.InitialResults = contractpb.InitialResults_INITIAL_RESULTS_INCLUDE
	client.state.Revision = 3
	output := queryOutput(1)
	prepared, err := intake.prepare(t.Context(), target.Watch, &contractpb.ResultChange{
		Output: &contractpb.OutputIdentity{
			QueryRevision: output.Revision, Incarnation: output.Incarnation,
			Sequence: output.Sequence, RowIdentity: output.Changes[0].Identity,
		},
		Trigger: output.Changes[0].Trigger, After: output.Changes[0].After,
	})
	require.NoError(t, err)
	result, err := intake.Accept(t.Context(), target, output)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &controlpb.Cursor{NextOrdinal: 1, Complete: true}, result.Cursor)
	require.Len(t, client.requests, 1)
	require.Equal(t, uint64(3), client.requests[0].Revision)
	protorequire.ProtoEqual(t, prepared, client.requests[0].Actions[0])
	require.Equal(t, uint64(1), target.Watch.Revision)
}

func TestIntakeRefusesAnotherCreationRevision(t *testing.T) {
	t.Parallel()
	intake, target, client := newIntake(t)
	client.state.CreationRevision = 2
	_, err := intake.Accept(t.Context(), target, queryOutput(1))
	require.ErrorContains(t, err, "definition changed")
	require.Empty(t, client.requests)
}

func queryOutput(count int) QueryOutput {
	output := QueryOutput{QueryID: "query", Revision: 1, Incarnation: "query-incarnation", Initial: true}
	for ordinal := range count {
		output.Changes = append(output.Changes, RowChange{
			Identity: strconv.Itoa(ordinal), Trigger: contractpb.Trigger_TRIGGER_ENTER,
			After: &structpb.Struct{Fields: map[string]*structpb.Value{"id": structpb.NewStringValue(strconv.Itoa(ordinal))}},
		})
	}
	return output
}

func TestIntakeInitialMatchingPolicy(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		include     bool
		once        bool
		actionCount int
	}{
		{"recurring ignores existing matches", false, false, 0},
		{"recurring includes existing matches", true, false, 1},
		{"condition satisfies existing match", false, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			intake, target, client := newIntake(t)
			if tc.include {
				target.Watch.Spec.InitialResults = contractpb.InitialResults_INITIAL_RESULTS_INCLUDE
			}
			if tc.once {
				client.once = true
				owner := &commonpb.WorkflowExecution{WorkflowId: "owner", RunId: "00000000-0000-4000-8000-000000000001"}
				target.Watch.Spec.Lifetime = &contractpb.WatchSpec_Once{Once: &contractpb.Once{Owner: owner}}
				target.Watch.Spec.Action.Operation = &contractpb.WorkflowAction_Signal{Signal: &contractpb.WorkflowSignal{
					Target: &contractpb.WorkflowSignal_Run{Run: owner}, SignalName: "matched",
				}}
			}
			result, err := intake.Accept(t.Context(), target, queryOutput(1))
			require.NoError(t, err)
			require.True(t, result.Complete)
			require.Equal(t, tc.once, result.Terminal)
			require.Len(t, client.requests, 1)
			require.Len(t, client.requests[0].Actions, tc.actionCount)
		})
	}
}

func TestIntakePreservesUnselectedOrdinals(t *testing.T) {
	t.Parallel()
	intake, target, client := newIntake(t)
	target.Watch.Spec.InitialResults = contractpb.InitialResults_INITIAL_RESULTS_INCLUDE
	output := queryOutput(5)
	output.Initial, output.Sequence = false, 11
	client.state.ProgressPhase = controlpb.PROGRESS_PHASE_LIVE
	client.state.Cursor = &controlpb.Cursor{Sequence: 10, Complete: true}
	output.Changes[1].Trigger, output.Changes[1].Before, output.Changes[1].After =
		contractpb.Trigger_TRIGGER_LEAVE, output.Changes[1].After, nil
	output.Changes[3].Trigger, output.Changes[3].Before, output.Changes[3].After =
		contractpb.Trigger_TRIGGER_LEAVE, output.Changes[3].After, nil
	result, err := intake.Accept(t.Context(), target, output)
	require.NoError(t, err)
	require.True(t, result.Complete)
	require.Len(t, client.requests, 3)
	var ordinals []uint64
	for _, request := range client.requests {
		require.LessOrEqual(t, request.ItemCount, uint64(2))
		for _, action := range request.Actions {
			ordinals = append(ordinals, action.Ordinal)
			start := action.GetStart()
			var envelope contractpb.ResultChange
			require.NoError(t, protojson.Unmarshal(start.Input.Payloads[0].Data, &envelope))
			require.Equal(t, start.RequestId, envelope.ActionId)
			require.Equal(t, uint32(action.Ordinal), envelope.Output.DiffOrdinal)
			require.Equal(t, output.Sequence, envelope.Output.Sequence)
		}
	}
	require.Equal(t, []uint64{0, 2, 4}, ordinals)
}

func TestIntakeResumesAfterLostAcceptanceResponse(t *testing.T) {
	t.Parallel()
	intake, target, client := newIntake(t)
	target.Watch.Spec.InitialResults = contractpb.InitialResults_INITIAL_RESULTS_INCLUDE
	client.loseResponse = true
	output := queryOutput(5)
	_, err := intake.Accept(t.Context(), target, output)
	require.ErrorContains(t, err, "response lost")
	require.Len(t, client.requests, 1)
	result, err := intake.Accept(t.Context(), target, output)
	require.NoError(t, err)
	require.True(t, result.Complete)
	var ordinals []uint64
	for _, request := range client.requests {
		for _, action := range request.Actions {
			ordinals = append(ordinals, action.Ordinal)
		}
	}
	require.Equal(t, []uint64{0, 1, 2, 3, 4}, ordinals)
	_, err = intake.Accept(t.Context(), target, output)
	require.NoError(t, err)
	require.Len(t, client.requests, 3)
}

func TestIntakeOnceLeavesUnacceptedTailUnacknowledged(t *testing.T) {
	t.Parallel()
	intake, target, client := newIntake(t)
	client.once = true
	owner := &commonpb.WorkflowExecution{WorkflowId: "owner", RunId: "00000000-0000-4000-8000-000000000001"}
	target.Watch.Spec.Lifetime = &contractpb.WatchSpec_Once{Once: &contractpb.Once{Owner: owner}}
	target.Watch.Spec.Action.Operation = &contractpb.WorkflowAction_Signal{Signal: &contractpb.WorkflowSignal{
		Target: &contractpb.WorkflowSignal_Run{Run: owner}, SignalName: "matched",
	}}
	result, err := intake.Accept(t.Context(), target, queryOutput(5))
	require.NoError(t, err)
	require.True(t, result.Terminal)
	require.False(t, result.Complete)
	require.Equal(t, uint64(1), result.Cursor.NextOrdinal)
	_, err = intake.Accept(t.Context(), target, queryOutput(5))
	require.NoError(t, err)
	require.Len(t, client.requests, 1)
}

func TestIntakeFinalizesEmptySnapshotBoundary(t *testing.T) {
	t.Parallel()
	intake, target, client := newIntake(t)
	client.state.ProgressPhase = controlpb.PROGRESS_PHASE_SNAPSHOTTING
	client.state.Cursor = &controlpb.Cursor{NextOrdinal: 3}
	result, err := intake.Accept(t.Context(), target, queryOutput(3))
	require.NoError(t, err)
	require.True(t, result.Complete)
	require.Len(t, client.requests, 1)
	require.Zero(t, client.requests[0].ItemCount)
	require.True(t, client.requests[0].Complete)
}

func TestIntakeCommitsEmptyOutput(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		initial bool
	}{
		{"empty snapshot", true},
		{"unchanged live result", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			intake, target, client := newIntake(t)
			output := queryOutput(0)
			output.Initial = tc.initial
			if !tc.initial {
				output.Sequence = 11
				client.state.ProgressPhase = controlpb.PROGRESS_PHASE_LIVE
				client.state.Cursor = &controlpb.Cursor{Sequence: 10, Complete: true}
			}
			result, err := intake.Accept(t.Context(), target, output)
			require.NoError(t, err)
			require.True(t, result.Complete)
			require.Len(t, client.requests, 1)
			protorequire.ProtoEqual(t, &controlpb.Cursor{Sequence: output.Sequence, Complete: true}, result.Cursor)
			require.Empty(t, client.requests[0].Actions)
			require.Zero(t, client.requests[0].ItemCount)
		})
	}
}

func TestIntakeEnforcesEncodedRequestLimit(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		bytes   int
		message string
	}{
		{"split large rows", 700_000, ""},
		{"single row too large", validation.MaxRequestBytes, "exceeds the control request size limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			intake, target, client := newIntake(t)
			target.Watch.Spec.InitialResults = contractpb.InitialResults_INITIAL_RESULTS_INCLUDE
			output := queryOutput(2)
			for index := range output.Changes {
				output.Changes[index].After.Fields["data"] = structpb.NewStringValue(strings.Repeat("x", tc.bytes))
			}
			result, err := intake.Accept(t.Context(), target, output)
			if tc.message != "" {
				require.ErrorContains(t, err, tc.message)
				require.Empty(t, client.requests)
				return
			}
			require.NoError(t, err)
			require.True(t, result.Complete)
			require.Len(t, client.requests, 2)
			for _, request := range client.requests {
				require.LessOrEqual(t, proto.Size(request), validation.MaxRequestBytes)
			}
		})
	}
}
