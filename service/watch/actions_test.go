package watch

import (
	"testing"

	watchpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

func testWatch() (*watchpb.Watch, *watchpb.ResultChange) {
	return &watchpb.Watch{
		Namespace: "test-namespace", Id: "test-watch", Revision: 7,
		Spec: &watchpb.WatchSpec{
			Query:   &watchpb.ResourceRef{Id: "test-query", Revision: 3},
			Trigger: watchpb.Trigger_TRIGGER_ENTER,
			Action: &watchpb.WorkflowAction{Operation: &watchpb.WorkflowAction_Start{
				Start: &watchpb.WorkflowStart{
					WorkflowId: "workflow-{action_id}", WorkflowType: "test-type", TaskQueue: "test-queue",
					Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{{
						Metadata: map[string][]byte{"encoding": []byte("binary/encrypted")},
						Data:     []byte{0, 255, 17},
					}}},
				},
			}},
		},
	}, &watchpb.ResultChange{
		Output: &watchpb.OutputIdentity{
			QueryRevision: 3, Incarnation: "query-incarnation", Sequence: 9, RowIdentity: "row", DiffOrdinal: 2,
		},
		Trigger: watchpb.Trigger_TRIGGER_ENTER,
		After:   &structpb.Struct{Fields: map[string]*structpb.Value{"value": structpb.NewNumberValue(1)}},
	}
}

func TestPrepareStartPreservesPayloadsAndFreezesResult(t *testing.T) {
	t.Parallel()
	watch, change := testWatch()
	originalWatch, originalChange := common.CloneProto(watch), common.CloneProto(change)
	action, err := PrepareAction(t.Context(), nil, watch, change)
	require.NoError(t, err)
	request, ok := action.Request.(*workflowservice.StartWorkflowExecutionRequest)
	require.True(t, ok)
	require.Equal(t, "workflow-"+request.RequestId, request.WorkflowId)
	require.Equal(t, watch.Namespace, request.Namespace)
	require.Len(t, request.Input.Payloads, 2)
	protorequire.ProtoEqual(t, watch.Spec.Action.GetStart().Input.Payloads[0], request.Input.Payloads[0])
	require.Equal(t, map[string][]byte{"encoding": []byte("json/plain")}, request.Input.Payloads[1].Metadata)
	var received watchpb.ResultChange
	require.NoError(t, protojson.Unmarshal(request.Input.Payloads[1].Data, &received))
	expected := common.CloneProto(change)
	expected.ActionId = request.RequestId
	protorequire.ProtoEqual(t, expected, &received)
	protorequire.ProtoEqual(t, originalWatch, watch)
	protorequire.ProtoEqual(t, originalChange, change)
	watch.Spec.Action.GetStart().Input.Payloads[0].Data[0] = 9
	require.Equal(t, byte(0), request.Input.Payloads[0].Data[0])
}

func TestPrepareInitialSnapshotAtSequenceZero(t *testing.T) {
	t.Parallel()
	watch, change := testWatch()
	change.Output.Sequence, change.Output.DiffOrdinal = 0, 0
	action, err := PrepareAction(t.Context(), nil, watch, change)
	require.NoError(t, err)
	request, ok := action.Request.(*workflowservice.StartWorkflowExecutionRequest)
	require.True(t, ok)
	var received watchpb.ResultChange
	require.NoError(t, protojson.Unmarshal(request.Input.Payloads[1].Data, &received))
	expected := common.CloneProto(change)
	expected.ActionId = request.RequestId
	protorequire.ProtoEqual(t, expected, &received)
}

func TestPrepareCurrentSignalPinsOneRun(t *testing.T) {
	t.Parallel()
	watch, change := testWatch()
	watch.Spec.Action.Operation = &watchpb.WorkflowAction_Signal{Signal: &watchpb.WorkflowSignal{
		Target:     &watchpb.WorkflowSignal_CurrentWorkflowId{CurrentWorkflowId: "target"},
		SignalName: "test-signal",
	}}
	response := &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: "target", RunId: "original-run"},
		},
	}
	client := workflowservicemock.NewMockWorkflowServiceClient(gomock.NewController(t))
	client.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(response, nil)
	action, err := PrepareAction(t.Context(), client, watch, change)
	require.NoError(t, err)
	response.WorkflowExecutionInfo.Execution.RunId = "successor-run"
	request, ok := action.Request.(*workflowservice.SignalWorkflowExecutionRequest)
	require.True(t, ok)
	protorequire.ProtoEqual(t, &commonpb.WorkflowExecution{WorkflowId: "target", RunId: "original-run"}, request.WorkflowExecution)
}

func TestPrepareSignalWithStartAppendsOnlyToSignalInput(t *testing.T) {
	t.Parallel()
	watch, change := testWatch()
	start := common.CloneProto(watch.Spec.Action.GetStart())
	watch.Spec.Action.Operation = &watchpb.WorkflowAction_SignalWithStart{
		SignalWithStart: &watchpb.WorkflowSignalWithStart{
			Start: start, SignalName: "test-signal", SignalInput: common.CloneProto(start.Input),
		},
	}
	for _, mode := range []watchpb.ResultPayload{
		watchpb.ResultPayload_RESULT_PAYLOAD_APPEND, watchpb.ResultPayload_RESULT_PAYLOAD_OMIT,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()
			input := common.CloneProto(watch)
			input.Spec.Action.ResultPayload = mode
			action, err := PrepareAction(t.Context(), nil, input, change)
			require.NoError(t, err)
			request, ok := action.Request.(*workflowservice.SignalWithStartWorkflowExecutionRequest)
			require.True(t, ok)
			protorequire.ProtoEqual(t, start.Input, request.Input)
			if mode == watchpb.ResultPayload_RESULT_PAYLOAD_APPEND {
				require.Len(t, request.SignalInput.Payloads, len(start.Input.Payloads)+1)
			} else {
				protorequire.ProtoEqual(t, start.Input, request.SignalInput)
			}
		})
	}
}

func TestPrepareActionRejectsMismatchedIdentityAndOneShotTarget(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*watchpb.Watch, *watchpb.ResultChange)
		error  string
	}{
		{"wrong query revision", func(_ *watchpb.Watch, change *watchpb.ResultChange) {
			change.Output.QueryRevision++
		}, "matching query output"},
		{"wrong trigger", func(_ *watchpb.Watch, change *watchpb.ResultChange) {
			change.Trigger = watchpb.Trigger_TRIGGER_LEAVE
		}, "watch trigger"},
		{"unsupported payload mode", func(watch *watchpb.Watch, _ *watchpb.ResultChange) {
			watch.Spec.Action.ResultPayload = 99
		}, "payload mode"},
		{"one-shot start", func(watch *watchpb.Watch, _ *watchpb.ResultChange) {
			watch.Spec.Lifetime = &watchpb.WatchSpec_Once{Once: &watchpb.Once{
				Owner: &commonpb.WorkflowExecution{WorkflowId: "owner", RunId: "owner-run"},
			}}
		}, "owning workflow run"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			watch, change := testWatch()
			tc.change(watch, change)
			_, err := PrepareAction(t.Context(), nil, watch, change)
			require.ErrorContains(t, err, tc.error)
		})
	}
}
