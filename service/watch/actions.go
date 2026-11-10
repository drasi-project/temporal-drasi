package watch

import (
	"context"
	"strings"

	watchpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	watchcontrol "go.temporal.io/server/chasm/lib/watch"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// PrepareAction resolves current-run targets before entering a CHASM transaction.
func PrepareAction(
	ctx context.Context,
	client workflowservice.WorkflowServiceClient,
	watch *watchpb.Watch,
	change *watchpb.ResultChange,
) (watchcontrol.Action, error) {
	output := change.GetOutput()
	if watch.GetNamespace() == "" || watch.GetId() == "" || watch.GetRevision() == 0 ||
		output.GetIncarnation() == "" || output.GetRowIdentity() == "" || output.GetQueryRevision() == 0 ||
		output.GetQueryRevision() != watch.GetSpec().GetQuery().GetRevision() {
		return watchcontrol.Action{}, serviceerror.NewInvalidArgument("watch action requires a matching query output identity")
	}
	id := watchcontrol.ActionID(watchcontrol.ActionIdentity{
		Namespace: watch.Namespace, WatchID: watch.Id, Revision: watch.Revision,
		Incarnation: output.Incarnation, Sequence: output.Sequence, Ordinal: uint64(output.DiffOrdinal), RowKey: output.RowIdentity,
	})
	envelope := common.CloneProto(change)
	envelope.ActionId = id
	action := watch.GetSpec().GetAction()
	if action == nil {
		return watchcontrol.Action{}, serviceerror.NewInvalidArgument("watch action is required")
	}
	if change.GetTrigger() == watchpb.Trigger_TRIGGER_UNSPECIFIED || change.Trigger != watch.Spec.Trigger {
		return watchcontrol.Action{}, serviceerror.NewInvalidArgument("query result change does not match the watch trigger")
	}
	if once := watch.GetSpec().GetOnce(); once != nil &&
		(once.GetOwner().GetRunId() == "" || !proto.Equal(once.Owner, action.GetSignal().GetRun())) {
		return watchcontrol.Action{}, serviceerror.NewInvalidArgument("one-shot condition must signal its owning workflow run")
	}
	result, err := resultPayload(action.ResultPayload, envelope)
	if err != nil {
		return watchcontrol.Action{}, err
	}
	var request proto.Message
	switch operation := action.Operation.(type) {
	case *watchpb.WorkflowAction_Start:
		start := operation.Start
		if err := validateStart(start); err != nil {
			return watchcontrol.Action{}, err
		}
		request = &workflowservice.StartWorkflowExecutionRequest{
			Namespace:  watch.Namespace,
			RequestId:  id,
			Identity:   "temporal-drasi-watch",
			WorkflowId: strings.ReplaceAll(start.WorkflowId, "{action_id}", id),
			WorkflowType: &commonpb.WorkflowType{
				Name: start.WorkflowType,
			},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: start.TaskQueue},
			Input:                    appendResult(start.Input, result),
			WorkflowExecutionTimeout: common.CloneProto(start.ExecutionTimeout),
			WorkflowRunTimeout:       common.CloneProto(start.RunTimeout),
			WorkflowTaskTimeout:      common.CloneProto(start.TaskTimeout),
			WorkflowIdReusePolicy:    start.ReusePolicy,
			WorkflowIdConflictPolicy: start.ConflictPolicy,
		}
	case *watchpb.WorkflowAction_Signal:
		signal := operation.Signal
		if signal == nil {
			return watchcontrol.Action{}, serviceerror.NewInvalidArgument("watch signal attributes are required")
		}
		execution, err := resolveSignalTarget(ctx, client, watch.Namespace, signal)
		if err != nil {
			return watchcontrol.Action{}, err
		}
		request = &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         watch.Namespace,
			RequestId:         id,
			Identity:          "temporal-drasi-watch",
			WorkflowExecution: execution,
			SignalName:        signal.SignalName,
			Input:             appendResult(signal.Input, result),
		}
	case *watchpb.WorkflowAction_SignalWithStart:
		signal := operation.SignalWithStart
		start := signal.GetStart()
		if err := validateStart(start); err != nil {
			return watchcontrol.Action{}, err
		}
		request = &workflowservice.SignalWithStartWorkflowExecutionRequest{
			Namespace:  watch.Namespace,
			RequestId:  id,
			Identity:   "temporal-drasi-watch",
			WorkflowId: strings.ReplaceAll(start.WorkflowId, "{action_id}", id),
			WorkflowType: &commonpb.WorkflowType{
				Name: start.WorkflowType,
			},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: start.TaskQueue},
			Input:                    common.CloneProto(start.Input),
			SignalName:               signal.SignalName,
			SignalInput:              appendResult(signal.SignalInput, result),
			WorkflowExecutionTimeout: common.CloneProto(start.ExecutionTimeout),
			WorkflowRunTimeout:       common.CloneProto(start.RunTimeout),
			WorkflowTaskTimeout:      common.CloneProto(start.TaskTimeout),
			WorkflowIdReusePolicy:    start.ReusePolicy,
			WorkflowIdConflictPolicy: start.ConflictPolicy,
		}
	default:
		return watchcontrol.Action{}, serviceerror.NewInvalidArgument("watch action operation is required")
	}
	return watchcontrol.Action{Ordinal: uint64(output.DiffOrdinal), RowKey: output.RowIdentity, Request: request}, nil
}

func validateStart(start *watchpb.WorkflowStart) error {
	if start == nil {
		return serviceerror.NewInvalidArgument("watch workflow start attributes are required")
	}
	return watchcontrol.ValidateStartPolicies(start.ReusePolicy, start.ConflictPolicy)
}

func resultPayload(mode watchpb.ResultPayload, change *watchpb.ResultChange) (*commonpb.Payload, error) {
	switch mode {
	case watchpb.ResultPayload_RESULT_PAYLOAD_OMIT:
		return nil, nil
	case watchpb.ResultPayload_RESULT_PAYLOAD_APPEND:
		encoded, err := protojson.Marshal(change)
		if err != nil {
			return nil, err
		}
		return &commonpb.Payload{
			Metadata: map[string][]byte{"encoding": []byte("json/plain")},
			Data:     encoded,
		}, nil
	default:
		return nil, serviceerror.NewInvalidArgument("unsupported watch result payload mode")
	}
}

func resolveSignalTarget(
	ctx context.Context,
	client workflowservice.WorkflowServiceClient,
	namespace string,
	signal *watchpb.WorkflowSignal,
) (*commonpb.WorkflowExecution, error) {
	execution := common.CloneProto(signal.GetRun())
	if workflowID := signal.GetCurrentWorkflowId(); workflowID != "" {
		response, err := client.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: namespace,
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		})
		if err != nil {
			return nil, err
		}
		execution = common.CloneProto(response.GetWorkflowExecutionInfo().GetExecution())
		if execution.GetWorkflowId() != workflowID {
			return nil, serviceerror.NewInternal("resolved watch target does not match the requested workflow")
		}
	}
	if execution.GetRunId() == "" {
		return nil, serviceerror.NewFailedPrecondition("watch signal target has no resolved workflow run")
	}
	return execution, nil
}

func appendResult(input *commonpb.Payloads, result *commonpb.Payload) *commonpb.Payloads {
	cloned := common.CloneProto(input)
	if result == nil {
		return cloned
	}
	if cloned == nil {
		cloned = &commonpb.Payloads{}
	}
	cloned.Payloads = append(cloned.Payloads, common.CloneProto(result))
	return cloned
}
