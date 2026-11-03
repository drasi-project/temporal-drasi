package watch

import (
	"context"
	"math"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	controlpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type (
	Intake struct {
		Control   controlpb.ControlServiceClient
		Workflows workflowservice.WorkflowServiceClient
	}

	IntakeTarget struct {
		NamespaceID string
		ExecutionID string
		OwnerEpoch  uint64
		Watch       *contractpb.Watch
	}

	QueryOutput struct {
		QueryID     string
		Revision    uint64
		Incarnation string
		Sequence    uint64
		Initial     bool
		Changes     []RowChange
	}

	RowChange struct {
		Identity string
		Trigger  contractpb.Trigger
		Before   *structpb.Struct
		After    *structpb.Struct
	}

	IntakeResult struct {
		Cursor *controlpb.Cursor
		// Complete permits acknowledging this entire native output.
		Complete bool
		// Terminal requires closing the subscription, including any unaccepted tail.
		Terminal bool
	}
)

// Accept commits bounded chunks before the caller acknowledges native output.
func (i *Intake) Accept(ctx context.Context, target IntakeTarget, output QueryOutput) (*IntakeResult, error) {
	if err := validateOutput(target, output); err != nil {
		return nil, err
	}
	description, err := i.Control.DescribeExecution(ctx, &controlpb.DescribeExecutionRequest{
		NamespaceId: target.NamespaceID, ExecutionId: target.ExecutionID,
	})
	if err != nil {
		return nil, err
	}
	state := description.GetState()
	if err := target.checkAssignment(state, output.Incarnation); err != nil {
		return nil, err
	}
	result := &IntakeResult{Cursor: state.Cursor, Terminal: terminal(state.Status)}
	start := uint64(0)
	if state.ProgressPhase != controlpb.PROGRESS_PHASE_WAITING_FOR_SNAPSHOT {
		if output.Sequence < state.GetCursor().GetSequence() ||
			(output.Sequence == state.GetCursor().GetSequence() && state.GetCursor().GetComplete()) {
			result.Complete = true
			return result, nil
		}
		if output.Sequence == state.GetCursor().GetSequence() {
			start = state.GetCursor().GetNextOrdinal()
		}
	}
	if result.Terminal {
		return result, nil
	}
	if state.Status != controlpb.STATUS_RUNNING {
		return nil, serviceerror.NewFailedPrecondition("watch is not accepting changes")
	}
	if state.MaxBatch == 0 || start > uint64(len(output.Changes)) {
		return nil, serviceerror.NewInternal("watch intake limits or accepted progress are invalid")
	}
	for {
		request, err := i.chunk(ctx, target, output, start, state)
		if err != nil {
			return nil, err
		}
		response, err := i.Control.AcceptOutput(ctx, request)
		if err != nil {
			return nil, err
		}
		if err := confirmAcceptance(response.GetCursor(), request, uint64(len(output.Changes))); err != nil {
			return nil, err
		}
		result.Cursor = response.Cursor
		result.Complete = result.Cursor.Complete
		result.Terminal = target.Watch.Spec.GetOnce() != nil && len(request.Actions) != 0
		if result.Complete || result.Terminal {
			return result, nil
		}
		start = result.Cursor.NextOrdinal
	}
}

func (target IntakeTarget) checkAssignment(state *controlpb.State, incarnation string) error {
	if state.GetCreationRevision() != target.Watch.Revision || state.GetRevision() == 0 ||
		state.GetOwnerEpoch() != target.OwnerEpoch ||
		state.GetNamespace() != target.Watch.Namespace || state.GetWatchId() != target.Watch.Id ||
		(state.GetIncarnation() != "" && state.Incarnation != incarnation) {
		return serviceerror.NewFailedPrecondition("watch output assignment or definition changed")
	}
	return nil
}

func validateOutput(target IntakeTarget, output QueryOutput) error {
	if target.NamespaceID == "" || target.ExecutionID == "" || target.OwnerEpoch == 0 ||
		target.Watch.GetNamespace() == "" || target.Watch.GetId() == "" || target.Watch.GetRevision() == 0 ||
		output.QueryID != target.Watch.GetSpec().GetQuery().GetId() || output.Revision == 0 ||
		output.Revision != target.Watch.GetSpec().GetQuery().GetRevision() || output.Incarnation == "" ||
		(!output.Initial && output.Sequence == 0) || uint64(len(output.Changes)) > math.MaxUint32 {
		return serviceerror.NewInvalidArgument("query output does not match the watch definition")
	}
	for _, row := range output.Changes {
		if err := row.validate(output.Initial); err != nil {
			return err
		}
	}
	return nil
}

func (row RowChange) validate(initial bool) error {
	if row.Identity == "" || (initial && row.Trigger != contractpb.Trigger_TRIGGER_ENTER) {
		return serviceerror.NewInvalidArgument("query output row identity or initial trigger is invalid")
	}
	switch row.Trigger {
	case contractpb.Trigger_TRIGGER_ENTER:
		if row.After == nil || row.Before != nil {
			return serviceerror.NewInvalidArgument("entered query output requires an after row")
		}
	case contractpb.Trigger_TRIGGER_UPDATE:
		if row.Before == nil || row.After == nil {
			return serviceerror.NewInvalidArgument("updated query output requires before and after rows")
		}
	case contractpb.Trigger_TRIGGER_LEAVE:
		if row.Before == nil || row.After != nil {
			return serviceerror.NewInvalidArgument("left query output requires a before row")
		}
	default:
		return serviceerror.NewInvalidArgument("query output trigger is invalid")
	}
	return nil
}

func confirmAcceptance(cursor *controlpb.Cursor, request *controlpb.AcceptOutputRequest, total uint64) error {
	if cursor == nil || cursor.Sequence != request.Sequence ||
		cursor.NextOrdinal < request.FirstOrdinal+request.ItemCount ||
		cursor.NextOrdinal > total || (cursor.Complete && cursor.NextOrdinal != total) ||
		(!cursor.Complete && cursor.NextOrdinal == request.FirstOrdinal) {
		return serviceerror.NewInternal("watch acceptance did not confirm the submitted output chunk")
	}
	return nil
}

func terminal(status controlpb.Status) bool {
	return status == controlpb.STATUS_SATISFIED ||
		status == controlpb.STATUS_CANCELLED ||
		status == controlpb.STATUS_TIMED_OUT
}

func (i *Intake) chunk(
	ctx context.Context,
	target IntakeTarget,
	output QueryOutput,
	start uint64,
	state *controlpb.State,
) (*controlpb.AcceptOutputRequest, error) {
	request := &controlpb.AcceptOutputRequest{
		NamespaceId: target.NamespaceID, ExecutionId: target.ExecutionID,
		Revision: state.Revision, OwnerEpoch: target.OwnerEpoch,
		Incarnation: output.Incarnation, Sequence: output.Sequence,
		Initial: output.Initial, FirstOrdinal: start,
		Complete: start == uint64(len(output.Changes)),
	}
	include := !output.Initial || target.Watch.Spec.GetOnce() != nil ||
		target.Watch.Spec.InitialResults == contractpb.InitialResults_INITIAL_RESULTS_INCLUDE
	for ordinal := start; ordinal < min(start+uint64(state.MaxBatch), uint64(len(output.Changes))); ordinal++ {
		row := output.Changes[ordinal]
		selected := include && row.Trigger == target.Watch.Spec.Trigger
		if selected {
			action, err := i.prepare(ctx, target.Watch, &contractpb.ResultChange{
				Output: &contractpb.OutputIdentity{
					QueryRevision: output.Revision, Incarnation: output.Incarnation,
					Sequence: output.Sequence, RowIdentity: row.Identity, DiffOrdinal: uint32(ordinal),
				},
				Trigger: row.Trigger, Before: row.Before, After: row.After,
			})
			if err != nil {
				return nil, err
			}
			request.Actions = append(request.Actions, action)
		}
		request.ItemCount++
		request.Complete = ordinal+1 == uint64(len(output.Changes))
		if proto.Size(request) > validation.MaxRequestBytes {
			if selected {
				request.Actions = request.Actions[:len(request.Actions)-1]
			}
			request.ItemCount--
			request.Complete = false
			if request.ItemCount == 0 {
				return nil, serviceerror.NewResourceExhausted(
					enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, "watch action exceeds the control request size limit")
			}
			break
		}
		if selected && target.Watch.Spec.GetOnce() != nil {
			break
		}
	}
	return request, nil
}

func (i *Intake) prepare(
	ctx context.Context,
	watch *contractpb.Watch,
	change *contractpb.ResultChange,
) (*controlpb.PreparedAction, error) {
	action, err := PrepareAction(ctx, i.Workflows, watch, change)
	if err != nil {
		return nil, err
	}
	prepared := &controlpb.PreparedAction{Ordinal: action.Ordinal, RowIdentity: action.RowKey}
	switch request := action.Request.(type) {
	case *workflowservice.StartWorkflowExecutionRequest:
		prepared.Operation = &controlpb.PreparedAction_Start{Start: request}
	case *workflowservice.SignalWorkflowExecutionRequest:
		prepared.Operation = &controlpb.PreparedAction_Signal{Signal: request}
	case *workflowservice.SignalWithStartWorkflowExecutionRequest:
		prepared.Operation = &controlpb.PreparedAction_SignalWithStart{SignalWithStart: request}
	default:
		return nil, serviceerror.NewInternal("watch action has an unsupported workflow request")
	}
	return prepared, nil
}
