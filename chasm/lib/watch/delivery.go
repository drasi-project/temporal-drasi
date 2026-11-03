package watch

import (
	"cmp"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	watchpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

// DescribeDelivery projects retained metadata without exposing the frozen request.
func DescribeDelivery(state *watchpb.State, intent *watchpb.Intent) (*contractpb.Delivery, error) {
	if state.GetNamespace() == "" || state.GetWatchId() == "" || state.GetCreationRevision() == 0 ||
		intent.GetActionId() == "" || intent.GetWorkflowId() == "" || intent.GetOutput().GetQueryRevision() == 0 ||
		intent.GetOutput().GetIncarnation() == "" || intent.GetOutput().GetRowIdentity() == "" ||
		intent.GetUpdatedAt().CheckValid() != nil {
		return nil, serviceerror.NewDataLoss("retained delivery metadata is incomplete")
	}
	output := intent.Output
	revision, err := queryRevision(state.Definition)
	if err != nil {
		return nil, err
	}
	if output.QueryRevision != revision || output.Incarnation != state.Incarnation || intent.ActionId != ActionID(ActionIdentity{
		Namespace: state.Namespace, WatchID: state.WatchId, Revision: state.CreationRevision,
		Incarnation: output.Incarnation, Sequence: output.Sequence,
		Ordinal: uint64(output.DiffOrdinal), RowKey: output.RowIdentity,
	}) {
		return nil, serviceerror.NewDataLoss("retained delivery output identity is inconsistent")
	}
	result := &contractpb.Delivery{
		ActionId: intent.ActionId,
		Watch:    &contractpb.ResourceRef{Id: state.WatchId, Revision: state.CreationRevision},
		Output:   common.CloneProto(output), Attempts: intent.Attempts,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: intent.WorkflowId, RunId: cmp.Or(intent.RunId, intent.TargetRunId),
		},
		Message: intent.Error, UpdatedAt: common.CloneProto(intent.UpdatedAt),
	}
	switch intent.Status {
	case watchpb.DELIVERY_STATUS_PENDING:
		result.State = contractpb.DeliveryState_DELIVERY_STATE_PENDING
		if intent.Attempts > 1 || intent.Attempts > 0 && !intent.InFlight {
			result.State = contractpb.DeliveryState_DELIVERY_STATE_RETRYING
		}
	case watchpb.DELIVERY_STATUS_DELIVERED:
		result.State = contractpb.DeliveryState_DELIVERY_STATE_DELIVERED
	case watchpb.DELIVERY_STATUS_FAILED:
		result.State = contractpb.DeliveryState_DELIVERY_STATE_BLOCKED
	case watchpb.DELIVERY_STATUS_AMBIGUOUS:
		result.State = contractpb.DeliveryState_DELIVERY_STATE_AMBIGUOUS
	default:
		return nil, serviceerror.NewDataLoss("retained delivery status is invalid")
	}
	return result, nil
}

func queryRevision(definition []byte) (uint64, error) {
	var watch contractpb.Watch
	if err := proto.Unmarshal(definition, &watch); err != nil ||
		watch.GetSpec().GetQuery().GetId() == "" || watch.GetSpec().GetQuery().GetRevision() == 0 {
		return 0, serviceerror.NewDataLoss("persisted watch query identity is invalid")
	}
	return watch.Spec.Query.Revision, nil
}
