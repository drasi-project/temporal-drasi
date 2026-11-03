package watch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

type NativeBinding struct {
	Target       IntakeTarget
	Subscription native.Subscription
}

func (binding NativeBinding) decode(event native.Event) (QueryOutput, error) {
	query := binding.Target.Watch.GetSpec().GetQuery()
	if binding.Subscription.ID == "" || binding.Subscription.QueryID == "" || binding.Subscription.QueryRevision == 0 ||
		query.GetId() == "" || query.GetRevision() == 0 {
		return QueryOutput{}, serviceerror.NewInvalidArgument("native watch binding is incomplete")
	}
	if event.SubscriptionID != binding.Subscription.ID || event.QueryID != binding.Subscription.QueryID ||
		event.QueryRevision != binding.Subscription.QueryRevision {
		return QueryOutput{}, serviceerror.NewFailedPrecondition("native output does not match the retained watch binding")
	}
	output, err := DecodeNativeOutput(event)
	if err != nil {
		return QueryOutput{}, err
	}
	output.QueryID = query.Id
	output.Revision = query.Revision
	return output, nil
}

func DecodeNativeOutput(event native.Event) (QueryOutput, error) {
	output := QueryOutput{
		QueryID: event.QueryID, Revision: event.QueryRevision,
		Incarnation: event.Incarnation, Sequence: event.Sequence,
	}
	switch event.Kind {
	case native.EventSnapshot:
		if len(event.Changes) != 0 {
			return QueryOutput{}, serviceerror.NewDataLoss("native query snapshot contains delta changes")
		}
		output.Initial = true
		for _, row := range event.Rows {
			value, err := decodeNativeRow(row.Value)
			if err != nil {
				return QueryOutput{}, err
			}
			output.Changes = append(output.Changes, RowChange{
				Identity: row.Key, Trigger: contractpb.Trigger_TRIGGER_ENTER, After: value,
			})
		}
	case native.EventDelta:
		if len(event.Rows) != 0 {
			return QueryOutput{}, serviceerror.NewDataLoss("native query delta contains snapshot rows")
		}
		for _, diff := range event.Changes {
			row, err := decodeNativeDiff(diff)
			if err != nil {
				return QueryOutput{}, err
			}
			output.Changes = append(output.Changes, row)
		}
	default:
		return QueryOutput{}, serviceerror.NewDataLoss("native query output kind is unsupported")
	}
	return output, nil
}

func decodeNativeRow(data json.RawMessage) (*structpb.Struct, error) {
	var row structpb.Struct
	if err := protojson.Unmarshal(data, &row); err != nil {
		return nil, serviceerror.NewDataLoss("native query output row is not a valid JSON object")
	}
	return &row, nil
}

func decodeNativeDiff(diff native.Diff) (RowChange, error) {
	row := RowChange{Identity: strconv.FormatUint(diff.RowSignature, 10)}
	var err error
	switch diff.Type {
	case "ADD":
		row.Trigger = contractpb.Trigger_TRIGGER_ENTER
		row.After, err = decodeNativeRow(diff.Data)
	case "DELETE":
		row.Trigger = contractpb.Trigger_TRIGGER_LEAVE
		row.Before, err = decodeNativeRow(diff.Data)
	case "UPDATE":
		row.Trigger = contractpb.Trigger_TRIGGER_UPDATE
		row.Before, err = decodeNativeRow(diff.Before)
		if err == nil {
			row.After, err = decodeNativeRow(diff.After)
		}
	default:
		return RowChange{}, serviceerror.NewDataLoss("native query result change type is unsupported")
	}
	return row, err
}

// ConsumeNative never acknowledges an output whose acceptance was not confirmed.
func (i *Intake) ConsumeNative(
	ctx context.Context,
	runtime *native.Runtime,
	binding NativeBinding,
	event native.Event,
) (*IntakeResult, error) {
	output, err := binding.decode(event)
	if err != nil {
		return nil, err
	}
	result, err := i.Accept(ctx, binding.Target, output)
	if err != nil {
		return nil, err
	}
	if result.Terminal {
		_, err = runtime.Apply(ctx, native.UnsubscribeCommand{SubscriptionID: event.SubscriptionID})
		return result, err
	}
	if result.Complete {
		identity, err := json.Marshal(event.Receipt(""))
		if err != nil {
			return nil, fmt.Errorf("encode native acceptance identity: %w", err)
		}
		digest := sha256.Sum256(identity)
		if err := runtime.Ack(ctx, event.Receipt(hex.EncodeToString(digest[:]))); err != nil {
			return nil, err
		}
	}
	return result, nil
}
