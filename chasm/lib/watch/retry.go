package watch

import (
	"bytes"
	"context"
	"crypto/sha256"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	watchpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	"go.temporal.io/server/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (h *handler) RetryDelivery(ctx context.Context, request *watchpb.RetryDeliveryRequest) (*watchpb.RetryDeliveryResponse, error) {
	ns, ref, err := h.resolve(request)
	if err != nil {
		return nil, err
	}
	if err := validation.ValidateRequest(request.Command); err != nil {
		return nil, err
	}
	if request.Command.Namespace != ns.Name().String() {
		return nil, serviceerror.NewInvalidArgument("delivery retry namespace does not match the authoritative namespace")
	}
	receipt, _, err := chasm.UpdateComponent(ctx, ref, (*Component).retryDelivery, request.Command)
	if err != nil {
		return nil, err
	}
	if receipt.RejectionCode != watchpb.RETRY_REJECTION_CODE_UNSPECIFIED {
		return nil, serviceerror.FromStatus(status.New(codes.Code(receipt.RejectionCode), receipt.RejectionMessage))
	}
	return &watchpb.RetryDeliveryResponse{Delivery: receipt.Delivery}, nil
}

func (c *Component) retryDelivery(
	ctx chasm.MutableContext,
	request *contractpb.RetryDeliveryRequest,
) (*watchpb.RetryReceipt, error) {
	if request.Namespace != c.Namespace || request.Id != c.WatchId {
		return nil, serviceerror.NewInvalidArgument("delivery retry does not match its Watch execution")
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return nil, err
	}
	fingerprint := sha256.Sum256(encoded)
	if receipt, ok := c.RetryReceipts[request.RequestId]; ok {
		if !bytes.Equal(receipt.Fingerprint, fingerprint[:]) {
			return nil, serviceerror.NewInvalidArgument("request_id was used with different retry content")
		}
		return common.CloneProto(receipt), nil
	}
	if len(c.RetryReceipts) >= 1024 {
		return nil, retryExhausted()
	}
	receipt := &watchpb.RetryReceipt{Fingerprint: fingerprint[:]}
	intent, err := c.retryIntent(ctx, request.ActionId, request.AcknowledgeAmbiguous)
	if err != nil {
		switch serviceerror.ToStatus(err).Code() {
		case codes.NotFound:
			receipt.RejectionCode = watchpb.RETRY_REJECTION_CODE_NOT_FOUND
		case codes.FailedPrecondition:
			receipt.RejectionCode = watchpb.RETRY_REJECTION_CODE_FAILED_PRECONDITION
		default:
			return nil, err
		}
		receipt.RejectionMessage = err.Error()
	} else {
		pending := common.CloneProto(intent)
		pending.Status, pending.Error = watchpb.DELIVERY_STATUS_PENDING, ""
		pending.UpdatedAt = timestamppb.New(ctx.Now(c))
		receipt.Delivery, err = DescribeDelivery(c.State, pending)
		if err != nil {
			return nil, err
		}
	}
	next := common.CloneProto(c.State)
	if next.RetryReceipts == nil {
		next.RetryReceipts = make(map[string]*watchpb.RetryReceipt)
	}
	next.RetryReceipts[request.RequestId] = receipt
	if proto.Size(next) > 1<<20 {
		return nil, retryExhausted()
	}
	if intent != nil {
		c.authorizeRetry(ctx, intent, request.AcknowledgeAmbiguous)
	}
	c.State = next
	return common.CloneProto(receipt), nil
}

func retryExhausted() error {
	return serviceerror.NewResourceExhausted(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
		"Watch retry receipt capacity is exhausted; retained command identities cannot be evicted",
	)
}
