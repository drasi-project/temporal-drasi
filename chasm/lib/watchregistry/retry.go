package watchregistry

import (
	"bytes"
	"context"
	"slices"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/watch"
	watchpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (c *Component) retryAuthorization(input mutationInput, record *registrypb.Record) (*registrypb.MutationResult, error) {
	if input.request.RetryIncarnation == "" {
		return &registrypb.MutationResult{Rejection: &registrypb.Rejection{
			Code: registrypb.REJECTION_CODE_NOT_FOUND, Message: "watch delivery is no longer retained",
		}}, nil
	}
	for _, revision := range c.Revisions {
		if revision.GetWatch().GetId() != input.info.public.GetId() || revision.Incarnation != input.request.RetryIncarnation {
			continue
		}
		id, err := watch.ExecutionID(input.request.NamespaceId, input.info.public.GetId(), input.request.RetryIncarnation)
		if err != nil {
			return nil, err
		}
		return &registrypb.MutationResult{Record: record, RetryExecutionId: id}, nil
	}
	return nil, serviceerror.NewInvalidArgument("delivery retry does not identify a retained Watch lifetime")
}

func (c *Component) retryRecord(m mutation) (*registrypb.Record, error) {
	previous, exists := c.Current[resourceKey(registrypb.RESOURCE_KIND_WATCH, m.public.GetId())]
	if !exists {
		return nil, serviceerror.NewNotFound("watch does not exist")
	}
	if m.public.(*contractpb.RetryDeliveryRequest).ExpectedRevision != previous.GetWatch().GetRevision() {
		return nil, serviceerror.NewFailedPrecondition("resource revision changed")
	}
	return common.CloneProto(previous), nil
}

func (c *Component) readDeliveryRetry(input readInput) (*registrypb.ReadResponse, error) {
	fingerprint, err := mutationFingerprint(&registrypb.MutateRequest{
		NamespaceId: input.namespaceID, ExecutionId: BusinessID,
		Operation: &registrypb.MutateRequest_RetryDelivery{RetryDelivery: input.retry},
	})
	if err != nil {
		return nil, err
	}
	receipt, ok := c.Receipts[input.retry.RequestId]
	if !ok {
		return &registrypb.ReadResponse{}, nil
	}
	if !bytes.Equal(receipt.Fingerprint, fingerprint[:]) {
		return nil, serviceerror.NewInvalidArgument("request_id was used with a different method or content")
	}
	return &registrypb.ReadResponse{DeliveryRetry: common.CloneProto(receipt.Result)}, nil
}

func (h *frontendHandler) RetryDelivery(
	ctx context.Context,
	request *contractpb.RetryDeliveryRequest,
) (*contractpb.Delivery, error) {
	if err := validation.ValidateRequest(request); err != nil {
		return nil, err
	}
	id, err := h.namespaces.GetNamespaceID(namespace.Name(request.Namespace))
	if err != nil {
		return nil, err
	}
	read, err := h.client.Read(ctx, &registrypb.ReadRequest{
		NamespaceId: id.String(), ExecutionId: BusinessID,
		Operation: &registrypb.ReadRequest_DeliveryRetry{DeliveryRetry: request},
	})
	if err != nil && serviceerror.ToStatus(err).Code() != codes.NotFound {
		return nil, err
	}
	if err == nil && read == nil {
		return nil, serviceerror.NewDataLoss("registry returned no retry lookup")
	}
	authorization := read.GetDeliveryRetry()
	if authorization == nil {
		incarnation, err := h.findDeliveryLifetime(ctx, id.String(), request)
		if err != nil {
			return nil, err
		}
		authorization, err = h.client.Mutate(ctx, &registrypb.MutateRequest{
			NamespaceId: id.String(), ExecutionId: BusinessID, RetryIncarnation: incarnation,
			Operation: &registrypb.MutateRequest_RetryDelivery{RetryDelivery: request},
		})
		if err != nil {
			return nil, err
		}
	}
	if authorization == nil {
		return nil, serviceerror.NewDataLoss("registry returned no retry authorization")
	}
	if rejection := authorization.Rejection; rejection != nil {
		return nil, serviceerror.FromStatus(status.New(codes.Code(rejection.Code), rejection.Message))
	}
	if authorization.RetryExecutionId == "" {
		return nil, serviceerror.NewDataLoss("registry returned an unbound retry authorization")
	}
	response, err := h.control.RetryDelivery(ctx, &watchpb.RetryDeliveryRequest{
		NamespaceId: id.String(), ExecutionId: authorization.RetryExecutionId, Command: request,
	})
	if err != nil {
		return nil, err
	}
	if response.GetDelivery().GetActionId() != request.ActionId ||
		response.GetDelivery().GetWatch().GetId() != request.Id {
		return nil, serviceerror.NewDataLoss("control returned an invalid retry receipt")
	}
	return response.Delivery, nil
}

func (h *frontendHandler) findDeliveryLifetime(
	ctx context.Context,
	namespaceID string,
	request *contractpb.RetryDeliveryRequest,
) (string, error) {
	lookup := &registrypb.WatchHistoryLookup{Id: request.Id, Limit: 128}
	for range MaxMutations {
		history, err := h.client.Read(ctx, &registrypb.ReadRequest{
			NamespaceId: namespaceID, ExecutionId: BusinessID,
			Operation: &registrypb.ReadRequest_WatchHistory{WatchHistory: lookup},
		})
		if serviceerror.ToStatus(err).Code() == codes.NotFound {
			return "", nil
		}
		if err != nil {
			return "", err
		}
		if history == nil || history.Generation == 0 || len(history.WatchLifetimes) == 0 {
			return "", serviceerror.NewDataLoss("registry returned incomplete Watch retry history")
		}
		for _, lifetime := range history.WatchLifetimes {
			if lifetime == nil || lifetime.Incarnation == "" || lifetime.FirstRevision <= lookup.AfterRevision {
				return "", serviceerror.NewDataLoss("registry returned unordered Watch retry history")
			}
			deliveries, err := h.lifetimeDeliveries(ctx, namespaceID, &contractpb.ListDeliveriesRequest{
				Namespace: request.Namespace, Id: request.Id,
			}, lifetime)
			if err != nil {
				return "", err
			}
			if slices.ContainsFunc(deliveries, func(delivery *contractpb.Delivery) bool { return delivery.ActionId == request.ActionId }) {
				return lifetime.Incarnation, nil
			}
			lookup.AfterRevision = lifetime.FirstRevision
		}
		if !history.MoreLifetimes {
			return "", nil
		}
	}
	return "", exhausted("Watch lifetime lookup exceeds the retained registry limit")
}
