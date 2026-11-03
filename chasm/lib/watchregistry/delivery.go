package watchregistry

import (
	"cmp"
	"context"
	"encoding/base64"
	"slices"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/watch"
	watchpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func (h *frontendHandler) ListDeliveries(
	ctx context.Context,
	request *contractpb.ListDeliveriesRequest,
) (*contractpb.ListDeliveriesResponse, error) {
	if err := validation.ValidateRequest(request); err != nil {
		return nil, err
	}
	id, err := h.namespaces.GetNamespaceID(namespace.Name(request.Namespace))
	if err != nil {
		return nil, err
	}
	cursor, err := deliveryCursor(request, id.String())
	if err != nil {
		return nil, err
	}
	size := int(cmp.Or(request.PageSize, 50))
	response := &contractpb.ListDeliveriesResponse{}
	// Bound fan-out even when most retained lifetimes never acquired a runtime.
	for range 8 {
		lifetime, more, err := h.readDeliveryLifetime(ctx, cursor)
		if err != nil {
			return nil, err
		}
		if lifetime == nil {
			return response, nil
		}
		deliveries, err := h.lifetimeDeliveries(ctx, id.String(), request, lifetime)
		if err != nil {
			return nil, err
		}
		full, err := appendDeliveries(response, cursor, lifetime, deliveries, size)
		if err != nil {
			return nil, err
		}
		if full {
			return deliveryPage(response, cursor)
		}
		cursor.LifetimeRevision = lifetime.FirstRevision
		cursor.WithinLifetime, cursor.Sequence, cursor.Ordinal = false, 0, 0
		if !more {
			return response, nil
		}
		if len(response.Deliveries) == size {
			return deliveryPage(response, cursor)
		}
	}
	return deliveryPage(response, cursor)
}

func appendDeliveries(
	response *contractpb.ListDeliveriesResponse,
	cursor *registrypb.DeliveryCursor,
	lifetime *registrypb.WatchLifetime,
	deliveries []*contractpb.Delivery,
	size int,
) (bool, error) {
	for _, delivery := range deliveries {
		output := delivery.Output
		if cursor.WithinLifetime && (output.Sequence < cursor.Sequence ||
			output.Sequence == cursor.Sequence && output.DiffOrdinal <= cursor.Ordinal) {
			continue
		}
		response.Deliveries = append(response.Deliveries, delivery)
		if len(response.Deliveries) > size || proto.Size(response)+1024 > MaxPageBytes {
			response.Deliveries = response.Deliveries[:len(response.Deliveries)-1]
			if len(response.Deliveries) == 0 {
				return false, exhausted("delivery exceeds the page byte limit")
			}
			return true, nil
		}
		cursor.LifetimeRevision = lifetime.FirstRevision
		cursor.WithinLifetime, cursor.Sequence, cursor.Ordinal = true, output.Sequence, output.DiffOrdinal
	}
	return false, nil
}

func (h *frontendHandler) readDeliveryLifetime(
	ctx context.Context,
	cursor *registrypb.DeliveryCursor,
) (*registrypb.WatchLifetime, bool, error) {
	after := cursor.LifetimeRevision
	if cursor.WithinLifetime {
		after--
	}
	history, err := h.client.Read(ctx, &registrypb.ReadRequest{
		NamespaceId: cursor.NamespaceId, ExecutionId: BusinessID,
		Operation: &registrypb.ReadRequest_WatchHistory{WatchHistory: &registrypb.WatchHistoryLookup{
			Id: cursor.WatchId, AfterRevision: after, Generation: cursor.Generation, Limit: 1,
		}},
	})
	if err != nil {
		return nil, false, err
	}
	if history == nil || history.Generation == 0 || len(history.WatchLifetimes) > 1 ||
		(history.MoreLifetimes && len(history.WatchLifetimes) == 0) {
		return nil, false, serviceerror.NewDataLoss("registry returned invalid Watch history")
	}
	if cursor.Generation != 0 && cursor.Generation != history.Generation {
		return nil, false, serviceerror.NewFailedPrecondition("registry changed during delivery pagination; restart the list")
	}
	cursor.Generation = history.Generation
	if len(history.WatchLifetimes) == 0 {
		if cursor.WithinLifetime {
			return nil, false, serviceerror.NewFailedPrecondition("delivery lifetime is no longer retained; restart the list")
		}
		return nil, false, nil
	}
	lifetime := history.WatchLifetimes[0]
	if lifetime == nil || lifetime.Incarnation == "" || lifetime.FirstRevision <= after ||
		lifetime.LastRevision < lifetime.FirstRevision {
		return nil, false, serviceerror.NewDataLoss("registry returned an invalid Watch lifetime")
	}
	if cursor.WithinLifetime && cursor.LifetimeRevision != lifetime.FirstRevision {
		return nil, false, serviceerror.NewFailedPrecondition("delivery lifetime changed during pagination; restart the list")
	}
	return lifetime, history.MoreLifetimes, nil
}

func deliveryCursor(request *contractpb.ListDeliveriesRequest, namespaceID string) (*registrypb.DeliveryCursor, error) {
	cursor := &registrypb.DeliveryCursor{NamespaceId: namespaceID, WatchId: request.Id}
	if request.PageToken == "" {
		return cursor, nil
	}
	encoded, err := base64.RawURLEncoding.DecodeString(request.PageToken)
	if err != nil || proto.Unmarshal(encoded, cursor) != nil ||
		cursor.NamespaceId != namespaceID || cursor.WatchId != request.Id ||
		cursor.Generation == 0 || cursor.LifetimeRevision == 0 ||
		(!cursor.WithinLifetime && (cursor.Sequence != 0 || cursor.Ordinal != 0)) {
		return nil, serviceerror.NewInvalidArgument("page token does not match this namespace and Watch delivery list")
	}
	return cursor, nil
}

func deliveryPage(response *contractpb.ListDeliveriesResponse, cursor *registrypb.DeliveryCursor) (*contractpb.ListDeliveriesResponse, error) {
	encoded, err := proto.Marshal(cursor)
	if err != nil {
		return nil, err
	}
	response.NextPageToken = base64.RawURLEncoding.EncodeToString(encoded)
	return response, nil
}

func (h *frontendHandler) lifetimeDeliveries(
	ctx context.Context,
	namespaceID string,
	request *contractpb.ListDeliveriesRequest,
	lifetime *registrypb.WatchLifetime,
) ([]*contractpb.Delivery, error) {
	id, err := watch.ExecutionID(namespaceID, request.Id, lifetime.Incarnation)
	if err != nil {
		return nil, err
	}
	control, err := h.control.DescribeExecution(ctx, &watchpb.DescribeExecutionRequest{
		NamespaceId: namespaceID, ExecutionId: id,
	})
	if serviceerror.ToStatus(err).Code() == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	state := control.GetState()
	if state.GetNamespace() != request.Namespace || state.GetWatchId() != request.Id ||
		state.GetCreationRevision() < lifetime.FirstRevision || state.GetCreationRevision() > lifetime.LastRevision {
		return nil, serviceerror.NewDataLoss("Watch control does not match its retained lifetime")
	}
	deliveries := make([]*contractpb.Delivery, 0, len(control.Deliveries))
	for _, intent := range control.Deliveries {
		delivery, err := watch.DescribeDelivery(state, intent)
		if err != nil {
			return nil, err
		}
		deliveries = append(deliveries, delivery)
	}
	slices.SortFunc(deliveries, func(a, b *contractpb.Delivery) int {
		if order := cmp.Compare(a.Output.Sequence, b.Output.Sequence); order != 0 {
			return order
		}
		return cmp.Compare(a.Output.DiffOrdinal, b.Output.DiffOrdinal)
	})
	return deliveries, nil
}
