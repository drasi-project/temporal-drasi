package watch

import (
	"context"
	"slices"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	watchpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/softassert"
	"google.golang.org/protobuf/proto"
)

type handler struct {
	watchpb.UnimplementedControlServiceServer
	namespaces namespace.Registry
	enabled    dynamicconfig.BoolPropertyFnWithNamespaceFilter
	limits     Limits
}

type keyedRequest interface {
	proto.Message
	GetNamespaceId() string
	GetExecutionId() string
}

func newHandler(namespaces namespace.Registry, dc *dynamicconfig.Collection) *handler {
	return &handler{
		namespaces: namespaces,
		enabled:    dynamicconfig.EnableDrasiWatches.Get(dc),
		limits:     Limits{Pending: 128, Batch: 32, Recent: 128, PendingBytes: 1 << 20},
	}
}

func (h *handler) resolve(request keyedRequest) (*namespace.Namespace, chasm.ComponentRef, error) {
	if request == nil || !request.ProtoReflect().IsValid() || proto.Size(request) > validation.MaxRequestBytes {
		return nil, chasm.ComponentRef{}, serviceerror.NewInvalidArgument("watch control request is missing or exceeds the size limit")
	}
	id, err := primitives.ValidateUUID(request.GetNamespaceId())
	if err != nil || id == "" || request.GetExecutionId() == "" || len(request.GetExecutionId()) > 512 {
		return nil, chasm.ComponentRef{}, serviceerror.NewInvalidArgument("watch control requires a namespace UUID and execution ID")
	}
	ns, err := h.namespaces.GetNamespaceByID(namespace.ID(id))
	if err != nil {
		return nil, chasm.ComponentRef{}, err
	}
	ref := chasm.NewComponentRef[*Component](chasm.ExecutionKey{NamespaceID: id, BusinessID: request.GetExecutionId()})
	return ns, ref, nil
}

func (h *handler) checkEnabled(ns *namespace.Namespace) error {
	if ns.IsGlobalNamespace() {
		return serviceerror.NewFailedPrecondition("Drasi watches do not support global namespaces")
	}
	if !h.enabled(ns.Name().String()) {
		return serviceerror.NewFailedPrecondition("Drasi watches are disabled for the namespace")
	}
	return nil
}

func (h *handler) CreateExecution(ctx context.Context, request *watchpb.CreateExecutionRequest) (*watchpb.ExecutionResponse, error) {
	ns, ref, err := h.resolve(request)
	if err != nil {
		return nil, err
	}
	if err := h.checkEnabled(ns); err != nil {
		return nil, err
	}
	input, err := h.createInput(ns, request)
	if err != nil {
		return nil, err
	}
	_, err = chasm.StartExecution(ctx,
		chasm.ExecutionKey{NamespaceID: request.NamespaceId, BusinessID: request.ExecutionId},
		New, input,
		chasm.WithRequestID(request.RequestId),
		chasm.WithBusinessIDPolicy(chasm.BusinessIDReusePolicyRejectDuplicate, chasm.BusinessIDConflictPolicyUseExisting),
	)
	if err != nil {
		return nil, err
	}
	response, err := describeExecution(ctx, ref)
	if err != nil {
		return nil, err
	}
	state := response.State
	var original, retry contractpb.Watch
	if err := proto.Unmarshal(state.Definition, &original); err != nil {
		return nil, serviceerror.NewInternal("persisted watch definition is malformed")
	}
	if err := proto.Unmarshal(request.Definition, &retry); err != nil {
		return nil, serviceerror.NewInternal("validated watch definition is malformed")
	}
	// Lifecycle intent and observed health are not part of the immutable definition.
	original.DesiredState, retry.DesiredState = contractpb.DesiredState_DESIRED_STATE_UNSPECIFIED, contractpb.DesiredState_DESIRED_STATE_UNSPECIFIED
	original.Status, retry.Status = nil, nil
	if state.CreationRevision != request.Revision || state.WatchId != request.WatchId ||
		!proto.Equal(&original, &retry) {
		return nil, serviceerror.NewAlreadyExists("watch execution is bound to another definition")
	}
	return response, nil
}

func (h *handler) createInput(ns *namespace.Namespace, request *watchpb.CreateExecutionRequest) (Create, error) {
	var resource contractpb.Watch
	if err := proto.Unmarshal(request.Definition, &resource); err != nil {
		return Create{}, serviceerror.NewInvalidArgument("watch definition is malformed")
	}
	if request.Revision == 0 || resource.Namespace != ns.Name().String() ||
		resource.Id != request.WatchId || resource.Revision != request.Revision {
		return Create{}, serviceerror.NewInvalidArgument("watch definition does not match its execution identity")
	}
	if err := validation.ValidateRequest(&contractpb.CreateWatchRequest{
		Namespace: resource.Namespace, Id: resource.Id, Spec: resource.Spec, RequestId: request.RequestId,
	}); err != nil {
		return Create{}, err
	}
	once := resource.GetSpec().GetOnce()
	if request.Once != (once != nil) || !proto.Equal(request.Deadline, once.GetDeadline()) {
		return Create{}, serviceerror.NewInvalidArgument("watch lifetime does not match its definition")
	}
	var deadline time.Time
	if request.Deadline != nil {
		if err := request.Deadline.CheckValid(); err != nil {
			return Create{}, serviceerror.NewInvalidArgument("watch deadline is invalid")
		}
		deadline = request.Deadline.AsTime()
	}
	return Create{
		Namespace: resource.Namespace, ID: resource.Id, Revision: resource.Revision, Definition: request.Definition,
		Once: request.Once, Deadline: deadline, Limits: h.limits,
	}, nil
}

func (h *handler) DescribeExecution(ctx context.Context, request *watchpb.DescribeExecutionRequest) (*watchpb.ExecutionResponse, error) {
	_, ref, err := h.resolve(request)
	if err != nil {
		return nil, err
	}
	return describeExecution(ctx, ref)
}

func describeExecution(ctx context.Context, ref chasm.ComponentRef) (*watchpb.ExecutionResponse, error) {
	return chasm.ReadComponent(ctx, ref,
		func(component *Component, ctx chasm.Context, _ chasm.NoValue) (*watchpb.ExecutionResponse, error) {
			response := &watchpb.ExecutionResponse{State: common.CloneProto(component.State)}
			for _, id := range append(slices.Clone(component.PendingIds), component.RecentIds...) {
				field, ok := component.Intents[id]
				if !softassert.That(ctx.Logger(), ok, "watch delivery index references a missing intent") {
					return nil, serviceerror.NewInternal("watch delivery state is inconsistent")
				}
				intent := common.CloneProto(field.Get(ctx))
				intent.Request = nil
				response.Deliveries = append(response.Deliveries, intent)
			}
			return response, nil
		}, nil)
}

func (h *handler) AcceptOutput(ctx context.Context, request *watchpb.AcceptOutputRequest) (*watchpb.AcceptOutputResponse, error) {
	ns, ref, err := h.resolve(request)
	if err != nil {
		return nil, err
	}
	if err := h.checkEnabled(ns); err != nil {
		return nil, err
	}
	if request.OwnerEpoch == 0 || len(request.Actions) > int(h.limits.Batch) {
		return nil, serviceerror.NewInvalidArgument("watch output requires an assignment and a bounded action batch")
	}
	batch := Batch{
		Revision: request.Revision, OwnerEpoch: request.OwnerEpoch, Incarnation: request.Incarnation,
		Sequence: request.Sequence, FirstOrdinal: request.FirstOrdinal, ItemCount: request.ItemCount,
		Complete: request.Complete, Initial: request.Initial,
	}
	for _, action := range request.Actions {
		if action == nil {
			return nil, serviceerror.NewInvalidArgument("watch output action is missing")
		}
		prepared := Action{Ordinal: action.Ordinal, RowKey: action.RowIdentity}
		switch operation := action.Operation.(type) {
		case *watchpb.PreparedAction_Start:
			prepared.Request = operation.Start
		case *watchpb.PreparedAction_Signal:
			prepared.Request = operation.Signal
		case *watchpb.PreparedAction_SignalWithStart:
			prepared.Request = operation.SignalWithStart
		default:
			return nil, serviceerror.NewInvalidArgument("watch output action operation is missing")
		}
		batch.Actions = append(batch.Actions, prepared)
	}
	receipt, _, err := chasm.UpdateComponent(ctx, ref, (*Component).Accept, batch)
	if err != nil {
		return nil, err
	}
	return &watchpb.AcceptOutputResponse{Cursor: receipt.Cursor, ActionIds: receipt.ActionIDs, Duplicate: receipt.Duplicate}, nil
}

func (h *handler) MutateExecution(ctx context.Context, request *watchpb.MutateExecutionRequest) (*watchpb.ExecutionResponse, error) {
	ns, ref, err := h.resolve(request)
	if err != nil {
		return nil, err
	}
	if request.ExpectedRevision == 0 {
		return nil, serviceerror.NewInvalidArgument("watch mutation requires an expected revision")
	}
	if request.GetClaim() != nil {
		if err := h.checkEnabled(ns); err != nil {
			return nil, err
		}
	}
	_, _, err = chasm.UpdateComponent(ctx, ref,
		func(component *Component, ctx chasm.MutableContext, input *watchpb.MutateExecutionRequest) (chasm.NoValue, error) {
			if input.ExpectedRevision != component.Revision {
				return nil, serviceerror.NewFailedPrecondition("watch revision changed")
			}
			if input.OwnerEpoch != 0 && input.OwnerEpoch != component.OwnerEpoch {
				return nil, serviceerror.NewFailedPrecondition("watch runtime assignment changed")
			}
			switch mutation := input.Mutation.(type) {
			case *watchpb.MutateExecutionRequest_Claim:
				if mutation.Claim == nil {
					return nil, serviceerror.NewInvalidArgument("watch assignment is missing")
				}
				return nil, component.Claim(input.ExpectedRevision, mutation.Claim.PreviousEpoch, mutation.Claim.NewEpoch)
			case *watchpb.MutateExecutionRequest_Pause:
				if mutation.Pause == nil {
					return nil, serviceerror.NewInvalidArgument("watch pause attributes are missing")
				}
				return nil, component.Pause(input.ExpectedRevision, mutation.Pause.Paused)
			case *watchpb.MutateExecutionRequest_Cancel:
				return nil, component.Cancel(input.ExpectedRevision)
			default:
				return nil, serviceerror.NewInvalidArgument("watch mutation is missing")
			}
		}, request)
	if err != nil {
		return nil, err
	}
	return describeExecution(ctx, ref)
}
