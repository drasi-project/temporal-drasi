package watchregistry

import (
	"context"
	"errors"

	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type handler struct {
	registrypb.UnimplementedRegistryServiceServer
	namespaces namespace.Registry
	enabled    dynamicconfig.BoolPropertyFnWithNamespaceFilter
	limits     *registrypb.Limits
}

type keyedRequest interface {
	proto.Message
	GetNamespaceId() string
	GetExecutionId() string
}

func newHandler(namespaces namespace.Registry, dc *dynamicconfig.Collection, limits *registrypb.Limits) (*handler, error) {
	if err := validateLimits(limits); err != nil {
		return nil, err
	}
	return &handler{
		namespaces: namespaces, enabled: dynamicconfig.EnableDrasiWatches.Get(dc), limits: limits,
	}, nil
}

func (h *handler) resolve(request keyedRequest) (*namespace.Namespace, chasm.ExecutionKey, error) {
	if request == nil || !request.ProtoReflect().IsValid() || proto.Size(request) > validation.MaxRequestBytes+1024 {
		return nil, chasm.ExecutionKey{}, serviceerror.NewInvalidArgument("registry request is missing or exceeds the size limit")
	}
	id, err := primitives.ValidateUUID(request.GetNamespaceId())
	if err != nil || id == "" || request.GetExecutionId() != BusinessID {
		return nil, chasm.ExecutionKey{}, serviceerror.NewInvalidArgument("registry requires a namespace UUID and the immutable registry execution ID")
	}
	ns, err := h.namespaces.GetNamespaceByID(namespace.ID(id))
	if err != nil {
		return nil, chasm.ExecutionKey{}, err
	}
	return ns, chasm.ExecutionKey{NamespaceID: id, BusinessID: BusinessID}, nil
}

func (h *handler) Mutate(ctx context.Context, request *registrypb.MutateRequest) (*registrypb.MutationResult, error) {
	ns, key, err := h.resolve(request)
	if err != nil {
		return nil, err
	}
	info, err := decodeMutation(request)
	if err != nil {
		return nil, err
	}
	if err := validation.ValidateRequest(info.public); err != nil {
		return nil, err
	}
	if info.public.GetNamespace() != ns.Name().String() {
		return nil, serviceerror.NewInvalidArgument("request namespace does not match the authoritative namespace")
	}
	input := mutationInput{request: request, info: info}
	switch {
	case ns.IsGlobalNamespace():
		input.acceptanceError = "Drasi watches do not support global namespaces"
	case !h.enabled(ns.Name().String()):
		input.acceptanceError = "Drasi watches are disabled for the namespace"
	default:
	}
	result, err := chasm.UpdateWithStartExecution(ctx, key,
		func(_ chasm.MutableContext, _ mutationInput) (*Component, error) {
			return newComponent(ns.Name().String(), h.limits)
		},
		(*Component).apply, input,
		chasm.WithBusinessIDPolicy(chasm.BusinessIDReusePolicyRejectDuplicate, chasm.BusinessIDConflictPolicyUseExisting),
	)
	if err != nil {
		return nil, err
	}
	if rejection := result.UpdateOutput.Rejection; rejection != nil {
		return nil, serviceerror.FromStatus(status.New(codes.Code(rejection.Code), rejection.Message))
	}
	return result.UpdateOutput, nil
}

func (h *handler) Read(ctx context.Context, request *registrypb.ReadRequest) (*registrypb.ReadResponse, error) {
	ns, key, err := h.resolve(request)
	if err != nil {
		return nil, err
	}
	input, err := decodeRead(request, ns.Name().String())
	if err != nil {
		return nil, err
	}
	response, err := chasm.ReadComponent(ctx, chasm.NewComponentRef[*Component](key), (*Component).read, input)
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok && input.list && input.token == "" {
		return &registrypb.ReadResponse{}, nil
	}
	return response, err
}
