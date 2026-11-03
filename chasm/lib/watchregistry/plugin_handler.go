package watchregistry

import (
	"context"
	"errors"
	"unicode/utf8"

	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type clusterPluginHandler struct {
	registrypb.UnimplementedClusterPluginServiceServer
	namespaces namespace.Registry
	limits     *registrypb.ClusterPluginLimits
}

func newClusterPluginHandler(namespaces namespace.Registry) *clusterPluginHandler {
	return &clusterPluginHandler{namespaces: namespaces, limits: defaultClusterPluginLimits()}
}

func (h *clusterPluginHandler) resolve(request keyedRequest) (chasm.ExecutionKey, error) {
	if request == nil || !request.ProtoReflect().IsValid() || proto.Size(request) > 128<<10 {
		return chasm.ExecutionKey{}, serviceerror.NewInvalidArgument("cluster plugin request is missing or exceeds its byte limit")
	}
	if request.GetNamespaceId() != primitives.SystemNamespaceID || request.GetExecutionId() != ClusterPluginBusinessID {
		return chasm.ExecutionKey{}, serviceerror.NewInvalidArgument("cluster plugin intent requires the fixed system namespace and execution ID")
	}
	ns, err := h.namespaces.GetNamespaceByID(namespace.ID(primitives.SystemNamespaceID))
	if err != nil {
		return chasm.ExecutionKey{}, err
	}
	if ns.ID().String() != primitives.SystemNamespaceID || ns.Name().String() != primitives.SystemLocalNamespace || ns.IsGlobalNamespace() {
		return chasm.ExecutionKey{}, serviceerror.NewFailedPrecondition("cluster plugin intent requires the registered local system namespace")
	}
	return chasm.ExecutionKey{NamespaceID: primitives.SystemNamespaceID, BusinessID: ClusterPluginBusinessID}, nil
}

func (h *clusterPluginHandler) InstallClusterPlugin(ctx context.Context, request *registrypb.InstallClusterPluginRequest) (*registrypb.InstallClusterPluginResponse, error) {
	key, err := h.resolve(request)
	if err != nil {
		return nil, err
	}
	if err := validation.ValidateRequest(request.Request); err != nil {
		return nil, err
	}
	if resolution, ok := request.Resolution.(*registrypb.InstallClusterPluginRequest_Rejection); ok {
		if resolution == nil {
			return nil, serviceerror.NewInvalidArgument("cluster plugin resolution requires a semantic rejection")
		}
		if err := validateClusterPluginRejection(resolution.Rejection); err != nil {
			return nil, err
		}
	}
	result, err := chasm.UpdateWithStartExecution(ctx, key,
		func(chasm.MutableContext, *registrypb.InstallClusterPluginRequest) (*clusterPlugins, error) {
			return newClusterPlugins(h.limits)
		}, (*clusterPlugins).install, request,
		chasm.WithBusinessIDPolicy(chasm.BusinessIDReusePolicyRejectDuplicate, chasm.BusinessIDConflictPolicyUseExisting),
	)
	if err != nil {
		return nil, err
	}
	if rejection := result.UpdateOutput.GetRejection(); rejection != nil {
		return nil, serviceerror.FromStatus(status.New(codes.Code(rejection.Code), rejection.Message))
	}
	return result.UpdateOutput, nil
}

func validateClusterPluginRejection(rejection *registrypb.Rejection) error {
	if rejection == nil {
		return serviceerror.NewInvalidArgument("cluster plugin resolution requires a semantic rejection")
	}
	switch rejection.Code {
	case registrypb.REJECTION_CODE_INVALID_ARGUMENT, registrypb.REJECTION_CODE_NOT_FOUND,
		registrypb.REJECTION_CODE_ALREADY_EXISTS, registrypb.REJECTION_CODE_RESOURCE_EXHAUSTED,
		registrypb.REJECTION_CODE_FAILED_PRECONDITION:
	default:
		return serviceerror.NewInvalidArgument("cluster plugin resolution requires a semantic rejection")
	}
	if rejection.Message == "" || len(rejection.Message) > 1024 || !utf8.ValidString(rejection.Message) {
		return serviceerror.NewInvalidArgument("cluster plugin rejection requires a bounded message")
	}
	return nil
}

func (h *clusterPluginHandler) LoadClusterPlugin(ctx context.Context, request *registrypb.LoadClusterPluginRequest) (*registrypb.LoadClusterPluginResponse, error) {
	key, err := h.resolve(request)
	if err != nil {
		return nil, err
	}
	if err := validation.ValidateRequest(request.Request); err != nil {
		return nil, err
	}
	if request.Rejection != nil {
		if err := validateClusterPluginRejection(request.Rejection); err != nil {
			return nil, err
		}
	}
	result, err := chasm.UpdateWithStartExecution(ctx, key,
		func(chasm.MutableContext, *registrypb.LoadClusterPluginRequest) (*clusterPlugins, error) {
			return newClusterPlugins(h.limits)
		}, (*clusterPlugins).load, request,
		chasm.WithBusinessIDPolicy(chasm.BusinessIDReusePolicyRejectDuplicate, chasm.BusinessIDConflictPolicyUseExisting),
	)
	if err != nil {
		return nil, err
	}
	if rejection := result.UpdateOutput.GetRejection(); rejection != nil {
		return nil, serviceerror.FromStatus(status.New(codes.Code(rejection.Code), rejection.Message))
	}
	return result.UpdateOutput, nil
}

func (h *clusterPluginHandler) ReadClusterPlugins(ctx context.Context, request *registrypb.ReadClusterPluginsRequest) (*registrypb.ReadClusterPluginsResponse, error) {
	key, err := h.resolve(request)
	if err != nil {
		return nil, err
	}
	switch operation := request.Operation.(type) {
	case *registrypb.ReadClusterPluginsRequest_Snapshot:
		if !operation.Snapshot {
			return nil, serviceerror.NewInvalidArgument("cluster plugin snapshot selector must be true")
		}
	case *registrypb.ReadClusterPluginsRequest_RequestId:
		if operation.RequestId == "" || len(operation.RequestId) > 128 || !utf8.ValidString(operation.RequestId) {
			return nil, serviceerror.NewInvalidArgument("cluster plugin receipt requires a bounded request_id")
		}
	default:
		return nil, serviceerror.NewInvalidArgument("cluster plugin read selector is required")
	}
	response, err := chasm.ReadComponent(ctx, chasm.NewComponentRef[*clusterPlugins](key), (*clusterPlugins).read, request)
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok && request.GetSnapshot() {
		return &registrypb.ReadClusterPluginsResponse{}, nil
	}
	return response, err
}
