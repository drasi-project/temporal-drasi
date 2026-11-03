package watchassignment

import (
	"context"
	"encoding/hex"
	"path"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/protobuf/proto"
)

type handler struct {
	assignmentpb.UnimplementedAssignmentServiceServer
	namespaces namespace.Registry
	enabled    dynamicconfig.BoolPropertyFnWithNamespaceFilter
}

type keyedRequest interface {
	proto.Message
	GetNamespaceId() string
	GetExecutionId() string
}

func newHandler(namespaces namespace.Registry, dc *dynamicconfig.Collection) *handler {
	return &handler{namespaces: namespaces, enabled: dynamicconfig.EnableDrasiWatches.Get(dc)}
}

func (h *handler) resolve(request keyedRequest) (*namespace.Namespace, chasm.ExecutionKey, error) {
	if request == nil || !request.ProtoReflect().IsValid() || proto.Size(request) > 16<<10 {
		return nil, chasm.ExecutionKey{}, serviceerror.NewInvalidArgument("assignment request is missing or exceeds the size limit")
	}
	id, err := primitives.ParseUUID(request.GetNamespaceId())
	if err != nil || id.String() == "" || id.String() != request.GetNamespaceId() || request.GetExecutionId() != BusinessID {
		return nil, chasm.ExecutionKey{}, serviceerror.NewInvalidArgument("assignment requires a canonical namespace UUID and the immutable assignment execution ID")
	}
	ns, err := h.namespaces.GetNamespaceByID(namespace.ID(id.String()))
	if err != nil {
		return nil, chasm.ExecutionKey{}, err
	}
	return ns, chasm.ExecutionKey{NamespaceID: id.String(), BusinessID: BusinessID}, nil
}

func (h *handler) Begin(ctx context.Context, request *assignmentpb.BeginRequest) (*assignmentpb.State, error) {
	ns, key, err := h.resolve(request)
	if err != nil {
		return nil, err
	}
	for _, id := range []string{request.StorageHostId, request.ProcessBootId} {
		canonical, err := primitives.ParseUUID(id)
		if err != nil || canonical.String() == "" || canonical.String() != id {
			return nil, serviceerror.NewInvalidArgument("storage_host_id and process_boot_id must be canonical UUIDs")
		}
	}
	if request.RequestId == "" || len(request.RequestId) > 128 {
		return nil, serviceerror.NewInvalidArgument("assignment requires a request_id of at most 128 bytes")
	}
	input := beginInput{request: request}
	switch {
	case ns.IsGlobalNamespace():
		input.acceptanceError = "Drasi watches do not support global namespaces"
	case !h.enabled(ns.Name().String()):
		input.acceptanceError = "Drasi watches are disabled for the namespace"
	default:
	}
	result, err := chasm.UpdateWithStartExecution(ctx, key,
		func(chasm.MutableContext, beginInput) (*Component, error) {
			return &Component{State: &assignmentpb.State{}}, nil
		}, (*Component).begin, input,
		chasm.WithBusinessIDPolicy(chasm.BusinessIDReusePolicyRejectDuplicate, chasm.BusinessIDConflictPolicyUseExisting),
	)
	if err != nil {
		return nil, err
	}
	return result.UpdateOutput, nil
}

func (h *handler) Describe(ctx context.Context, request *assignmentpb.DescribeRequest) (*assignmentpb.State, error) {
	_, key, err := h.resolve(request)
	if err != nil {
		return nil, err
	}
	return chasm.ReadComponent(ctx, chasm.NewComponentRef[*Component](key), (*Component).describe, nil)
}

func (h *handler) Seal(ctx context.Context, request *assignmentpb.SealRequest) (*assignmentpb.State, error) {
	_, key, err := h.resolve(request)
	if err != nil {
		return nil, err
	}
	grant, cut := request.Grant, request.Checkpoint
	if grant == nil || grant.NamespaceId != request.NamespaceId || grant.InstanceId != request.NamespaceId || grant.Epoch == 0 {
		return nil, serviceerror.NewInvalidArgument("Seal requires a grant for this namespace and instance")
	}
	if cut == nil || cut.InstanceId != grant.InstanceId || cut.OwnerEpoch != grant.Epoch ||
		len(cut.Directory) > 4096 || cut.Directory == "/" || !path.IsAbs(cut.Directory) || path.Clean(cut.Directory) != cut.Directory {
		return nil, serviceerror.NewInvalidArgument("Seal requires a checkpoint for this grant at a canonical absolute directory")
	}
	digest, err := hex.DecodeString(cut.ManifestSha256)
	if err != nil || len(digest) != 32 || hex.EncodeToString(digest) != cut.ManifestSha256 {
		return nil, serviceerror.NewInvalidArgument("Seal requires the checkpoint manifest SHA256")
	}
	if request.RequestId == "" || len(request.RequestId) > 128 {
		return nil, serviceerror.NewInvalidArgument("assignment requires a request_id of at most 128 bytes")
	}
	state, _, err := chasm.UpdateComponent(ctx, chasm.NewComponentRef[*Component](key), (*Component).seal, request)
	return state, err
}
