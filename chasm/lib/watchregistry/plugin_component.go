package watchregistry

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"maps"
	"math"
	"slices"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// ClusterPluginBusinessID retains cluster plugin request identities across host changes.
const (
	ClusterPluginBusinessID       = "drasi-cluster-plugins-v1"
	MaxClusterPlugins             = 256
	MaxClusterPluginMetadataBytes = 64 << 10
)

type clusterPlugins struct {
	chasm.UnimplementedComponent
	*registrypb.ClusterPluginState
}

var _ chasm.RootComponent = (*clusterPlugins)(nil)

func defaultClusterPluginLimits() *registrypb.ClusterPluginLimits {
	return &registrypb.ClusterPluginLimits{Installations: 128, Requests: 1024, StateBytes: 1 << 20}
}

func newClusterPlugins(limits *registrypb.ClusterPluginLimits) (*clusterPlugins, error) {
	if limits == nil || limits.Installations == 0 || limits.Installations > MaxClusterPlugins ||
		limits.Requests == 0 || limits.Requests > MaxMutations ||
		limits.StateBytes < 4096 || limits.StateBytes > MaxStateBytes {
		return nil, serviceerror.NewInvalidArgument("invalid cluster plugin limits")
	}
	return &clusterPlugins{ClusterPluginState: &registrypb.ClusterPluginState{
		Installations: make(map[string]*contractpb.Plugin),
		Receipts:      make(map[string]*registrypb.ClusterPluginReceipt),
		DesiredLoads:  make(map[string]string),
		Limits:        common.CloneProto(limits),
	}}, nil
}

func (*clusterPlugins) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (*clusterPlugins) ContextMetadata(chasm.Context) map[string]string { return nil }

func (*clusterPlugins) Terminate(chasm.MutableContext, chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, serviceerror.NewFailedPrecondition("cluster plugin intent retains request identities and cannot be terminated")
}

func clusterPluginFingerprint(method string, request proto.Message) ([]byte, error) {
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return nil, err
	}
	fingerprint := sha256.Sum256(append([]byte(method+"\x00"), encoded...))
	return fingerprint[:], nil
}

// ValidateClusterPluginSnapshot rejects incomplete and host-dependent durable metadata.
func ValidateClusterPluginSnapshot(response *registrypb.ReadClusterPluginsResponse) error {
	if response == nil || response.Receipt != nil || proto.Size(response) > MaxStateBytes ||
		len(response.Plugins) > MaxClusterPlugins ||
		response.Generation < uint64(len(response.Plugins)) ||
		(response.Generation != 0 && len(response.Plugins) == 0) {
		return serviceerror.NewDataLoss("cluster plugin snapshot is missing, incomplete or exceeds its limits")
	}
	byDigest := make(map[string]*contractpb.Plugin, len(response.Plugins))
	previous := ""
	for _, plugin := range response.Plugins {
		if plugin == nil || proto.Size(plugin) > MaxClusterPluginMetadataBytes ||
			plugin.Digest <= previous || plugin.Compatible || plugin.Message != "" {
			return serviceerror.NewDataLoss("cluster plugin snapshot contains invalid or host-dependent metadata")
		}
		if err := validation.ValidateRequest(&contractpb.InstallPluginRequest{
			CatalogId: plugin.CatalogId, PluginId: plugin.Id, Version: plugin.Version,
			Digest: plugin.Digest, RequestId: "snapshot",
		}); err != nil {
			return serviceerror.NewDataLoss(fmt.Sprintf("cluster plugin snapshot contains an invalid selection: %s", err))
		}
		for _, descriptor := range plugin.Descriptors {
			if descriptor == nil || descriptor.Supported {
				return serviceerror.NewDataLoss("cluster plugin snapshot contains invalid or host-dependent descriptors")
			}
		}
		byDigest[plugin.Digest] = plugin
		previous = plugin.Digest
	}
	for id, digest := range response.DesiredLoads {
		plugin, ok := byDigest[digest]
		if !ok || plugin.Id != id {
			return serviceerror.NewDataLoss("cluster plugin desired load does not identify a retained installation")
		}
	}
	return nil
}

func (c *clusterPlugins) install(_ chasm.MutableContext, request *registrypb.InstallClusterPluginRequest) (*registrypb.InstallClusterPluginResponse, error) {
	fingerprint, err := clusterPluginFingerprint("InstallPlugin", request.Request)
	if err != nil {
		return nil, err
	}
	requestID := request.Request.RequestId
	if receipt, ok := c.Receipts[requestID]; ok {
		if !bytes.Equal(receipt.Fingerprint, fingerprint) {
			return nil, serviceerror.NewInvalidArgument("request_id was used with a different method or content")
		}
		return c.installResponse(receipt)
	}
	if len(c.Receipts) >= int(c.Limits.Requests) {
		return nil, exhausted("cluster plugin request identity capacity is exhausted")
	}
	next := common.CloneProto(c.ClusterPluginState)
	if next.Installations == nil {
		next.Installations = make(map[string]*contractpb.Plugin)
	}
	receipt := &registrypb.ClusterPluginReceipt{Fingerprint: fingerprint}
	switch resolution := request.Resolution.(type) {
	case *registrypb.InstallClusterPluginRequest_Rejection:
		receipt.Outcome = &registrypb.ClusterPluginReceipt_Rejection{Rejection: common.CloneProto(resolution.Rejection)}
	case *registrypb.InstallClusterPluginRequest_Plugin, nil:
		artifact, err := c.installation(request)
		if err != nil {
			code := serviceerror.ToStatus(err).Code()
			if code != codes.FailedPrecondition && code != codes.ResourceExhausted {
				return nil, err
			}
			receipt.Outcome = &registrypb.ClusterPluginReceipt_Rejection{Rejection: &registrypb.Rejection{
				Code: registrypb.RejectionCode(code), Message: err.Error(),
			}}
		} else {
			if _, ok := c.Installations[artifact.Digest]; !ok {
				next.Generation++
				next.Installations[artifact.Digest] = artifact
			}
			receipt.Outcome = &registrypb.ClusterPluginReceipt_Digest{Digest: artifact.Digest}
		}
	default:
		return nil, serviceerror.NewInvalidArgument("cluster plugin resolution is invalid")
	}
	if err := c.commit(next, requestID, receipt); err != nil {
		return nil, err
	}
	return c.installResponse(receipt)
}

func (c *clusterPlugins) commit(next *registrypb.ClusterPluginState, requestID string, receipt *registrypb.ClusterPluginReceipt) error {
	if next.Receipts == nil {
		next.Receipts = make(map[string]*registrypb.ClusterPluginReceipt)
	}
	next.Receipts[requestID] = receipt
	if proto.Size(next) > int(c.Limits.StateBytes) {
		return exhausted("cluster plugin retained state capacity is exhausted")
	}
	c.ClusterPluginState = next
	return nil
}

func (c *clusterPlugins) load(_ chasm.MutableContext, request *registrypb.LoadClusterPluginRequest) (*registrypb.LoadClusterPluginResponse, error) {
	fingerprint, err := clusterPluginFingerprint("LoadPlugin", request.Request)
	if err != nil {
		return nil, err
	}
	requestID := request.Request.RequestId
	if receipt, ok := c.Receipts[requestID]; ok {
		if !bytes.Equal(receipt.Fingerprint, fingerprint) {
			return nil, serviceerror.NewInvalidArgument("request_id was used with a different method or content")
		}
		return c.loadResponse(receipt)
	}
	if len(c.Receipts) >= int(c.Limits.Requests) {
		return nil, exhausted("cluster plugin request identity capacity is exhausted")
	}
	next := common.CloneProto(c.ClusterPluginState)
	rejection := request.Rejection
	if rejection == nil {
		if err := c.selectLoad(next, request.Request.Digest); err != nil {
			code := serviceerror.ToStatus(err).Code()
			if code != codes.NotFound && code != codes.ResourceExhausted {
				return nil, err
			}
			rejection = &registrypb.Rejection{Code: registrypb.RejectionCode(code), Message: err.Error()}
		}
	}
	receipt := &registrypb.ClusterPluginReceipt{Fingerprint: fingerprint}
	if rejection != nil {
		receipt.Outcome = &registrypb.ClusterPluginReceipt_Rejection{Rejection: common.CloneProto(rejection)}
	} else {
		receipt.Outcome = &registrypb.ClusterPluginReceipt_Digest{Digest: request.Request.Digest}
	}
	if err := c.commit(next, requestID, receipt); err != nil {
		return nil, err
	}
	return c.loadResponse(receipt)
}

func (c *clusterPlugins) selectLoad(next *registrypb.ClusterPluginState, digest string) error {
	artifact, ok := next.Installations[digest]
	if !ok {
		return serviceerror.NewNotFound("cluster plugin artifact has no installation intent")
	}
	if artifact == nil || artifact.Id == "" || artifact.Digest != digest {
		return serviceerror.NewDataLoss("cluster plugin installation has an invalid identity")
	}
	if next.DesiredLoads[artifact.Id] == digest {
		return nil
	}
	if next.Generation == math.MaxUint64 {
		return exhausted("cluster plugin generation is exhausted")
	}
	if next.DesiredLoads == nil {
		next.DesiredLoads = make(map[string]string)
	}
	next.DesiredLoads[artifact.Id] = digest
	next.Generation++
	return nil
}

func (c *clusterPlugins) loadResponse(receipt *registrypb.ClusterPluginReceipt) (*registrypb.LoadClusterPluginResponse, error) {
	switch outcome := receipt.Outcome.(type) {
	case *registrypb.ClusterPluginReceipt_Digest:
		if artifact, ok := c.Installations[outcome.Digest]; !ok || artifact == nil {
			return nil, serviceerror.NewDataLoss("cluster plugin receipt has no retained artifact")
		}
		return &registrypb.LoadClusterPluginResponse{Outcome: &registrypb.LoadClusterPluginResponse_Digest{
			Digest: outcome.Digest,
		}}, nil
	case *registrypb.ClusterPluginReceipt_Rejection:
		return &registrypb.LoadClusterPluginResponse{Outcome: &registrypb.LoadClusterPluginResponse_Rejection{
			Rejection: common.CloneProto(outcome.Rejection),
		}}, nil
	default:
		return nil, serviceerror.NewDataLoss("cluster plugin receipt has no outcome")
	}
}

func (c *clusterPlugins) installation(request *registrypb.InstallClusterPluginRequest) (*contractpb.Plugin, error) {
	if existing, ok := c.Installations[request.Request.Digest]; ok {
		if !sameClusterPluginSelection(request.Request, existing) {
			return nil, serviceerror.NewFailedPrecondition("artifact digest already has a different immutable selection")
		}
		return existing, nil
	}
	artifact, err := clusterPluginArtifact(request.Request, request.GetPlugin())
	if err != nil {
		return nil, err
	}
	if len(c.Installations) >= int(c.Limits.Installations) {
		return nil, exhausted("cluster plugin installation capacity is exhausted")
	}
	if c.Generation == math.MaxUint64 {
		return nil, exhausted("cluster plugin generation is exhausted")
	}
	return artifact, nil
}

func sameClusterPluginSelection(request *contractpb.InstallPluginRequest, artifact *contractpb.Plugin) bool {
	return artifact != nil && request.CatalogId == artifact.CatalogId &&
		request.PluginId == artifact.Id && request.Version == artifact.Version && request.Digest == artifact.Digest
}

func clusterPluginArtifact(request *contractpb.InstallPluginRequest, artifact *contractpb.Plugin) (*contractpb.Plugin, error) {
	if !sameClusterPluginSelection(request, artifact) {
		return nil, serviceerror.NewDataLoss("resolved plugin does not match the requested immutable selection")
	}
	if proto.Size(artifact) > MaxClusterPluginMetadataBytes {
		return nil, exhausted("cluster plugin metadata exceeds its byte limit")
	}
	value := common.CloneProto(artifact)
	value.Compatible, value.Message = false, ""
	for _, descriptor := range value.Descriptors {
		if descriptor == nil {
			return nil, serviceerror.NewDataLoss("resolved plugin contains a missing descriptor")
		}
		descriptor.Supported = false
	}
	return value, nil
}

func (c *clusterPlugins) installResponse(receipt *registrypb.ClusterPluginReceipt) (*registrypb.InstallClusterPluginResponse, error) {
	switch outcome := receipt.Outcome.(type) {
	case *registrypb.ClusterPluginReceipt_Digest:
		artifact, ok := c.Installations[outcome.Digest]
		if !ok || artifact == nil {
			return nil, serviceerror.NewDataLoss("cluster plugin receipt has no retained artifact")
		}
		return &registrypb.InstallClusterPluginResponse{Outcome: &registrypb.InstallClusterPluginResponse_Plugin{
			Plugin: common.CloneProto(artifact),
		}}, nil
	case *registrypb.ClusterPluginReceipt_Rejection:
		return &registrypb.InstallClusterPluginResponse{Outcome: &registrypb.InstallClusterPluginResponse_Rejection{
			Rejection: common.CloneProto(outcome.Rejection),
		}}, nil
	default:
		return nil, serviceerror.NewDataLoss("cluster plugin receipt has no outcome")
	}
}

func (c *clusterPlugins) read(_ chasm.Context, request *registrypb.ReadClusterPluginsRequest) (*registrypb.ReadClusterPluginsResponse, error) {
	response := &registrypb.ReadClusterPluginsResponse{Generation: c.Generation}
	if request.GetRequestId() != "" {
		receipt, ok := c.Receipts[request.GetRequestId()]
		if !ok {
			return nil, serviceerror.NewNotFound("cluster plugin request identity not found")
		}
		response.Receipt = common.CloneProto(receipt)
		return response, nil
	}
	for _, digest := range slices.Sorted(maps.Keys(c.Installations)) {
		response.Plugins = append(response.Plugins, common.CloneProto(c.Installations[digest]))
	}
	response.DesiredLoads = maps.Clone(c.DesiredLoads)
	return response, nil
}
