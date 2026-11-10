package watchregistry

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"go.temporal.io/api/serviceerror"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	watchclient "go.temporal.io/server/client/watch"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type pluginObserver interface {
	ListCatalogs(context.Context, *contractpb.ListCatalogsRequest) (*contractpb.ListCatalogsResponse, error)
	ListPlugins(context.Context, *contractpb.ListPluginsRequest) (*contractpb.ListPluginsResponse, error)
	GetPlugin(context.Context, *contractpb.GetPluginRequest) (*contractpb.Plugin, error)
	ObservePlugins(context.Context, uint64, []string) (map[string][]*contractpb.PluginHostStatus, error)
}

type pluginFrontendHandler struct {
	contractpb.UnimplementedPluginServiceServer
	client     registrypb.ClusterPluginServiceClient
	observer   pluginObserver
	authorizer authorization.Authorizer
	logger     log.Logger
}

func newPluginFrontendHandler(
	client registrypb.ClusterPluginServiceClient,
	observer *watchclient.Observer,
	authorizer authorization.Authorizer,
	logger log.Logger,
) *pluginFrontendHandler {
	return &pluginFrontendHandler{client: client, observer: observer, authorizer: authorizer, logger: logger}
}

var pluginMethods = []string{
	contractpb.PluginService_GetCapabilities_FullMethodName,
	contractpb.PluginService_ListCatalogs_FullMethodName,
	contractpb.PluginService_ListPlugins_FullMethodName,
	contractpb.PluginService_GetPlugin_FullMethodName,
	contractpb.PluginService_InstallPlugin_FullMethodName,
	contractpb.PluginService_ListInstallations_FullMethodName,
	contractpb.PluginService_GetInstallation_FullMethodName,
	contractpb.PluginService_LoadPlugin_FullMethodName,
}

func (h *pluginFrontendHandler) GetCapabilities(ctx context.Context, request *contractpb.GetPluginCapabilitiesRequest) (*contractpb.PluginCapabilities, error) {
	if err := validation.ValidateRequest(request); err != nil {
		return nil, err
	}
	result := &contractpb.PluginCapabilities{
		ApiVersion: "v1", MaxRequestBytes: validation.MaxRequestBytes,
		Message: "Plugin management is unavailable",
	}
	methods := pluginMethods[:1]
	if h.observer != nil {
		_, err := h.observer.ListCatalogs(ctx, &contractpb.ListCatalogsRequest{PageSize: 1})
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err == nil {
			result.Enabled = true
			result.Message = "Installation and loading record durable intent; inspect every host for actual verification and loading"
			methods = pluginMethods
		} else if serviceerror.ToStatus(err).Code() == codes.Unavailable {
			h.logger.Warn("Plugin management capabilities unavailable", tag.Error(err))
		} else {
			return nil, err
		}
	}
	var err error
	result.AllowedMethods, err = capabilityMethods(ctx, h.authorizer, h.logger, "", request, methods)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (h *pluginFrontendHandler) ListCatalogs(ctx context.Context, request *contractpb.ListCatalogsRequest) (*contractpb.ListCatalogsResponse, error) {
	if err := validation.ValidateRequest(request); err != nil {
		return nil, err
	}
	return h.observer.ListCatalogs(ctx, request)
}

func (h *pluginFrontendHandler) ListPlugins(ctx context.Context, request *contractpb.ListPluginsRequest) (*contractpb.ListPluginsResponse, error) {
	if err := validation.ValidateRequest(request); err != nil {
		return nil, err
	}
	return h.observer.ListPlugins(ctx, request)
}

func (h *pluginFrontendHandler) GetPlugin(ctx context.Context, request *contractpb.GetPluginRequest) (*contractpb.Plugin, error) {
	if err := validation.ValidateRequest(request); err != nil {
		return nil, err
	}
	return h.observer.GetPlugin(ctx, request)
}

func (h *pluginFrontendHandler) receipt(ctx context.Context, method, requestID string, request proto.Message) (string, bool, error) {
	response, err := h.client.ReadClusterPlugins(ctx, &registrypb.ReadClusterPluginsRequest{
		NamespaceId: primitives.SystemNamespaceID, ExecutionId: ClusterPluginBusinessID,
		Operation: &registrypb.ReadClusterPluginsRequest_RequestId{RequestId: requestID},
	})
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if response == nil || response.Receipt == nil {
		return "", false, serviceerror.NewDataLoss("cluster plugin receipt is missing")
	}
	fingerprint, err := clusterPluginFingerprint(method, request)
	if err != nil {
		return "", false, err
	}
	if !bytes.Equal(response.Receipt.Fingerprint, fingerprint) {
		return "", true, serviceerror.NewInvalidArgument("request_id was used with a different method or content")
	}
	switch outcome := response.Receipt.Outcome.(type) {
	case *registrypb.ClusterPluginReceipt_Digest:
		if err := validation.ValidateRequest(&contractpb.GetInstallationRequest{Digest: outcome.Digest}); err != nil {
			return "", true, serviceerror.NewDataLoss("cluster plugin receipt has an invalid artifact digest")
		}
		return outcome.Digest, true, nil
	case *registrypb.ClusterPluginReceipt_Rejection:
		return "", true, publicPluginRejection(outcome.Rejection)
	default:
		return "", true, serviceerror.NewDataLoss("cluster plugin receipt has no outcome")
	}
}

func publicPluginRejection(rejection *registrypb.Rejection) error {
	if err := validateClusterPluginRejection(rejection); err != nil {
		return serviceerror.NewDataLoss("cluster plugin receipt contains an invalid rejection")
	}
	return serviceerror.FromStatus(status.New(codes.Code(rejection.Code), rejection.Message))
}

func (h *pluginFrontendHandler) InstallPlugin(ctx context.Context, request *contractpb.InstallPluginRequest) (*contractpb.PluginInstallation, error) {
	if err := validation.ValidateRequest(request); err != nil {
		return nil, err
	}
	request = common.CloneProto(request)
	digest, replay, err := h.receipt(ctx, "InstallPlugin", request.RequestId, request)
	if err != nil {
		return nil, err
	}
	if replay {
		return h.GetInstallation(ctx, &contractpb.GetInstallationRequest{Digest: digest})
	}
	artifact, resolveErr := h.observer.GetPlugin(ctx, &contractpb.GetPluginRequest{
		CatalogId: request.CatalogId, PluginId: request.PluginId, Version: request.Version,
	})
	if resolveErr == nil && !sameClusterPluginSelection(request, artifact) {
		resolveErr = serviceerror.NewFailedPrecondition("plugin catalog release does not match the requested immutable digest")
	}
	private := &registrypb.InstallClusterPluginRequest{
		NamespaceId: primitives.SystemNamespaceID, ExecutionId: ClusterPluginBusinessID, Request: request,
		Resolution: &registrypb.InstallClusterPluginRequest_Plugin{Plugin: artifact},
	}
	if resolveErr != nil {
		code := serviceerror.ToStatus(resolveErr).Code()
		switch code {
		case codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.ResourceExhausted, codes.FailedPrecondition:
			private.Resolution = &registrypb.InstallClusterPluginRequest_Rejection{Rejection: &registrypb.Rejection{
				Code: registrypb.RejectionCode(code), Message: resolveErr.Error(),
			}}
		default:
			return nil, resolveErr
		}
	}
	response, err := h.client.InstallClusterPlugin(ctx, private)
	if err != nil {
		return nil, err
	}
	if rejection := response.GetRejection(); rejection != nil {
		return nil, publicPluginRejection(rejection)
	}
	if !sameClusterPluginSelection(request, response.GetPlugin()) {
		return nil, serviceerror.NewDataLoss("cluster plugin installation returned a different immutable selection")
	}
	return h.GetInstallation(ctx, &contractpb.GetInstallationRequest{Digest: request.Digest})
}

func (h *pluginFrontendHandler) LoadPlugin(ctx context.Context, request *contractpb.LoadPluginRequest) (*contractpb.PluginInstallation, error) {
	if err := validation.ValidateRequest(request); err != nil {
		return nil, err
	}
	request = common.CloneProto(request)
	digest, replay, err := h.receipt(ctx, "LoadPlugin", request.RequestId, request)
	if err != nil {
		return nil, err
	}
	if !replay {
		response, err := h.client.LoadClusterPlugin(ctx, &registrypb.LoadClusterPluginRequest{
			NamespaceId: primitives.SystemNamespaceID, ExecutionId: ClusterPluginBusinessID, Request: request,
		})
		if err != nil {
			return nil, err
		}
		if rejection := response.GetRejection(); rejection != nil {
			return nil, publicPluginRejection(rejection)
		}
		digest = response.GetDigest()
	}
	if digest != request.Digest {
		return nil, serviceerror.NewDataLoss("cluster plugin load returned a different immutable digest")
	}
	return h.GetInstallation(ctx, &contractpb.GetInstallationRequest{Digest: digest})
}

func (h *pluginFrontendHandler) snapshot(ctx context.Context) (*registrypb.ReadClusterPluginsResponse, error) {
	response, err := h.client.ReadClusterPlugins(ctx, &registrypb.ReadClusterPluginsRequest{
		NamespaceId: primitives.SystemNamespaceID, ExecutionId: ClusterPluginBusinessID,
		Operation: &registrypb.ReadClusterPluginsRequest_Snapshot{Snapshot: true},
	})
	if err != nil {
		return nil, err
	}
	if err := ValidateClusterPluginSnapshot(response); err != nil {
		return nil, err
	}
	return common.CloneProto(response), nil
}

func (h *pluginFrontendHandler) installations(
	ctx context.Context,
	snapshot *registrypb.ReadClusterPluginsResponse,
	plugins []*contractpb.Plugin,
) ([]*contractpb.PluginInstallation, error) {
	if len(plugins) == 0 {
		return nil, nil
	}
	digests := make([]string, len(plugins))
	for i, plugin := range plugins {
		digests[i] = plugin.Digest
	}
	observations, err := h.observer.ObservePlugins(ctx, snapshot.Generation, digests)
	if err != nil {
		return nil, err
	}
	result := make([]*contractpb.PluginInstallation, len(plugins))
	for i, plugin := range plugins {
		hosts := observations[plugin.Digest]
		if len(hosts) == 0 {
			return nil, serviceerror.NewUnavailable("no Watch member is available to observe plugin installation")
		}
		if len(hosts) > watchclient.MaxPluginHosts {
			return nil, serviceerror.NewDataLoss("plugin installation observation exceeds its host limit")
		}
		result[i] = &contractpb.PluginInstallation{
			Plugin: plugin, LoadRequested: snapshot.DesiredLoads[plugin.Id] == plugin.Digest, Hosts: hosts,
		}
	}
	return result, nil
}

func (h *pluginFrontendHandler) GetInstallation(ctx context.Context, request *contractpb.GetInstallationRequest) (*contractpb.PluginInstallation, error) {
	if err := validation.ValidateRequest(request); err != nil {
		return nil, err
	}
	snapshot, err := h.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	for _, plugin := range snapshot.Plugins {
		if plugin.Digest == request.Digest {
			installations, err := h.installations(ctx, snapshot, []*contractpb.Plugin{plugin})
			if err != nil {
				return nil, err
			}
			return installations[0], nil
		}
	}
	return nil, serviceerror.NewNotFound("plugin artifact has no installation intent")
}

func (h *pluginFrontendHandler) ListInstallations(ctx context.Context, request *contractpb.ListInstallationsRequest) (*contractpb.ListInstallationsResponse, error) {
	if err := validation.ValidateRequest(request); err != nil {
		return nil, err
	}
	snapshot, err := h.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	after, err := installationPageAfter(snapshot, request.PageToken)
	if err != nil {
		return nil, err
	}
	limit := int(request.PageSize)
	if limit == 0 {
		limit = 50
	}
	selected := make([]*contractpb.Plugin, 0, min(limit, len(snapshot.Plugins)))
	reserved := 4096
	more := false
	for _, plugin := range snapshot.Plugins {
		if plugin.Digest <= after {
			continue
		}
		reservedBytes := proto.Size(plugin) + watchclient.MaxPluginHosts*watchclient.MaxPluginHostStatusBytes + 32
		if len(selected) == limit || reserved+reservedBytes > validation.MaxRequestBytes {
			more = true
			break
		}
		selected = append(selected, plugin)
		reserved += reservedBytes
	}
	response := &contractpb.ListInstallationsResponse{}
	response.Installations, err = h.installations(ctx, snapshot, selected)
	if err != nil {
		return nil, err
	}
	if more {
		if len(selected) == 0 {
			return nil, exhausted("plugin installation exceeds the page byte limit")
		}
		response.NextPageToken = base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(
			"installations-v1:%d:%s", snapshot.Generation, selected[len(selected)-1].Digest,
		)))
	}
	if proto.Size(response) > validation.MaxRequestBytes {
		return nil, serviceerror.NewDataLoss("plugin installation observations exceed the reserved page byte limit")
	}
	return response, nil
}

func installationPageAfter(snapshot *registrypb.ReadClusterPluginsResponse, token string) (string, error) {
	if token == "" {
		return "", nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != token {
		return "", serviceerror.NewInvalidArgument("invalid plugin installation page token")
	}
	fields := strings.SplitN(string(decoded), ":", 3)
	if len(fields) != 3 || fields[0] != "installations-v1" {
		return "", serviceerror.NewInvalidArgument("invalid plugin installation page token")
	}
	generation, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil || strconv.FormatUint(generation, 10) != fields[1] {
		return "", serviceerror.NewInvalidArgument("invalid plugin installation page token")
	}
	if generation != snapshot.Generation {
		return "", serviceerror.NewFailedPrecondition("plugin intent changed during pagination; restart the list")
	}
	after := fields[2]
	if !slices.ContainsFunc(snapshot.Plugins, func(plugin *contractpb.Plugin) bool { return plugin.Digest == after }) {
		return "", serviceerror.NewInvalidArgument("plugin installation page token has no retained artifact")
	}
	return after, nil
}
