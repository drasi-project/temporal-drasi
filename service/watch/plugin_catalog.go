package watch

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/watchregistry"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func pluginPageOffset(token, boot, scope string) (uint32, error) {
	if token == "" {
		return 0, nil
	}
	data, err := base64.RawURLEncoding.Strict().DecodeString(token)
	fields := strings.Split(string(data), ":")
	if err != nil || len(fields) != 4 || fields[0] != "1" {
		return 0, serviceerror.NewInvalidArgument("plugin page token is invalid")
	}
	offset, err := strconv.ParseUint(fields[3], 10, 32)
	if err != nil || offset == 0 || strconv.FormatUint(offset, 10) != fields[3] {
		return 0, serviceerror.NewInvalidArgument("plugin page token offset is invalid")
	}
	if fields[1] != boot || fields[2] != scope {
		return 0, serviceerror.NewFailedPrecondition("plugin catalog host or filters changed; restart the list")
	}
	return uint32(offset), nil
}

func pluginPageToken(boot, scope string, offset uint32) string {
	return base64.RawURLEncoding.EncodeToString(fmt.Appendf(nil, "1:%s:%s:%d", boot, scope, offset))
}

func pluginPageScope(method, catalog, search string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(method+"\x00"+catalog+"\x00"+search)))
}

func (s *Service) pluginOperation(ctx context.Context, command native.Command) (native.Result, error) {
	if err := ctx.Err(); err != nil {
		return native.Result{}, err
	}
	s.mu.Lock()
	controller, closing := s.plugins, s.pluginClosing
	s.mu.Unlock()
	if controller == nil || closing {
		return native.Result{}, serviceerror.NewUnavailable("Watch plugin management runtime is unavailable")
	}
	attempt, err := controller.runtime.Plugin(command)
	if err != nil {
		return native.Result{}, err
	}
	return attempt.Wait(ctx)
}

func (s *Service) pluginRead(ctx context.Context, command native.Command) (native.Result, error) {
	result, err := s.pluginOperation(ctx, command)
	if err == nil {
		return result, nil
	}
	if ctx.Err() != nil {
		return native.Result{}, ctx.Err()
	}
	if serviceerror.ToStatus(err).Code() == codes.Unavailable {
		return native.Result{}, err
	}
	if nativeError, ok := errors.AsType[*native.Error](err); ok {
		switch nativeError.Code {
		case native.CodeInvalidArgument:
			return native.Result{}, serviceerror.NewInvalidArgument("plugin catalog request is invalid")
		case native.CodeNotFound:
			return native.Result{}, serviceerror.NewNotFound("plugin release or catalog is not configured")
		case native.CodeConflict:
			return native.Result{}, serviceerror.NewResourceExhausted(
				enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, "Watch plugin management runtime is busy")
		case native.CodeCancelled, native.CodeDeadline:
			return native.Result{}, serviceerror.NewUnavailable("Watch plugin catalog operation was interrupted")
		case native.CodeGap, native.CodeInternal:
		default:
			return native.Result{}, serviceerror.NewDataLoss("native plugin catalog returned an unknown error code")
		}
	}
	if errors.Is(err, native.ErrClosed) || errors.Is(err, native.ErrDisabled) {
		return native.Result{}, serviceerror.NewUnavailable("Watch plugin management runtime is unavailable")
	}
	s.params.Logger.Error("Watch plugin catalog read failed", tag.Error(err))
	return native.Result{}, serviceerror.NewInternal("Watch plugin catalog read failed")
}

func (s *Service) ListCatalogs(ctx context.Context, request *contractpb.ListCatalogsRequest) (*contractpb.ListCatalogsResponse, error) {
	if err := validation.ValidateRequest(request); err != nil {
		return nil, err
	}
	scope := pluginPageScope("catalogs", "", "")
	offset, err := pluginPageOffset(request.PageToken, s.bootID, scope)
	if err != nil {
		return nil, err
	}
	result, err := s.pluginRead(ctx, native.ListCatalogsCommand{})
	if err != nil {
		return nil, err
	}
	if len(result.Catalogs) > 33 {
		return nil, serviceerror.NewDataLoss("native catalog count exceeds its limit")
	}
	catalogs := make([]*contractpb.Catalog, 0, len(result.Catalogs))
	seen := make(map[string]struct{}, len(result.Catalogs))
	for _, catalog := range result.Catalogs {
		if err := validation.ValidateRequest(&contractpb.ListPluginsRequest{CatalogId: catalog.ID}); err != nil ||
			catalog.ID == "" || !utf8.ValidString(catalog.DisplayName) || len(catalog.DisplayName) > 256 {
			return nil, serviceerror.NewDataLoss("native catalog metadata is invalid")
		}
		if _, ok := seen[catalog.ID]; ok {
			return nil, serviceerror.NewDataLoss("native catalog identities are duplicated")
		}
		seen[catalog.ID] = struct{}{}
		catalogs = append(catalogs, &contractpb.Catalog{Id: catalog.ID, DisplayName: catalog.DisplayName})
	}
	slices.SortFunc(catalogs, func(a, b *contractpb.Catalog) int { return strings.Compare(a.Id, b.Id) })
	if int(offset) > len(catalogs) {
		return nil, serviceerror.NewInvalidArgument("plugin catalog page offset exceeds the catalog")
	}
	end := min(len(catalogs), int(offset)+int(pageSize(request.PageSize)))
	response := &contractpb.ListCatalogsResponse{Catalogs: catalogs[offset:end]}
	if end < len(catalogs) {
		response.NextPageToken = pluginPageToken(s.bootID, scope, uint32(end))
	}
	return response, nil
}

func pageSize(size int32) uint32 {
	if size == 0 {
		return 50
	}
	return uint32(size)
}

func (s *Service) ListPlugins(ctx context.Context, request *contractpb.ListPluginsRequest) (*contractpb.ListPluginsResponse, error) {
	if err := validation.ValidateRequest(request); err != nil {
		return nil, err
	}
	scope := pluginPageScope("plugins", request.CatalogId, request.Search)
	offset, err := pluginPageOffset(request.PageToken, s.bootID, scope)
	if err != nil {
		return nil, err
	}
	// Select the bounded page before fetching any registry metadata.
	limit := min(pageSize(request.PageSize), 16)
	result, err := s.pluginRead(ctx, native.DiscoverPluginsCommand{
		CatalogID: request.CatalogId, Search: request.Search, Offset: offset, Limit: limit,
	})
	if err != nil {
		return nil, err
	}
	response, more, err := projectPluginPage(request, result)
	if err != nil {
		return nil, err
	}
	if more {
		response.NextPageToken = pluginPageToken(s.bootID, scope, offset+uint32(len(response.Plugins)))
	}
	return response, nil
}

func projectPluginPage(request *contractpb.ListPluginsRequest, result native.Result) (*contractpb.ListPluginsResponse, bool, error) {
	limit := min(pageSize(request.PageSize), 16)
	if len(result.DiscoveredPlugins) > int(limit) || result.MorePlugins && len(result.DiscoveredPlugins) != int(limit) {
		return nil, false, serviceerror.NewDataLoss("native plugin page has an invalid size")
	}
	response := &contractpb.ListPluginsResponse{}
	previous := ""
	for _, discovered := range result.DiscoveredPlugins {
		plugin, err := publicDiscoveredPlugin(discovered)
		if err != nil {
			return nil, false, err
		}
		key := plugin.CatalogId + "\x00" + plugin.Id + "\x00" + plugin.Version
		if key <= previous {
			return nil, false, serviceerror.NewDataLoss("native plugin page contains duplicated or unordered selections")
		}
		previous = key
		if request.CatalogId != "" && plugin.CatalogId != request.CatalogId || !strings.Contains(plugin.Id, request.Search) {
			return nil, false, serviceerror.NewDataLoss("native plugin page does not match the requested filters")
		}
		response.Plugins = append(response.Plugins, plugin)
		if proto.Size(response) > validation.MaxRequestBytes-4096 {
			response.Plugins = response.Plugins[:len(response.Plugins)-1]
			break
		}
	}
	return response, len(response.Plugins) < len(result.DiscoveredPlugins) || result.MorePlugins, nil
}

func (s *Service) GetPlugin(ctx context.Context, request *contractpb.GetPluginRequest) (*contractpb.Plugin, error) {
	if err := validation.ValidateRequest(request); err != nil {
		return nil, err
	}
	result, err := s.pluginRead(ctx, native.InspectPluginCommand{Selection: native.PluginSelection{
		CatalogID: request.CatalogId, PluginID: request.PluginId, Version: request.Version,
	}})
	if err != nil {
		return nil, err
	}
	if result.Plugin == nil || result.Plugin.CatalogID != request.CatalogId ||
		result.Plugin.ID != request.PluginId || result.Plugin.Version != request.Version {
		return nil, serviceerror.NewDataLoss("native plugin inspection does not match the selected release")
	}
	return publicDiscoveredPlugin(*result.Plugin)
}

func publicDiscoveredPlugin(discovered native.DiscoveredPlugin) (*contractpb.Plugin, error) {
	if err := validation.ValidateRequest(&contractpb.InstallPluginRequest{
		CatalogId: discovered.CatalogID, PluginId: discovered.ID, Version: discovered.Version,
		Digest: discovered.Digest, RequestId: "metadata-validation",
	}); err != nil {
		return nil, serviceerror.NewDataLoss("native plugin metadata has an invalid artifact identity")
	}
	plugin := &contractpb.Plugin{
		CatalogId: discovered.CatalogID, Id: discovered.ID, Version: discovered.Version, Digest: discovered.Digest,
		Platform: discovered.Platform, AbiVersion: discovered.ABIVersion, SdkVersion: discovered.SDKVersion,
		CoreVersion: discovered.CoreVersion, Compatible: discovered.Compatible, Message: discovered.Message,
	}
	if len(discovered.Descriptors) > 128 {
		return nil, serviceerror.NewDataLoss("native plugin descriptor count exceeds its limit")
	}
	seen := make(map[string]struct{}, len(discovered.Descriptors))
	for _, descriptor := range discovered.Descriptors {
		category := pluginCategory(descriptor.Category)
		if descriptor.Plugin != plugin.Id || category == contractpb.PluginCategory_PLUGIN_CATEGORY_UNSPECIFIED {
			return nil, serviceerror.NewDataLoss("native plugin descriptor has an invalid identity or category")
		}
		if err := validation.ValidateRequest(&contractpb.GetPluginRequest{
			CatalogId: plugin.CatalogId, PluginId: descriptor.Component, Version: plugin.Version,
		}); err != nil {
			return nil, serviceerror.NewDataLoss("native plugin descriptor has an invalid component identity")
		}
		key := string(descriptor.Category) + "\x00" + descriptor.Component
		if _, ok := seen[key]; ok {
			return nil, serviceerror.NewDataLoss("native plugin descriptors are duplicated")
		}
		seen[key] = struct{}{}
		schema, err := publicPluginSchema(descriptor)
		if err != nil {
			return nil, err
		}
		if descriptor.Supported && category != contractpb.PluginCategory_PLUGIN_CATEGORY_SOURCE && category != contractpb.PluginCategory_PLUGIN_CATEGORY_BOOTSTRAP {
			return nil, serviceerror.NewDataLoss("native plugin descriptor claims unsupported materialization")
		}
		plugin.Descriptors = append(plugin.Descriptors, &contractpb.PluginDescriptor{
			Component: descriptor.Component, Category: category, ConfigurationSchema: schema, Supported: descriptor.Supported,
		})
	}
	if proto.Size(plugin) > watchregistry.MaxClusterPluginMetadataBytes {
		return nil, serviceerror.NewResourceExhausted(
			enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, "plugin metadata exceeds its byte limit")
	}
	if _, err := proto.Marshal(plugin); err != nil {
		return nil, serviceerror.NewDataLoss("native plugin metadata contains invalid text")
	}
	return plugin, nil
}

func publicPluginSchema(descriptor native.PluginDescriptor) (*structpb.Struct, error) {
	document := &structpb.Struct{}
	if len(descriptor.ConfigurationSchema) > watchregistry.MaxClusterPluginMetadataBytes ||
		document.UnmarshalJSON(descriptor.ConfigurationSchema) != nil {
		return nil, serviceerror.NewDataLoss("native plugin descriptor schema is invalid or exceeds its byte limit")
	}
	selected, ok := document.Fields[descriptor.SchemaName]
	if descriptor.SchemaName == "" || !ok || selected.GetStructValue() == nil {
		return nil, serviceerror.NewDataLoss("native plugin descriptor schema root is invalid")
	}
	// Keep SDK-local references resolvable beneath the selected public schema.
	schema := &structpb.Struct{Fields: map[string]*structpb.Value{
		"components": structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
			"schemas": structpb.NewStructValue(document),
		}}),
	}}
	for field, value := range selected.GetStructValue().Fields {
		if field == "components" {
			return nil, serviceerror.NewDataLoss("native plugin descriptor schema root conflicts with its references")
		}
		schema.Fields[field] = value
	}
	return schema, nil
}

func pluginCategory(category native.PluginCategory) contractpb.PluginCategory {
	switch category {
	case native.PluginSource:
		return contractpb.PluginCategory_PLUGIN_CATEGORY_SOURCE
	case native.PluginBootstrap:
		return contractpb.PluginCategory_PLUGIN_CATEGORY_BOOTSTRAP
	case native.PluginReaction:
		return contractpb.PluginCategory_PLUGIN_CATEGORY_REACTION
	case native.PluginIdentity:
		return contractpb.PluginCategory_PLUGIN_CATEGORY_IDENTITY
	case native.PluginSecretStore:
		return contractpb.PluginCategory_PLUGIN_CATEGORY_SECRET_STORE
	default:
		return contractpb.PluginCategory_PLUGIN_CATEGORY_UNSPECIFIED
	}
}
