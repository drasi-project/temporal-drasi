package watch

import (
	"context"
	"strings"
	"time"
	"unicode/utf8"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/watchservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func (o *Observer) catalogClient() (watchservice.WatchServiceClient, error) {
	resolver, err := o.membership.GetResolver(primitives.WatchService)
	if err != nil {
		return nil, err
	}
	members, err := pluginMembers(resolver.AvailableMembers())
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, serviceerror.NewUnavailable("no Watch member is available for plugin catalog reads")
	}
	return watchservice.NewWatchServiceClient(o.rpc.CreateWatchGRPCConnection(members[0].address)), nil
}

func (o *Observer) catalogRead(ctx context.Context, timeout time.Duration, call func(context.Context, watchservice.WatchServiceClient) error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case o.pluginSlots <- struct{}{}:
		defer func() { <-o.pluginSlots }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	client, err := o.catalogClient()
	if err != nil {
		return err
	}
	return call(ctx, client)
}

// ListCatalogs reads one management runtime, independently of namespace placement.
func (o *Observer) ListCatalogs(ctx context.Context, request *contractpb.ListCatalogsRequest) (*contractpb.ListCatalogsResponse, error) {
	if err := validation.ValidateRequest(request); err != nil {
		return nil, err
	}
	request = common.CloneProto(request)
	var response *contractpb.ListCatalogsResponse
	err := o.catalogRead(ctx, 2*time.Second, func(ctx context.Context, client watchservice.WatchServiceClient) error {
		var err error
		response, err = client.ListCatalogs(ctx, request,
			grpc.MaxCallRecvMsgSize(validation.MaxRequestBytes), grpc.MaxCallSendMsgSize(validation.MaxRequestBytes))
		return err
	})
	if err != nil {
		return nil, err
	}
	if response == nil || proto.Size(response) > validation.MaxRequestBytes || len(response.Catalogs) > 33 ||
		len(response.Catalogs) > catalogPageSize(request.PageSize) || !validCatalogContinuation(len(response.Catalogs), response.NextPageToken) {
		return nil, serviceerror.NewDataLoss("Watch member returned an invalid catalog page")
	}
	previous := ""
	for _, catalog := range response.Catalogs {
		if catalog.GetId() <= previous || len(catalog.DisplayName) > 256 || !utf8.ValidString(catalog.DisplayName) {
			return nil, serviceerror.NewDataLoss("Watch member returned invalid catalog metadata or ordering")
		}
		if err := validation.ValidateRequest(&contractpb.ListPluginsRequest{CatalogId: catalog.Id}); err != nil {
			return nil, serviceerror.NewDataLoss("Watch member returned an invalid catalog identity")
		}
		previous = catalog.Id
	}
	return response, nil
}

func catalogPageSize(size int32) int {
	if size == 0 {
		return 50
	}
	return int(size)
}

func validCatalogContinuation(size int, token string) bool {
	return len(token) <= 4096 && utf8.ValidString(token) && (size != 0 || token == "")
}

func validCatalogPlugin(plugin *contractpb.Plugin) bool {
	if plugin == nil || proto.Size(plugin) > 64<<10 || plugin.SignerIdentity != "" || plugin.Provenance != "" {
		return false
	}
	if err := validation.ValidateRequest(&contractpb.InstallPluginRequest{
		CatalogId: plugin.CatalogId, PluginId: plugin.Id, Version: plugin.Version,
		Digest: plugin.Digest, RequestId: "metadata-validation",
	}); err != nil {
		return false
	}
	_, err := proto.Marshal(plugin)
	return err == nil
}

func (o *Observer) ListPlugins(ctx context.Context, request *contractpb.ListPluginsRequest) (*contractpb.ListPluginsResponse, error) {
	if err := validation.ValidateRequest(request); err != nil {
		return nil, err
	}
	request = common.CloneProto(request)
	var response *contractpb.ListPluginsResponse
	err := o.catalogRead(ctx, 30*time.Second, func(ctx context.Context, client watchservice.WatchServiceClient) error {
		var err error
		response, err = client.ListPlugins(ctx, request,
			grpc.MaxCallRecvMsgSize(validation.MaxRequestBytes), grpc.MaxCallSendMsgSize(validation.MaxRequestBytes))
		return err
	})
	if err != nil {
		return nil, err
	}
	if response == nil || proto.Size(response) > validation.MaxRequestBytes ||
		len(response.Plugins) > min(catalogPageSize(request.PageSize), 16) ||
		!validCatalogContinuation(len(response.Plugins), response.NextPageToken) {
		return nil, serviceerror.NewDataLoss("Watch member returned an invalid plugin catalog page")
	}
	previous := ""
	for _, plugin := range response.Plugins {
		if !validCatalogPlugin(plugin) || request.CatalogId != "" && plugin.CatalogId != request.CatalogId ||
			!strings.Contains(plugin.Id, request.Search) {
			return nil, serviceerror.NewDataLoss("Watch member returned invalid or unselected plugin metadata")
		}
		key := plugin.CatalogId + "\x00" + plugin.Id + "\x00" + plugin.Version
		if key <= previous {
			return nil, serviceerror.NewDataLoss("Watch member returned duplicated or unordered plugin selections")
		}
		previous = key
	}
	return response, nil
}

func (o *Observer) GetPlugin(ctx context.Context, request *contractpb.GetPluginRequest) (*contractpb.Plugin, error) {
	if err := validation.ValidateRequest(request); err != nil {
		return nil, err
	}
	request = common.CloneProto(request)
	var response *contractpb.Plugin
	err := o.catalogRead(ctx, 30*time.Second, func(ctx context.Context, client watchservice.WatchServiceClient) error {
		var err error
		response, err = client.GetPlugin(ctx, request,
			grpc.MaxCallRecvMsgSize(validation.MaxRequestBytes), grpc.MaxCallSendMsgSize(validation.MaxRequestBytes))
		return err
	})
	if err != nil {
		return nil, err
	}
	if !validCatalogPlugin(response) || response.CatalogId != request.CatalogId ||
		response.Id != request.PluginId || response.Version != request.Version {
		return nil, serviceerror.NewDataLoss("Watch member returned invalid or unselected plugin metadata")
	}
	return response, nil
}
