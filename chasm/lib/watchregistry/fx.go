package watchregistry

import (
	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"go.temporal.io/server/chasm"
	watchpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	watchclient "go.temporal.io/server/client/watch"
	"go.temporal.io/server/common"
	"go.uber.org/fx"
	"google.golang.org/grpc"
)

// Module installs the default finite limits. Use ModuleWithLimits instead to configure them.
var Module = ModuleWithLimits(DefaultLimits())

var FrontendModule = fx.Module(
	"chasm.lib.watchregistry.frontend",
	fx.Provide(registrypb.NewRegistryServiceLayeredClient),
	fx.Provide(watchpb.NewControlServiceLayeredClient),
	fx.Provide(assignmentpb.NewAssignmentServiceLayeredClient),
	fx.Provide(watchclient.NewObserver),
	fx.Provide(newFrontendHandler),
	fx.Provide(registrypb.NewClusterPluginServiceLayeredClient),
	fx.Provide(newPluginFrontendHandler),
	fx.Provide(func(handler *frontendHandler) contractpb.WatchServiceServer {
		return validation.WrapWatchService(handler)
	}),
	fx.Provide(func(handler *pluginFrontendHandler) contractpb.PluginServiceServer {
		return validation.WrapPluginService(handler)
	}),
	fx.Invoke(func(server *grpc.Server, watches contractpb.WatchServiceServer, plugins contractpb.PluginServiceServer) {
		contractpb.RegisterWatchServiceServer(server, watches)
		contractpb.RegisterPluginServiceServer(server, plugins)
	}),
)

func ModuleWithLimits(limits *registrypb.Limits) fx.Option {
	limits = common.CloneProto(limits)
	return fx.Module(
		"chasm.lib.watchregistry",
		fx.Provide(func() *registrypb.Limits { return common.CloneProto(limits) }),
		fx.Provide(newHandler),
		fx.Provide(newClusterPluginHandler),
		fx.Provide(func(handler *handler, plugins *clusterPluginHandler) *Library {
			return &Library{handler: handler, pluginHandler: plugins}
		}),
		fx.Invoke(func(registry *chasm.Registry, library *Library) error { return registry.Register(library) }),
	)
}
