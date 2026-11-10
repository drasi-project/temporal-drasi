package watchregistry

import (
	"go.temporal.io/server/chasm"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"google.golang.org/grpc"
)

type Library struct {
	chasm.UnimplementedLibrary
	handler       *handler
	pluginHandler *clusterPluginHandler
}

func (*Library) Name() string { return "drasiregistry" }

func (*Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Component]("namespace"),
		chasm.NewRegistrableComponent[*clusterPlugins]("clusterPlugins"),
	}
}

func (l *Library) RegisterServices(server *grpc.Server) {
	registrypb.RegisterRegistryServiceServer(server, l.handler)
	registrypb.RegisterClusterPluginServiceServer(server, l.pluginHandler)
}
