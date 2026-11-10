package watch

import (
	"go.temporal.io/server/chasm"
	watchpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	"google.golang.org/grpc"
)

type Library struct {
	chasm.UnimplementedLibrary
	Dispatch *DispatchHandler
	Deadline *DeadlineHandler
	handler  *handler
}

func (l *Library) RegisterServices(server *grpc.Server) {
	watchpb.RegisterControlServiceServer(server, l.handler)
}

func (*Library) Name() string {
	return "drasi"
}

func (*Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Component]("watch", chasm.WithBusinessIDAlias("WatchId")),
	}
}

func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask("dispatch", l.Dispatch),
		chasm.NewRegistrablePureTask("deadline", l.Deadline),
	}
}
