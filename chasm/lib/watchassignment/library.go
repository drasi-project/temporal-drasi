package watchassignment

import (
	"go.temporal.io/server/chasm"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	"google.golang.org/grpc"
)

type Library struct {
	chasm.UnimplementedLibrary
	handler *handler
}

func (*Library) Name() string { return "drasiassignment" }

func (*Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Component]("namespace"),
	}
}

func (l *Library) RegisterServices(server *grpc.Server) {
	assignmentpb.RegisterAssignmentServiceServer(server, l.handler)
}
