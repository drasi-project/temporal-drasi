package watch

import (
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"chasm.lib.watch",
	fx.Provide(newHandler),
	fx.Provide(func(client workflowservice.WorkflowServiceClient, handler *handler) *Library {
		return &Library{
			Dispatch: &DispatchHandler{Client: client, CallTimeout: 30 * time.Second},
			Deadline: &DeadlineHandler{},
			handler:  handler,
		}
	}),
	fx.Invoke(func(registry *chasm.Registry, library *Library) error {
		return registry.Register(library)
	}),
)
