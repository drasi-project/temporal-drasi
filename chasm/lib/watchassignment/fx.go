package watchassignment

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"chasm.lib.watchassignment",
	fx.Provide(newHandler),
	fx.Provide(func(handler *handler) *Library { return &Library{handler: handler} }),
	fx.Invoke(func(registry *chasm.Registry, library *Library) error {
		return registry.Register(library)
	}),
)
