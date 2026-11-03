//go:build !temporal_with_drasi || !cgo

package native

import "context"

type Runtime struct{}

// PluginAttempt is unavailable without native hosting.
type PluginAttempt struct{}

func Enabled() bool { return false }

func ValidateQuery(context.Context, Language, string) (bool, error) { return false, ErrDisabled }

func Open(context.Context, Config) (*Runtime, error) { return nil, ErrDisabled }

func (*Runtime) Apply(context.Context, Command) (Result, error) { return Result{}, ErrDisabled }

func (*Runtime) Plugin(Command) (*PluginAttempt, error) { return nil, ErrDisabled }

func (*PluginAttempt) Wait(context.Context) (Result, error) { return Result{}, ErrDisabled }

func (*Runtime) AdmitPluginSource(context.Context, NodeDefinition, SecretResolver) (Result, error) {
	return Result{}, ErrDisabled
}

func (*Runtime) Poll(context.Context, int) ([]Event, error) { return nil, ErrDisabled }

func (*Runtime) Ack(context.Context, Receipt) error { return ErrDisabled }

func (*Runtime) Close(context.Context) error { return ErrDisabled }
