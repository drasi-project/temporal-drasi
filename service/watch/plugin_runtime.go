package watch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/watch/native"
)

type runtimePluginObservation struct {
	generation uint64
	observedAt time.Time
	state      native.PluginHostState
	problem    string
	retryAt    time.Time
}

// runtimePlugins never materializes a source or obtains namespace credentials.
type runtimePlugins struct {
	runtime    *native.Runtime
	instanceID string
	management bool
	logger     log.Logger
	cancel     context.CancelFunc
	done       chan struct{}
	wake       chan struct{}

	mu           sync.Mutex
	view         *pluginIntent
	targets      []*contractpb.Plugin
	next         int
	observations map[string]runtimePluginObservation
}

func newRuntimePlugins(runtime *native.Runtime, instanceID string, management bool, logger log.Logger) *runtimePlugins {
	ctx, cancel := context.WithCancel(context.Background())
	controller := &runtimePlugins{
		runtime: runtime, instanceID: instanceID, management: management, logger: logger,
		cancel: cancel, done: make(chan struct{}), wake: make(chan struct{}, 1),
		observations: make(map[string]runtimePluginObservation),
	}
	go controller.run(ctx)
	return controller
}

func (p *runtimePlugins) update(view *pluginIntent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.view != nil {
		if view.snapshot.Generation < p.view.snapshot.Generation {
			// A graph publication can race the next complete service cut.
			return nil
		}
		if err := view.follows(p.view); err != nil {
			return err
		}
		if view.snapshot.Generation == p.view.snapshot.Generation {
			return nil
		}
	}
	p.view, p.targets, p.next = view, nil, 0
	retained := make(map[string]runtimePluginObservation)
	for _, plugin := range view.snapshot.Plugins {
		if p.management || view.snapshot.DesiredLoads[plugin.Id] == plugin.Digest {
			p.targets = append(p.targets, plugin)
			retained[plugin.Digest] = p.observations[plugin.Digest]
		}
	}
	p.observations = retained
	select {
	case p.wake <- struct{}{}:
	default:
	}
	return nil
}

func (p *runtimePlugins) close(ctx context.Context) error {
	p.cancel()
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *runtimePlugins) run(ctx context.Context) {
	defer close(p.done)
	for ctx.Err() == nil {
		view, plugin := p.nextPlugin(time.Now())
		if plugin != nil {
			observed := p.converge(ctx, plugin, view.snapshot.DesiredLoads[plugin.Id] == plugin.Digest)
			if ctx.Err() != nil {
				return
			}
			observed.generation = view.snapshot.Generation
			p.mu.Lock()
			changed := false
			if p.view == view {
				changed = p.observations[plugin.Digest].problem != observed.problem
				p.observations[plugin.Digest] = observed
			}
			p.mu.Unlock()
			if changed && observed.problem != "" {
				p.logger.Warn("Watch plugin reconciliation failed",
					tag.NewStringTag("runtime-id", p.instanceID), tag.NewStringTag("plugin-digest", plugin.Digest),
					tag.NewStringTag("problem", observed.problem))
			}
		}
		// Yield between artifacts, including capacity refusals, rather than letting
		// a runtime monopolize the process-wide native completion owners.
		timer := time.NewTimer(backoff.Jitter(250*time.Millisecond, 0.5))
		select {
		case <-ctx.Done():
		case <-p.wake:
		case <-timer.C:
		}
		timer.Stop()
	}
}

func (p *runtimePlugins) nextPlugin(now time.Time) (*pluginIntent, *contractpb.Plugin) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for range len(p.targets) {
		plugin := p.targets[p.next]
		p.next = (p.next + 1) % len(p.targets)
		previous := p.observations[plugin.Digest]
		if previous.generation != p.view.snapshot.Generation || !now.Before(previous.retryAt) {
			return p.view, plugin
		}
	}
	return nil, nil
}

func (p *runtimePlugins) command(ctx context.Context, command native.Command) (native.Result, error) {
	attempt, err := p.runtime.Plugin(command)
	if err != nil {
		return native.Result{}, err
	}
	// Only shutdown cancels this wait. The immutable attempt remains attached to
	// actual native completion while newer intent is installed independently.
	result, err := attempt.Wait(ctx)
	if nativeError, ok := errors.AsType[*native.Error](err); ok && len(nativeError.Message) > 1024 {
		return native.Result{}, serviceerror.NewDataLoss("native plugin diagnostic exceeds its byte limit")
	}
	return result, err
}

func (p *runtimePlugins) probe(ctx context.Context, plugin *contractpb.Plugin) (*native.PluginObservation, error) {
	result, err := p.command(ctx, native.GetPluginInstallationCommand{Digest: plugin.Digest})
	if err != nil {
		return nil, err
	}
	observed := result.Installation
	if observed == nil || observed.HostID != p.instanceID ||
		observed.Plugin.CatalogID != plugin.CatalogId || observed.Plugin.ID != plugin.Id ||
		observed.Plugin.Version != plugin.Version || observed.Plugin.Digest != plugin.Digest ||
		len(observed.Message) > 1024 {
		return nil, serviceerror.NewDataLoss("native plugin observation does not match the runtime and immutable selection")
	}
	switch observed.State {
	case native.PluginAvailable, native.PluginFailed, native.PluginIncompatible, native.PluginRestartRequired:
	case native.PluginInstalled, native.PluginLoaded:
		if !observed.Plugin.Compatible {
			return nil, serviceerror.NewDataLoss("native plugin success has no compatible installation")
		}
	default:
		return nil, serviceerror.NewDataLoss("native plugin observation has an unknown state")
	}
	return observed, nil
}

func (p *runtimePlugins) converge(ctx context.Context, plugin *contractpb.Plugin, load bool) runtimePluginObservation {
	observed, err := p.probe(ctx, plugin)
	if err != nil {
		if nativeError, ok := errors.AsType[*native.Error](err); !ok || nativeError.Code != native.CodeNotFound {
			return failedRuntimePlugin(err)
		}
	}
	if observed == nil || observed.State == native.PluginAvailable || observed.State == native.PluginFailed {
		_, err = p.command(ctx, native.InstallPluginCommand{
			Selection: native.PluginSelection{CatalogID: plugin.CatalogId, PluginID: plugin.Id, Version: plugin.Version},
			Digest:    plugin.Digest,
		})
		if err != nil {
			return p.afterFailure(ctx, plugin, err)
		}
		observed, err = p.probe(ctx, plugin)
		if err != nil {
			return failedRuntimePlugin(err)
		}
	}
	if load && observed.State == native.PluginInstalled {
		_, err = p.command(ctx, native.LoadPluginCommand{Digest: plugin.Digest})
		if err != nil {
			return p.afterFailure(ctx, plugin, err)
		}
		observed, err = p.probe(ctx, plugin)
		if err != nil {
			return failedRuntimePlugin(err)
		}
	}
	return runtimePluginResult(observed)
}

func (p *runtimePlugins) afterFailure(ctx context.Context, plugin *contractpb.Plugin, failure error) runtimePluginObservation {
	// A terminal error can follow effects. Observe the actual cache and resident
	// state instead of inferring that installation or loading did not happen.
	observed, err := p.probe(ctx, plugin)
	if err != nil {
		return failedRuntimePlugin(errors.Join(failure, fmt.Errorf("observe plugin after failure: %w", err)))
	}
	result := runtimePluginResult(observed)
	if result.problem == "" {
		result.problem = failure.Error()
	}
	return result
}

func failedRuntimePlugin(err error) runtimePluginObservation {
	delay := 30 * time.Second
	if nativeError, ok := errors.AsType[*native.Error](err); ok && nativeError.Code == native.CodeConflict {
		delay = backoff.Jitter(time.Second, 0.5)
	}
	return runtimePluginObservation{problem: err.Error(), retryAt: time.Now().Add(delay)}
}

func runtimePluginResult(observed *native.PluginObservation) runtimePluginObservation {
	now := time.Now()
	result := runtimePluginObservation{
		observedAt: now, state: observed.State, problem: observed.Message, retryAt: now.Add(time.Minute),
	}
	return result
}
