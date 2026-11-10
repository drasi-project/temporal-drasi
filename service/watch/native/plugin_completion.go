//go:build temporal_with_drasi && cgo

package native

/*
#include <stdlib.h>
#include "temporal_drasi.h"
*/
import "C"

import (
	"context"
	"encoding/json"
	"errors"
	"math"
)

var pluginOwners = make(chan struct{}, 8)

// PluginAttempt retains one immutable operation across canceled waits.
type PluginAttempt struct {
	runtime  *Runtime
	sequence uint64
	command  []byte
	started  bool
	done     chan struct{}
	payload  []byte
	err      error
}

func isPluginManagement(operation string) bool {
	switch operation {
	case "descriptors", "validate_plugin_sources", "list_catalogs", "discover_plugins", "inspect_plugin",
		"install_plugin", "load_plugin", "list_plugin_installations", "get_plugin_installation":
		return true
	default:
		return false
	}
}

// Plugin freezes a management command without starting native work.
func (r *Runtime) Plugin(command Command) (*PluginAttempt, error) {
	payload, err := marshalCommand(command)
	if err != nil {
		return nil, err
	}
	if !isPluginManagement(command.operation()) {
		return nil, &Error{Code: CodeInvalidArgument, Message: "plugin attempt requires a management command"}
	}
	return r.pluginAttempt(payload)
}

func (r *Runtime) pluginAttempt(payload []byte) (*PluginAttempt, error) {
	if len(payload) > 1<<20 {
		return nil, &Error{Code: CodeInvalidArgument, Message: "plugin command exceeds its byte limit"}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.handle == 0 {
		return nil, ErrClosed
	}
	if r.pluginClosing {
		return nil, &Error{Code: CodeConflict, Message: "native plugin admission is sealed"}
	}
	if r.nextPlugin == math.MaxUint64 {
		return nil, &Error{Code: CodeConflict, Message: "plugin attempt sequence exhausted"}
	}
	r.nextPlugin++
	return &PluginAttempt{
		runtime: r, sequence: r.nextPlugin, command: payload, done: make(chan struct{}),
	}, nil
}

// Wait owns native execution until completion, independently of this caller's context.
func (a *PluginAttempt) Wait(ctx context.Context) (Result, error) {
	if a == nil || a.runtime == nil {
		return Result{}, &Error{Code: CodeInvalidArgument, Message: "plugin attempt has no runtime"}
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	r := a.runtime
	r.mu.Lock()
	if !a.started {
		if r.handle == 0 {
			r.mu.Unlock()
			return Result{}, ErrClosed
		}
		if r.pluginClosing || a.sequence <= r.lastPlugin {
			r.mu.Unlock()
			return Result{}, &Error{Code: CodeConflict, Message: "plugin attempt is sealed or stale"}
		}
		if r.plugin != nil {
			r.mu.Unlock()
			return Result{}, &Error{Code: CodeConflict, Message: "native plugin work is still in progress"}
		}
		if err := ctx.Err(); err != nil {
			r.mu.Unlock()
			return Result{}, err
		}
		select {
		case pluginOwners <- struct{}{}:
		default:
			r.mu.Unlock()
			return Result{}, &Error{Code: CodeConflict, Message: "plugin completion owner capacity exhausted"}
		}
		a.started, r.lastPlugin, r.plugin = true, a.sequence, a
		go a.run(r.handle)
	}
	r.mu.Unlock()
	select {
	case <-ctx.Done():
		return Result{}, ctx.Err()
	case <-a.done:
	}
	if nativeError, ok := errors.AsType[*Error](a.err); ok {
		return Result{}, &Error{Code: nativeError.Code, Message: nativeError.Message}
	}
	if a.err != nil {
		return Result{}, a.err
	}
	var result Result
	err := json.Unmarshal(a.payload, &result)
	return result, err
}

func (a *PluginAttempt) run(handle uint64) {
	var payload []byte
	var failure error
	defer func() {
		if recover() != nil {
			failure = &Error{Code: CodeInternal, Message: "panic contained in native plugin completion owner"}
		}
		<-pluginOwners
		r := a.runtime
		r.mu.Lock()
		a.payload, a.err = payload, failure
		r.plugin = nil
		close(a.done)
		r.mu.Unlock()
	}()
	payload, failure = callPluginJoined(handle, a.command)
	if failure == nil {
		var result Result
		failure = json.Unmarshal(payload, &result)
	}
}

func callPluginJoined(handle uint64, command []byte) ([]byte, error) {
	data := C.CBytes(command)
	defer C.free(data)
	status := C.temporal_drasi_v1_plugin_run_joined(
		C.uint64_t(handle), (*C.uint8_t)(data), C.size_t(len(command)),
	)
	payload, err := takeStatus(status)
	if err != nil {
		return nil, err
	}
	if status.code != 0 {
		var nativeError Error
		if err := json.Unmarshal(payload, &nativeError); err != nil {
			return nil, &Error{Code: CodeInternal, Message: "native library returned an invalid error"}
		}
		return nil, &nativeError
	}
	return payload, nil
}

func (r *Runtime) quiescePlugins(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	r.pluginClosing = true
	pending := r.plugin
	r.mu.Unlock()
	if pending == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-pending.done:
		return nil
	}
}
