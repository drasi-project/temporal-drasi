//go:build temporal_with_drasi && cgo

package native

/*
#cgo CFLAGS: -I${SRCDIR}/../../../native/drasi/include
#cgo LDFLAGS: -L${SRCDIR}/../../../native/drasi/lib -ltemporal_drasi
#cgo darwin,!temporal_drasi_packaged LDFLAGS: -Wl,-rpath,${SRCDIR}/../../../native/drasi/lib
#cgo linux,!temporal_drasi_packaged LDFLAGS: -Wl,-rpath,${SRCDIR}/../../../native/drasi/lib
#include <stdlib.h>
#include "temporal_drasi.h"
*/
import "C"

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"
	"unsafe"
)

type Runtime struct {
	mu            sync.Mutex
	handle        uint64
	nextPlugin    uint64
	lastPlugin    uint64
	plugin        *PluginAttempt
	pluginClosing bool
}

func Enabled() bool { return true }

func ValidateQuery(ctx context.Context, language Language, text string) (bool, error) {
	if C.temporal_drasi_v1_abi_version() != ABIVersion {
		return false, &Error{Code: CodeInternal, Message: "native ABI version does not match the Go wrapper"}
	}
	payload, err := json.Marshal(struct {
		Language Language `json:"language"`
		Text     string   `json:"text"`
	}{language, text})
	if err != nil {
		return false, err
	}
	data := C.CBytes(payload)
	defer C.free(data)
	result, err := invoke(ctx, func(cancel C.uint64_t, timeout C.uint32_t) C.temporal_drasi_v1_status {
		return C.temporal_drasi_v1_validate_query((*C.uint8_t)(data), C.size_t(len(payload)), cancel, timeout)
	})
	if err != nil {
		return false, err
	}
	var response struct {
		Valid *bool `json:"valid"`
	}
	if err := json.Unmarshal(result, &response); err != nil {
		return false, err
	}
	if response.Valid == nil {
		return false, &Error{Code: CodeInternal, Message: "native compiler returned no validation result"}
	}
	return *response.Valid, nil
}

func Open(ctx context.Context, config Config) (*Runtime, error) {
	if C.temporal_drasi_v1_abi_version() != ABIVersion {
		return nil, &Error{Code: CodeConflict, Message: "native ABI version does not match the Go wrapper"}
	}
	if config.Version == 0 {
		config.Version = ABIVersion
	}
	payload, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	data := C.CBytes(payload)
	defer C.free(data)
	result, err := invoke(ctx, func(cancel C.uint64_t, timeout C.uint32_t) C.temporal_drasi_v1_status {
		return C.temporal_drasi_v1_open((*C.uint8_t)(data), C.size_t(len(payload)), cancel, timeout)
	})
	if err != nil {
		return nil, err
	}
	var opened struct {
		Handle uint64 `json:"handle"`
	}
	if err := json.Unmarshal(result, &opened); err != nil {
		return nil, err
	}
	if opened.Handle == 0 {
		return nil, &Error{Code: CodeInternal, Message: "native open returned a zero runtime handle"}
	}
	return &Runtime{handle: opened.Handle}, nil
}

func (r *Runtime) runtimeHandle() (C.uint64_t, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.handle == 0 {
		return 0, ErrClosed
	}
	return C.uint64_t(r.handle), nil
}

func (r *Runtime) Apply(ctx context.Context, command Command) (Result, error) {
	handle, err := r.runtimeHandle()
	if err != nil {
		return Result{}, err
	}
	payload, err := marshalCommand(command)
	if err != nil {
		return Result{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if isPluginManagement(command.operation()) {
		attempt, err := r.pluginAttempt(payload)
		if err != nil {
			return Result{}, err
		}
		return attempt.Wait(ctx)
	}
	if command.operation() == "checkpoint" {
		if err := r.quiescePlugins(ctx); err != nil {
			return Result{}, err
		}
	}
	data := C.CBytes(payload)
	defer C.free(data)
	result, err := invoke(ctx, func(cancel C.uint64_t, timeout C.uint32_t) C.temporal_drasi_v1_status {
		return C.temporal_drasi_v1_apply(handle, (*C.uint8_t)(data), C.size_t(len(payload)), cancel, timeout)
	})
	if err != nil {
		return Result{}, err
	}
	var response Result
	err = json.Unmarshal(result, &response)
	return response, err
}

func (r *Runtime) Poll(ctx context.Context, limit int) ([]Event, error) {
	if limit < 1 || limit > 64 {
		return nil, &Error{Code: CodeInvalidArgument, Message: "poll limit must be in 1..64"}
	}
	handle, err := r.runtimeHandle()
	if err != nil {
		return nil, err
	}
	for {
		result, err := invoke(ctx, func(cancel C.uint64_t, timeout C.uint32_t) C.temporal_drasi_v1_status {
			return C.temporal_drasi_v1_poll(handle, C.uint32_t(limit), 250, cancel, timeout)
		})
		if err != nil {
			return nil, err
		}
		var response struct {
			Events []Event `json:"events"`
		}
		if err := json.Unmarshal(result, &response); err != nil {
			return nil, err
		}
		if len(response.Events) > 0 {
			return response.Events, nil
		}
	}
}

func (r *Runtime) Ack(ctx context.Context, receipt Receipt) error {
	handle, err := r.runtimeHandle()
	if err != nil {
		return err
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	data := C.CBytes(payload)
	defer C.free(data)
	_, err = invoke(ctx, func(cancel C.uint64_t, timeout C.uint32_t) C.temporal_drasi_v1_status {
		return C.temporal_drasi_v1_ack(handle, (*C.uint8_t)(data), C.size_t(len(payload)), cancel, timeout)
	})
	return err
}

func (r *Runtime) Close(ctx context.Context) error {
	handle, err := r.runtimeHandle()
	if errors.Is(err, ErrClosed) {
		return nil
	}
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := r.quiescePlugins(ctx); err != nil {
		return err
	}
	_, err = invoke(ctx, func(cancel C.uint64_t, timeout C.uint32_t) C.temporal_drasi_v1_status {
		return C.temporal_drasi_v1_close(handle, cancel, timeout)
	})
	nativeError, nativeOK := errors.AsType[*Error](err)
	if err == nil || (nativeOK && nativeError.Code == CodeNotFound) {
		r.mu.Lock()
		r.handle = 0
		r.mu.Unlock()
		return nil
	}
	return err
}

func invoke(ctx context.Context, call func(C.uint64_t, C.uint32_t) C.temporal_drasi_v1_status) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	timeout := 30 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		timeout = min(timeout, time.Until(deadline))
	}
	milliseconds := max(1, timeout.Milliseconds())
	token := C.temporal_drasi_v1_cancel_new()
	cancelled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		C.temporal_drasi_v1_cancel(token)
		close(cancelled)
	})
	defer func() {
		if !stop() {
			<-cancelled
		}
		C.temporal_drasi_v1_cancel_free(token)
	}()
	status := call(token, C.uint32_t(milliseconds))
	result, err := takeStatus(status)
	if err != nil {
		return nil, err
	}
	if status.code == 0 {
		return result, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if status.code == C.TEMPORAL_DRASI_V1_DEADLINE {
		return nil, context.DeadlineExceeded
	}
	if status.code == C.TEMPORAL_DRASI_V1_CANCELLED {
		return nil, context.Canceled
	}
	var nativeError Error
	if err := json.Unmarshal(result, &nativeError); err != nil {
		return nil, &Error{Code: CodeInternal, Message: "native library returned an invalid error"}
	}
	return nil, &nativeError
}

func takeStatus(status C.temporal_drasi_v1_status) ([]byte, error) {
	defer C.temporal_drasi_v1_buffer_free(status.buffer.allocation)
	if status.reserved != 0 || status.buffer.len > 16<<20 || status.buffer.data == nil || status.buffer.allocation == 0 {
		return nil, &Error{Code: CodeInternal, Message: "native library returned an invalid buffer"}
	}
	return C.GoBytes(unsafe.Pointer(status.buffer.data), C.int(status.buffer.len)), nil
}
