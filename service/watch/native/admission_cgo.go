//go:build temporal_with_drasi && cgo

package native

/*
#include <stdlib.h>
#include "temporal_drasi.h"

static void temporal_drasi_clear(uint8_t *data, size_t length) {
    volatile uint8_t *bytes = data;
    while (length != 0) {
        *bytes++ = 0;
        length--;
    }
}
*/
import "C"

import (
	"context"
	"encoding/json"
	"runtime"
	"time"
	"unsafe"
)

var credentialTransfers = make(chan struct{}, 8)

func wipeCredential(value []byte) {
	if len(value) != 0 {
		C.temporal_drasi_clear((*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(value))), C.size_t(len(value)))
		runtime.KeepAlive(value)
	}
}

// AdmitPluginSource never places resolved values in declarative JSON. A native
// preflight avoids credential reads for invalid definitions and resident retries.
func (r *Runtime) AdmitPluginSource(ctx context.Context, definition NodeDefinition, resolve SecretResolver) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	select {
	case credentialTransfers <- struct{}{}:
		defer func() { <-credentialTransfers }()
	default:
		return Result{}, &Error{Code: CodeConflict, Message: "plugin credential transfer capacity exhausted"}
	}
	payload, err := json.Marshal(definition)
	if err != nil {
		return Result{}, err
	}
	if len(payload) > 1<<20 {
		return Result{}, &Error{Code: CodeInvalidArgument, Message: "plugin definition exceeds its byte limit"}
	}
	var detached NodeDefinition
	if err := json.Unmarshal(payload, &detached); err != nil {
		return Result{}, err
	}
	recipe, ok := detached.Recipe.(PluginSourceRecipe)
	if !ok {
		return Result{}, &Error{Code: CodeInvalidArgument, Message: "managed plugin admission requires a plugin Source recipe"}
	}
	preflight, err := r.Apply(ctx, checkPluginSourceCommand{Definition: detached})
	if err != nil {
		return Result{}, err
	}
	if preflight.MaterializationRequired == nil {
		return Result{}, &Error{Code: CodeInternal, Message: "native plugin preflight returned no materialization result"}
	}
	handle, err := r.runtimeHandle()
	if err != nil {
		return Result{}, err
	}
	definitionData := C.CBytes(payload)
	defer C.free(definitionData)
	valuesData := C.malloc(credentialPacketBytes)
	defer func() {
		C.temporal_drasi_clear((*C.uint8_t)(valuesData), credentialPacketBytes)
		C.free(valuesData)
	}()
	target := unsafe.Slice((*byte)(valuesData), credentialPacketBytes)
	if !*preflight.MaterializationRequired {
		recipe = PluginSourceRecipe{}
	}
	length, err := encodeAdmissionValues(ctx, recipe, resolve, target, wipeCredential)
	if err != nil {
		return Result{}, err
	}
	result, err := invoke(ctx, func(cancel C.uint64_t, timeout C.uint32_t) C.temporal_drasi_v1_status {
		return C.temporal_drasi_v1_admit_plugin_source(
			handle, (*C.uint8_t)(definitionData), C.size_t(len(payload)),
			(*C.uint8_t)(valuesData), C.size_t(length), cancel, timeout,
		)
	})
	if err != nil {
		return Result{}, err
	}
	var response Result
	err = json.Unmarshal(result, &response)
	return response, err
}
