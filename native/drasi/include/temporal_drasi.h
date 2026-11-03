#ifndef TEMPORAL_DRASI_V1_H
#define TEMPORAL_DRASI_V1_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* All handles are opaque nonzero integers. Caller memory is never retained. */
typedef struct {
    uint64_t allocation;
    const uint8_t *data;
    size_t len;
} temporal_drasi_v1_buffer;

/* Both success and error payloads are owned UTF-8 JSON. Always free allocation. */
typedef struct {
    int32_t code;
    uint32_t reserved;
    temporal_drasi_v1_buffer buffer;
} temporal_drasi_v1_status;

enum {
    TEMPORAL_DRASI_V1_OK = 0,
    TEMPORAL_DRASI_V1_INVALID_ARGUMENT = 1,
    TEMPORAL_DRASI_V1_NOT_FOUND = 2,
    TEMPORAL_DRASI_V1_CONFLICT = 3,
    TEMPORAL_DRASI_V1_CANCELLED = 4,
    TEMPORAL_DRASI_V1_DEADLINE = 5,
    TEMPORAL_DRASI_V1_GAP = 6,
    TEMPORAL_DRASI_V1_INTERNAL = 7,
    TEMPORAL_DRASI_V1_FAILED_PRECONDITION = 8
};

uint32_t temporal_drasi_v1_abi_version(void);
uint64_t temporal_drasi_v1_cancel_new(void);
int32_t temporal_drasi_v1_cancel(uint64_t cancel);
int32_t temporal_drasi_v1_cancel_free(uint64_t cancel);
int32_t temporal_drasi_v1_buffer_free(uint64_t allocation);
/* Stateless compilation does not open a runtime, persistent store or subscription. */
temporal_drasi_v1_status temporal_drasi_v1_validate_query(
    const uint8_t *data, size_t length, uint64_t cancel, uint32_t timeout_ms);
temporal_drasi_v1_status temporal_drasi_v1_open(
    const uint8_t *data, size_t length, uint64_t cancel, uint32_t timeout_ms);
temporal_drasi_v1_status temporal_drasi_v1_apply(
    uint64_t runtime, const uint8_t *data, size_t length,
    uint64_t cancel, uint32_t timeout_ms);
/* Management-only call. No caller deadline detaches its native execution.
 * The host must retain a bounded owner until this call returns.
 */
temporal_drasi_v1_status temporal_drasi_v1_plugin_run_joined(
    uint64_t runtime, const uint8_t *data, size_t length);
/* Public definition JSON and private TDC1 credentials are separate inputs.
 * TDC1: magic, little-endian u32 count, then records with u8 component
 * (0=Source, 1=bootstrap) and five u32-length-prefixed byte strings:
 * field, provider, key, version, value. No trailing bytes are permitted.
 * Inputs are copied before scheduling, including on cancellation/deadline.
 * The caller must clear its private packet after every call.
 */
temporal_drasi_v1_status temporal_drasi_v1_admit_plugin_source(
    uint64_t runtime, const uint8_t *definition, size_t definition_length,
    const uint8_t *values, size_t values_length,
    uint64_t cancel, uint32_t timeout_ms);
temporal_drasi_v1_status temporal_drasi_v1_poll(
    uint64_t runtime, uint32_t limit, uint32_t wait_ms,
    uint64_t cancel, uint32_t timeout_ms);
temporal_drasi_v1_status temporal_drasi_v1_ack(
    uint64_t runtime, const uint8_t *data, size_t length,
    uint64_t cancel, uint32_t timeout_ms);
temporal_drasi_v1_status temporal_drasi_v1_close(
    uint64_t runtime, uint64_t cancel, uint32_t timeout_ms);

#ifdef __cplusplus
}
#endif
#endif
