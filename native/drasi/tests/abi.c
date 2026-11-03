#include "temporal_drasi.h"
#include <assert.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

_Static_assert(offsetof(temporal_drasi_v1_buffer, len) == 16, "unsupported buffer layout");
_Static_assert(offsetof(temporal_drasi_v1_status, buffer) == 8, "unsupported status layout");
_Static_assert(sizeof(temporal_drasi_v1_status) == 32, "unsupported status size");

static char *consume(temporal_drasi_v1_status status, int32_t expected) {
    assert(status.reserved == 0);
    assert(status.buffer.data != NULL);
    assert(status.buffer.len < (16u << 20));
    char *result = malloc(status.buffer.len + 1);
    assert(result != NULL);
    memcpy(result, status.buffer.data, status.buffer.len);
    result[status.buffer.len] = '\0';
    if (status.code != expected) {
        fprintf(stderr, "expected status %d, received %d: %s\n", expected, status.code, result);
        abort();
    }
    assert(temporal_drasi_v1_buffer_free(status.buffer.allocation) == 0);
    assert(temporal_drasi_v1_buffer_free(status.buffer.allocation) == TEMPORAL_DRASI_V1_NOT_FOUND);
    return result;
}

int main(int argc, char **argv) {
    assert(argc == 2);
    assert(temporal_drasi_v1_abi_version() == 1);
    uint64_t cancel = temporal_drasi_v1_cancel_new();
    const char *compilations[] = {
        "{\"language\":\"cypher\",\"text\":\"MATCH (n) RETURN count(n) AS total\"}",
        "{\"language\":\"gql\",\"text\":\"MATCH (n) RETURN count(n) AS total\"}"
    };
    for (size_t i = 0; i < sizeof(compilations) / sizeof(compilations[0]); i++) {
        char *result = consume(temporal_drasi_v1_validate_query(
            (const uint8_t *)compilations[i], strlen(compilations[i]), cancel, 30000), 0);
        assert(strcmp(result, "{\"valid\":true}") == 0);
        free(result);
    }
    const char *invalid_query = "{\"language\":\"cypher\",\"text\":\"MATCH (\"}";
    char *invalid = consume(temporal_drasi_v1_validate_query(
        (const uint8_t *)invalid_query, strlen(invalid_query), cancel, 30000), 0);
    assert(strcmp(invalid, "{\"valid\":false}") == 0);
    free(invalid);
    free(consume(temporal_drasi_v1_validate_query(NULL, 4, cancel, 1000), TEMPORAL_DRASI_V1_INVALID_ARGUMENT));
    free(consume(temporal_drasi_v1_open(NULL, 4, cancel, 1000), TEMPORAL_DRASI_V1_INVALID_ARGUMENT));
    char config[8192];
    int written = snprintf(config, sizeof(config),
        "{\"version\":1,\"state_dir\":\"%s\",\"owner_epoch\":1,\"instance_id\":\"c-abi\","
        "\"sources\":[{\"id\":\"feed\",\"kind\":\"temporal\"}],"
        "\"queries\":[{\"id\":\"q\",\"revision\":1,\"language\":\"cypher\","
        "\"text\":\"MATCH (n:Item) RETURN n.amount * 2 AS doubled\",\"sources\":[\"feed\"]}]}", argv[1]);
    assert(written > 0 && (size_t)written < sizeof(config));
    char *opened = consume(temporal_drasi_v1_open((const uint8_t *)config, strlen(config), cancel, 30000), 0);
    char *handle_field = strstr(opened, "\"handle\":");
    assert(handle_field != NULL);
    uint64_t handle = strtoull(handle_field + strlen("\"handle\":"), NULL, 10);
    free(opened);
    assert(handle != 0);

    const char *catalogs = "{\"op\":\"list_catalogs\"}";
    char *catalog_result = consume(temporal_drasi_v1_apply(handle, (const uint8_t *)catalogs, strlen(catalogs), cancel, 30000), 0);
    assert(strstr(catalog_result, "\"catalogs\":[]") != NULL);
    free(catalog_result);
    const char *injected = "{\"op\":\"discover_plugins\",\"registry_prefix\":\"attacker.example/plugins\"}";
    free(consume(temporal_drasi_v1_apply(handle, (const uint8_t *)injected, strlen(injected), cancel, 1000), TEMPORAL_DRASI_V1_INVALID_ARGUMENT));
    const char *load = "{\"op\":\"load_plugin\",\"digest\":\"sha256:unknown\"}";
    free(consume(temporal_drasi_v1_apply(handle, (const uint8_t *)load, strlen(load), cancel, 1000), TEMPORAL_DRASI_V1_NOT_FOUND));

    const char *feed =
        "{\"op\":\"feed\",\"source_id\":\"feed\",\"position\":1,\"change\":{"
        "\"kind\":\"insert\",\"element_id\":\"item\",\"labels\":[\"Item\"],"
        "\"effective_from\":1,\"properties\":{\"amount\":12}}}";
    free(consume(temporal_drasi_v1_apply(handle, (const uint8_t *)feed, strlen(feed), cancel, 30000), 0));
    const char *snapshot = "{\"op\":\"snapshot\",\"query_id\":\"q\"}";
    int evaluated = 0;
    for (int attempt = 0; attempt < 1000 && !evaluated; attempt++) {
        char *result = consume(temporal_drasi_v1_apply(handle, (const uint8_t *)snapshot, strlen(snapshot), cancel, 30000), 0);
        evaluated = strstr(result, "\"doubled\":24") != NULL;
        free(result);
    }
    assert(evaluated);

    free(consume(temporal_drasi_v1_poll(handle, 1, 100, cancel, 1), TEMPORAL_DRASI_V1_DEADLINE));
    assert(temporal_drasi_v1_cancel(cancel) == 0);
    free(consume(temporal_drasi_v1_validate_query(
        (const uint8_t *)compilations[0], strlen(compilations[0]), cancel, 1000), TEMPORAL_DRASI_V1_CANCELLED));
    free(consume(temporal_drasi_v1_apply(handle, (const uint8_t *)snapshot, strlen(snapshot), cancel, 1000), TEMPORAL_DRASI_V1_CANCELLED));
    assert(temporal_drasi_v1_cancel_free(cancel) == 0);
    cancel = temporal_drasi_v1_cancel_new();
    free(consume(temporal_drasi_v1_close(handle, cancel, 30000), 0));
    free(consume(temporal_drasi_v1_poll(handle, 1, 0, cancel, 1000), TEMPORAL_DRASI_V1_NOT_FOUND));
    assert(temporal_drasi_v1_cancel_free(cancel) == 0);
    puts("C ABI: stateless compilation, real evaluation, catalog isolation, layouts, buffers, cancellation, deadline and close passed");
    return 0;
}
