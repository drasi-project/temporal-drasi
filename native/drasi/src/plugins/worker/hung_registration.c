#include <fcntl.h>
#include <stddef.h>
#include <unistd.h>

typedef struct {
    const char *ptr;
    size_t len;
} FfiStr;

typedef struct {
    FfiStr sdk_version;
    FfiStr core_version;
    FfiStr lib_version;
    FfiStr plugin_version;
    FfiStr target_triple;
    FfiStr git_commit;
    FfiStr build_timestamp;
} PluginMetadata;

#define STR(value) {value, sizeof(value) - 1}

static const PluginMetadata metadata = {
    STR("0.14.0"), STR("0.5.10"), STR("0.9.3"), STR("1.0.0"),
    STR(TARGET_TRIPLE), STR("fixture"), STR("fixture")
};

const PluginMetadata *drasi_plugin_metadata(void) {
    return &metadata;
}

void *drasi_plugin_init(void) {
    int ready = open(READY_PATH, O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (ready >= 0) {
        const char marker = 'R';
        (void)write(ready, &marker, 1);
        (void)close(ready);
    }
    for (;;) {
        pause();
    }
}
