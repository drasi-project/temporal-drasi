#include "temporal_drasi.h"
#include <stdio.h>

int main(void) {
    if (temporal_drasi_v1_abi_version() != 1) {
        fputs("Native Drasi ABI version mismatch\n", stderr);
        return 1;
    }
    return 0;
}
