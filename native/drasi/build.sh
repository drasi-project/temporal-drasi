#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
if [[ -n "${CARGO_BUILD_TARGET:-}" || -n "${CARGO_TARGET_DIR:-}" ]]; then
    echo "Native library publication requires the native default Cargo target directory" >&2
    exit 1
fi
mkdir -p .verification/build lib
export TMPDIR="$PWD/.verification/build"
python3 verify-dependencies.py
case "${1:-debug}" in
    debug) cargo build --locked --target-dir target; profile=debug ;;
    release) cargo build --locked --target-dir target --release; profile=release ;;
    *) echo "usage: $0 [debug|release]" >&2; exit 1 ;;
esac
case "$(uname -s)" in
    Darwin) library=libtemporal_drasi.dylib ;;
    Linux) library=libtemporal_drasi.so ;;
    *) echo "Unsupported native build platform" >&2; exit 1 ;;
esac
probe="$TMPDIR/native-link-$$"
if [[ -e "$probe" || -L "$probe" ]]; then
    echo "Native link probe already exists" >&2
    exit 1
fi
trap 'rm -f "$probe"' EXIT
cc -std=c11 -Wall -Wextra -Werror -Iinclude tests/abi-link.c \
    -L"target/$profile" -ltemporal_drasi "-Wl,-rpath,$PWD/target/$profile" -o "$probe"
"$probe"
bash publish-library.sh "target/$profile/temporal-drasi-plugin-worker" "lib/temporal-drasi-plugin-worker"
bash publish-library.sh "target/$profile/$library" "lib/$library"
