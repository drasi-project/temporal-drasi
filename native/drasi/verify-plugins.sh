#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."
: "${TEMPORAL_DRASI_PLUGIN_CONFIG:?Set an administrator-authored Config JSON file with trusted signed PostgreSQL source and separate bootstrap pins}"
if [[ ! -f "$TEMPORAL_DRASI_PLUGIN_CONFIG" ]]; then
    echo "The configured plugin fixture file does not exist" >&2
    exit 1
fi
case "$TEMPORAL_DRASI_PLUGIN_CONFIG" in
    /*) ;;
    *) export TEMPORAL_DRASI_PLUGIN_CONFIG="$PWD/$TEMPORAL_DRASI_PLUGIN_CONFIG" ;;
esac
native="$PWD/native/drasi"
mkdir -p "$native/.verification/build" "$native/.verification/go"
export TMPDIR="$native/.verification/build"
export GOTMPDIR="$native/.verification/go"
CGO_ENABLED=1 go test -count=1 -tags test_dep,integration,temporal_with_drasi \
    ./service/watch/native -run '^TestNativeConfiguredSignedPostgresPlugins$' -v
