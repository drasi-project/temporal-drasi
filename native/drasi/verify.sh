#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."
python3 - <<'PY'
import sys

if sys.version_info < (3, 12):
    sys.exit("native verification requires Python 3.12 or newer")
PY
native="$PWD/native/drasi"
mkdir -p "$native/lib" "$native/.verification/build" "$native/.verification/go"
export TMPDIR="$native/.verification/build"
export GOTMPDIR="$native/.verification/go"
export RUST_LOG="${RUST_LOG:-error}"
python3 "$native/prepare-core.py"
python3 "$native/tests/test-prepare-core.py"
python3 "$native/tests/test-sdk-recovery.py"
CGO_ENABLED=0 go test -count=1 -tags test_dep ./cmd/tools/getproto ./cmd/tools/protogen
CGO_ENABLED=0 go test -count=1 -tags test_dep \
    -run '^(TestEncodeExecutionFeedPage|TestExecutionFeed.*)$' \
    ./common/persistence ./common/persistence/serialization \
    ./service/history ./service/history/executionfeed ./service/history/shard ./service/history/workflow
CGO_ENABLED=1 go test -count=1 -tags test_dep,integration \
    -run '^TestExecutionFeedPrimaryCommitAndReplay$' ./service/history/executionfeed

bash "$native/tests/publish-library.sh"
cargo fmt --manifest-path "$native/Cargo.toml" -- --check
cargo build --manifest-path "$native/Cargo.toml" --locked \
    -p temporal-drasi-test-plugin -p temporal-drasi-other-test-plugin
python3 "$native/tests/test-sdk-diagnostics.py"
python3 - "$native/Cargo.toml" <<'PY'
import subprocess
import sys

markers = [
    b"tdrasi-credential-",
    b"private-managed-source-sentinel",
    b"private-managed-bootstrap-sentinel",
    b"private-instance-diagnostic-sentinel",
    b"credential-value-must-remain-private",
    b"${TEMPORAL_DRASI_NOT_AN_ENVIRONMENT_LOOKUP}",
    b"private-bootstrap-value",
    b"namespace-two-private-value",
    b"must-not-appear-in-diagnostics",
    b"private-value",
    b"private-source",
    b"private-quarantined",
    b"private-input",
    b"opaque-test-value",
]
result = subprocess.run(
    ["cargo", "test", "--manifest-path", sys.argv[1], "--locked"],
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    check=False,
)
output = result.stdout
exposed = any(marker in output for marker in markers)
for marker in markers:
    output = output.replace(marker, b"[credential test marker redacted]")
sys.stdout.buffer.write(output)
if exposed:
    print("Native diagnostics exposed credential/capability test markers", file=sys.stderr)
sys.exit(result.returncode or int(exposed))
PY
cargo clippy --manifest-path "$native/Cargo.toml" --locked --all-targets -- -D warnings
cargo build --manifest-path "$native/Cargo.toml" --locked

case "$(uname -s)" in
    Darwin) library=libtemporal_drasi.dylib ;;
    Linux) library=libtemporal_drasi.so ;;
    *) echo "Unsupported native verification platform" >&2; exit 1 ;;
esac
bash "$native/publish-library.sh" "$native/target/debug/$library" "$native/lib/$library"
if [[ "$(uname -s)" == Darwin ]]; then
    otool -D "$native/lib/$library" | tail -n 1 | grep -Fx "@rpath/$library"
fi
cc -std=c11 -Wall -Wextra -Werror -I"$native/include" "$native/tests/abi.c" \
    -L"$native/lib" -ltemporal_drasi "-Wl,-rpath,$native/lib" -o "$native/.verification/abi-test"
state="$native/.verification/c-abi-state-$$"
if [[ -e "$state" ]]; then
    echo "Remove the previous C ABI fixture directory before rerunning: $state" >&2
    exit 1
fi
trap 'rm -rf "$state"' EXIT
"$native/.verification/abi-test" "$state"
CGO_ENABLED=1 go test -count=1 -tags test_dep,temporal_with_drasi \
    ./service/watch/native ./service/watch ./client/watch ./chasm/lib/watch ./chasm/lib/watchregistry ./chasm/lib/watchassignment
CGO_ENABLED=0 go test -count=1 -tags test_dep \
    ./service/watch/native ./service/watch ./client/watch ./chasm/lib/watch ./chasm/lib/watchregistry
CGO_ENABLED=0 go test -count=1 -tags test_dep,temporal_with_drasi \
    ./service/watch/native ./service/watch ./client/watch ./chasm/lib/watch ./chasm/lib/watchregistry
CGO_ENABLED=1 go test -count=1 -timeout 8m -tags test_dep,integration,temporal_with_drasi \
    -run '^(TestDrasiNativeOutputStartsWorkflow|TestDrasiDeliveryRetryRPC|TestDrasiDefinitionAPIsWithoutNativeRuntime|TestDrasiSourceCredentialAPIs|TestDrasiNamespaceAssignmentRPC|TestDrasiClusterPluginIntentRPC|TestDrasiGraphCooperativeOwnership|TestDrasiGraphProcessCrashRecovery|TestDrasiGraphReconcilesDefinitionsAndRetainedAliases)$' ./tests
CGO_ENABLED=0 go test -count=1 -timeout 8m -tags test_dep,integration \
    -run '^(TestDrasiDeliveryRetryRPC|TestDrasiDefinitionAPIsWithoutNativeRuntime|TestDrasiSourceCredentialAPIs|TestDrasiNamespaceAssignmentRPC|TestDrasiClusterPluginIntentRPC)$' ./tests
CGO_ENABLED=1 go test -count=1 -timeout 4m -tags test_dep,integration,temporal_with_drasi \
    -run '^(TestDrasiWatchServerRole|TestExecutionFeedHistoryRPC|TestDrasiServerOptionsDoNotReadCredentials|TestWatchServiceIsOptIn)$' ./temporal
python3 "$native/tests/test-package.py"
