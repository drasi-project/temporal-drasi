#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export GOWORK=off
export GOTMPDIR="$PWD/.tools/go-build"
mkdir -p "$GOTMPDIR" "$PWD/.tools/bin"
if [[ ! -x node_modules/.bin/buf ]]; then
  npm ci --no-audit --no-fund
fi
install_tool() {
  local name="$1" package="$2" version="$3"
  if [[ ! -x ".tools/bin/$name" ]] || [[ "$(cat ".tools/bin/$name.version" 2>/dev/null || true)" != "$version" ]]; then
    GOBIN="$PWD/.tools/bin" go install "$package@$version"
    printf '%s\n' "$version" > ".tools/bin/$name.version"
  fi
}
install_tool protoc-gen-go google.golang.org/protobuf/cmd/protoc-gen-go v1.36.6
install_tool protoc-gen-go-grpc google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.5.1
install_tool protoc-gen-grpc-gateway github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway v2.26.3
install_tool protoc-gen-openapiv2 github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2 v2.26.3
python3 scripts/fetch-protos.py
python3 scripts/generate.py "$@"
