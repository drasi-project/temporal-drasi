FROM golang:1.27.0-bookworm AS go
FROM rust:1.96.1-bookworm AS rust
FROM python:3.12.13-bookworm

COPY --from=go /usr/local/go /usr/local/go
COPY --from=rust /usr/local/cargo /usr/local/cargo
COPY --from=rust /usr/local/rustup /usr/local/rustup
ENV PATH="/usr/local/go/bin:/usr/local/cargo/bin:${PATH}" \
    CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    GOMAXPROCS=2 \
    CARGO_BUILD_JOBS=2 \
    CARGO_PROFILE_DEV_DEBUG=0 \
    CARGO_PROFILE_TEST_DEBUG=0 \
    RUST_LOG=error \
    TEMPORAL_TEST_LOG_LEVEL=error

RUN apt-get update && apt-get install --no-install-recommends -y \
    clang libclang-dev llvm-dev libssl-dev pkg-config protobuf-compiler cmake util-linux \
    && rustup component add clippy rustfmt

WORKDIR /workspace
