FROM golang:1.25-bookworm AS build

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --default-toolchain stable --profile minimal

ENV PATH=/root/.cargo/bin:$PATH

WORKDIR /build

COPY Cargo.toml build.rs ./
COPY src/ ./src/
COPY bridge/ ./bridge/
COPY examples/ ./examples/

WORKDIR /build/examples/e2e
# The consumer ships on a `scratch` image (empty: no libc, no dynamic loader),
# so the binary must be fully static, carrying its own C runtime. That is what
# `+crt-static` below does. Two wrinkles:
#   - Set it per target triple (CARGO_TARGET_<triple>_RUSTFLAGS), not as a
#     global RUSTFLAGS: a global flag also hits proc-macros and build scripts,
#     which compile for the build host and cannot be static.
#   - Always a glibc (-gnu) triple, never musl: the Go engine cannot run as a
#     static guest on musl (see docs/internal/MUSL.md).
ARG TARGETARCH
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) triple=x86_64-unknown-linux-gnu;  flagvar=CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS ;; \
      arm64) triple=aarch64-unknown-linux-gnu; flagvar=CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUSTFLAGS ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    rustup target add "$triple"; \
    export "$flagvar=-C target-feature=+crt-static"; \
    cargo build -p consumer --release --target "$triple"; \
    cp "target/$triple/release/consumer" /consumer; \
    (readelf -l /consumer | grep -i interp || echo "no INTERP segment => fully static")

# - - - - - - - - - - - - - - - - - #

FROM scratch
COPY --from=build /consumer /consumer
ENTRYPOINT ["/consumer"]
