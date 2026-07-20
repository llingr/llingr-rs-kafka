# Building and llingr-kafka Packaging

Ship the **llingr-demux** Go engine with your Rust application in one of two
ways: statically linked into a single binary (the default), or as a shared library
deployed beside it.

## Packaging modes

|  | Single binary (default) | Side-binary (`LLINGR_LINK=shared`) |
|---|---|---|
| Artifact | one self-contained executable | your binary plus `libllingr.so`/`.dylib` beside it |
| Engine linkage | static `libllingr.a`, built and linked by `cargo build` | shared library built once, resolved at runtime via RPATH |
| Deployment | copy one file; `scratch` images work | copy two files; needs a glibc base image (a dynamic binary needs a loader) |
| Engine updates | rebuild the application | replace the library file; the ABI check at startup refuses a mismatched engine |
| Choose it when | you want the smallest artifact and deployment footprint | the engine is managed as its own versioned artifact, or several binaries share one engine build |

Both modes build the same engine from the same source, so the toolchain and
examples below apply to both. The rest of this page describes the single-binary
mode unless it says otherwise; the side-binary mode has
[its own section](#side-binary-mode-a-shared-engine-beside-the-binary).

## Required Toolchain

Supported platforms are Linux with glibc for runtime environments, and macOS more
typical for development; Windows and Alpine Linux packaging is not currently provided. 

- **Rust toolchain** - 2021 / MSRV 1.78+
- **C compiler** - compiles CGO interfaces linking the engine into Rust binaries
- **Go 1.25+** - engine compile-time dependency

## Examples 

On Linux, C from `sudo apt-get install -y build-essential`, Go 1.25+ from an
official tarball (distro Go versions are often outdated), and Rust from rustup.

### Stock Debian / AMD64

```sh
# Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# C
sudo apt-get update && sudo apt-get install -y build-essential
# Go
curl -fsSL -o go.tgz "https://dl.google.com/go/go1.25.12.linux-amd64.tar.gz" \
  && sudo tar -C /usr/local -xzf go.tgz \
  && echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.profile \
  && source ~/.profile
```

### Stock RedHat / AMD64

```sh
# Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# C
sudo dnf install -y gcc
# Go
curl -fsSL -o go.tgz "https://dl.google.com/go/go1.25.12.linux-amd64.tar.gz" \
  && sudo tar -C /usr/local -xzf go.tgz \
  && echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.profile \
  && source ~/.profile
```

### macOS / ARM64

On macOS, `xcode-select --install` provides the C compiler, linker, and SDK; the
full Xcode app is not required. The binary always links `libSystem` dynamically,
and the crate will emit CoreFoundation and Security frameworks automatically.

```zsh
# Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# C
xcode-select --install
# Go
curl -fsSL -o go.tgz "https://dl.google.com/go/go1.25.12.darwin-arm64.tar.gz" \
  && sudo tar -C /usr/local -xzf go.tgz \
  && echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.zprofile \
  && source ~/.zprofile
```


### Docker scratch / AMD64

```dockerfile
FROM rust:1.83-bookworm AS build

# install Go
RUN curl -fsSL -o go.tgz "https://dl.google.com/go/go1.25.12.linux-amd64.tar.gz" \
 && tar -C /usr/local -xzf go.tgz
ENV PATH=$PATH:/usr/local/go/bin

# standalone static binary for a scratch stage, not required if you stay on image with glibc
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C target-feature=+crt-static"

# build -> /app, example:
# COPY . .
# RUN cargo build --release --target x86_64-unknown-linux-gnu \
#  && cp target/x86_64-unknown-linux-gnu/release/<your-bin> /app

FROM scratch
COPY --from=build /app /app
ENTRYPOINT ["/app"]
```

--------------------------------------------------------------------------

# Detail

The build script chooses its behaviour from two environment variables:
`LLINGR_LINK` selects the link mode (`static`, the default, or `shared`), and
`LLINGR_LIB_DIR` points at a prebuilt engine. In static mode with no
`LLINGR_LIB_DIR`, `cargo build` compiles the engine from the bundled Go source;
with `LLINGR_LIB_DIR` set it links the prebuilt `libllingr.a` there and skips
Go entirely. In shared mode `LLINGR_LIB_DIR` is required and names the
directory holding `libllingr.so`/`.dylib`.

The build script does not shell out to Docker because rust-analyzer and CI
sandboxes run build scripts constantly and a `cargo build` must stay
deterministic; if Go is missing it fails with a message suggesting three
approaches:

1. **Install Go 1.25 or newer** (and a C compiler), then `cargo build` will
   compile the engine from source as usual.
2. **Build your whole application inside the provided builder image**
   (`docker/Dockerfile`, which contains both Go and Rust), so the machine
   needs only Docker.
3. **Build the engine once in the provided container and point cargo at it.**
   `make engine` builds `dist/<target-triple>/libllingr.a`; setting
   `LLINGR_LIB_DIR=dist/<target-triple>` makes `cargo build` link that prebuilt
   archive and skip Go entirely. This suits CI caches and air-gapped builds. If
   `LLINGR_LIB_DIR` points somewhere the archive is not, the build warns and
   prints the path rather than failing with an opaque linker error.

## What static linking means here

The engine links as a static C archive (`libllingr.a`), which the build script
links into the application binary along with the Go runtime's C dependencies
(`-lpthread -lm -ldl`). The result is a single self-contained executable: there
is no `libllingr.so` to deploy beside it, no runtime library resolution, and no
`LD_LIBRARY_PATH`. For a fully static binary with no dynamic loader, the
prerequisite for a `scratch` image, build with the static C runtime, scoped to
your target triple:

```sh
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C target-feature=+crt-static"
cargo build --release --target x86_64-unknown-linux-gnu
```

Scope the flag to the target with `CARGO_TARGET_<TRIPLE>_RUSTFLAGS` and an
explicit `--target`; do not set it globally through `RUSTFLAGS`. llingr-kafka
has proc-macro dependencies, serde's and prometheus-client's derive macros,
and a proc-macro cannot be built as a static library, so a global `+crt-static`
fails the build with "cannot produce proc-macro ... target does not support these
crate types". The target-scoped form makes cargo compile proc-macros and build
scripts for the host without the flag and apply `+crt-static` only to the final
binary. On ARM, use `aarch64-unknown-linux-gnu` and the matching
`CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUSTFLAGS`.

The engine's own DNS is pure Go, built with the `netgo` tag, so the usual
static-glibc caveat about `libc` name resolution does not bite: broker addresses
resolve through Go's resolver, not the C `nsswitch` machinery.

## Side-binary mode: a shared engine beside the binary

Build the engine once as a shared library, link the application against it with
`LLINGR_LINK=shared`, and deploy the library next to the binary:

```sh
# 1. Build the shared engine, once per target triple:
make engine LINK=shared
# -> dist/<target-triple>/libllingr.so   (libllingr.dylib on macOS)

# 2. Point the application build at it:
export LLINGR_LINK=shared
export LLINGR_LIB_DIR=/path/to/dist/<target-triple>
cargo build --release

# 3. Deploy the library beside the binary:
#   order-processor/
#   |- order-processor    # the application binary
#   |- libllingr.so       # the engine, beside it
```

The build emits two RPATH entries: `$ORIGIN` (`@loader_path` on macOS) first,
so the deployed pair works anywhere it is copied as a unit, and the absolute
`LLINGR_LIB_DIR` second, so `cargo test` and `cargo run` binaries under
`target/` find the engine during development without copying it around. There
is no `ldconfig`, no `LD_LIBRARY_PATH`, and no system install.

In this mode the engine is a versioned build artifact, not something
`cargo build` compiles: `LLINGR_LIB_DIR` is required, and the build fails with
the remedy if it is unset. Replacing the deployed library with another engine
build is safe against silent mismatch, because the crate calls
`llingr_abi_version` at startup and refuses to run against a library built for
a different FFI contract.

On macOS, `make engine LINK=shared` stamps the dylib's install name as
`@rpath/libllingr.dylib` at link time and applies the ad-hoc code signature
Apple Silicon requires, so there is no `install_name_tool` step. Note that
`MODE=docker` (or `MODE=auto` resolving to Docker on a machine without Go)
produces a **Linux** `libllingr.so`: use that for container deployment, and the
native route for local macOS work.

Relative to the single binary this mode costs one more file in the deployment
and a glibc base image such as `debian:stable-slim` instead of `scratch`,
because a dynamically linked binary needs a loader. It gives you an engine that
several binaries can share, application rebuilds that never touch the Go
toolchain, and engine updates by file replacement plus restart rather than an
application rebuild.

## Using the Makefile

The `Makefile` is the single entry point, hiding the cgo and cross flags behind
four variables:

```
MODE    ?= auto      # native | docker | auto (detect)
LIBC    ?= glibc     # glibc | musl (not currently provided, fails with the upstream message)
PROFILE ?= release   # release | debug
LINK    ?= static    # static | shared (the engine artifact `make engine` produces)
```

* `make toolchains` reports what is installed and what `MODE=auto` will do;
* `make build` builds the crate; 
* `make engine` builds the standalone engine for `LLINGR_LIB_DIR` consumers:
  the static archive by default, the shared library with `LINK=shared`.
  - `MODE=auto` resolves to a native build when Go 1.25+, a C compiler, and cargo are all present, 
     otherwise builds inside the builder image
  - `MODE=docker` re-invokes the same target with `MODE=native` inside that image**, so the
    containerised build runs identical commands to the native one.

Two targets help when provisioning a build environment:

* `make doctor` proves that this environment can actually build and link the engine,
  not just that the tools are present: it builds the engine archive and then links a
  test binary against it, printing one `PROVISIONED` or `NOT PROVISIONED` verdict;
  it is native-only, so run it inside the environment you are validating.
* `make test` is self-sufficient on a fresh clone: it builds the engine before its
  checks, and does not require a `make engine` run. The full target
  list and the contributor-facing detail are in `docs/internal/BUILDING.md`.


# Cross-compilation

Cross-compilation is fiddly because cgo compiles the engine's C linker with a C 
compiler that must target the foreign architecture, so prefer a native runner for
the target architecture where one is available.

An alternative is to build the engine on a machine of the target architecture once
and include it using `LLINGR_LIB_DIR`.

The following has four parts that all point at the same target:

 * a cross C toolchain
 * the matching Rust target
 * `CC`/`CXX` pointed at the cross compiler
 * A Rust linker pointed at the same cross compiler.

### Linux x86_64 building for Linux aarch64:

```sh
sudo apt-get install -y gcc-aarch64-linux-gnu
rustup target add aarch64-unknown-linux-gnu
# The linker variable is target-scoped, so it is harmless to leave exported:
export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc
# Pass CC/CXX command-scoped (not exported) so they cannot leak into a later build:
CC=aarch64-linux-gnu-gcc CXX=aarch64-linux-gnu-g++ \
  cargo build --release --target aarch64-unknown-linux-gnu
```

## macOS Apple Silicon building for macOS Intel

This needs no environment variables because `build.rs` handles the darwin-to-darwin
cross, auto-supplying `CC="cc -arch x86_64"` (an explicit `CC` still takes precendence):

```sh
rustup target add x86_64-apple-darwin
cargo build --release --target x86_64-apple-darwin
```

The build script maps the cargo target to `GOOS`/`GOARCH`, respecting an
explicit `GOOS`/`GOARCH`export, and for a non-darwin cross build with `CC`
unset fails early.

For the Linux cross recipe, pass `CC`/`CXX` command-scoped as above rather than
exporting them: `build.rs` trusts a set `CC`, so a `CC`/`CXX` export left over
from a cross build poisons the next native build with wrong-architecture objects;
if unexported, `unset CC CXX GOOS GOARCH` before building natively again, since
the darwin recipe sets nothing and there is nothing to leak.


## Creating A Build Image

To provision a CI runner or custom container that compiles llingr-kafka, the
toolchain needs a Go 1.25+ toolchain, Rust at edition 2021 and MSRV 1.78, and a C
compiler (`gcc` or `clang`); on Linux the default approach uses is glibc.

The franz-only engine links no external C library and there is no librdkafka, so
the image stays minimal. The build sets `CGO_ENABLED=1` itself and emits the system
libraries it requires: `-lpthread -lm -ldl` on Linux and CoreFoundation and Security
on macOS, so the image only supplies the compiler and linker.

`make` is optional, only for the Makefile entry points, and `git` is not needed.

The source-of-truth versions live in the repository: the Rust MSRV in
`Cargo.toml`'s `rust-version` field, and the Go floor in `bridge/go.mod`'s `go`
directive, currently `go 1.25.0`, with `build.rs` enforcing `>= 1.25`. The proven
image recipe is the official Go image plus rustup:

```dockerfile
FROM golang:1.25-bookworm
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --default-toolchain stable --profile minimal
ENV PATH=/root/.cargo/bin:$PATH
```

Do not base a build image on Alpine today (musl is a deliberate build-time
failure; see the musl section below). The full requirements matrix, the musl
provisioning-ahead note, and the runtime-libc detail are in
`docs/internal/BUILDING.md`.


## Deploying to a scratch image

Because the single-binary mode's output is self-contained, the deployment image can be
`scratch` containing only a binary and the kernel; the side-binary mode cannot use
`scratch` and takes a glibc image instead. This is both the smallest artefact and the
smallest attack surface. The pattern is a single build stage with both toolchains, a static build, and
a `scratch` runtime stage that copies just the binary:

```dockerfile
FROM golang:1.25-bookworm AS build
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --default-toolchain stable --profile minimal
ENV PATH=/root/.cargo/bin:$PATH
# Fully static, scoped to the target triple (NOT a global RUSTFLAGS, which would
# try to build the crate's proc-macro dependencies static and fail). Use the
# aarch64 triple and its CARGO_TARGET_AARCH64_... variable on ARM.
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C target-feature=+crt-static"

WORKDIR /app
# With llingr-kafka as an ordinary crates.io dependency, cargo fetches the crate and
# its bundled Go bridge, and the crate's build script compiles the engine from those
# fetched sources inside this stage.
COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/
RUN rustup target add x86_64-unknown-linux-gnu \
 && cargo build --release --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/your-app /your-app

FROM scratch AS runtime
COPY --from=build /your-app /your-app
ENTRYPOINT ["/your-app"]
```

The single build stage contains both toolchains because the crate's build script
compiles the Go engine during `cargo build`, so the same stage that builds your
Rust code also builds the bridge, even though your Dockerfile never mentions Go
or the bridge: cargo brings them in with the dependency. The crate's own
`examples/e2e/Dockerfile` uses a path-dependency variant of this same
pattern: it additionally copies the crate's `src/` and `bridge/` into the build
stage because it builds llingr-kafka in-tree rather than fetching it from
crates.io. That is a consequence of the example living beside the crate, not
something a crates.io consumer does; `docs/example.md` walks through it. If you
cannot use `scratch`, whether a mandated base image or in-container debugging, a
distroless or debian-slim runtime stage works the same way; it is larger only
because it ships a libc.


## Third-party notices

Static linking embeds third-party Go components in your binary, and some of their
licences require attribution when you distribute the binary. The side-binary mode
moves the same components into `libllingr.so`, and distributing the library
carries the same obligation. These components are
invisible to Rust-side tooling, since cargo, cargo-deny, and the crates.io
metadata never see them, so the obligation is easy to miss. The one you must
carry is the Kafka client: **franz-go (`github.com/twmb/franz-go`) is BSD-3-Clause**,
whose licence requires its notice and licence text to accompany binary distributions;
other transitive Go dependencies carry their own permissive notices.

The repository ships a `THIRD-PARTY-NOTICES` file listing these components,
generated from the exact pinned Go modules the engine is built against, plus a
script to regenerate it when the pinned engine version moves.

## musl status

llingr-kafka runs on Linux with glibc and does not support musl/Alpine. The
embedded Go runtime segfaults during its own initialisation on musl, for two
upstream Go reasons that no musl tuning, build flag, or DNS setting fixes. The
static link mode this crate ships is the shortest path to eventual musl support,
but the fix is unmerged upstream, so the build fails with the issue tracking info.

Build against glibc (e.g. Debian or Ubuntu family images) for now. The full record and
change instructions for when the upstream fix is merged are in `docs/internal/MUSL.md`.
