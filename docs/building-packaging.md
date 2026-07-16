# Building and packaging

The goal here is a self-contained binary you can drop into a tiny container
image. Because llingr-kafka links the engine statically, that is exactly what
you get: one binary with no shared library beside it, no `rpath`, and no
`LD_LIBRARY_PATH` to set, which slides cleanly into a `scratch` image of roughly
16 MB. This page covers the ordinary build, the ways to build when Go is not on
the machine, cross-compilation, and shipping the result, along with the
third-party-notices obligation and the musl status you plan around.

## The ordinary build

Most of the time you build with plain `cargo build`. The crate's build script
compiles the engine from source the first time into a static archive, fetching
the pinned Go modules from the module proxy and verifying them against `go.sum`,
and links it into your binary. After that first build the archive is cached like any
build artefact, so subsequent builds are ordinary-speed.

That first build needs a toolchain:

- **A recent stable Rust toolchain.**
- **Go 1.25 or newer** on `PATH`. The engine is compiled from source during the
  build, so Go is a build-time requirement, never a runtime one.
- **A C compiler.** cgo compiles the engine's C glue with it.

Supported platforms are Linux with glibc for production and macOS for
development. Documentation builds are the exception to the Go requirement: under
`DOCS_RS` the build script emits nothing, so `DOCS_RS=1 cargo doc --no-deps`
succeeds with no Go toolchain present, which is how docs.rs builds the crate.

## Platforms

Find your operating system and chip; the build lines up three things for it, the
Rust target triple, the Go `GOOS`/`GOARCH`, and the C compiler cgo uses, and they
must all agree; a mismatch is the commonest cause of a broken link. On a machine
building for its own architecture, they agree automatically.

| Platform | Rust target triple | GOOS / GOARCH | C compiler |
|---|---|---|---|
| Linux x86_64 (reference) | `x86_64-unknown-linux-gnu` | `linux` / `amd64` | `gcc` (`build-essential`) |
| Linux aarch64 (Graviton, Ampere) | `aarch64-unknown-linux-gnu` | `linux` / `arm64` | `gcc`, or `aarch64-linux-gnu-gcc` to cross-build |
| macOS Apple Silicon | `aarch64-apple-darwin` | `darwin` / `arm64` | Apple `clang` (Command Line Tools) |
| macOS Intel | `x86_64-apple-darwin` | `darwin` / `amd64` | Apple `clang` (Command Line Tools) |
| Windows (native) | not supported | not supported | use WSL2 (see below) |

**Building natively on the target chip is the simplest path, so use it where you
can.** On Linux, `sudo apt-get install -y build-essential`, Go 1.25+ from the
official tarball since distro Go is often too old, and Rust from rustup. On
macOS, `xcode-select --install` gives you the C compiler, linker, and SDK; you
do not need the full Xcode app.

**macOS has two honest limits.** There is no fully-static macOS binary, because
Mach-O has no static libc, so drop-into-`scratch` deployment is Linux only; on
macOS the binary always links `libSystem` dynamically. And musl does not apply
on macOS. The crate emits the CoreFoundation and Security frameworks for you
on macOS.

**Windows** is out of scope and untested natively: Go's cgo uses the MinGW
toolchain while Rust defaults to MSVC, and reconciling that for a linked
c-archive is non-trivial. The supported route is WSL2, which is just the Linux
path above; install the Linux toolchain inside WSL2 and build as on any Linux
host.

## When Go is not on the machine

The build script never shells out to Docker on its own, because rust-analyzer
and CI sandboxes run build scripts constantly and a `cargo build` must stay
deterministic, so if Go is missing it fails with a message naming three
remedies:

1. **Install Go 1.25 or newer** (and a C compiler), then `cargo build` compiles
   the engine from source as usual.
2. **Build the engine once in the provided container and point cargo at it.**
   `make engine` builds `dist/<target-triple>/libllingr.a`; setting
   `LLINGR_LIB_DIR=dist/<target-triple>` makes `cargo build` link that prebuilt
   archive and skip Go entirely. This suits CI caches and air-gapped builds. If
   `LLINGR_LIB_DIR` points somewhere the archive is not, the build warns and
   names the path rather than failing with an opaque linker error.
3. **Build your whole application inside the provided builder image**
   (`docker/Dockerfile`, which contains both Go and Rust), so the machine
   needs only Docker.

## What static linking means here

The engine links as a static C archive (`libllingr.a`), which the build script
folds into your binary along with the Go runtime's C dependencies
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

## Building through the Makefile

The `Makefile` is the single entry point, hiding the cgo and cross flags behind
three variables:

```
MODE    ?= auto      # native | docker | auto (detect)
LIBC    ?= glibc     # glibc | musl (musl fails with the upstream message)
PROFILE ?= release   # release | debug
```

`make toolchains` reports what is installed and what `MODE=auto` will do;
`make build` builds the crate; `make engine` builds the standalone archive for
`LLINGR_LIB_DIR` consumers. `MODE=auto` resolves to a native build when Go 1.25+,
a C compiler, and cargo are all present, and otherwise builds inside the builder
image; **`MODE=docker` re-invokes the same target with `MODE=native` inside that
image**, so the containerised build runs identical commands to the native one.

Two targets are worth knowing when you are provisioning a build environment.
`make doctor` proves that this environment can actually build and link the engine,
not just that the tools are present: it builds the engine archive and then links a
test binary against it, printing one `PROVISIONED` or `NOT PROVISIONED` verdict;
it is native-only, so run it inside the environment you are validating. And
`make test` is self-sufficient on a fresh clone: it builds the engine before its
checks, so you never need to run `make engine` by hand first. The full target
list and the contributor-facing detail are in `docs/internal/BUILDING.md`.

## Cross-compilation

Prefer a native runner for the target architecture where you have one;
cross-compilation is the fiddly case, because cgo compiles the engine's C glue
with a C compiler that must target the foreign architecture. The recipe has four
parts that all point at the same target: a cross C toolchain, the matching Rust
target, `CC`/`CXX` pointed at the cross compiler, and the Rust linker pointed at
the same cross compiler. Linux x86_64 building for Linux aarch64:

```sh
sudo apt-get install -y gcc-aarch64-linux-gnu
rustup target add aarch64-unknown-linux-gnu
# The linker variable is target-scoped, so it is harmless to leave exported:
export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc
# Pass CC/CXX command-scoped (not exported) so they cannot leak into a later build:
CC=aarch64-linux-gnu-gcc CXX=aarch64-linux-gnu-g++ \
  cargo build --release --target aarch64-unknown-linux-gnu
```

macOS Apple Silicon building for macOS Intel needs no environment variables,
because `build.rs` handles the darwin-to-darwin cross for you, auto-supplying
`CC="cc -arch x86_64"` with an explicit `CC` still winning:

```sh
rustup target add x86_64-apple-darwin
cargo build --release --target x86_64-apple-darwin
```

The build script maps the cargo target to `GOOS`/`GOARCH` for you, respecting an
explicit `GOOS`/`GOARCH` you export, and for a non-darwin cross build with `CC`
unset it fails early and says exactly that, rather than letting the host compiler
reject the foreign architecture deep in cgo. For the Linux cross recipe, pass
`CC`/`CXX` command-scoped as above rather than exporting them: `build.rs` trusts a
set `CC`, so a `CC`/`CXX` export left over from a cross build poisons the next
native build with wrong-architecture objects; if you did export them, `unset CC
CXX GOOS GOARCH` before building natively again, since the darwin recipe sets
nothing and there is nothing to leak. The alternative to the whole recipe is to build the
engine on a machine of the target architecture once and bring it in with
`LLINGR_LIB_DIR`. Both recipes above are validated on real hardware.

## Building your own build image

To provision a CI runner or custom container that compiles llingr-kafka, the
toolchain needs a Go 1.25+ toolchain, Rust at edition 2021 and MSRV 1.78, and a C
compiler (`gcc` or `clang`); on Linux the default path is glibc. The franz-only
engine links no external C library and there is no librdkafka, so the image stays
minimal. The build sets `CGO_ENABLED=1` itself and emits the system libraries it
needs, `-lpthread -lm -ldl` on Linux and CoreFoundation and Security on macOS, so
the image only supplies the compiler and linker. `make` is optional, only for the
Makefile entry points, and `git` is not needed.

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

Because the binary is self-contained, the deployment image can be `scratch`: no
libc, no shell, no package manager, nothing but your binary and the kernel. That
is both the smallest artefact and the smallest attack surface. The pattern is a
single build stage with both toolchains, a static build, and a `scratch`
runtime stage that copies just the binary:

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
# Copy only YOUR application. With llingr-kafka as an ordinary crates.io
# dependency, cargo fetches the crate and its bundled Go bridge, and the crate's
# build script compiles the engine from those fetched sources inside this stage.
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
`examples/e2e/Dockerfile.consumer` uses a path-dependency variant of this same
pattern: it additionally copies the crate's `src/` and `bridge/` into the build
stage because it builds llingr-kafka in-tree rather than fetching it from
crates.io. That is a consequence of the example living beside the crate, not
something a crates.io consumer does; `docs/example.md` walks through it. If you
cannot use `scratch`, whether a mandated base image or in-container debugging, a
distroless or debian-slim runtime stage works the same way; it is larger only
because it ships a libc.

## The third-party-notices obligation

Static linking embeds third-party Go components in your binary, and some of their
licences require attribution when you distribute the binary. These components are
invisible to Rust-side tooling, since cargo, cargo-deny, and the crates.io
metadata never see them, so the obligation is easy to miss. The one you must
carry is the Kafka client: **franz-go (`github.com/twmb/franz-go`) is BSD-3-Clause**, whose
licence requires its notice and licence text to accompany binary distributions;
other transitive Go dependencies carry their own permissive notices.

The repository ships a `THIRD-PARTY-NOTICES` file listing these components,
generated from the exact pinned Go modules the engine is built against, plus a
script to regenerate it when the pinned engine version moves. When you distribute
a binary built from llingr-kafka, include `THIRD-PARTY-NOTICES` alongside it (in
the image, in the release archive, wherever your users can find it). This
obligation is independent of the AGPL/commercial choice you took the crate under;
it comes from the bundled permissive components. The dual-licence side of things
is in `docs/licensing.md`.

## musl status

llingr-kafka runs on Linux with glibc and does not support musl/Alpine. The
embedded Go runtime segfaults during its own initialisation on musl, for two
upstream Go reasons that no musl tuning, build flag, or DNS setting fixes. The
static link mode this crate ships is the shortest path to eventual musl support,
but the fix is unmerged upstream, so the build fails loudly with the
tracking-issue links if you target a `*-musl` triple, rather than producing a
binary that crashes at start. Build against glibc (Debian or Ubuntu family
images) for now. The full record and the flip instructions for when the upstream
fix lands are in `docs/internal/MUSL.md`.
