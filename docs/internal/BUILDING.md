# Building (contributor notes)

This note is for people working on llingr-kafka itself: how the engine is built
from source during `cargo build`, what the build script does in what order, how
the Makefile ties it together, and the ABI discipline you keep when you touch the
boundary. The user-facing build and deployment guide is
`docs/building-packaging.md`; this page is the mechanics behind it.

Conceptually there are two artefacts. The Go engine compiles into a static C
archive (`libllingr.a`), and the Rust crate links it. Unlike the older modular
binding there is no shared library and no adapter matrix: one crate, the franz
path only, static linking only.

## What `build.rs` does, in order

The crate's `build.rs` builds and links the engine during `cargo build`. Its
behaviour, in order (the landed script is the source of truth):

1. **`DOCS_RS` set: emit nothing and return.** A docs.rs build has no network
   and no Go toolchain, so the script compiles and links nothing. This is why
   `DOCS_RS=1 cargo doc --no-deps` succeeds with no Go present.
2. **`LLINGR_LIB_DIR` set: link the prebuilt archive and skip Go entirely.** The
   script links the `libllingr.a` found in that directory and does not invoke Go.
   If the file is not there it emits a `cargo::warning` (not an error, so `cargo
   check` and rust-analyzer keep working) naming the resolved path and pointing
   at `make engine`, which builds the archive into `dist/<target-triple>/`. This
   path serves CI caches, air-gapped hosts, and `make engine` consumers.
3. **Otherwise: build the engine from `bridge/` with the Go toolchain.** The
   script requires Go, maps the cargo target to `GOOS`/`GOARCH`, and runs `go
   build` to produce the archive in `OUT_DIR`.

Docker is never invoked from `build.rs`. rust-analyzer runs build scripts
constantly, and CI sandboxes and docs.rs have no daemon, so a `cargo build` stays
deterministic; when Go is missing the script fails with a message naming the
Docker remedies rather than silently shelling out.

## The Go toolchain requirement and the three remedies

The from-source path requires **Go 1.25 or newer** on `PATH`. `build.rs` runs
`go version`, parses it, and fails if Go is absent or older than 1.25. The
failure message names three remedies, the same three in the user docs:

1. Install Go 1.25 or newer (and a C compiler), so `cargo build` compiles the
   engine from source.
2. Build the engine once with `make engine` (which can use Docker) and set
   `LLINGR_LIB_DIR=dist/<target-triple>`.
3. Build the whole application inside the provided builder image
   (`docker/Dockerfile`).

## Platforms and toolchains

llingr-kafka builds on Linux and macOS, and a build has to line up three things
for a given machine: the Rust target triple, the Go `GOOS`/`GOARCH`, and the C
compiler cgo uses. Find your row, and the rest of this section is the setup.

| Platform | Rust target triple | GOOS / GOARCH | C compiler and linker |
|---|---|---|---|
| Linux x86_64 (the reference path) | `x86_64-unknown-linux-gnu` | `linux` / `amd64` | `gcc` (from `build-essential`) |
| Linux aarch64 (AWS Graviton, Ampere) | `aarch64-unknown-linux-gnu` | `linux` / `arm64` | `gcc` natively, or `aarch64-linux-gnu-gcc` when cross-building |
| macOS Apple Silicon | `aarch64-apple-darwin` | `darwin` / `arm64` | Apple `clang` from the Xcode Command Line Tools |
| macOS Intel | `x86_64-apple-darwin` | `darwin` / `amd64` | Apple `clang` from the Xcode Command Line Tools |
| Windows (native) | not supported | not supported | not supported (use WSL2; see below) |

**All three columns must agree**, and a mismatch between them is the single most
common cause of a broken link. `build.rs` derives `GOOS`/`GOARCH` from the cargo
target for you and fails early on a cross build with no `CC` set, which is this
rule enforced rather than left to chance. On a machine building for its own
architecture, the agreement is automatic.

### Native builds, per platform (the recommended path)

Building natively on the target architecture (an ARM machine for `aarch64`, an
Intel Mac for `x86_64-apple-darwin`) is the simplest path and the one to reach
for first: the three columns line up on their own and there is no cross toolchain
to wire. Where you have a native runner for an architecture, use it.

- **Linux x86_64 and Linux aarch64.** Install a C toolchain, Go, and Rust:

  ```sh
  sudo apt-get install -y build-essential   # gcc, make, and friends
  # Go 1.25+ from the official tarball (distro packages are often older):
  #   https://go.dev/dl/  ->  extract to /usr/local/go, put its bin on PATH
  # Rust from https://rustup.rs
  ```

  aarch64 is identical; the native `gcc` targets the host arch. Gotcha: a
  distro's packaged Go is frequently older than 1.25, which `build.rs` rejects;
  install from the tarball if `go version` is behind.

- **macOS Apple Silicon and macOS Intel.** Install the Xcode Command Line Tools,
  Go, and Rust:

  ```sh
  xcode-select --install    # the C compiler, linker, and macOS SDK headers
  # Go 1.25+ from https://go.dev/dl/ (or Homebrew); Rust from https://rustup.rs
  ```

  Gotcha: you do NOT need the full Xcode app (a large download for nothing); the
  Command Line Tools provide the `clang`, linker, and macOS SDK that cgo and the
  final link need.

`rustup` installs the host target by default, so a native build needs no `rustup
target add`; that command is only for cross-compilation, below.

### macOS: what is and is not possible

Two honest limits govern macOS builds, both from the platform, not the crate:

- **There is no fully-static macOS binary.** Mach-O has no static libc, so the Go
  c-archive always links `libSystem` dynamically on macOS. The drop-into-a-
  `scratch`-image, zero-userland deployment is therefore **Linux only**; on
  macOS the binary always has a dynamic dependency on the system libraries.
  This is not something you can flag your way around.
- **musl does not apply on macOS.** musl, its build-time failure included, is
  a Linux concern; it has no meaning on darwin.

The one thing the crate does for you on macOS is automatically emit the
`CoreFoundation` and `Security` frameworks at link time on `darwin` targets, for
the darwin trust-store backend of Go's `crypto/x509`. Universal (fat) binaries are
not produced as a single build; if you need one, build each architecture
separately and merge them with `lipo`, which is untested here and offered only as
a pointer.

One benign note for macOS: running the bridge's tests with `go test -race`, as
`make coverage` does, prints a harmless linker warning on macOS only,
`ld: warning: ... malformed LC_DYSYMTAB ...`. It does not affect the build or the
test results; ignore it.

### Windows

Native Windows is **out of scope and untested**, and the barrier is real rather
than a documentation gap: Go's cgo on Windows builds with the MinGW `gcc`
toolchain while Rust defaults to the MSVC toolchain, and reconciling that
mismatch for a linked c-archive is non-trivial. The supported route on a Windows
machine is **WSL2**, which is simply the Linux `x86_64` (or `aarch64`) path above
and needs no special treatment: install the Linux toolchain inside your WSL2
distribution and build as on any Linux host.

## Target mapping and the go build

The script maps the cargo target OS to `GOOS`, `linux` to `linux` and `macos` to
`darwin`, and the arch to `GOARCH`, `x86_64` to `amd64` and `aarch64` to `arm64`,
and panics on anything outside that set. It sets `GOOS`/`GOARCH` only when the
caller has not, so a deliberate cross build that exports them wins.

cgo compiles the engine's C glue with the host `cc` unless `CC` names a cross
toolchain. So when `TARGET` differs from `HOST` and `CC` is unset, the script
fails early with the actual fix (set `CC`/`CXX` to a cross toolchain such as
`aarch64-linux-gnu-gcc`, or set `LLINGR_LIB_DIR` to a prebuilt library) rather
than letting the host compiler reject the foreign architecture with an opaque
error deep in cgo. A caller who has already set `CC` is trusted to have pointed
it at the right toolchain, and `CC`/`CXX` are passed through to cgo.

The build itself is fixed:

```
CGO_ENABLED=1 go build -tags netgo -buildmode c-archive -ldflags "-s -w" \
  -o $OUT_DIR/libllingr.a .
```

`netgo` gives pure-Go DNS so the archive works on scratch images with no
`nsswitch` machinery, and `-s -w` strips the Go-side symbol tables. Both are
hard-coded; there is no build-tag or strip override.
The link directives emitted are `rustc-link-search` on the
archive's directory, `rustc-link-lib=static=llingr`, and the Go runtime's C
dependencies after it on the link line: `-lpthread -lm -ldl`.

On macOS targets, `build.rs` additionally emits the `CoreFoundation` and
`Security` system frameworks. The engine's TLS trust store reaches the macOS
system roots through those frameworks (the darwin backend of Go's `crypto/x509`),
and a static archive leaves those symbol references unresolved for the final
link, so they must be named explicitly. The Linux link line is unchanged; this
emit is macOS-only.

## Cross-compilation

Cross-compiling, building for an architecture other than the one you are on, is
the genuinely fiddly case, because cgo compiles the engine's C glue with a C
compiler and that compiler has to target the foreign architecture. Prefer a
native runner for the target arch where you have one; reach for cross-compilation
when you do not. The recipe has four parts that must all point at the same
target:

1. Install a cross C toolchain for the target (for example `gcc-aarch64-linux-gnu`
   on a Debian/Ubuntu x86_64 host provides `aarch64-linux-gnu-gcc`).
2. Add the matching Rust target: `rustup target add aarch64-unknown-linux-gnu`.
3. Point cgo at the cross compiler: `CC=aarch64-linux-gnu-gcc` (and
   `CXX=aarch64-linux-gnu-g++`).
4. Point the Rust linker at the same cross compiler, either with the
   `CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER` environment variable or a
   `[target.aarch64-unknown-linux-gnu] linker = "aarch64-linux-gnu-gcc"` entry in
   `.cargo/config.toml`.

`build.rs` derives `GOOS`/`GOARCH` from the cargo target for you (so you do not
set them by hand) and respects an explicit `GOOS`/`GOARCH` if you export one, and
it fails early, before the opaque cgo error, if you cross-build with `CC` unset
(exit 101): `engine build: cross-compiling to x86_64-unknown-linux-gnu but CC is
unset. cgo builds the engine's C glue with the host compiler, which cannot target
another architecture. Set CC (and CXX) to a cross toolchain, e.g.
CC=aarch64-linux-gnu-gcc CXX=aarch64-linux-gnu-g++, or set LLINGR_LIB_DIR to a
prebuilt library.` The alternative to the whole recipe is to build the engine
once on a machine of the target architecture and bring it in with
`LLINGR_LIB_DIR`.

Worked example, an aarch64 host building for Linux x86_64 (validated on real
hardware: the archive members and the linked binary were verified as x86-64 with
`file` and `readelf`):

```sh
sudo apt-get install -y gcc-x86-64-linux-gnu g++-x86-64-linux-gnu
rustup target add x86_64-unknown-linux-gnu
# The linker variable is target-scoped, so it is harmless to leave exported:
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc
# Pass CC/CXX command-scoped (not exported) so they cannot leak into a later build:
CC=x86_64-linux-gnu-gcc CXX=x86_64-linux-gnu-g++ \
  cargo build --release --target x86_64-unknown-linux-gnu
```

The reverse direction, an x86_64 host building for aarch64, is symmetric: install
`gcc-aarch64-linux-gnu g++-aarch64-linux-gnu`, and use the `aarch64-linux-gnu-*`
compilers, the `aarch64-unknown-linux-gnu` target, and the matching
`CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER`.

Worked example, macOS Apple Silicon building for macOS Intel: it needs no
environment variables, because `build.rs` handles the darwin-to-darwin
cross for you by auto-supplying `CC="cc -arch x86_64"` (or `arm64`), with an
explicit `CC` still winning if you set one:

```sh
rustup target add x86_64-apple-darwin
cargo build --release --target x86_64-apple-darwin
```

This is validated on real hardware: `lipo` reports the built `libllingr.a` as
"architecture: x86_64" and `file` confirms a Mach-O x86_64 executable. As a
stronger datapoint, `cargo test --target x86_64-apple-darwin` does not merely
build: the full suite (105 tests plus 2 doctests) runs under Rosetta 2, including
the FFI boundary tests that call into the x86_64 Go engine, and the
CoreFoundation and Security frameworks link for the cross target just as they do
natively.

The sticky-exports gotcha below applies to the Linux cross recipe, which does set
`CC`/`CXX`; the darwin recipe above sets nothing, so there is nothing to leak.

Returning to native builds afterwards is easy if you keep to the prefix form, and
worth a warning if you did not: `CC`/`CXX` exports are sticky. `build.rs`
deliberately trusts a set `CC` and passes it to cgo (the target-mapping section
above), so a shell that still exports the cross compiler after a cross build
poisons the next native build with wrong-architecture objects or an opaque link
failure. The one-line prefix form in the examples above avoids this entirely,
because nothing is left set to forget; the `CARGO_TARGET_<TRIPLE>_LINKER` variable
is target-scoped by construction and harmless to leave exported, which is why it
is the better home for the linker half. If you did export `CC`/`CXX` (and
`GOOS`/`GOARCH`), run `unset CC CXX GOOS GOARCH` before building natively again.

Both cross recipes above are validated on real hardware.

## Building your own build image

If you are provisioning a CI runner or a custom container to compile llingr-kafka,
here is exactly what the toolchain must provide, so you do not have to
reverse-engineer it. The reassuring headline: the franz-only path links **no
external C library** and there is no librdkafka, so the image stays minimal, a
Go toolchain plus Rust plus a C compiler and nothing broker-specific.

### Toolchain requirements

| Requirement | Value | Source of truth |
|---|---|---|
| Rust | edition 2021, MSRV 1.78 | the `rust-version` field in `Cargo.toml` (what `cargo` checks); treat it as authoritative if it moves |
| Go | 1.25 or newer | the `go` directive in `bridge/go.mod` (currently `go 1.25.0`); `build.rs` enforces `>= 1.25` |
| C compiler | `gcc` or `clang` | cgo needs one to compile the c-archive shim; no version floor beyond what the Go release supports |
| libc (Linux) | glibc | policy: the reference environment is `golang:1.25-bookworm` (glibc 2.36); older glibc is untested |

The MSRV of 1.78 is verified, not inherited: the full suite is green on exactly
1.78, and 1.77 refuses to build with "requires rustc 1.78 or newer". What drives
it is the `llingr-nexus` 0.10.0 dependency, which declares `rust-version = "1.78"`,
and cargo enforces dependency MSRV floors. The crate's own code needs nothing
newer than `std::mem::offset_of`, stable since 1.77, so if `llingr-nexus`
lowered its floor, this crate's intrinsic floor would drop to 1.77. Either way,
`Cargo.toml`'s `rust-version` stays the source of truth.

Notes that save an image builder time:

- **cgo is enabled by the build, not the image.** `build.rs` sets `CGO_ENABLED=1`
  itself, so you do not set it in the image.
- **The c-archive's system libraries are emitted by `build.rs`**, not configured
  by the image: `-lpthread -lm -ldl` on Linux, plus the `CoreFoundation` and
  `Security` frameworks on macOS. The image just needs the compiler and linker.
- **`make` is optional and `git` is not needed.** `make` is only the entry point
  for the Makefile targets; a plain `cargo build` needs no `make`. `git` is not
  required: crates.io and the Go module proxy both fetch over HTTPS without it,
  and the reference builder image installs neither `git` nor anything
  broker-specific, only `make` and `libclang-dev` for the abi-check bindgen.
- **The libc floor is policy, not a magic number.** A binary built against a
  given glibc requires at least that glibc at runtime, unless it is built fully
  static with `crt-static` (Linux only). Build on the oldest glibc you intend to
  run on, or build static.

### Environment for an image builder

In the default native case the build reads exactly two things from the
environment: **`CC`** (the C compiler, if not the default `cc`) and the **cargo
target triple** (implicit for a native build). Nothing else. A cross build adds
the variables from the cross-compilation section above (`CC`/`CXX` and the
per-target Rust linker).

### A minimal glibc build image

The proven recipe is the official Go image plus rustup; `docker/Dockerfile`
is the living instance this skeleton is derived from:

```dockerfile
FROM golang:1.25-bookworm
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --default-toolchain stable --profile minimal
ENV PATH=/root/.cargo/bin:$PATH
# golang:1.25-bookworm already provides go, gcc, cc, and curl; add `make` only if
# you use the Makefile entry points.
```

That is enough to `cargo build` the crate: the build script compiles the engine
from source in the same image. Do not base a build image on Alpine today; for the
future musl variant see the note below and `docs/internal/MUSL.md`.

### Provisioning ahead for musl

musl is a loud build-time failure today, by design (`build.rs` rejects a `*-musl`
target rather than producing a binary that segfaults at start; the full record is
in `docs/internal/MUSL.md`). If you are provisioning an image ahead of the
upstream fix landing, the musl variant will additionally need the Rust `*-musl`
target (`rustup target add ...-musl`) and a musl C toolchain (`musl-tools`, which
provides `musl-gcc`). Until the fix lands, none of that helps: the build fails on
purpose.

### Verifying an image is correctly provisioned

Run `make doctor` inside the image (or on the freshly provisioned host). It proves
the environment can build and link the engine, not merely that the tools are
present: it builds the engine archive, then compiles and links a test binary
against it, and prints one `PROVISIONED` line or `NOT PROVISIONED: stage
'<engine|link>' failed` with a non-zero exit. It is native-only by design, so run
it in the environment you are validating rather than a Docker wrapper. See the
Makefile section above for the full description.

### What the produced binary needs at runtime

- **Linux, default (dynamic):** a glibc at least as new as the one the binary was
  built against, and nothing else (no librdkafka, no broker library).
- **Linux, `crt-static`:** nothing; the binary is fully self-contained and
  drops into a `scratch` or distroless image (see the scratch-image deployment in
  `docs/building-packaging.md`).
- **macOS:** always a dynamic dependency on `libSystem` (plus the CoreFoundation
  and Security frameworks); there is no static option on macOS.

## The musl seam and the panic-profile warning

Two more `build.rs` behaviours worth knowing:

- **The musl seam.** When the target's environment is `musl`
  (`CARGO_CFG_TARGET_ENV == "musl"`), the script panics immediately with the
  upstream-blocker message and the tracking-issue links, rather than producing a
  binary that segfaults in the Go runtime's init. This is one of three seams that
  must contain the same message (the others are the Makefile and
  `docker/Dockerfile`); the full record and the flip instructions are in
  `docs/internal/MUSL.md`.
- **The `panic = "abort"` warning.** The panic-to-dead-letter contract relies on
  unwinding, so the script emits a loud `cargo::warning` when it detects `panic =
  "abort"` in the profile. It warns rather than fails, because a consumer may
  knowingly accept a whole-process abort on the first handler panic; keep this
  warning in place.

## The Makefile: the single entry point

The `Makefile` hides every incantation behind three variables:

```
MODE    ?= auto      # native | docker | auto (detect)
LIBC    ?= glibc     # glibc | musl (musl fails with the upstream message)
PROFILE ?= release   # release | debug
```

`MODE=auto` resolves to native when Go 1.25+, a C compiler, and cargo are all
present, and to docker otherwise, an error if docker is also missing. **Docker
mode re-invokes the same make target with `MODE=native` inside the builder
image** `docker/Dockerfile`, which is `golang:1.25-bookworm` plus Rust
stable, make, and libclang for abi-check's bindgen, with the repo bind-mounted at
`/work` and the Go and cargo caches as named volumes. So the native and docker
paths run identical commands and no logic is duplicated. `make ... LIBC=musl`
fails with the shared musl message.

| Target | What it does |
|---|---|
| `make toolchains` | Reports go/cc/cargo/docker presence and versions, the host target triple, and what `MODE=auto` resolves to |
| `make engine` | Builds `dist/<target-triple>/libllingr.a` alone (for `LLINGR_LIB_DIR` consumers and CI caches; ordinary `make build` does not need it) and prints the `LLINGR_LIB_DIR` command to link it |
| `make build` | `cargo build --locked`, honouring MODE/LIBC/PROFILE |
| `make test` | Builds the engine, then `go test ./...` in `bridge/`, `cargo test --locked`, and the `abi-check` build. Self-sufficient on a fresh clone: it builds the engine first so `abi-check` finds the cgo-emitted header, with no manual `make engine` needed |
| `make doctor` | Proves this environment can build AND link the engine, not just that the tools are present; one `PROVISIONED` / `NOT PROVISIONED` verdict (see below) |
| `make lint` | `cargo fmt --check` and `cargo clippy --all-targets --locked -- -D warnings` (the same commands as CI) |
| `make docs-check` | Compiles every fenced `rust` doc sample as a `no_run` doctest (the same command as the docs-check CI job) |
| `make coverage` | Rust and Go-bridge coverage measurement (see the coverage section below) |
| `make example` / `example-up` / `example-down` / `example-verify` | The end-to-end example stack via docker compose (see `docs/example.md`) |
| `make clean` | Removes `dist/`, `target/`, `abi-check/target/`, and the example and builder images |

The `make engine`, `make test`, `make doctor`, and `make coverage` targets need
the Go composition root (`bridge/go.mod`, present) and a Go toolchain, because
they build the engine or the bridge tests.

`make doctor` is the target that validates a build image or a freshly
provisioned host: run it inside the environment you are checking. It is
native-only by design (it never shells out to Docker, which would validate the
wrong environment). It is stronger than `make toolchains`, because it exercises
the full chain and names the stage that fails: stage 1 builds
`dist/<target-triple>/libllingr.a` (proving Go, cgo, and the C toolchain), and
stage 2 compiles and LINKS the crate's test binary against that prebuilt archive
through `LLINGR_LIB_DIR` (a real link resolving the static engine and
`-lpthread -lm -ldl`, which a `cargo check` would not do). It prints a single
`PROVISIONED` line, or `NOT PROVISIONED: stage '<engine|link>' failed` with a
non-zero exit.

## Coverage

`make coverage` measures both domains of the crate: `cargo llvm-cov` over the
Rust modules in `src/`, and `go test -coverprofile` over the Go bridge in
`bridge/`. It needs a Go toolchain on `PATH`, because the Rust tests build the
bridge through `build.rs`. It is a measurement command that emits
`coverage-rust.lcov` and `bridge/coverage-bridge.out` with a human-readable
summary per domain; the regression gate lives in CI, not in this target.

CI enforces local, deterministic regression floors rather than ratchets:
`RUST_MIN_LINES` = 92, currently 94.36% lines, and `GO_BRIDGE_MIN_LINES` = 74,
currently 76.9% statements, with codecov as the reporting layer under the `rust`
and `go-bridge` flags. The floors live in `.github/workflows/coverage.yml`; that file is the
source of truth if the numbers move.

The floors are deliberately below 100%, because some paths are not unit-coverable
and are covered by other suites instead, honestly accounted for rather than
chased with brittle tests:

- The **live engine lifecycle** (the success paths of `build()` and `run()`), the
  **C-ABI `//export` functions**, and the **live-broker paths** cannot be unit
  tested in-process. They are exercised by the Rust boundary tests (which feed
  synthesised C inputs to the trampolines against the linked archive) and by the
  compose example end-to-end run (`make example-verify`, see `docs/example.md`).
- The **`panic = "abort"` path** is excluded, because the test profile is
  `unwind`; the abort behaviour is a build-time warning, documented in
  `docs/internal/ARCHITECTURE.md`, not a runtime path a test can take.

## The ABI discipline

The FFI contract version is **v1**, defined in two places that move together:
`abiVersion` in the bridge's `main.go` and `LLINGR_ABI_VERSION` in `src/ffi.rs`.
Bump both on any change to an exported signature or a callback typedef, record
why next to each, and rebuild the engine. `Builder::build()` refuses to run
against a mismatched library, turning a skewed layout into a clean startup error
instead of silent corruption. The `abi-check/` tool is the compile-time drift
guard: it includes the crate's real `src/ffi.rs` through a `#[path]` attribute
(so it never builds the Go engine), reads the cgo-emitted header at
`dist/<target-triple>/libllingr.h`, regenerates the C contract from it, and fails
the build if the declarations drifted. It keeps a LOCAL, gitignored lockfile
(`abi-check/abi.lock`) recording the contract version and signature (at v1,
`version = 1`, signature `e419c328dcfbc4d2`). A deliberate ABI change is made with
`UPDATE_ABI_LOCK=1`, so the lock is never shared or reviewed. The boundary
internals this guards are described in `docs/internal/ARCHITECTURE.md`.

## Updating the pinned engine

The engine version lives in `bridge/go.mod` (published releases only, never a
`replace` to a local checkout). To move it: update the module version in `bridge/go.mod`,
run `go mod tidy` in `bridge/`, rebuild, and run `make test`. If the nexus
contract changed (new metrics fields, new traits, a changed callback
signature), the Rust types and the ABI version must move with it: extend
`ffi.rs` and the affected modules, and increment both ABI constants when any C
signature changed. Updating the pinned engine does not do that for you.

## Repository layout

```
Cargo.toml            package llingr-kafka, links = "llingr", no cargo features
build.rs              builds bridge/ into a static c-archive and links it (above)
bridge/               the Go composition root (package main, AGPL), pins published modules
src/
  lib.rs              public API and re-exports (nexus contract types at the root)
  ffi.rs              raw extern "C" declarations, repr(C) types, the ABI constant
  engine.rs           Builder + Llingr (run/stop/emergency_stop/snapshot)
  config.rs           DemuxConfig -> bridge config JSON
  options.rs          the typed Options Kafka-client builder
  trampolines.rs      C callback marshalling + catch_unwind
  logging.rs          engine log -> log facade, target "llingr"
  metrics.rs          Prometheus sinks, Metrics::serve / registry, the exporter
  snapshot.rs         typed serde structs over the snapshot document
abi-check/            the bindgen drift guard (dev tooling, not published)
docker/Dockerfile     the both-toolchains image MODE=docker builds inside
Makefile              the single entry point
docs/                 you are here (docs/internal/ for contributor notes)
```

The boundary modules (`src/ffi.rs`, `src/engine.rs`, `src/config.rs`,
`src/trampolines.rs`, `src/snapshot.rs`), the Go composition root `bridge/`, and
`abi-check/` were adapted from the llingr-rs donor working tree and are all
present.
