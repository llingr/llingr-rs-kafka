# musl status and the libc seam

This note exists so nobody re-litigates or re-discovers why llingr-kafka is
glibc-only today, and so that whoever picks up musl support when the upstream
blocker lands knows exactly where the three seams are and how to flip them.
Short version: musl is parked on an unmerged Go fix, not designed out. The crate
is built so that enabling it later is a small, localised change, and every
place that could hard-code the libc is parameterised and defaults to glibc.

## Current status

llingr-kafka runs on Linux with glibc and builds on macOS for development. It
does not run on musl/Alpine in any link mode: the embedded Go runtime segfaults
during its own initialisation, before any application code runs. This is not a
musl misconfiguration, and no musl tuning, build flag, or DNS setting fixes it.
The cause is two upstream Go issues, one of which our chosen link mode sidesteps
and one of which we do not hit.

## Why it fails: the Go runtime as a guest library

Our direction is the awkward one. cgo-on-musl works in general when Go is the
*host* process linking a C library, as with confluent-kafka-go on Alpine. We do
the reverse: the Go runtime is a *guest* library inside a non-Go process, your
Rust binary, and it has to bootstrap itself from that host.

- **Missing argv/envp, the blocker that matters.** When the Go runtime is
  embedded through `c-shared` or `c-archive`, it bootstraps from an ELF
  `.init_array` constructor and assumes glibc's convention of passing
  `(argc, argv, envp)` to that constructor. The ELF specification does not
  require this, and musl does not pass it, so the Go runtime dereferences
  garbage and crashes in runtime init. Tracked as
  [golang/go#13492](https://github.com/golang/go/issues/13492); the fix is
  [PR #69325](https://github.com/golang/go/pull/69325), still unmerged.
- **Initial-Exec TLS, a second blocker but only for the dlopen route.** Go
  forces the Initial-Exec TLS model, which musl deliberately refuses for
  dynamically loaded libraries. That is
  [golang/go#48596](https://github.com/golang/go/issues/48596); a
  general-dynamic TLS proposal,
  [golang/go#71953](https://github.com/golang/go/issues/71953), is a
  work in progress. The musl side of the refusal is
  [this commit](https://git.musl-libc.org/cgit/musl/commit/?id=5c2f46a214fceeee3c3e41700c51415e0a4f1acd).

## Why static linking is the shortest path

llingr-kafka links the engine as a static `c-archive`, and that choice is the
shortest path to musl support, not a coincidence. A statically linked
`c-archive` puts the runtime's thread-local storage in the main program, so the
Initial-Exec TLS blocker above does not apply: it is a dlopen problem, and there
is no dlopen here. That leaves only the first blocker, the missing argv/envp in
`.init_array`. So the day PR #69325, or an equivalent, merges into a Go release
we can pin, static-c-archive-on-musl becomes reachable with only that one fix,
while the dlopen route would still be waiting on the TLS work as well.

## What is not the cause

Pure-Go DNS (the `netgo` build tag) is a red herring here. The crash happens in
runtime init, which runs before any name resolution, so `netgo` versus the cgo
resolver makes no difference to it. Do not spend time toggling DNS settings
chasing this; the segfault predates DNS entirely.

## The empirical record

This is verified, not theorised. The
`llingr-rs-packaging-examples/rust/README.md` investigation ran the engine on
musl in every link mode (dynamic, static, and static-pie) and recorded a
segfault in Go's runtime init every time. The glibc build of the same code, in
the same modes, runs fine. When the upstream fix lands and someone re-tests,
that README is the record to update alongside this note.

## The seam: exactly three places, all defaulting to glibc

Everything that could bind the crate to a libc is parameterised. There are
exactly three seams, and nothing else may hard-code the libc. When you enable
musl, you touch these three and only these three.

1. **The `*-musl` branch in `build.rs`.** Today it is the honest failure:
   when the target's C environment is musl, detected via
   `CARGO_CFG_TARGET_ENV == "musl"`, the build panics immediately with a message
   that names the target, explains the `.init_array` argv/envp cause, and
   includes both issue links: golang/go#13492 with fix PR #69325, and
   golang/go#48596 for the dlopen TLS blocker. A musl target therefore produces
   a clear build error rather than a binary that segfaults at start. When the
   upstream fix lands, this branch becomes the CC and target-triple mapping for
   musl, exactly as the glibc branch maps the triple to `GOOS`/`GOARCH` and the
   C toolchain today.
2. **`ARG LIBC` in `docker/Dockerfile`.** The builder image is
   parameterised on `LIBC`, which selects the base image and the Rust target.
   It defaults to `glibc`, a Debian base; the musl branch exits with the same
   upstream-blocker message the build.rs branch emits, so the Docker path is
   honest in the same way. Flipping musl on means giving this branch a real
   musl base image and Rust target instead of the early exit.
3. **This document.** It holds the two issue links and the flip instructions, so
   the knowledge lives in one place and the code seams stay terse. Keep the
   links here current; the build.rs and Dockerfile messages should point a
   reader at this note.

All three seams are landed and emit the same message; `build.rs` and
`docker/Dockerfile` both point a reader here.

## Flip instructions (for when the blocker clears)

When a Go release that includes the argv/envp fix (PR #69325 or equivalent) is
available and pinnable:

1. Pin the bridge's `go.mod` to a Go toolchain version that includes the fix, and
   rebuild the engine on musl to confirm the runtime now initialises.
2. Replace the `*-musl` early-failure branch in `build.rs` with the real musl
   target and C-toolchain mapping, mirroring the glibc branch, so a musl
   target triple builds instead of failing.
3. Replace the musl early-exit branch behind `ARG LIBC` in
   `docker/Dockerfile` with a real musl base image and Rust target.
4. Re-run the `llingr-rs-packaging-examples/rust` musl investigation to confirm
   the segfault is gone in the static `c-archive` mode, and update that README
   and this note with the new status.
5. Only then advertise musl support in the user-facing docs
   (`docs/building-packaging.md` and the README build-modes section); until all
   of the above pass, musl stays documented as unsupported.
