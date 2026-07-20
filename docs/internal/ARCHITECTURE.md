# Architecture

This note is for contributors. It explains how llingr-kafka embeds the real
llingr-demux engine, what crosses the FFI boundary and in what form, and the ABI
discipline that keeps the two sides in step. The user-facing behaviour built on
top of this lives in the `docs/` pages; here we are under the hood.

## The design of the binding

llingr-kafka does not reimplement the engine. It embeds the actual Go
llingr-demux engine and its franz-go broker layer, compiled together into a
single static C archive (`libllingr.a`) with Go's `c-archive` build mode, and
links that archive directly into the Rust binary. Rust exchanges data with it
across a small, versioned C ABI. The consequence to internalise is that
everything the engine guarantees in Go it guarantees here, because it is the
same engine running unmodified: per-key ordering, contiguous gap-buffer offset
commits, drain-before-rebalance, and the at-least-once contract are not
re-derived on the Rust side.

This crate is deliberately one crate and one broker. There is no adapter
abstraction: the franz-go path is unconditional. By default the archive links
statically, which is what makes the deployable a single self-contained binary
with no `.so` beside it; the alternative side-binary mode (`LLINGR_LINK=shared`)
links the same engine as a shared library deployed beside the binary, with the
same FFI surface and the same ABI handshake.

```
Kafka / RedPanda / MSK
    |
Go engine, linked into the application binary as a static c-archive (libllingr.a)
    |   franz-go (pure Go) polls the broker
    |   -> llingr-demux pipeline: poll -> FNV-1a key route -> per-key workers
    |      -> gap-buffer offset committer -> broker commit
    |
C FFI boundary (function pointers)                [src/ffi.rs: extern "C" + ABI constant]
    |
Rust safe wrapper (this crate)                    [engine.rs, trampolines.rs, config.rs,
    |                                               options.rs, logging.rs, metrics.rs, snapshot.rs]
    |   contract vocabulary from the llingr-nexus crate (Message, Traits, handler traits)
    |
Your application (ProcessHandler and DeadLetterHandler)
```

## The two sides of the boundary

**The Go bridge (`bridge/`).** A small `package main`, AGPL-3.0-only like the
engine, compiled into the archive. It contains the C preamble (callback typedefs
and the trampolines cgo needs, because cgo cannot invoke a C function pointer
directly), the exported C functions, the JSON configuration contract and its
validation, and the adapter wiring, collapsed to the franz path only. It pins
the published Go modules (`llingr-demux`, `llingr-adapter-franz`, `llingr-nexus`
Go, and `twmb/franz-go`) in its own `go.mod`/`go.sum`, so the engine is fetched
and verified from the Go module proxy rather than any local checkout.

**The Rust side (this crate).** A thin, safe wrapper split across the `src/`
modules: `ffi.rs` (the raw `extern "C"` declarations, the `repr(C)` types, and
the ABI version constant), `engine.rs` (the `Builder`, the `Llingr` handle, and
the run/stop/emergency_stop/snapshot lifecycle), `trampolines.rs` (the C
callback marshalling that catches handler panics), `config.rs` (the
`DemuxConfig` engine tuning serialised to the bridge's config JSON), `options.rs`
(the typed `Options` Kafka-client builder), `logging.rs` (the engine-log to
`log`-facade routing under the target `llingr`), `metrics.rs` (the Prometheus
sinks, `Metrics::serve`/`registry`, and the built-in exporter), and `snapshot.rs`
(the typed serde structs over the snapshot document). The contract vocabulary
(`Message`, `Traits`, the handler traits) is not defined here: it lives in, and
is versioned by, the published `llingr-nexus` crate, and is re-exported at this
crate's root so one `use llingr_kafka::...` suffices.

One-instance-per-process is enforced at the Cargo level as well as at runtime.
`Cargo.toml` declares `links = "llingr"`, so cargo refuses a dependency graph in
which two crates claim the same native library; the Go runtime is process-global,
so a second engine in one process is never valid.

## What crosses raw, and what crosses as a document

The rubric, fixed at design time, is: **the ABI carries observations and
events; documents cross as documents.** Applying it:

- **Per-message metrics cross raw.** Each metrics callback delivers raw C scalars
  (the trait bit field, queue depth, partition, offset, and four timings). No
  Prometheus text and no JSON exists anywhere on the Go side of the metrics path;
  the Rust `metrics.rs` aggregates the scalars into prometheus-client registries
  and renders the exposition format only at scrape time.
- **Bandwidth telemetry crosses raw.** Broker topology and per-partition byte
  counters cross as `repr(C)` structs, their arrays as C-allocated arenas valid
  for the callback's duration, with layouts pinned by `offset_of!` tests.
- **Records cross raw.** The process and dead-letter callbacks hand over raw
  pointers and lengths, with headers as a `repr(C)` array. A `value_len` of `-1`
  marks a NULL value (a tombstone), which is distinct from an empty value.
- **The snapshot crosses as a document.** `llingr_take_snapshot` returns the
  engine's canonical JSON document as a C string (freed with
  `llingr_free_string`), and `snapshot.rs` ships typed serde structs over it.
  The snapshot is cold, large, and fast-evolving diagnostics, so a `repr(C)`
  layout would chain every new diagnostic field to an ABI version increment; the JSON document
  is deliberately byte-identical to what the Go engine's own HTTP snapshot
  handler serves, and serde's tolerance of unknown fields gives forward
  compatibility.

## The exported C ABI

The bridge exports these functions (matching the `extern "C"` declarations in the
landed `src/ffi.rs`):

| Function | Purpose |
|---|---|
| `llingr_abi_version()` | Returns the FFI contract version; checked before anything else |
| `llingr_init(json, len, err_buf, err_cap, err_len_out)` | Parses config, builds the demux consumer and the franz adapter, connects. Returns 0 or a stable negative code with error text in `err_buf` |
| `llingr_run()` | Subscribes and blocks until shutdown or a fatal error |
| `llingr_stop()` | Graceful shutdown: drain in-flight work, commit, return from run |
| `llingr_emergency_stop(reason, len)` | Abandon in-flight work and stop now; the shutdown callback fires with the reason |
| `llingr_take_snapshot()` | Point-in-time engine state as a C-allocated JSON string; caller frees with `llingr_free_string` |
| `llingr_free_string(s)` | Releases a string returned by the bridge |
| `llingr_on_process` / `llingr_on_deadletter` / `llingr_on_metrics` / `llingr_on_shutdown` / `llingr_on_log` / `llingr_on_bandwidth` | Register the C function pointers Go calls; registering the bandwidth callback also enables bandwidth collection |

## Panic and failure containment

A Rust panic unwinding out of an `extern "C"` function aborts the process, so
every trampoline body runs under `catch_unwind`. What a caught panic does then
depends on whether the handler has a failure path:

- A panicking `ProcessHandler` becomes a dead-lettered message, the same outcome
  as returning an error: the trampoline reports failure and the message is routed
  to the dead-letter handler.
- A panicking `DeadLetterHandler` is caught the same way and treated as the
  dead-letter handler failing, identical to it returning an error: the dead-letter
  trampoline returns the same failure code for a caught panic as for an `Err`, and
  a dead-letter failure triggers the engine's emergency shutdown. This is
  deliberate: a message that can be neither processed nor dead-lettered must not
  have its offset advanced, so the engine stops rather than lose it.
- Panics in the handlers that have no failure path (metrics, log, shutdown,
  bandwidth) are swallowed after a stderr diagnostic, because there is nothing to
  route them to.

All of this relies on the `unwind` panic profile, which is Rust's default. The
crate's `build.rs` detects `panic = "abort"` at build time and emits a loud
`cargo::warning`, not a hard error, because a consumer may knowingly accept
process-per-message semantics: under `abort` the first handler panic kills the
whole process before `catch_unwind` can run. The `ProcessHandler` and
`DeadLetterHandler` panic behaviour above is confirmed in the landed
`src/trampolines.rs`, where both the `Err` return and a caught panic yield the
same dead-letter failure code.

Symmetrically, a Go panic escaping to the C boundary would kill the host process,
so the bridge recovers at the entry points that execute engine code
(`llingr_init`, where the engine deliberately panics on invalid configuration,
and `llingr_run`), converting them to error codes and text.

## Memory ownership across the boundary

The rules are few and strict, and the user-facing consequence (copy out what you
keep) is documented in `docs/processing.md`:

- **Message data flows Go-to-Rust as borrowed pointers**, valid only for the
  duration of the callback. `Message<'a>` encodes this as a lifetime; retaining
  the bytes past the callback is not a crash but silently wrong data, because the
  buffers are recycled. Copy out anything kept with `to_vec` or `to_string`.
- **Error text flows Rust-to-Go through a Go-owned buffer** `err_buf`, of fixed
  capacity and truncated at a UTF-8 boundary, so no allocation crosses the
  allocator boundary in either direction.
- **Strings from Go have no UTF-8 guarantee**, since they can embed broker bytes,
  so dead-letter reasons, shutdown reasons, and log lines are decoded lossily on
  the Rust side. The partition key is the exception: the adapter guarantees it
  UTF-8-safe by construction.

## Callback registration and threading

Callback registration is thread-safe and internally synchronised: the
`llingr_on_*` setters publish through an atomic pointer that every engine
goroutine reads through, so a host may register on one thread and initialise on
another without providing its own ordering; registration must complete before
`llingr_init` (the set is sealed on the first successful init, and later
registration calls are ignored with a stderr notice). Under the hood the setters
copy-on-write under a registration mutex and publish with an atomic store, and
every read site (the per-message closures on engine goroutines, the log emitter,
the build-time enable checks) goes through the matching atomic load; that
store/load pair is the one synchronising edge the Go memory model guarantees,
which is why the bridge supplies it itself rather than trusting the host's own
ordering across the two separate cgo calls. A failed init does not seal the set,
so the facade's documented build-retry path re-registers cleanly.

## ABI versioning and the drift guard

The FFI contract has an integer version, currently **v1**, defined twice on
purpose: `abiVersion` in the bridge's `main.go` and `LLINGR_ABI_VERSION` in
`src/ffi.rs`. `Builder::build()` compares the crate's constant with the loaded
library's report and refuses to run on a mismatch, converting what would be
silent memory corruption (a skewed struct layout or signature) into a clean
startup error. Both constants move together on any change to an exported
signature or a callback typedef, with the reason recorded next to each. v1 is the
first released contract; the unpublished revisions that preceded it were
renumbered away.

A separate `abi-check/` tool is the compile-time drift guard. It includes the
crate's real FFI declarations from `src/ffi.rs` through a `#[path]` attribute, so
it never builds the Go engine itself; discovers the cgo-emitted header at
`dist/<target-triple>/libllingr.h`; regenerates the C contract from it; and fails
compilation if the declarations have drifted. It keeps a LOCAL,
gitignored lockfile `abi-check/abi.lock`, listed in `.gitignore`, that records
the contract version and a signature; at ABI v1 the lock reads `version = 1` with
signature `e419c328dcfbc4d2`. A deliberate ABI change is made with
`UPDATE_ABI_LOCK=1`, so the lock is never shared or reviewed, and `make test`
builds `abi-check` as part of the suite.
