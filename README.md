# llingr-kafka

An ordered, per-key-concurrent Kafka consumer for Rust, with the engine, the
broker client, offset management, Prometheus metrics, and log routing already
built in. You implement one trait to process messages; llingr-kafka supplies
everything else, compiled into your binary. One crate to depend on, one broker
family to point it at (Apache Kafka and any Kafka-compatible broker such as
RedPanda or Amazon MSK), and no runtime services to run alongside it.

Underneath the safe Rust surface is the real llingr-demux engine: written in Go,
its offset and pipeline mechanisms formally verified with TLA+, and compiled into
a static C archive that links directly into your Rust binary. Everything the engine guarantees in Go it guarantees here,
because it is the same engine. Per-key ordering, contiguous offset commits,
drain-before-rebalance, and at-least-once delivery all run unmodified; the Rust
side is a thin, safe wrapper that hands each message to your handler.

The one thing it buys you: no head-of-line blocking. A plain Kafka consumer
processes a partition one record at a time, so a slow message for one key stalls
every other key on that partition. llingr-kafka routes each record by key to its
own concurrent worker, so a slow key holds up only itself while offsets still
commit in contiguous order.

## Quick start

```rust
use llingr_kafka::{Builder, Options, AutoOffsetReset, Metrics, DemuxConfig,
                   Message, Traits, ProcessHandler, DeadLetterHandler};
use std::time::Duration;

struct Orders;
impl ProcessHandler for Orders {
    fn process(&self, msg: &Message) -> Result<Traits, Box<dyn std::error::Error>> {
        // msg borrows Go-owned memory for this call only; copy out what you keep.
        let body = msg.value_str().unwrap_or("<binary>");
        println!("key={} partition={} offset={} value={}",
            msg.key_str().unwrap_or(""), msg.partition(), msg.offset(), body);
        Ok(Traits::none())
    }
}

// Required: a message that fails must have somewhere to go before its offset
// commits, or it would be dropped silently. Logging is the minimum; a real
// deployment publishes to a DLQ topic, a table, or object store.
struct DeadLetters;
impl DeadLetterHandler for DeadLetters {
    fn handle(&self, msg: &Message, error: &str) -> Result<(), Box<dyn std::error::Error>> {
        eprintln!("dead-letter key={} reason={}", msg.key_str().unwrap_or(""), error);
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let password = std::env::var("KAFKA_SASL_PASSWORD")?;

    let engine = Builder::new("orders", Orders, DeadLetters)
        .brokers("b1:9092,b2:9092")
        .consumer_group("orders-svc")
        .options(Options::new()                        // typed Kafka client options
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .session_timeout(Duration::from_secs(30))
            .tls_ca_location("/etc/ssl/ca.pem")
            .sasl_scram_sha256("svc", &password))
        .demux(DemuxConfig::new().concurrent_keys(500)) // engine tuning, optional
        .metrics(Metrics::serve("0.0.0.0:9464", "/metrics")) // optional
        .build()?;

    let stop = engine.stopper();   // Send closure for a signal-watcher thread
    let _ = stop;                  // wire it to your signals: see docs/operations.md
    engine.run()?;                 // blocks until stop() or emergency_stop()
    Ok(())
}
```

The required shape is a topic and two handlers; everything else on the builder
is optional. `Builder`, `Message`, `Traits`, `ProcessHandler`, and
`DeadLetterHandler` all come from a single `use llingr_kafka::...`: the handler
traits and the message and trait types are defined in the shared `llingr-nexus`
contract crate and re-exported at the llingr-kafka root.

## Adding it to your project

```sh
cargo add llingr-kafka
```

There are no cargo features to choose. The crate is pre-baked: the engine, the
franz-go Kafka client, the Prometheus metrics, and the log routing are all
present in every build. Building it needs a Go toolchain the first time (see
[Building](#building) below); the compiled engine is then cached like any build
artefact.

## What you get

- **Head-of-line-blocking-free ordered processing.** Per-key concurrent workers
  with contiguous offset commits: a slow key does not stall the partition.
  Deep dive: [`docs/processing.md`](docs/processing.md).
- **Kafka and Kafka-compatible brokers**, spoken by the pure-Go franz-go client.
  No librdkafka, no CGO broker library. The same build works against Apache
  Kafka, RedPanda, and Amazon MSK. Options reference:
  [`docs/kafka-options.md`](docs/kafka-options.md).
- **Security for real clusters**: TLS and mutual TLS, and SASL PLAIN,
  SCRAM-SHA-256, and SCRAM-SHA-512, with AWS_MSK_IAM and OAUTHBEARER (OIDC) in
  scope as a later phase. Full guide: [`docs/security.md`](docs/security.md).
- **Typed engine tuning**: thirteen `DemuxConfig` knobs, each with an engine
  default and validated at startup. Reference:
  [`docs/configuration.md`](docs/configuration.md).
- **Prometheus metrics** switched on by one builder call, served from a built-in
  scrape endpoint or mounted on an HTTP server you already run. Catalogue:
  [`docs/metrics.md`](docs/metrics.md).
- **Engine logs through the Rust `log` facade** under the target `llingr`, with
  no wiring and no logger parameter. Details:
  [`docs/logging.md`](docs/logging.md).
- **A self-contained deployable**: the engine links statically, so the whole
  consumer is one binary that drops into a `scratch` image of roughly 16 MB.
  Build and packaging: [`docs/building-packaging.md`](docs/building-packaging.md).

## Configuration

The topic is the builder's first argument. `brokers` and `consumer_group` are
required. Kafka client options go through `Options` (typed, franz-go), and
engine tuning goes through `DemuxConfig`. All thirteen engine knobs are exposed
with their defaults; unset knobs use the engine default, and the engine
validates ranges at startup and reports a clean error, never a crash. The full
knob-by-knob reference, with units and when to change each, is in
[`docs/configuration.md`](docs/configuration.md).

```rust
# use llingr_kafka::DemuxConfig;
use std::time::Duration;
let demux = DemuxConfig::new()
    .concurrent_keys(500)                     // default 250, max 5000
    .poll_timeout(Duration::from_millis(100)); // default 100ms
# let _ = demux;
```

## Kafka client options

`Options` is the single home for all Kafka client configuration, a typed builder
over the franz-go client: offset reset, client id, static membership,
session/heartbeat/rebalance timeouts, partition assignment strategies, fetch
tuning, and the full TLS/SASL security surface. Anything the typed builder does
not cover is reachable on the same `Options` builder as librdkafka-style string
key/value pairs (`Options::new().kafka_option(key, value)`, or `.kafka_options(pairs)`
for many at once), which the bridge translates into the equivalent franz-go
option. Nothing is silently ignored: an unknown key fails at `build()` with the
full list of supported keys, and conflicting security keys fail with a specific
message (typed setters and string keys are validated together as one unit).

**Coverage.** The full option surface, cross-checked against every
consumer-relevant franz-go `kgo` option, is in
[`docs/kafka-options.md`](docs/kafka-options.md): the typed setters, the
`llingr.` namespace options (poll-error tuning, retries, concurrent fetches), the
string escape hatch, and the complete table of deliberately excluded options with
their reasons (the auto-commit family, rebalance callbacks, share groups, the
producer surface, and more, each excluded because the engine owns that part of
the consumer). Every option is either supported, or excluded with a reason, or
fails loudly at `build()` with the 41-key supported-key list. Security coverage
is in [`docs/security.md`](docs/security.md).

## Security

The security setters compute the wire protocol for you: any `tls_*` setter
enables TLS, any `sasl_*` setter enables SASL, and the combination becomes
`ssl`, `sasl_plaintext`, or `sasl_ssl`. What is supported today, and what is
not:

| Capability | Status |
|---|---|
| TLS (server authentication) | supported |
| mTLS (client certificates, file paths and inline PEM) | supported |
| SASL PLAIN | supported |
| SASL SCRAM-SHA-256 / SCRAM-SHA-512 | supported |
| AWS_MSK_IAM | in scope, later phase |
| SASL OAUTHBEARER (OIDC client-credentials) | in scope, later phase |
| SASL GSSAPI (Kerberos) | not supported |
| Custom token-callback IdP flows | not supported |

This covers Confluent Cloud (API keys are SASL PLAIN over TLS), RedPanda,
self-hosted SASL/SCRAM clusters, and mTLS shops. One worked example per
mechanism, the credential-chain sources for AWS_MSK_IAM, and the full
misconfiguration error catalogue are in [`docs/security.md`](docs/security.md).

## Metrics

Prometheus metrics are built in and switched on by one builder call, in either
of two modes:

```rust
# use llingr_kafka::Metrics;
// Mode 1: built-in scrape endpoint on its own thread, OpenMetrics text at the path.
let served = Metrics::serve("0.0.0.0:9464", "/metrics");
// Mode 2: no server; take a handle and serve its output from your own HTTP stack.
let (metrics, handle) = Metrics::registry();
# let _ = (served, metrics, handle);
```

`serve(addr, path)` runs a small synchronous HTTP endpoint on its own thread and
serves OpenMetrics text at your chosen path. `registry()` runs no server: it
takes no arguments and returns a `(Metrics, MetricsHandle)` pair over a
crate-owned registry, so you pass the `Metrics` to the builder's `.metrics(...)`
and serve the `MetricsHandle`'s OpenMetrics output from whatever HTTP framework
you already run (the handle serves empty exposition text until `build()` and live
data afterwards). The complete metric catalogue (names, types, labels,
meanings), the bandwidth telemetry that rides along, and a worked example of
mounting on your own server are in [`docs/metrics.md`](docs/metrics.md).

## Logging

Engine log lines flow into the Rust `log` facade under the target `llingr`, so
whatever logger your application installed receives them alongside your own
output. There is no logger parameter by design: the `log` facade is
process-global, so you install a logger once and the engine's lines appear.

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init(); // the entire wiring; then RUST_LOG=llingr=debug to raise it
    // ... build and run the engine ...
    Ok(())
}
```

Level mapping, `tracing` via `tracing-log`, and filtering by target are covered
in [`docs/logging.md`](docs/logging.md).

## Operations

`engine.run()` blocks the calling thread until shutdown, so you arrange shutdown
first: `engine.stopper()` returns a `Send` closure you move to a signal-watcher
thread. `engine.stop()` drains in-flight work, commits, and releases `run()`
(the clean path: zero duplicates on a rolling restart, provided the drain
completes within `drain_timeout`, default 20 seconds; work a drain timeout
abandons is redelivered). `engine.emergency_stop`
stops now without draining, so abandoned in-flight work is redelivered on
restart: llingr-kafka is at-least-once, so make your processing idempotent. An
optional shutdown handler fires exactly once with a reason. `engine.snapshot()`
(typed) and `engine.snapshot_json()` (the canonical JSON document) expose the
consumer's live state for an operational endpoint. Only one engine may exist per
process, because the Go runtime is process-global. Signal handling, the
shutdown callback, thread budgeting, and liveness are all in
[`docs/operations.md`](docs/operations.md).

## Toolchain requirements

- **Rust**: a recent stable toolchain.
- **Go 1.25 or newer** on `PATH`, for the first build: the engine is compiled
  from source into a static archive during `cargo build`, then cached.
- **A C compiler** (cgo compiles the engine's C glue). Cross-compiling needs
  `CC` naming a cross toolchain; the build fails early and clearly if it is
  missing.

Supported platforms: Linux with glibc (production), and macOS for development.
musl/Alpine is not supported: the embedded Go runtime segfaults during its own
initialisation, for two upstream Go reasons that no musl tuning fixes, and the
build fails loudly with the tracking-issue links if you target a `*-musl`
triple. The record is in [`docs/internal/MUSL.md`](docs/internal/MUSL.md).

## Building

Most of the time you build with plain `cargo build`: the build script compiles
the engine from source the first time (fetching the pinned Go modules from the
module proxy, verified against `go.sum`) and links the static archive into your
binary. Docs.rs builds work with no Go toolchain (the build script emits nothing
under `DOCS_RS`). Docker is never invoked from the build script; a `cargo build`
stays deterministic.

If Go is not on the machine, there are three remedies, and the build script's
error message names all three:

1. **Install Go 1.25 or newer** (and a C compiler), then `cargo build` compiles
   the engine from source as usual.
2. **Build the engine once in the provided container** and point cargo at it:
   `make engine` produces `dist/<target-triple>/libllingr.a` in the builder
   image, then set `LLINGR_LIB_DIR=dist/<target-triple>` so `cargo build` links
   the prebuilt archive and skips Go entirely. This suits CI caches and
   air-gapped builds.
3. **Build your whole application in the provided container**
   (`docker/Dockerfile.builder`, which carries both Go and Rust), so the machine
   needs only Docker.

The `Makefile` is the single entry point: `make toolchains` reports what is
present and what the build will do, `make build` honours native/Docker mode, and
`make engine` produces the standalone archive for `LLINGR_LIB_DIR` users. Full
detail, including cross-compilation and scratch-image deployment, is in
[`docs/building-packaging.md`](docs/building-packaging.md).

## Licence

llingr-kafka is dual-licensed:

```
AGPL-3.0-only OR LicenseRef-Llingr-Commercial
```

The crate statically links the AGPL-dual-licensed llingr-demux engine, so the
whole crate, and any binary built from it, is AGPL plus commercial. Under
`AGPL-3.0-only` you may use it freely provided you meet the copyleft
obligations, which for a network service means offering your users the complete
corresponding source. For proprietary, closed-source, or SaaS use without those
obligations, a commercial licence is available from Llingr Software Ltd: contact
`license@llingr.io`. The plain-terms explanation is in
[`docs/licensing.md`](docs/licensing.md).

Because the engine links statically, your binary embeds third-party Go
components whose licences require attribution in binary distributions, notably
franz-go (BSD-3-Clause). The repository ships a `THIRD-PARTY-NOTICES` file listing
them; include it alongside any binary you distribute. Details in
[`docs/licensing.md`](docs/licensing.md).

## Documentation

| Page | What it covers |
|---|---|
| [`docs/index.md`](docs/index.md) | What llingr-kafka is, when to use it, the capability map, how the pages fit together |
| [`docs/getting-started.md`](docs/getting-started.md) | Toolchain requirements and a first consumer end to end against RedPanda |
| [`docs/processing.md`](docs/processing.md) | `ProcessHandler`, `DeadLetterHandler`, `Traits`: per-key ordering, at-least-once and dedupe, the panic-to-dead-letter contract |
| [`docs/configuration.md`](docs/configuration.md) | Every `DemuxConfig` engine knob: meaning, default, units, when to change it |
| [`docs/kafka-options.md`](docs/kafka-options.md) | Typed `Options`, the string escape hatch, the full coverage matrix, unknown-key behaviour |
| [`docs/security.md`](docs/security.md) | TLS/mTLS, SASL, AWS_MSK_IAM, OAUTHBEARER; one worked example each; the unsupported list and error catalogue |
| [`docs/metrics.md`](docs/metrics.md) | Both activation modes, the metric catalogue, bandwidth telemetry, mounting on your own server |
| [`docs/logging.md`](docs/logging.md) | The `log` facade, target `llingr`, level mapping, env_logger and tracing-log |
| [`docs/operations.md`](docs/operations.md) | run/stop/emergency_stop, the shutdown callback, snapshots, signal handling, one instance per process |
| [`docs/building-packaging.md`](docs/building-packaging.md) | Native and Docker builds, static linking, `LLINGR_LIB_DIR`, cross-compilation, scratch images, notices, musl |
| [`docs/example.md`](docs/example.md) | Walkthrough of the end-to-end example: what each piece proves and how to adapt it |
| [`docs/licensing.md`](docs/licensing.md) | The dual licence in plain terms and the notices obligation |
| [`docs/troubleshooting.md`](docs/troubleshooting.md) | The init-error catalogue, runtime failure modes, and what a shutdown reason tells you |

Contributor notes live under [`docs/internal/`](docs/internal/):
`ARCHITECTURE.md`, `BUILDING.md`, and
[`MUSL.md`](docs/internal/MUSL.md).
