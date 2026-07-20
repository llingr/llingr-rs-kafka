# llingr-kafka

A per-key ordered, concurrent Kafka consumer engine for Rust. This allows consumers to
scale vertically as well as horizontally, significantly improve both latency and throughput,
control broker/infrastructure costs, improve utilization and reduce operational complexity.

Integration requires two traits:

  * `ProcessHandler` to process a message
  * `DeadLetterHandler` to catch processing errors

Prometheus metrics can be enabled with a builder argument.

**Why**: Burst capacity and scaling headroom for event-driven systems. A conventional consumer
processes each partition in serial, limiting concurrency to the partition count, and per-partition
message stalls cause 'head of line blocking'. The llingr-kafka engine avoids this by routing
each message (by partition key) to a dedicated in-memory channel. This preserves per-key ordering
while allowing a single partition to fan out into hundreds/thousands of concurrent workers.

Coupled with a consumer-per-partition - for example with a consumer instance on each of twelve
partitions - concurrency can be increased 2-3 orders of magnitude beyond conventional deployments
using default settings, and 4-5 orders of magnitude in more specialised scenarios the config
already supports.

Per-key ordering is preserved, broker rebalances are carefully orchestrated to avoid
duplicates (healthy infrastructure gives zero duplicates), and at-least-once
guarantees processing through infrastructure outages, application OOMs etc.

**How**: The engine is [llingr-demux](https://llingr.io), written in Go with
TLA+-verified offset and pipeline mechanisms, compiled to a static C archive
and linked into your binary. The one practical consequence: the first
`cargo build` needs **Go 1.25+** and a C compiler on the machine or Docker
container. No Go on the machine? See [Building](#building) where Docker is used
for packaging.

Visit [https://llingr.io](https://llingr.io) for more background.

## Quick start

Requirements:

 * Rust MSRV 1.78 / edition 2021
 * Go 1.25+
 * C compiler

Also Docker to run examples, and for builds where hosts lack the above dependencies.

Start an example broker and create a topic:

```sh
docker run -d --name redpanda -p 9092:9092 \
  docker.redpanda.com/redpandadata/redpanda:v24.2.7 \
  redpanda start --smp=1 --overprovisioned --node-id=0 --check=false \
  --kafka-addr=PLAINTEXT://0.0.0.0:9092 \
  --advertise-kafka-addr=PLAINTEXT://localhost:9092

docker exec redpanda rpk topic create orders -p 3
```

Create an example project:

```sh
cargo new orders-consumer && cd orders-consumer
cargo add llingr-kafka log env_logger
```

Replace `src/main.rs`:

```rust
use llingr_kafka::{Builder, Options, AutoOffsetReset, Message, Traits,
                   ProcessHandler, DeadLetterHandler};

struct Orders;
impl ProcessHandler for Orders {
    fn process(&self, msg: &Message) -> Result<Traits, Box<dyn std::error::Error>> {
        // Called once per message; in offset order per key, concurrently across keys.
        // Message bytes are borrowed for this call only: copy out anything you keep.
        log::info!("key={} partition={} offset={} value={}",
            msg.key_str().unwrap_or(""), msg.partition(), msg.offset(),
            msg.value_str().unwrap_or("<binary>"));
        Ok(Traits::none())
    }
}

// Required: a failed message must land somewhere before its offset commits.
struct DeadLetters;
impl DeadLetterHandler for DeadLetters {
    fn handle(&self, msg: &Message, error: &str) -> Result<(), Box<dyn std::error::Error>> {
        log::error!("dead-letter key={} reason={}", msg.key_str().unwrap_or(""), error);
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init(); // engine logs arrive via the `log` facade, target `llingr`

    let engine = Builder::new("orders", Orders, DeadLetters)
        .brokers("localhost:9092")
        .consumer_group("quickstart")
        .options(Options::new().auto_offset_reset(AutoOffsetReset::Earliest))
        .build()?;

    engine.run()?; // blocks; Ctrl-C to exit (graceful shutdown: docs/operations.md)
    Ok(())
}
```

```sh
# Run the example
RUST_LOG=info cargo run

# Publish test messages (different shell)
echo '{"orderId":"o-1"}' | docker exec -i redpanda rpk topic produce orders --key o-1
echo '{"orderId":"o-2"}' | docker exec -i redpanda rpk topic produce orders --key o-2
echo '{"orderId":"o-3"}' | docker exec -i redpanda rpk topic produce orders --key o-3
echo '{"orderId":"o-4"}' | docker exec -i redpanda rpk topic produce orders --key o-4
echo '{"orderId":"o-5"}' | docker exec -i redpanda rpk topic produce orders --key o-5
```

Logged messages have travelled through the broker, consumer engine and ProcessHandler.
A narrated version of this walkthrough:
[getting-started](https://github.com/llingr/llingr-rs-kafka/blob/main/docs/getting-started.md).

## The handler contract

The engine _provides_ the concurrency so handlers can be implemented as ordinary blocking
functions.

- Returning `Ok` from `process` tells the engine the work is durably completed
  and the offset can safely commit. So: finish the work, then return.
- **Never spawn-and-return.** Handing work to another thread or task and
  returning early silently breaks at-least-once delivery, per-key ordering,
  and backpressure. For more throughput, increase the `concurrent_keys` setting
  (the default is 250).
- Async I/O is fine driven to completion _inside_ the handler: keep a tokio
  `Handle` on your handler struct and `block_on` the work before returning.
- All state in a `Message` (`key()`, `value()`, headers) is valid **only**
  for the duration of the call; use a `to_vec()` copy to retain memory that
  outlives the function call.
- A handler panic is caught at the FFI boundary and becomes a dead letter
  (relies on Rust's default `panic = "unwind"`).

The full contract, with the reasoning:
[processing](https://github.com/llingr/llingr-rs-kafka/blob/main/docs/processing.md).

## A production consumer

More options are available on the builder; this example shows SASL/SCRAM auth over TLS.

Security mechanisms supported: TLS, mTLS, SASL PLAIN, SCRAM-SHA-256/512, AWS_MSK_IAM,
OAUTHBEARER (OIDC), and GCP IAM. Kerberos/GSSAPI is not supported at this time.

See [examples/auth](https://github.com/llingr/llingr-rs-kafka/tree/main/examples/auth) for
an example of each authentication method.

```rust
use llingr_kafka::{Builder, Options, AutoOffsetReset, DemuxConfig, Metrics};
use std::time::Duration;

let password = std::env::var("KAFKA_SASL_PASSWORD")?;

let engine = Builder::new("orders", Orders, DeadLetters)
    .brokers("b1:9092,b2:9092")
    .consumer_group("orders-svc")
    .options(Options::new()
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .session_timeout(Duration::from_secs(30))
        .tls_ca_location("/etc/ssl/ca.pem")
        .sasl_scram_sha256("svc", &password))        // IAM, OIDC, mTLS: docs/security.md
    .demux(DemuxConfig::new().concurrent_keys(500))  // default 250, max 5000
    .metrics(Metrics::serve("0.0.0.0:9464", "/metrics"))
    .build()?;

let stop = engine.stopper(); // Send closure: call from a signal-watcher thread
engine.run()?;               // blocks until stop() completes its drain
```


## Packaging: two modes

| | Single binary (default) | Side-binary (`LLINGR_LINK=shared`) |
|---|---|---|
| You deploy | one self-contained executable; `scratch` image of roughly 16 MB | your binary plus `libllingr.so` beside it, resolved by RPATH; glibc base image |
| Engine updates | rebuild the application | replace the library file; a startup ABI check refuses a mismatched engine |
| Build with | `cargo build` | `make engine LINK=shared`, then `LLINGR_LINK=shared LLINGR_LIB_DIR=dist/<triple> cargo build` |
| Choose it when | you want the smallest artefact and simplest deploy | several binaries share one engine, or the engine ships as its own versioned artefact |

Both modes build the same engine from the same source. Scratch-image
Dockerfiles, cross-compilation, and the full decision detail:
[building-packaging](https://github.com/llingr/llingr-rs-kafka/blob/main/docs/building-packaging.md).

## Building

`cargo build` compiles the engine from the bundled Go source on first build,
fetching the pinned Go modules. The build script does not shell out to Docker,
so if Go is missing, the build fails with an error naming three remedies:

1. **Install Go 1.25+** (and a C compiler); `cargo build` does the rest.
2. **Use a prebuilt engine**: `make engine` (builds in Docker when Go is
   absent), then `LLINGR_LIB_DIR=dist/<target-triple> cargo build` links the
   archive and skips Go entirely. Suits CI caches and air-gapped builds.
3. **Build everything in the provided image** (`docker/Dockerfile`, Go + Rust
   included): the machine needs only Docker.

`make toolchains` reports what is installed and what the build will do;
`make doctor` proves an environment can build and link the engine end to end.

## Platform support

- **Linux (glibc)**: the production target, any distro supporting glibc
- **macOS**: development, needs Xcode C compiler
- **musl/Alpine**: not currently supported - the runtime cannot (yet) initialise as an
  embedded library on musl. When the (Go, upstream) issue is resolved this
  support will be added.
- **Windows**: not supported.

## Documentation

Background docs are available on https://llingr.io, with more detailed Rust-specific
documentation available in the GitHub repository:

| Page | Covers |
|---|---|
| [index](https://github.com/llingr/llingr-rs-kafka/blob/main/docs/index.md) | What llingr-kafka is, when to use it, how the pages fit together |
| [getting-started](https://github.com/llingr/llingr-rs-kafka/blob/main/docs/getting-started.md) | First consumer end to end against a local RedPanda |
| [processing](https://github.com/llingr/llingr-rs-kafka/blob/main/docs/processing.md) | The handler contract: ordering, at-least-once, panics, trait bits |
| [configuration](https://github.com/llingr/llingr-rs-kafka/blob/main/docs/configuration.md) | Every engine setting: meaning, default, when to change it |
| [kafka-options](https://github.com/llingr/llingr-rs-kafka/blob/main/docs/kafka-options.md) | Typed client options, the string escape hatch, the coverage matrix |
| [security](https://github.com/llingr/llingr-rs-kafka/blob/main/docs/security.md) | TLS/mTLS and every SASL mechanism, one worked example each |
| [metrics](https://github.com/llingr/llingr-rs-kafka/blob/main/docs/metrics.md) | Both activation modes and the complete metric catalogue |
| [logging](https://github.com/llingr/llingr-rs-kafka/blob/main/docs/logging.md) | The `log` facade, target `llingr`, env_logger and tracing |
| [operations](https://github.com/llingr/llingr-rs-kafka/blob/main/docs/operations.md) | run/stop/emergency_stop, signals, snapshots, liveness |
| [building-packaging](https://github.com/llingr/llingr-rs-kafka/blob/main/docs/building-packaging.md) | Both packaging modes, cross-compilation, scratch images |
| [example](https://github.com/llingr/llingr-rs-kafka/blob/main/docs/example.md) | The end-to-end example stack, piece by piece |
| [troubleshooting](https://github.com/llingr/llingr-rs-kafka/blob/main/docs/troubleshooting.md) | Error catalogue: build-time, `build()`, and runtime |
| [licensing](https://github.com/llingr/llingr-rs-kafka/blob/main/docs/licensing.md) | The dual licence in plain terms |

## Licence

```
AGPL-3.0-only OR LicenseRef-Llingr-Commercial
```

The crate statically links the llingr-demux engine, so an application built
with it forms a single combined work with the engine. Under `AGPL-3.0-only`,
distributing that work, or operating it as a network service, requires making
its complete corresponding source available under the same licence.

For proprietary, closed-source, or SaaS use without those obligations, a commercial
licence is available from Llingr Software Ltd: [license@llingr.io](mailto:license@llingr.io).

Distributed binaries embed Go components that require attribution, notably
franz-go (BSD-3-Clause): ship the bundled `THIRD-PARTY-NOTICES` file alongside
any binary you distribute. Plain-terms guide:
[licensing](https://github.com/llingr/llingr-rs-kafka/blob/main/docs/licensing.md).
