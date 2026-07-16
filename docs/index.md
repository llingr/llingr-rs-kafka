# llingr-kafka

llingr-kafka gives a Rust application an ordered, per-key-concurrent Kafka
consumer with the batteries already fitted. You implement one trait to process
messages; the crate supplies the engine, the broker client, offset management,
Prometheus metrics, and log routing, all compiled into your binary. There is
one crate to depend on, one broker family to point it at (Apache Kafka and any
Kafka-compatible broker such as RedPanda or Amazon MSK), and no runtime
services to deploy alongside it.

Underneath the safe Rust layer is the real llingr-demux engine. That engine
is written in Go, has its offset and pipeline mechanisms formally verified with
TLA+, and is compiled into a static C archive that links directly into your Rust
binary. Everything the engine guarantees in
Go it guarantees here, because it is the same engine: per-key ordering,
contiguous offset commits, drain-before-rebalance, and at-least-once delivery
all run unmodified. The Rust side is a thin, safe wrapper that receives every
message as a callback from a Go worker thread and hands it to your handler.

## What you get

The single value proposition is head-of-line-blocking-free ordered processing.
A plain Kafka consumer processes a partition's records one at a time, so a slow
message for one key stalls every other key sharing that partition. llingr-kafka
routes each record by key to its own concurrent worker, so a slow key holds up
only itself while offsets still commit in contiguous order. You keep per-key
ordering and gain partition-wide throughput.

Around that core the crate bundles the pieces a production consumer needs:

- **Kafka and Kafka-compatible brokers**, spoken by the pure-Go franz-go
  client. No C broker library, no librdkafka, no CGO surprises. The same build
  works against Apache Kafka, RedPanda, and Amazon MSK without configuration
  changes.
- **Security** for real clusters: TLS and mutual TLS, and SASL with the PLAIN,
  SCRAM-SHA-256, SCRAM-SHA-512, AWS_MSK_IAM, OAUTHBEARER (OIDC
  client-credentials), and GCP IAM mechanisms. Kerberos/GSSAPI and custom
  token-callback flows are out of scope and documented as unsupported.
- **Typed engine tuning**: thirteen demux settings for worker concurrency,
  buffer sizes, and timeouts, exposed with their engine defaults and each
  validated at startup.
- **Prometheus metrics** baked in and switched on by one builder call, served
  either from a built-in scrape endpoint or mounted on an HTTP server you
  already run.
- **Engine logs through the Rust `log` facade** under the target `llingr`, so
  they flow through whatever logger your application installed, whether
  env_logger or tracing through tracing-log, with no wiring. There is no logger
  parameter by design.
- **A self-contained deployable**: the engine links statically, so the whole
  consumer is one binary with no shared library beside it and no
  `LD_LIBRARY_PATH` to manage. It ships in a `scratch` container image of
  roughly 16 MB.

## When to use it, and when not

Reach for llingr-kafka when you are writing a Rust service that consumes from
Kafka or a Kafka-compatible broker, you need per-key ordering, and a slow or
uneven per-key workload would otherwise cause head-of-line blocking on a
partition. It suits high-throughput consumers where processing latency varies
by key and you would rather scale concurrency inside the process than shard the
topic into ever more partitions.

Look elsewhere when your broker is not Kafka-compatible, since this crate is
franz-go and Kafka-only by design with no adapter abstraction and no other
broker; when you must deploy on musl/Alpine today, because the Go runtime does
not yet initialise on musl (see the platform note below and
`docs/internal/MUSL.md`); or when your processing has no per-key ordering
requirement, in which case a plain consumer is simpler.

## The smallest working consumer

The required configuration is a topic and two handlers: one that processes each message
and one that receives any message that fails. Everything else on the builder is
optional.

```rust
use llingr_kafka::{Builder, Message, Traits, ProcessHandler, DeadLetterHandler};

struct Orders;
impl ProcessHandler for Orders {
    fn process(&self, msg: &Message) -> Result<Traits, Box<dyn std::error::Error>> {
        // msg.value() is borrowed for the duration of this call only; copy out
        // anything you keep past it. Return the trait bits you want recorded.
        let body = msg.value_str().unwrap_or("<binary>");
        println!("key={} partition={} offset={} value={}",
            msg.key_str().unwrap_or(""), msg.partition(), msg.offset(), body);
        Ok(Traits::none())
    }
}

// A dead-letter handler is required: a message that fails must have somewhere to
// go before its offset commits, or it would be dropped silently. Logging is the
// minimum; a real deployment publishes to a DLQ topic, a table, or object store.
struct DeadLetters;
impl DeadLetterHandler for DeadLetters {
    fn handle(&self, msg: &Message, error: &str) -> Result<(), Box<dyn std::error::Error>> {
        eprintln!("dead-letter key={} reason={}", msg.key_str().unwrap_or(""), error);
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = Builder::new("orders", Orders, DeadLetters)
        .brokers("localhost:9092")
        .consumer_group("orders-svc")
        .build()?;

    // run() blocks until a stop; get a Send closure first to trigger it from a
    // signal-watcher thread. See docs/operations.md for the full pattern.
    let _stop = engine.stopper();
    engine.run()?;
    Ok(())
}
```

`Builder`, `Message`, `Traits`, `ProcessHandler`, and `DeadLetterHandler` are
all reachable from a single `use llingr_kafka::...`. The handler traits and the
message and trait types are defined in the shared `llingr-nexus` contract crate
and re-exported at the llingr-kafka root, so one import line covers them.

## Platform note

llingr-kafka runs on Linux with glibc, and builds on macOS for local
development. It does not run on musl/Alpine: the embedded Go runtime segfaults
during its own initialisation on musl, for two upstream Go reasons that no musl
tuning fixes. The static link mode this crate ships is the shortest path to
eventual musl support, and the build fails loudly with the tracking-issue links
if you target a `*-musl` triple rather than producing a binary that crashes at
start. The full record is in `docs/internal/MUSL.md`.

## The documentation set

The README contains the succinct form of every capability with one sample each;
each page below extends a README section into full depth. Every page is written
to stand alone, so a page retrieved on its own repeats the context it needs
rather than sending you elsewhere.

| Page | What it covers |
|---|---|
| `docs/index.md` | This page: what llingr-kafka is, when to use it, the capability map, and how the pages fit together |
| `docs/getting-started.md` | Toolchain requirements (Go 1.25+ and Rust, or the Docker path) and a first consumer end to end against RedPanda |
| `docs/processing.md` | The `ProcessHandler`, `DeadLetterHandler`, and `Traits`: per-key ordering, at-least-once semantics and dedupe guidance, the panic-to-dead-letter contract, and the trait bit field |
| `docs/configuration.md` | Every `DemuxConfig` engine setting: what it does, its default, its units, and when to change it |
| `docs/kafka-options.md` | The typed `Options` reference, the raw string escape hatch, the full kgo coverage matrix, and the fail-loudly behaviour for unknown keys |
| `docs/security.md` | TLS/mTLS and SASL (PLAIN, SCRAM-SHA-256/512), AWS_MSK_IAM, and OAUTHBEARER OIDC, one worked example per mechanism, the explicit unsupported list, and the misconfiguration error catalogue |
| `docs/metrics.md` | The two Prometheus activation modes, the complete metric catalogue, bandwidth telemetry, and a worked example of mounting scrape output on your own server |
| `docs/logging.md` | Engine logs through the `log` facade: the `llingr` target, the level mapping, and env_logger and tracing-log worked examples |
| `docs/operations.md` | `run`/`stop`/`emergency_stop` semantics, the exactly-once shutdown callback, duplicate delivery after an emergency stop, snapshots (typed and JSON), signal handling, and the one-instance-per-process rule |
| `docs/building-packaging.md` | Native and Docker builds, static linking, `LLINGR_LIB_DIR`, cross-compilation, scratch-image deployment, the THIRD-PARTY-NOTICES obligation, and musl status |
| `docs/example.md` | A walkthrough of the end-to-end example: what each piece proves and how to adapt it |
| `docs/licensing.md` | The dual licence in plain terms: what AGPL-3.0-only means for a binary that embeds this crate, when the commercial licence applies, and who to contact |
| `docs/troubleshooting.md` | The init-error catalogue, runtime failure modes, and what a shutdown reason tells you |

Contributor-facing notes live under `docs/internal/`: `ARCHITECTURE.md` covers
the FFI boundary and how the Go engine embeds, `BUILDING.md` the build model and
ABI discipline, and `MUSL.md` the parked-upstream musl record and the flip
instructions for when it lands.
