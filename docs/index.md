# llingr-kafka

A per-key ordered, concurrent Kafka / Redpanda message-processing engine that allows consumers
to scale both horizontally AND vertically. This provides massive scaling headroom, improving
throughput, latency, broker utilization, and reducing operational complexity and
broker/infrastructure costs.

Integration requires two traits:

  * `ProcessHandler` to process a message
  * `DeadLetterHandler` to catch processing errors

Prometheus metrics can be enabled with a builder argument.


## Capabilities

An important value proposition is side-stepping head-of-line blocking while
keeping per-key ordering - poison messages won't stall entire partitions, and it becomes
unnecessary to provision many broker partitions to achieve high levels of concurrency;
the llingr-demux engine adds 250x concurrency by default (this multiplier is configurable).

The details are reasonably complex, but the deliverable can best be summarised as providing
a robust async/concurrent solution for normally serial consumers - something that is notoriously difficult to get right.

The engine itself has been implemented close to the silicon - with mechanical sympathy -
so that it uses minimal CPU. This makes it fast and effectively invisible, leaving the majority
of compute resources for business logic and other application processing.

Coupled with horizontal scaling - for example with a consumer instance on each of twelve
partitions - concurrency can be increased 2-3 orders of magnitude beyond conventional deployments
using default settings, and 4-5 orders of magnitude is possible in more specialised scenarios (the
config already accommodates this with a maximum 5000x concurrency PER-CONSUMER INSTANCE).

Per-key ordering is preserved, broker rebalances are carefully orchestrated to avoid
duplicates (healthy infrastructure gives zero duplicates), and at-least-once
guarantees processing through infrastructure outages, application OOMs etc.

---

Around that core the crate bundles production consumer needs:

- **Kafka/Redpanda broker client** This works with Apache Kafka, Redpanda, Amazon MSK etc.
- **Security** TLS, mTLS, SASL PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, AWS_MSK_IAM, OAUTHBEARER
  (OIDC client-credentials), and GCP IAM mechanisms. Kerberos/GSSAPI and custom
  token-callback flows are out of scope and documented as unsupported largely due to timelines
  for testing but this can be added in future if required.
- **Engine tuning**: llingr-demux configuration settings for worker concurrency,
  buffer sizes, and timeouts. The defaults are sensible so should work for most workloads.
- **Prometheus metrics** included and enabled using a builder call. The accumulating metrics
  can be served either from a built-in scrape endpoint or mounted into an existing HTTP server.
- **Engine logs through the Rust `log` facade** using the target `llingr`, flows through whichever
  logger the application uses.
- **Two packaging modes**: by default the engine links statically, so the consumer-engine becomes
  part of a regular application binary. Alternatively the engine can be built once as a shared
  library with `make engine LINK=shared`, linked against with `LLINGR_LINK=shared` and
  `LLINGR_LIB_DIR`, and deployed beside the application binary, where RPATH resolves it at
  runtime and a startup ABI check validates it.

## Use-cases

llingr-kafka suits a Rust service that consumes from Kafka or a Kafka-compatible
broker, requires per-key ordering, and has a need for high-throughput, high-scale processing;
typical examples include B2C systems for large user-bases, but any applications where both
high-throughput and low-latency is needed without exploding broker costs. For medium-scale systems
with uneven per-message processing times that would otherwise cause head-of-line blocking, the
llingr-demux engine provides a means to circumvent constraints that are typical in more conventional
consumer applications.

Any application that needs at-least-once processing guarantees, clean (and fast) rebalances
and extensive processing telemetry can also benefit from the engine even if scale is not a current
concern - the idea is that the massive scaling headroom is available, but the core capabilities
and processing invariants are still extremely useful in the majority of event-driven systems.

Platform support is Linux with glibc, and macOS for local development; musl and
Alpine are covered in the platform note below.

## Short Example

The llingr-demux engine needs three things:

1. A topic name 
2. A message processing callback function - implementation provided in the host application
3. A dead-letter sink - implementation provided in the host application

Everything else provided in the builder is optional.

```rust
use llingr_kafka::{Builder, Message, Traits, ProcessHandler, DeadLetterHandler};

struct Orders;
impl ProcessHandler for Orders {
    fn process(&self, msg: &Message) -> Result<Traits, Box<dyn std::error::Error>> {
        // msg.value() is borrowed for the duration of this call; anything
        // retained after the function returns MUST BE COPIED out.
        let body = msg.value_str().unwrap_or("<binary>");
        println!("key={} partition={} offset={} value={}",
            msg.key_str().unwrap_or(""), msg.partition(), msg.offset(), body);
        Ok(Traits::none())
    }
}

// A dead-letter handler is required: a core guarantee is to never drop messages, so
// dead-letter routing is necessary to avoid message loss. This example uses a logger
// but a proper DLQ or object store is preferable
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
all reachable from `use llingr_kafka::...`. The handler traits and the
message and trait types are defined in the shared `llingr-nexus` contract crate
and re-exported at the llingr-kafka root, so a single import line is sufficient.

## Platform note

llingr-kafka runs on Linux with glibc, and builds have also been tested on macOS for
local development. Alpine (/musl) is not currently supported, nor is Windows.
Two upstream (Go) issues are blocking proper musl support, and as soon as these
are resolved musl/Alpine capability will be adopted; until then a musl target will
fail a build validation step. For details see `docs/internal/MUSL.md`.

---

## Documentation

- [Getting started](getting-started.md) - toolchain, and a first consumer end to end
- [Processing](processing.md) - the two handler traits, ordering, dead letters, trait bits
- [Configuration](configuration.md) - every engine setting, with defaults and units
- [Kafka options](kafka-options.md) - typed broker options and the raw string escape hatch
- [Security](security.md) - TLS, mTLS, and each SASL mechanism
- [Metrics](metrics.md) - the two activation modes and the metric catalogue
- [Logging](logging.md) - the `llingr` target and level mapping
- [Operations](operations.md) - run, stop, shutdown, snapshots, signal handling
- [Building and packaging](building-packaging.md) - builds, both link modes, deployment
- [Example](example.md) - the end-to-end example, and how to adapt it
- [Licensing](licensing.md) - the dual licence in plain terms
- [Troubleshooting](troubleshooting.md) - init errors, runtime failures, shutdown reasons

Contributor notes:

- [Architecture](internal/ARCHITECTURE.md) - the FFI boundary and how the Go engine embeds
- [Building](internal/BUILDING.md) - the build model and ABI discipline
- [musl](internal/MUSL.md) - the upstream blockers and the flip instructions
