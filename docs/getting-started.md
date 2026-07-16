# Getting started

This walkthrough takes you from an empty directory to a running llingr-kafka
consumer reading live messages from a local RedPanda broker, in a few minutes.
By the end you will have a broker running in Docker, a topic with a few
messages, and a Rust binary that consumes them and logs each one. It is
deliberately self-contained: everything runs on your machine, and nothing here
depends on the crate's own example stack.

RedPanda is used as the broker because it is Kafka-compatible, needs no
ZooKeeper, and reaches a healthy single node in seconds. Anything you do here
works identically against Apache Kafka or Amazon MSK; only the broker address
changes.

## Prerequisites

You need a way to build the crate and a broker to point it at.

- **To build natively:** a recent stable Rust toolchain, **Go 1.25 or newer** on
  your `PATH`, and a **C compiler**. The engine is compiled from source into a
  static archive during the first `cargo build` and cached afterwards, so the Go
  toolchain is a build-time requirement, not a runtime one. Supported platforms
  are Linux with glibc and macOS for development.
- **If you cannot install Go**, Docker is enough. Build the whole crate inside
  the provided builder image with `make build MODE=docker`, or build just the
  engine once with `make engine` (which uses Docker when Go is absent) and set
  `LLINGR_LIB_DIR=dist/<target-triple>` so a plain `cargo build` then links the
  prebuilt archive without Go. Both paths are detailed in
  `docs/building-packaging.md`.
- **Docker**, to run the local RedPanda broker below.

## Step 1: run a local RedPanda

Start a single-node RedPanda in Docker, listening on the standard Kafka port
9092:

```sh
docker run -d --name redpanda -p 9092:9092 -p 9644:9644 \
  docker.redpanda.com/redpandadata/redpanda:v24.2.7 \
  redpanda start --smp=1 --overprovisioned --node-id=0 --check=false \
  --kafka-addr=PLAINTEXT://0.0.0.0:9092 \
  --advertise-kafka-addr=PLAINTEXT://localhost:9092
```

Give it a few seconds, then confirm it is healthy:

```sh
docker exec redpanda rpk cluster health
```

`rpk` is RedPanda's CLI, and it ships inside the container, so you run it with
`docker exec` rather than installing anything. This mirrors the pinned image and
flags in the crate's own example stack (`example/docker-compose.yml`,
`redpandadata/redpanda:v24.2.7`); the only difference is the advertised address,
`localhost` here so you can reach the broker from the host, versus the container
name on the compose network.

## Step 2: create the topic

Create an `orders` topic with a few partitions, so per-key routing has something
to spread across:

```sh
docker exec redpanda rpk topic create orders -p 3
```

## Step 3: start a new Rust project and add the crate

```sh
cargo new orders-consumer
cd orders-consumer
cargo add llingr-kafka
cargo add log env_logger
```

`llingr-kafka` is the consumer crate. `log` and `env_logger` are how you see
output: the engine routes its own log lines into the `log` facade under the
target `llingr`, and `env_logger` prints whatever the facade receives, so the
same two lines that light up your handler's logs also surface the engine's
lifecycle. There are no crate features to enable; llingr-kafka is pre-baked.

## Step 4: write the consumer

Replace `src/main.rs` with a minimal consumer. It implements the two required
handlers (one to process each message, one to catch failures), starts from the
earliest offset so it sees messages produced before it joined, and logs each
message it receives.

```rust
use llingr_kafka::{Builder, Options, AutoOffsetReset, Message, Traits,
                   ProcessHandler, DeadLetterHandler};

struct Orders;
impl ProcessHandler for Orders {
    fn process(&self, msg: &Message) -> Result<Traits, Box<dyn std::error::Error>> {
        log::info!("received key={} partition={} offset={} value={}",
            msg.key_str().unwrap_or(""),
            msg.partition(),
            msg.offset(),
            msg.value_str().unwrap_or("<binary>"));
        Ok(Traits::none())
    }
}

// Required: a failed message must have somewhere to go before its offset commits.
struct DeadLetters;
impl DeadLetterHandler for DeadLetters {
    fn handle(&self, msg: &Message, error: &str) -> Result<(), Box<dyn std::error::Error>> {
        log::error!("dead-letter key={} reason={}", msg.key_str().unwrap_or(""), error);
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init(); // prints log records, engine lines included

    let engine = Builder::new("orders", Orders, DeadLetters)
        .brokers("localhost:9092")
        .consumer_group("getting-started")
        .options(Options::new().auto_offset_reset(AutoOffsetReset::Earliest))
        .build()?;

    // run() blocks until the engine stops. Ctrl-C exits the process; for a
    // graceful drain-and-commit on shutdown, wire engine.stopper() to a signal
    // as shown in docs/operations.md.
    engine.run()?;
    Ok(())
}
```

## Step 5: run it and watch messages arrive

Run the consumer with info-level logging. The first build compiles the engine
from source, so it takes a little longer than a normal `cargo run`; subsequent
runs are fast.

```sh
RUST_LOG=info cargo run
```

You will see the engine's startup lines (subscription, partition assignment)
followed by your handler waiting for messages. Leave it running and, in another
terminal, produce a few records to the `orders` topic, giving each a key so you
can watch per-key routing:

```sh
echo '{"orderId":"o-1","sku":"SKU-1"}' | docker exec -i redpanda rpk topic produce orders --key o-1
echo '{"orderId":"o-2","sku":"SKU-2"}' | docker exec -i redpanda rpk topic produce orders --key o-2
echo '{"orderId":"o-3","sku":"SKU-3"}' | docker exec -i redpanda rpk topic produce orders --key o-3
```

Each `received ...` line in the consumer terminal is one message flowing the
whole way through: broker, the franz-go client, the engine, the FFI boundary,
and into your `process` handler. The `key` you produced with appears as the
message key, and the `partition` shows which partition its key routed to.

## Step 6: stop

Press Ctrl-C in the consumer terminal to stop it. That exits the process
immediately: it is safe (no acknowledged work is lost), but any message that was
mid-flight is redelivered on the next start, because nothing drained. For a
graceful shutdown that drains in-flight work and commits before exiting, wire
`engine.stopper()` to a `SIGINT`/`SIGTERM` handler as shown in
`docs/operations.md`; that is the pattern a production service uses.

When you are done, stop and remove the broker:

```sh
docker rm -f redpanda
```

## Where to go next

You now have the whole chain working. To go deeper:

- `docs/processing.md` for the handler contract in full: per-key ordering, the
  at-least-once delivery guarantee and how to make processing idempotent, the
  message accessors, and attaching your own trait bits.
- `docs/configuration.md` for the engine tuning knobs (worker concurrency,
  timeouts) and their defaults.
- `docs/kafka-options.md` and `docs/security.md` for connecting to a real
  cluster with TLS and SASL rather than a local plaintext broker.
- `docs/operations.md` for running in production: graceful shutdown, signal
  handling, snapshots, and liveness.
- The crate's own `example/` stack is the full end-to-end proof (a pure-Rust
  producer and a metrics-exposing consumer against RedPanda), walked through in
  `docs/example.md`.
