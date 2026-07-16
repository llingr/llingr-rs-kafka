# The end-to-end example

The `example/` directory is a real proof, not a demo. When `make example-verify`
exits 0, the whole chain worked against a real broker: a producer published a
thousand order events, and a llingr-kafka consumer processed every one of them
through the demux engine, the franz-go broker layer, the FFI boundary, the log
facade, and the metrics endpoint, with a per-message invariant checked on the
way. This page explains what the stack is, what each piece proves, how the exit
code becomes the proof, and how to adapt the pieces for your own use.

The whole chain is proven end to end: `make example-verify` exits 0, with the
producer delivering all 1000 messages and the consumer processing all 1000 with
the key invariant holding on every one, no dead letters, and a clean shutdown.
That exit-0 run is the standing proof everything on this page describes,
including the in-image build of the crate and its Go bridge.

## The stack

The example is a single Docker Compose stack on one network, in `example/`, with
four services:

- **`redpanda`**: one RedPanda node (image `redpandadata/redpanda:v24.2.7`),
  Kafka-compatible and needing no ZooKeeper. Its healthcheck is `rpk cluster
  health`, so nothing downstream starts until the broker reports healthy.
- **`topic-init`**: a one-shot that runs `rpk topic create orders -p 12` and
  exits. Twelve partitions is a deliberate choice: it gives per-key routing and
  out-of-order completion something real to spread across, so the engine's
  concurrency is genuinely exercised rather than trivially serial.
- **`producer`**: a pure-Rust producer (see below) that publishes 1000 order
  events and exits. It depends on `topic-init` completing successfully.
- **`consumer`**: the llingr-kafka consumer (see below). It starts alongside the
  producer (consuming from the earliest offset makes their ordering moot) and
  publishes port 9464 so `/metrics` can be curled mid-run.

## What the producer proves

The producer is written in pure Rust on `rskafka`, deliberately: no librdkafka,
no C toolchain, and no cmake anywhere, so the producer image stays C-free and
statically linkable. That choice shapes the code in two ways worth knowing,
because `rskafka` does not mirror librdkafka:

- **It partitions explicitly.** `rskafka` has one client per partition and no
  client-side key partitioner, so the producer computes the partition itself as
  a deterministic hash of the record key modulo the partition count (12). Any
  deterministic hash is correct here, because every `orderId` is unique, so the
  load spreads across partitions; it is not the Java default partitioner and
  makes no claim to match it.
- **Awaiting a produce is awaiting the acknowledgement.** `rskafka`'s `produce()`
  resolves on the broker's response, which carries the broker-assigned offsets
  the broker can only return after it has persisted the batch (the request uses
  acks all in-sync replicas, and there is no idempotence knob). So the producer
  awaits every send. This is at-least-once production; it claims no exactly-once
  or idempotence.

Each of the 1000 events gets a fresh v4 UUID as its `orderId`, and that
`orderId` is used both as the record key and inside the JSON body (with plausibly
varied customer, SKU, quantity, price, currency GBP, and an RFC3339
`placedAt`). Carrying the id in both places is what lets the consumer prove the
key survived the round trip. The producer awaits every produce, logs `DELIVERED
1000/1000`, and exits 0; any failure propagates and exits non-zero.

## What the consumer proves

The consumer is the llingr-kafka crate consuming the same topic. Several things
it does are each a deliberate part of the proof:

- **Engine logs flow through the `log` facade with no wiring.** The consumer
  installs `env_logger` and nothing more; the engine's own lines then appear
  under the target `llingr` alongside the consumer's, which is the whole
  demonstration that logging needs no logger parameter. `RUST_LOG=info` is the
  compose default.
- **The builder is the ordinary shape.** Topic `orders`, group `orders-example`,
  `AutoOffsetReset::Earliest`, `Metrics::serve("0.0.0.0:9464", "/metrics")`, and
  a shutdown handler that logs the reason.
- **The key invariant proves the plumbing.** The `ProcessHandler` parses the JSON
  and asserts that the record key equals the body `orderId`. That equality can
  only hold if the key survived producer, broker, franz-go, the engine, the FFI
  boundary, and the decode into a Rust `Message`, so the assertion is an
  end-to-end plumbing check on every single message. A parse failure or a
  mismatch returns an error, which routes the message to the dead-letter handler
  and marks the run failed.
- **Metrics move while it runs.** `Metrics::serve` exposes OpenMetrics text on
  port 9464 at `/metrics`, so during a run you can `curl localhost:9464/metrics`
  and watch the per-message counters advance.
- **It stops itself, cleanly.** Because `run()` blocks, a monitor thread watches
  a processed-message counter and calls the stopper once the expected count
  (1000) is reached, which releases `run()`. The consumer exits 0 only on the
  clean path (all 1000 processed, no dead letters, the invariant never violated);
  it exits 1 on any dead letter, any invariant violation, or a 120-second
  timeout.

## How the exit code is the proof

`make example-verify` brings the stack up detached with a build
(`docker compose up -d --build`), waits directly on the consumer container with
`docker wait` to capture its exit code, prints the stack logs so the run's
evidence is visible, tears the stack down unconditionally with
`docker compose down -v`, and exits with the consumer's captured code. So a
single command gives a single answer: exit 0 means the producer delivered all
1000, the consumer processed all 1000 with the key invariant holding on every
message, no message dead-lettered, and the consumer shut itself down through the
stopper. There is nothing to eyeball; the exit code is the whole verdict.

The detached-wait shape (rather than `docker compose up --exit-code-from
consumer`) is deliberate, and it is a useful thing to know if you adapt this
example: `--exit-code-from` implies `--abort-on-container-exit`, which would tear
the whole stack down the instant the one-shot `topic-init` container exits,
killing the broker out from under the consumer. Waiting on the consumer container
directly is the correct way to get a single verdict out of a topology that mixes
one-shot containers (`topic-init`, `producer`) with a long-running one
(`consumer`).

Because the consumer's image builds the entire crate, including the Go bridge,
inside the image (the static-scratch pattern in `example/Dockerfile.consumer`),
`example-verify` also exercises the from-source Docker build path on every run,
not just the runtime behaviour. The build and packaging mechanics behind that
image are in `docs/building-packaging.md`.

## Adapting the pieces

The example is a scaffold you can bend to your own shape:

- **Swap the payload and the work.** Replace the `Order` struct and the
  `ProcessHandler` body with your domain type and your processing. The key
  invariant is a proof device for the example; your handler does real work and
  returns whatever `Traits` bits you care about (see `docs/processing.md`).
- **Point at a real broker.** The consumer and producer both read `BROKERS` from
  the environment, so aim them at Apache Kafka, RedPanda, or Amazon MSK by
  changing that one value; add TLS or SASL through the `Options` builder as in
  `docs/security.md`.
- **Change the shape of the load.** The partition count (`topic-init` and the
  producer's `PARTITIONS`) and the message count (`COUNT`) are environment-driven;
  raise them to exercise heavier per-key concurrency.
- **Borrow the shutdown pattern.** The monitor-thread-plus-stopper structure is
  a clean template for any consumer that must stop on a condition; for
  signal-driven shutdown, see the `signal-hook` pattern in
  `docs/operations.md`.
