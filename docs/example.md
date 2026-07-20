# The end-to-end example

The `examples/e2e/` directory is a real proof, not a demo. When `make example-verify`
exits 0, the whole chain worked against a real, authenticated broker: a producer
published a thousand order events over SASL/SCRAM-SHA-256 with TLS, and a
llingr-kafka consumer processed every one of them through the demux engine, the
franz-go broker layer, the FFI boundary, the log facade, and the metrics
endpoint, with a per-message invariant checked on the way. This page explains
what the stack is, what each piece proves, how the exit code becomes the proof,
and how to adapt the pieces for your own use.

The whole chain is proven end to end: `make example-verify` exits 0, with the
producer delivering all 1000 messages and the consumer processing all 1000 with
the key invariant holding on every one, no dead letters, and a clean shutdown.
That exit-0 run is the standing proof everything on this page describes,
including the in-image build of the crate and its Go bridge and the full
SASL/SCRAM-over-TLS handshake against the broker.

## The stack

The example is a single Docker Compose stack on one network, in `examples/e2e/`. It is
secured with SASL/SCRAM-SHA-256 over TLS (`sasl_ssl`), so it exercises the
security configuration, not just the happy path. It has five services:

- **`cert-gen`**: a one-shot, image `alpine/openssl`, that mints a throwaway
  self-signed CA and a broker certificate into a shared `certs` volume, then
  exits. The broker certificate's SAN covers the compose service name `redpanda`
  and `localhost`. Nothing is committed: the CA and certificate live only in
  the volume, generated fresh per run.
- **`redpanda`**: one RedPanda node, image `redpandadata/redpanda:v24.2.7`,
  Kafka-compatible and needing no ZooKeeper, brought up with a `sasl_ssl`
  listener. Its healthcheck is `rpk cluster health` against the admin API, which
  is unauthenticated by default, so the probe works even with SASL enabled on the
  Kafka listener; nothing downstream starts until the broker reports healthy.
- **`init`**: a one-shot that creates the SCRAM-SHA-256 user `appuser` through the
  admin API, then creates the `orders` topic with 12 partitions over the
  `sasl_ssl` listener, and exits. Twelve partitions is deliberate: it gives
  per-key routing and out-of-order completion something real to spread across, so
  the engine's concurrency is genuinely exercised rather than trivially serial.
- **`producer`**: a plain franz-go producer in Go (see below) that publishes 1000
  order events over `sasl_ssl` and exits. It depends on `init` completing
  successfully.
- **`consumer`**: the llingr-kafka consumer, described below. It starts alongside
  the producer, since consuming from the earliest offset makes their ordering
  moot, authenticates the same way, and publishes port 9464 so `/metrics` can be
  curled mid-run.

## Certificates and the SCRAM user

The security material is bootstrapped by the two one-shot services, so a fresh
`make example-verify` is self-contained and commits no secrets. `cert-gen` runs
`scripts/gen-certs.sh`, which generates a self-signed CA (`ca-cert.pem`) and a
broker key and certificate (`server-key.pem`, `server-cert.pem`) into the shared
volume; the script is idempotent, so a re-run against a warm volume does not
rotate the CA out from under a running broker. RedPanda then serves TLS on its
Kafka listener from that certificate, and every client (producer, consumer, and
the `init` step itself) trusts the CA at `TLS_CA_LOCATION=/certs/ca-cert.pem`.

`init` creates the SCRAM credentials and the topic. It runs `rpk security user
create appuser -p apppassword --mechanism SCRAM-SHA-256` against the admin API
(which needs no auth), which is what makes `appuser` usable; the broker's cluster
config makes `appuser` a superuser and turns SASL on for the Kafka listener. It
then creates the topic over the authenticated listener, passing the SCRAM
credentials and the CA to `rpk`. The username and password (`appuser` /
`apppassword`) are the example's throwaway credentials, set in the compose file
and passed to the clients as `SASL_USERNAME` / `SASL_PASSWORD`.

## What the producer proves

The producer is a plain, standalone franz-go producer written in Go, in its own
Go module (`examples/e2e/producer`, pinning `franz-go` v1.21.5). It is a completely
normal ecosystem Kafka client on the produce side, with the Rust llingr-kafka
crate on the consume side; the two meet at the broker. Writing the producer in
Go keeps the example images fully C-free: it builds with `CGO_ENABLED=0` (pure
Go, `netgo` DNS) into a static binary on a `scratch` image, with no C toolchain
and no cmake anywhere in the stack.

As an ordinary franz-go client it does the ordinary things. franz-go's default
key-based partitioner routes each keyed record to a partition, and `ProduceSync`
blocks until every record is acknowledged by the broker (franz-go is an
idempotent producer by default, so that acknowledgement is `acks=all`). For
security it uses `kgo.SASL` with the SCRAM-SHA-256 mechanism and
`kgo.DialTLSConfig` trusting the example CA, which is env-driven: with
`SASL_USERNAME` set it authenticates over `sasl_ssl`, and without it (unused by
this stack) it would connect in plaintext.

Each of the 1000 events gets a fresh v4 UUID as its `orderId`, used both as the
record key and inside the JSON body (with plausibly varied customer, SKU,
quantity, price, currency GBP, and an RFC3339 `placedAt`). Carrying the id in
both places is what lets the consumer prove the key survived the round trip. The
producer awaits every record, logs `DELIVERED 1000/1000`, and exits 0; any
failure propagates and exits non-zero.

## What the consumer proves

The consumer is the llingr-kafka crate consuming the same topic. Several things
it does are each a deliberate part of the proof:

- **The security configuration runs end to end.** The consumer configures SASL/SCRAM and
  TLS from the environment: with `SASL_USERNAME` set it calls
  `Options::new().sasl_scram_sha256(user, pass).tls_ca_location(ca_path)`, and the
  typed setters compute `security.protocol=sasl_ssl` from the presence of both a
  SASL and a TLS setter. This is the exact `docs/security.md` SCRAM-over-TLS
  configuration, running for real against the broker on every `make example-verify`.
- **Engine logs flow through the `log` facade with no wiring.** The consumer
  installs `env_logger` and nothing more; the engine's own lines then appear
  under the target `llingr` alongside the consumer's, the whole demonstration that
  logging needs no logger parameter. `RUST_LOG=info` is the compose default.
- **The builder is the ordinary form.** Topic `orders`, group `orders-example`,
  `AutoOffsetReset::Earliest`, `Metrics::serve("0.0.0.0:9464", "/metrics")`, a
  shutdown handler that logs the reason, and the `Options` with the SCRAM/TLS
  settings above.
- **The key invariant proves the plumbing.** The `ProcessHandler` parses the JSON
  and asserts that the record key equals the body `orderId`. That equality can
  only hold if the key survived producer, broker, franz-go, the engine, the FFI
  boundary, and the decode into a Rust `Message`, so the assertion is an
  end-to-end plumbing check on every single message. A parse failure or a
  mismatch returns an error, routing the message to the dead-letter handler and
  marking the run failed.
- **Metrics move while it runs.** `Metrics::serve` exposes OpenMetrics text on
  port 9464 at `/metrics`, so during a run you can `curl localhost:9464/metrics`
  and watch the per-message counters advance.
- **It stops itself, cleanly.** Because `run()` blocks, a monitor thread watches a
  processed-message counter and calls the stopper once the expected count (1000)
  is reached, which releases `run()`. The consumer exits 0 only on the clean path
  (all 1000 processed, no dead letters, the invariant never violated); it exits 1
  on any dead letter, any invariant violation, or a 120-second timeout.

## How the exit code is the proof

`make example-verify` brings the stack up detached with a build
(`docker compose up -d --build`), waits directly on the consumer container with
`docker wait` to capture its exit code, prints the stack logs so the run's
evidence is visible, tears the stack down unconditionally with
`docker compose down -v`, and exits with the consumer's captured code. So a
single command gives a single answer: exit 0 means the producer delivered all
1000 over `sasl_ssl`, the consumer processed all 1000 with the key invariant
holding on every message, no message dead-lettered, and the consumer shut itself
down through the stopper. There is nothing to inspect manually; the exit code is the whole
verdict.

The detached-wait flow (rather than `docker compose up --exit-code-from
consumer`) is deliberate, and it is a useful thing to know if you adapt this
example: `--exit-code-from` implies `--abort-on-container-exit`, which would tear
the whole stack down the instant a one-shot container (`cert-gen` or `init`)
exits, killing the broker out from under the consumer. Waiting on the consumer
container directly is the correct way to get a single verdict out of a topology
that mixes one-shot containers (`cert-gen`, `init`, `producer`) with a
long-running one (`consumer`).

Because the consumer's image builds the entire crate, including the Go bridge,
inside the image (the static-scratch pattern in `examples/e2e/Dockerfile`),
`example-verify` also exercises the from-source Docker build path on every run,
not just the runtime behaviour. The build and packaging mechanics behind that
image are in `docs/building-packaging.md`.

## RedPanda configuration notes

Configuring RedPanda for `sasl_ssl` in this stack ran into three sharp edges
worth recording, because they are not obvious and they generalise to any
Dockerised RedPanda with TLS:

- **`rpk redpanda start` has no `--set` flag** in the v24 image, so the
  `sasl_ssl` listener and its TLS (certificate and key paths) cannot be passed on
  the command line; they live in a mounted `redpanda.yaml`. SASL enablement
  (`enable_sasl`) and the superuser are cluster config, set in `.bootstrap.yaml`.
- **`rpk` rewrites `redpanda.yaml` at startup**, so the config file cannot be a
  read-only bind mount (the rename-over-mount fails with "device busy"). The
  service copies the mounted configs into the container's writable
  `/etc/redpanda` before starting the broker, via a tiny shell entrypoint.
- **`advertised_rpc_api` must be an explicit address** (the service name
  `redpanda`), not `0.0.0.0`; a wildcard advertised RPC address does not resolve
  for intra-cluster use.

## Adapting the pieces

The example is a scaffold you can customise for your own needs:

- **Swap the payload and the work.** Replace the order type and the
  `ProcessHandler` body with your domain type and your processing. The key
  invariant is a proof device for the example; your handler does real work and
  returns whatever `Traits` bits you care about (see `docs/processing.md`).
- **Point at a real broker.** The consumer and producer both read `BROKERS`,
  `SASL_USERNAME`, `SASL_PASSWORD`, and `TLS_CA_LOCATION` from the environment, so
  aim them at Apache Kafka, RedPanda, or Amazon MSK by changing those values. The
  consumer's env-to-`Options` mapping is the SCRAM-over-TLS configuration from
  `docs/security.md`; for other mechanisms (mTLS, AWS_MSK_IAM, OAUTHBEARER) swap
  in the matching `Options` setters from that page.
- **Scale the load.** The topic's partition count, set by `init`, and the
  message count, `COUNT`, are configurable; raise them to exercise heavier
  per-key concurrency.
- **Borrow the shutdown pattern.** The monitor-thread-plus-stopper structure is a
  clean template for any consumer that must stop on a condition; for
  signal-driven shutdown, see the `signal-hook` pattern in `docs/operations.md`.
