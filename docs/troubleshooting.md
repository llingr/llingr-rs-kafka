# Troubleshooting

This page helps you recognise a failure and know the fix. Failures fall into
three groups: build-time failures (something wrong while `cargo build` compiles
the crate and its engine), initialisation failures (`Builder::build()` returns
an error before the consumer runs), and runtime shutdowns (the engine stops and
tells you why). Each error below is paired with its cause and what to do about
it.

## Build-time failures

**"Go toolchain not found on PATH" or "Go 1.25+ is required for the engine
build".** llingr-kafka compiles its Go engine during `cargo build`, so the build
needs Go 1.25 or newer (and a C compiler for cgo). The build script fails with
one of these messages and names three remedies, and any one of them fixes it:

1. Install Go 1.25 or newer (from `https://go.dev/dl/`) and a C compiler, then
   `cargo build` compiles the engine from source.
2. Build the engine once with `make engine` (which can use Docker) and set
   `LLINGR_LIB_DIR=dist/<target-triple>`, so `cargo build` links the prebuilt
   archive and skips Go entirely.
3. Build the whole application inside the provided builder image
   (`docker/Dockerfile`), so the machine needs only Docker.

The full build model is in `docs/building-packaging.md`.

**A `panic = "abort"` warning.** If your build profile sets `panic = "abort"`,
the build script emits a loud warning (not an error). It is telling you that the
panic-to-dead-letter contract is disabled: under `abort`, a panicking handler
aborts the whole process instead of being caught and dead-lettered. Remove
`panic = "abort"` (llingr-kafka needs the default `unwind` profile) unless a
whole-process abort per handler panic is genuinely what you want.

**A `*-musl` target fails immediately.** Targeting a musl triple fails the build
with the upstream-blocker links, because the Go runtime cannot initialise as a
static guest library on musl. Build against glibc. The full record is in
`docs/internal/MUSL.md`.

## Initialisation failures (`Builder::build()` returns an error)

`Builder::build()` returns a `Result`, and its error has a code and a
message. These are the cases you will hit.

**A broker that cannot be reached.** The client connects eagerly, during
`build()`, not lazily at the first poll, so a wrong or unreachable broker address
fails `build()` immediately with `franz adapter: failed to connect to broker:
unable to dial ...`; the tail is the underlying dial error. This is a good
operational property: a broker misconfiguration is caught at startup, where the
process can fail fast and be restarted by a supervisor, rather than after the
consumer appears to have started. Check the `brokers` address and network
reachability. The failure-domains table in `docs/operations.md` lists this under
"Broker unreachable at `build()`", consistent with the eager connect.

**An unknown Kafka option.** A `kafka_option(key, value)` or `kafka_options(pairs)`
key that the engine does not recognise fails `build()` with the full list of
supported keys, never a silent no-op, so a typo or an unsupported key is caught
at startup rather than quietly ignored. The escape hatch and its keys live on the
`Options` builder (documented in `docs/kafka-options.md`). For example,
`kafka_option("no.such.key", ...)` fails with:

`option "no.such.key" is not supported (supported: allow.auto.create.topics, auto.offset.reset, aws.profile, aws.region, aws.role.arn, aws.role.session.name, check.crcs, client.id, client.rack, connections.max.idle.ms, enable.metrics.push, enable.ssl.certificate.verification, fetch.max.bytes, fetch.max.wait.ms, fetch.min.bytes, fetch.wait.max.ms, gcp.credentials.file, gcp.principal, group.instance.id, heartbeat.interval.ms, isolation.level, llingr.client.log.level, llingr.max.concurrent.fetches, llingr.poll.error.backoff.ms, llingr.poll.error.bail.after.ms, llingr.poll.error.log.interval.ms, llingr.request.retries, llingr.retry.timeout.ms, max.partition.fetch.bytes, metadata.max.age.ms, partition.assignment.strategy, rebalance.timeout.ms, sasl.mechanism, sasl.mechanisms, sasl.oauthbearer.client.id, sasl.oauthbearer.client.secret, sasl.oauthbearer.extensions, sasl.oauthbearer.method, sasl.oauthbearer.scope, sasl.oauthbearer.token.endpoint.url, sasl.password, sasl.username, security.protocol, session.timeout.ms, socket.connection.setup.timeout.ms, ssl.ca.location, ssl.ca.pem, ssl.certificate.location, ssl.certificate.pem, ssl.endpoint.identification.algorithm, ssl.key.location, ssl.key.password, ssl.key.pem)`

The message lists only the supported keys and refers to no other adapter.

**Security misconfiguration.** Two shapes:

- **Set twice, two ways (caught on the Rust side).** Setting the same security
  key through both a typed setter and a string `kafka_option` pair on one
  `Options` builder is ambiguous and rejected at `build()` with:

  `Options: security key(s) set both via typed setters and kafka_option(): <keys>; configure each key with one style only`

  The fix is in the message: configure each key in one place, either the typed
  setter (for example `sasl_scram_sha256(...)`) or the string pair, not both.

- **Conflicting protocol and credentials (caught by the engine).** The security
  keys (`security.protocol`, `ssl.*`, `sasl.*`) are validated together as one
  unit, so combinations that cannot be right fail `build()` with a specific
  message. The exact text, for example: `security options were provided but
  security.protocol is not set (expected one of: plaintext, ssl, sasl_plaintext,
  sasl_ssl)`; `sasl.* options require security.protocol=sasl_plaintext or
  sasl_ssl (got "ssl")`; `sasl.mechanism is required with
  security.protocol=sasl_plaintext`; and `ssl.key.password is not supported
  (encrypted client keys need OpenSSL); decrypt the key before configuring it`.
  The full catalogue and one worked example per mechanism are in
  `docs/security.md`.

**Auth mechanism misconfiguration (AWS_MSK_IAM, OAUTHBEARER, GCP IAM).** These
mechanisms resolve their credentials for you, so static username/password and keys
belonging to a different mechanism are rejected, and the credentials are resolved
eagerly at `build()` so a bad source fails fast rather than stalling the first
handshake.
Representative messages: `sasl.mechanism=AWS_MSK_IAM requires
security.protocol=sasl_ssl (MSK IAM is TLS-only), got "..."`;
`sasl.username/sasl.password are not used with AWS_MSK_IAM (credentials come from
the AWS provider chain); remove them` (and the corresponding OAUTHBEARER message);
`aws.role.session.name requires aws.role.arn`; `aws.* options apply only to
sasl.mechanism=AWS_MSK_IAM (got ...)` (and the `sasl.oauthbearer.*` equivalent);
`OAUTHBEARER requires sasl.oauthbearer.token.endpoint.url`; and the eager-resolution
failures `AWS_MSK_IAM: resolving credentials from the AWS provider chain (env,
shared config/profile, STS, web identity, IMDS): ...` and `OAUTHBEARER: fetching
initial token: ...`. GCP IAM (OAUTHBEARER `method=gcp`) follows the same pattern:
`sasl.oauthbearer.method=gcp requires security.protocol=sasl_ssl (Google Cloud
Managed Service for Apache Kafka is TLS-only), got "..."`; `unable to determine
the GCP principal for the credentials: set gcp.principal (or the
GOOGLE_MANAGED_KAFKA_AUTH_PRINCIPAL environment variable) to the authenticating
identity's email`; and the eager-resolution failure `gcp: fetching initial token
from Application Default Credentials: ...`. The full catalogue with worked
examples is in `docs/security.md`.

**A rebalance timeout that does not clear the drain.** The engine drains
in-flight work during a rebalance before acknowledging the revoke, so the Kafka
`rebalance.timeout.ms` must exceed the engine's `drain_timeout`; a rebalance
timeout at or below the drain budget could evict the consumer mid-drain and
produce duplicates on every rebalance. This is now enforced at `build()`, with
the two durations interpolated, for example:

`rebalance.timeout.ms (15s) must exceed the engine drain timeout (20s): the engine drains in-flight work during a rebalance before acking the revoke, and a rebalance timeout at or below the drain budget can evict the consumer mid-drain, causing duplicates`

Raise `rebalance.timeout.ms` (an `Options` setter) above `drain_timeout` (a
`DemuxConfig` setting, default 20 seconds), or lower `drain_timeout`; the
defaults satisfy it. See `docs/configuration.md`.

**An ABI mismatch.** If the linked engine library reports a different FFI
contract version than the crate was compiled against, `build()` refuses to run
with `llingr ABI mismatch: crate expects 1, library reports N (rebuild libllingr
to match this crate)`, turning what would be silent memory corruption into a
clean startup error. It means the crate and the engine archive are out of step:
rebuild the engine so it matches the crate (a clean build, or rebuild whatever
prebuilt archive `LLINGR_LIB_DIR` points at). The ABI discipline is described in
`docs/internal/ARCHITECTURE.md`.

Every `build()` failure is an `LlingrError` with a numeric code, rendered as
`llingr error <code>: <message>`. The codes you may meet: **-1** for an ABI
mismatch and for the one-instance-per-process error below; **-2** for a malformed
configuration; **-5** for the `Options` typed-vs-string security-key conflict
above; and **-6** when a `Metrics::serve` endpoint cannot bind its address, with
the message `metrics endpoint failed to bind <addr>: <underlying error>`, where
the tail is the underlying bind error from the OS.

**"only one llingr instance per process".** The Go runtime is process-global, so
a second `build()` in the same process fails (`LlingrError` code -1). Run more
consumers as more processes, which is also how you scale a consumer group. See
`docs/operations.md`.

## Runtime shutdowns: what the reason tells you

When the engine stops, your `ShutdownHandler` receives a reason exactly once (and
the same information appears in the engine logs under the target `llingr`). The
reason tells you which path fired.

**"graceful shutdown".** A normal `stop()` completed: in-flight work drained,
offsets committed, `run()` returned. This is the clean path and the one you want
on a rolling restart. See `docs/operations.md`.

**A circuit-breaker reason, prefix `pipeline-worker: circuit-breaker triggered
processing partition ..., offset ...`.** This is an emergency shutdown, and the
literal "circuit-breaker" wording is the engine's own reason string. It reaches
you through two different doors:

- **The dead-letter write failed or panicked.** A message failed processing,
  went to your `DeadLetterHandler`, and that handler returned an error or
  panicked. Because a message that can be neither processed nor dead-lettered
  cannot have its offset safely committed, the engine stops rather than lose it.
  The fix is to make the dead-letter handler robust: give it bounded retries and
  a fallback so a transient blip in its store does not escalate into a shutdown
  (see `docs/processing.md`).
- **A worker could not be acquired in time.** The same internal breaker fires if
  dispatch waits longer than the `acquire_worker_timeout_circuit_breaker` engine
  setting (default 1 minute) for a free worker, which points at handlers that
  are stalled or far too slow for the load. Investigate handler latency; the
  setting is in `docs/configuration.md`.

Everywhere other than this quoted string, the narrative term for this event is
"emergency shutdown".

**A sustained-poll-error reason.** If polling the broker fails continuously for
ten minutes, because the broker is unreachable or authorisation was revoked,
the adapter triggers an emergency shutdown, and your shutdown handler receives a
reason of the form `partition <topic>[<n>] failing to fetch for 10m0s:
<underlying error>`; the logs contain a matching `stopping consumer after
sustained poll failure: <same>` line just before. It does not exit the process
for you; your shutdown handler fires and `run()` returns, and whether to exit so
a supervisor restarts you is your decision. The window defaults to ten minutes and
is configurable with the `poll_error_bail_after` option (range [1 minute, 1 hour],
or `0` to disable the bail), documented in `docs/kafka-options.md`. See the Liveness
section of `docs/operations.md`.

**Your own emergency-stop reason.** If you called `engine.emergency_stop(reason)`,
the reason your handler receives is the string you passed (an empty string
becomes a default description). Remember the consequence: an emergency stop
abandons in-flight work uncommitted, so those messages are redelivered on
restart, and llingr-kafka is at-least-once. See `docs/operations.md`.
