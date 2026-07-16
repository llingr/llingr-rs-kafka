# Kafka client options

You configure the Kafka client through the typed `Options` builder, which is the
single home for all client configuration: connection and consumer-group tuning,
fetch sizing, retries and timeouts, and (covered in `docs/security.md`) TLS and
SASL. Anything the typed setters do not cover is reachable on the same builder as
librdkafka-style string key/value pairs. The design principle throughout is that
nothing is silently ignored: an unknown key fails at `build()` with the full list
of supported keys, and options that would corrupt the engine's guarantees are
deliberately excluded and named here with their reasons, rather than quietly
accepted and ignored.

You attach an `Options` value with the builder's `.options(...)` hook. Every
typed setter emits a Kafka config key, which the engine translates into the
equivalent franz-go (`kgo`) option; the key names are the standard Kafka client
config names where one exists, so configuration written for another Kafka client
ports across.

## The typed client options

| Builder method | Key emitted | Translated to |
|---|---|---|
| `auto_offset_reset(AutoOffsetReset)` | `auto.offset.reset` | `kgo.ConsumeResetOffset` |
| `client_id(&str)` | `client.id` | `kgo.ClientID` |
| `group_instance_id(&str)` | `group.instance.id` | `kgo.InstanceID` (static membership) |
| `session_timeout(Duration)` | `session.timeout.ms` | `kgo.SessionTimeout` |
| `heartbeat_interval(Duration)` | `heartbeat.interval.ms` | `kgo.HeartbeatInterval` |
| `rebalance_timeout(Duration)` | `rebalance.timeout.ms` | `kgo.RebalanceTimeout`, validated to exceed the engine drain timeout (see below) |
| `partition_assignment_strategy(&[BalanceStrategy])` | `partition.assignment.strategy` | `kgo.Balancers` |
| `fetch_min_bytes(u32)` | `fetch.min.bytes` | `kgo.FetchMinBytes` |
| `fetch_max_bytes(u32)` | `fetch.max.bytes` | `kgo.FetchMaxBytes` |
| `max_partition_fetch_bytes(u32)` | `max.partition.fetch.bytes` | `kgo.FetchMaxPartitionBytes` |
| `fetch_max_wait(Duration)` | `fetch.max.wait.ms` | `kgo.FetchMaxWait` (the librdkafka spelling `fetch.wait.max.ms` is also accepted as a string pair) |
| `rack(&str)` | `client.rack` | `kgo.Rack` (rack-aware fetch-from-follower) |
| `metadata_max_age(Duration)` | `metadata.max.age.ms` | `kgo.MetadataMaxAge` |
| `dial_timeout(Duration)` | `socket.connection.setup.timeout.ms` | `kgo.DialTimeout` (TCP and TLS establishment; client default 10s) |
| `connection_idle_timeout(Duration)` | `connections.max.idle.ms` | `kgo.ConnIdleTimeout` (client default 30s) |
| `allow_auto_topic_creation()` | `allow.auto.create.topics=true` | `kgo.AllowAutoTopicCreation` (off by default; the broker must also permit it) |
| `disable_fetch_crc_validation()` | `check.crcs=false` | `kgo.DisableFetchCRCValidation` (validation is on by default) |
| `disable_client_metrics_push()` | `enable.metrics.push=false` | `kgo.DisableClientMetrics` (KIP-714; on by default where the broker supports it) |

The security setters (`tls*`, `sasl_*`, and the verification toggles) live on the
same builder and are documented in full, with one worked example per mechanism,
in `docs/security.md`.

**`rebalance_timeout` is validated against the engine.** The engine drains
in-flight work during a rebalance before acknowledging the revoke, so
`rebalance.timeout.ms` must exceed the `drain_timeout` engine knob (default 20
seconds), or `build()` fails; the defaults satisfy it. The error is in
`docs/troubleshooting.md`, and the drain relationship in `docs/configuration.md`.

## The llingr namespace: engine and adapter options

Some options have no standard librdkafka key name, either because they are
adapter-level behaviour or because they are franz-go client options without a
librdkafka analogue. These are namespaced under `llingr.` so they never collide
with a standard Kafka client key, and they are set through the same typed
builder:

| Builder method | Key emitted | Meaning and range |
|---|---|---|
| `client_log_level(ClientLogLevel)` | `llingr.client.log.level` | Verbosity of the franz-go client's own internal logging (distinct from the engine's; see `docs/logging.md`) |
| `request_retries(u32)` | `llingr.request.retries` | `kgo.RequestRetries` for non-fetch retryable requests (client default 20) |
| `retry_timeout(Duration)` | `llingr.retry.timeout.ms` | `kgo.RetryTimeout` (client default: the session timeout for group requests, 30s otherwise) |
| `max_concurrent_fetches(u32)` | `llingr.max.concurrent.fetches` | `kgo.MaxConcurrentFetches`; must be at least 1 (client default is unlimited, bounded by the broker count) |
| `poll_error_bail_after(Duration)` | `llingr.poll.error.bail.after.ms` | How long continuous poll failures are tolerated before the adapter triggers an emergency shutdown. `0` disables the bail; otherwise the range is [1 minute, 1 hour] and a value outside it fails init. Default 10 minutes |
| `poll_error_log_interval(Duration)` | `llingr.poll.error.log.interval.ms` | How often the ongoing poll failures are logged while the bail window runs; must be positive. Default 1 second |
| `poll_error_backoff(Duration)` | `llingr.poll.error.backoff.ms` | How long the adapter waits between failed poll attempts. `0` disables the backoff; otherwise at most 5 seconds. Default 25 milliseconds |

The poll-error trio governs the sustained-poll-error bail described in
`docs/operations.md`: `poll_error_bail_after` is the ten-minute window (now
tunable), and the shutdown it triggers carries the reason catalogued in
`docs/troubleshooting.md`.

## The string escape hatch

Anything the typed setters do not cover is reachable as librdkafka-style string
key/value pairs on the same `Options` builder, with `kafka_option(key, value)`
for one pair or `kafka_options(pairs)` for many:

```rust
# fn demo() -> Result<(), Box<dyn std::error::Error>> {
use llingr_kafka::{Options, AutoOffsetReset};
let opts = Options::new()
    .auto_offset_reset(AutoOffsetReset::Earliest)
    .kafka_option("isolation.level", "read_committed");
# let _ = opts; Ok(())
# }
```

Three behaviours to know:

- **Last write wins.** Repeating a key (across typed setters and string pairs, in
  call order) is deterministic: the last value set is the one used. This is the
  layered-config idiom, so a base configuration followed by overrides behaves as
  you expect.
- **A security key set two ways is rejected.** Setting the same security key
  through both a typed setter and a string pair on one builder is ambiguous and
  fails at `build()` (the exact message is in `docs/troubleshooting.md`).
  Configure each key in one place.
- **`bootstrap.servers` and `group.id` are reserved.** They are set with the
  builder's `.brokers(...)` and `.consumer_group(...)`, so passing them as string
  pairs is an init error.

`isolation.level` is a string-only key (there is no typed setter): `read_committed`
is accepted and is the safe default, and `read_uncommitted` is rejected outright
(see the exclusions below).

One nuance worth stating: a boolean key that merely restates a default is accepted
and sets nothing. `check.crcs=true`, `allow.auto.create.topics=false`, and
`enable.metrics.push=true` are all valid configuration that changes no behaviour
(the typed setters `disable_fetch_crc_validation()`, `allow_auto_topic_creation()`,
and `disable_client_metrics_push()` are how you change these from their defaults).

## Unknown keys fail loudly

Every key that is neither a supported option nor a recognised security key fails
initialisation with the complete supported-key list, never a silent no-op. For
example, `kafka_option("no.such.key", ...)` fails with the 41-key list:

`option "no.such.key" is not supported (supported: allow.auto.create.topics, auto.offset.reset, check.crcs, client.id, client.rack, connections.max.idle.ms, enable.metrics.push, enable.ssl.certificate.verification, fetch.max.bytes, fetch.max.wait.ms, fetch.min.bytes, fetch.wait.max.ms, group.instance.id, heartbeat.interval.ms, isolation.level, llingr.client.log.level, llingr.max.concurrent.fetches, llingr.poll.error.backoff.ms, llingr.poll.error.bail.after.ms, llingr.poll.error.log.interval.ms, llingr.request.retries, llingr.retry.timeout.ms, max.partition.fetch.bytes, metadata.max.age.ms, partition.assignment.strategy, rebalance.timeout.ms, sasl.mechanism, sasl.mechanisms, sasl.password, sasl.username, security.protocol, session.timeout.ms, socket.connection.setup.timeout.ms, ssl.ca.location, ssl.ca.pem, ssl.certificate.location, ssl.certificate.pem, ssl.endpoint.identification.algorithm, ssl.key.location, ssl.key.password, ssl.key.pem)`

## What is deliberately excluded, and why

The supported set above is curated. Many franz-go options are deliberately not
exposed, and because "not exposed" here means "fails at init", the reasons matter
and are given below. The theme is that the engine owns the parts of the consumer
that make its guarantees hold, so options that would let you fight those
guarantees are excluded rather than offered as footguns.

**Engine-owned (exposing them would corrupt the engine's contract):**

| Excluded option(s) | Reason |
|---|---|
| The auto-commit family (`DisableAutoCommit`, `GreedyAutoCommit`, `AutoCommitInterval`, `AutoCommitMarks`, `AutoCommitCallback`) | The adapter forces `DisableAutoCommit`: the engine's gap-buffer committer owns offset commits. The `DemuxConfig` `auto_commit_interval` knob is the cadence control |
| The rebalance callbacks (`OnPartitionsAssigned`/`Revoked`/`Lost`, `BlockRebalanceOnPoll`, `AdjustFetchOffsetsFn`, `OnOffsetsFetched`) | The adapter installs its own rebalance callbacks to run the engine's drain-before-revoke coordination |
| Topic selection (`ConsumeTopics`, `ConsumePartitions`, `ConsumeRegex`, `ConsumeExcludeTopics`) | One topic per consumer is the `Builder` contract; the adapter sets the topic |
| `ConsumeStartOffset` | Explicit start offsets bypass committed group offsets; `auto.offset.reset` is the supported reset policy |
| `FetchIsolationLevel(ReadUncommitted)` (`isolation.level=read_uncommitted`) | A guard rail: aborted transactional records would otherwise be processed and committed downstream |
| `KeepControlRecords` | Control records are transaction and commit machinery the engine must not process |
| `WithLogger` | The adapter owns the franz-go log bridge; `client_log_level` sets its verbosity |
| `KeepRetryableFetchErrors` | Retriable fetch errors belong to the adapter's poll-error machinery (the bail window); surfacing them would fight it |

**A different consumption model, or deferred pending validation:**

| Excluded option(s) | Reason |
|---|---|
| The share-group family (`ShareGroup`, `ShareMaxRecords`, `ShareMaxRecordsStrict`, `ShareAckCallback`) | KIP-932 share groups are a different consumption model; this engine is a classic consumer-group consumer |
| `GroupProtocol` | The KIP-848 next-generation rebalance protocol is not yet validated against the engine's drain coordination, so it is deferred deliberately |
| `RequireStableFetchOffsets` | KIP-447 interacts with the engine's commit timing and is deferred pending its own validation |

**Not part of a consumer:** the entire producer surface (`DefaultProduceTopic`
through `TransactionTimeout`, including acks, compression, idempotence, lingering,
and transactions) does not exist, because this crate is a consumer.

**Not expressible as configuration:** function-valued and plumbing options
(`Dialer`, `RetryBackoffFn`, `RetryTimeoutFn`, `WithHooks`, `WithPools`,
`WithContext`, `OnRebootstrapRequired`, `ConsumePreferringLagFn`,
`WithDecompressor`, `WithCompressor`, `UserMetricsFn`) are code, not config, so a
string or typed setter cannot carry them.

**Expert or niche, with no reasonable operator expectation:** a handful of
low-level tweaks (`RequestTimeoutOverhead`, `BrokerMaxWriteBytes`/`ReadBytes`,
`MetadataMinAge`, `MaxVersions`/`MinVersions`, `SoftwareNameAndVersion`,
`ConcurrentTransactionsBackoff`, `ConsiderMissingTopicDeletedAfter`,
`AlwaysRetryEOF`, `DisableFetchSessions`, `RecheckPreferredReplicaInterval`) are
excluded because the operator-facing surface for what they touch is elsewhere
(for example fetch sizing is the `fetch.max.bytes` family, and follower-fetch
tuning is `rack()`).

## A worked example

```rust
# fn demo() -> Result<(), Box<dyn std::error::Error>> {
use llingr_kafka::{Options, AutoOffsetReset, BalanceStrategy};
use std::time::Duration;

let opts = Options::new()
    .auto_offset_reset(AutoOffsetReset::Earliest)
    .client_id("orders-consumer-1")
    .session_timeout(Duration::from_secs(30))
    .fetch_max_bytes(50 * 1024 * 1024)
    .partition_assignment_strategy(&[BalanceStrategy::CooperativeSticky])
    .max_concurrent_fetches(8)
    .poll_error_bail_after(Duration::from_secs(300)) // shorten the bail window to 5 minutes
    .kafka_option("isolation.level", "read_committed");
# let _ = opts; Ok(())
# }
```

The security surface (TLS, mTLS, SASL PLAIN/SCRAM, and the scheduled AWS_MSK_IAM
and OAUTHBEARER mechanisms) is in `docs/security.md`; the engine tuning knobs
(worker concurrency, timeouts, and the `drain_timeout` that `rebalance_timeout`
is checked against) are in `docs/configuration.md`.
