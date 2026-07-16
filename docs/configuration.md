# Engine configuration

You tune the engine through `DemuxConfig`, a builder of thirteen typed settings
covering worker concurrency, buffer sizes, and the engine's internal timeouts.
The important thing to know first is that you rarely need it: every setting has an
engine default chosen for production, and leaving `DemuxConfig` off the builder
entirely runs on those defaults. Reach for it when you have a specific reason
(measured throughput, a known burst pattern, an unusually slow downstream), not as
a matter of course. This page gives every setting, its default, its units, and when
changing it is warranted.

You pass a `DemuxConfig` to the builder's `.demux(...)` method:

```rust
# use llingr_kafka::{Builder, DemuxConfig, ProcessHandler, DeadLetterHandler, Message, Traits};
# struct P; impl ProcessHandler for P { fn process(&self, _m: &Message) -> Result<Traits, Box<dyn std::error::Error>> { Ok(Traits::none()) } }
# struct D; impl DeadLetterHandler for D { fn handle(&self, _m: &Message, _e: &str) -> Result<(), Box<dyn std::error::Error>> { Ok(()) } }
use std::time::Duration;
# fn demo() -> Result<(), Box<dyn std::error::Error>> {
let engine = Builder::new("orders", P, D)
    .brokers("localhost:9092")
    .consumer_group("orders-svc")
    .demux(DemuxConfig::new()
        .concurrent_keys(500)                     // engine default 250, max 5000
        .drain_timeout(Duration::from_secs(30)))  // engine default 20s, range 2s to 55s
    .build()?;
# let _ = engine;
# Ok(())
# }
```

## How defaults and validation work

The rules are uniform, so you can reason about any setting the same way:

- **Unset means default.** Any setting you do not call keeps the engine's
  production default. A `DemuxConfig::new()` with nothing set is identical to
  passing no `DemuxConfig`.
- **Zero means default, not error.** Passing `0` (or a zero `Duration`) to a
  setting selects the engine default rather than failing, so a computed value that
  happens to be zero degrades to the default instead of breaking startup. The
  two exceptions are called out in the table: `commit_ingest_channel_len`, whose
  explicit values must fall in a fixed range, and `worker_shards_count`, where
  `1` is an error because shard counts must be a power of two of at least 2.
- **Ranges are validated at `build()`.** An explicit value outside a setting's
  range is a clean startup error (an `LlingrError` returned from `build()`),
  never a panic or a crash. You find out immediately, with the reason, rather
  than at runtime.
- **Durations are millisecond-granular and round up.** Every `Duration`-valued
  setting serialises to whole milliseconds, and a sub-millisecond remainder rounds
  up, so a non-zero `Duration` never silently becomes zero.

## The thirteen settings

| Method | Engine default | Range | What it controls |
|---|---|---|---|
| `concurrent_keys(u32)` | 250 | 1 to 5000 | Maximum number of concurrent per-key workers |
| `per_key_buffer_len(u32)` | 16 | 1 to 64 | Per-worker channel buffer length |
| `poll_timeout(Duration)` | 100ms | 20ms to 2s | Broker poll timeout |
| `auto_commit_interval(Duration)` | 5s | 250ms to 15s | Offset auto-commit interval |
| `drain_timeout(Duration)` | 20s | 2s to 55s | Cap on draining in-flight work at a rebalance or shutdown |
| `await_assignments_timeout(Duration)` | 50s | 5s to 5m | How long the initial subscribe waits for partition assignment |
| `commit_ingest_channel_len(u32)` | derived from `concurrent_keys` | 1000 to 200000 when set | Length of the commit ingest channel |
| `commit_partition_slice_len(u32)` | 400 | 50 to 2000 | Initial gap-buffer size per partition |
| `query_timeout(Duration)` | 5s | 1s to 10s | Broker query timeout |
| `acquire_worker_timeout_circuit_breaker(Duration)` | 1m | 15s to 15m | How long dispatch may wait for a free worker before the engine's circuit breaker fires |
| `worker_shards_count(u32)` | 16 | power of two, 2 to 64 | Number of worker shards (reduces lock contention on the worker map) |
| `rebalance_pause_polling_timeout(Duration)` | 30s | 10s to 10m | Cap on the polling pause during a rebalance |
| `acquire_commit_guard_timeout(Duration)` | 10s | 100ms to 30s | Timeout acquiring the commit guard |

## When to change each

Most deployments touch none of these. The few worth understanding:

**`concurrent_keys` (default 250, max 5000)** is the main throughput setting: the
ceiling on how many keys process at once, and the right place to buy throughput
in this engine. Raise it when your handlers spend most of their time waiting (a
database round-trip, an HTTP call) and you have the headroom, because more
waiting handlers can be in flight at once. Bear in mind it is also a thread
budget: each handler occupies an operating-system thread for the duration of its
`process` call, so a high `concurrent_keys` with slow handlers can pin a large
number of OS threads. So keep handlers short, and size this for the per-message
time and OS-thread cost you can afford (see the thread-budgeting note in
`docs/operations.md`). Async I/O is welcome: the synchronous handler already
makes the correct pattern the natural one, which is to drive the async work to
completion inside the call rather than spawning and returning early. The reasoning
is the completion contract in `docs/processing.md`.

**`per_key_buffer_len` (default 16, max 64)** is how many messages can queue
ahead of a single key's worker. Raising it smooths bursts on hot keys at the
cost of memory per active key; the default suits most workloads.

**`worker_shards_count` (default 16, power of two, 2 to 64)** shards the internal
worker map to reduce lock contention. The default is right for almost everyone;
raise it (to the next power of two) only if profiling shows contention on the
worker map under very high key cardinality. Note the sharp edge: it must be a
power of two of at least 2, so `1` is a startup error, while `0` selects the
default.

**`drain_timeout` (default 20s, range 2s to 55s)** caps how long the engine
waits for in-flight work to finish when a partition is revoked or the consumer
stops. It is the setting behind the graceful-stop guarantee: a graceful stop
produces zero duplicates only for work that drains within this window, and work
the drain cannot finish in time is abandoned uncommitted and redelivered on
restart (see `docs/operations.md`). Raise it if your handlers are legitimately
slow and you would rather wait than redeliver; keep it comfortably below your
Kafka client's rebalance timeout so a rebalance never evicts the consumer
mid-drain. That relationship is enforced, not just advised: the Kafka
`rebalance.timeout.ms`, an `Options` setter, must exceed `drain_timeout`, or
`build()` fails with `rebalance.timeout.ms (...) must exceed the engine drain
timeout (...)`, the two durations interpolated; the defaults satisfy it. The
error is catalogued in `docs/troubleshooting.md`.

**`await_assignments_timeout` (default 50s, range 5s to 5m)** is how long the
first `run()` waits to be assigned partitions before giving up. Raise it for a
large or slow-to-coordinate group where initial assignment legitimately takes
longer than the default.

**`auto_commit_interval` (default 5s, range 250ms to 15s)** is how often the
engine commits the advanced offset position. A shorter interval narrows the
redelivery window after an ungraceful exit at the cost of more commit traffic; a
longer one does the reverse.

The remaining settings (`poll_timeout`, `query_timeout`,
`acquire_worker_timeout_circuit_breaker`, `commit_ingest_channel_len`,
`commit_partition_slice_len`, `rebalance_pause_polling_timeout`,
`acquire_commit_guard_timeout`) govern internal timing and buffer sizing that
the defaults handle well. Change them only in response to a specific, measured
problem, and rely on the range validation at `build()` to catch a value that is
out of bounds.

## What DemuxConfig does not cover

`DemuxConfig` is engine tuning only. Two adjacent things live elsewhere, and two
are deliberately not configurable:

- **Broker connection and Kafka client options**, meaning brokers, consumer
  group, offset reset, timeouts, and security, are not `DemuxConfig`. They are
  set with `.brokers(...)`, `.consumer_group(...)`, and the `Options` builder,
  documented in `docs/kafka-options.md` and `docs/security.md`.
- **Logging** has no configuration setting here by design: engine logs flow into
  the Rust `log` facade under the target `llingr`, and you configure verbosity
  through your logger, for example `RUST_LOG=llingr=debug`, not through
  `DemuxConfig`. See `docs/logging.md`. If you came here looking for a logger
  setting, that is why there is none.
- **Envelope extraction**, how a record's key, partition, and offset are read
  from the broker message, is not exposed either: the crate uses the adapter's
  canonical, UTF-8-safe extraction as-is, so there is no setting to change it. What
  that extraction produces is described in `docs/processing.md`.
