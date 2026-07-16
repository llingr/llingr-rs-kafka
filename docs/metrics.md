# Metrics

You get Prometheus metrics for the consumer with one builder call, in either of
two shapes: a built-in scrape endpoint the crate serves for you, or a handle you
mount on an HTTP server you already run. Both expose the same series in the
OpenMetrics text format: per-message counters, gauges, and latency histograms,
plus optional wire-bandwidth telemetry. The capability is always compiled in;
constructing a `Metrics` value is what switches it on, and not configuring
metrics costs one dormant dependency and nothing at runtime.

The two activation shapes exist because Rust has no single standard HTTP server.
The built-in endpoint uses a small synchronous HTTP server (`tiny_http`) on its
own thread, so it never drags an async runtime into a crate whose engine runs on
Go threads with synchronous callbacks; the handle lets you avoid even that when
you already have a server to mount on.

## The two activation modes

```rust
# use llingr_kafka::{Metrics, MetricsHandle};
// Mode 1, built-in endpoint: bind an address and answer GET <path> with the
// exposition, on a dedicated thread.
let served: Metrics = Metrics::serve("0.0.0.0:9464", "/metrics");

// Mode 2, no server: take a handle and render the exposition yourself.
let (metrics, handle): (Metrics, MetricsHandle) = Metrics::registry();
# let _ = (served, metrics, handle);
```

You pass the `Metrics` value to the engine builder's `.metrics(...)` hook. What
each mode does:

- **`Metrics::serve(addr, path)`** binds `addr` (for example `"0.0.0.0:9464"`)
  and answers `GET path` (for example `"/metrics"`) with the OpenMetrics text on
  a dedicated thread. A non-GET request to the path gets `405`, any other path
  gets `404`. The bind happens when the engine is built, so an unbindable
  address is a clean build-time error naming the address; the endpoint then
  serves for the life of the engine.
- **`Metrics::registry()`** runs no server. It returns the `Metrics` value and a
  `MetricsHandle`. The handle is cheap to clone and safe to call from any thread
  at scrape frequency (it is `Clone`, and `Send + Sync`), and its
  `scrape() -> String` method renders the current exposition on demand. Before
  the engine is built the handle serves an empty exposition (zero series), so you
  can mount the route unconditionally and it simply starts returning data once
  the engine is up.

The engine wiring that realises the registry at build time (registering the
sinks, binding the endpoint in serve mode) lands with the engine module; the
metrics module itself, the sinks, the scrape rendering, and the `serve`/`registry`
surface described here are complete in the crate today.

## Mounting the handle on your own server

When you already run an HTTP server, use registry mode and serve the handle's
output yourself. Set the response content type to the exposition's media type,
which the crate exposes as the `OPENMETRICS_CONTENT_TYPE` constant
(`"application/openmetrics-text; version=1.0.0; charset=utf-8"`, matching what
Go's `promhttp` serves and what scrapers expect). The essential shape, framework
free, is just this:

```rust
use llingr_kafka::{Metrics, MetricsHandle, OPENMETRICS_CONTENT_TYPE};

// At startup: registry mode gives you the handle. Pass `metrics` to the
// builder's .metrics(...), and keep `handle` for your metrics route.
let (metrics, handle) = Metrics::registry();
# let _ = metrics;

// Your route handler, whatever HTTP framework you use, reduces to this: the
// content type, and the current exposition text.
fn metrics_response(handle: &MetricsHandle) -> (&'static str, String) {
    (OPENMETRICS_CONTENT_TYPE, handle.scrape())
}
# let _ = metrics_response(&handle);
```

In a real web framework it is the same two values wired into a route. For
example, with axum (illustrative, and not compiled by the docs check because it
would pull in the framework):

```rust,ignore
use axum::{routing::get, Router, http::header::CONTENT_TYPE};
use llingr_kafka::{MetricsHandle, OPENMETRICS_CONTENT_TYPE};

fn metrics_router(handle: MetricsHandle) -> Router {
    Router::new().route(
        "/metrics",
        get(move || {
            let handle = handle.clone();
            async move { ([(CONTENT_TYPE, OPENMETRICS_CONTENT_TYPE)], handle.scrape()) }
        }),
    )
}
```

## The per-message metric catalogue

Every per-message series is named `llingr_engine_*` and carries the same five
labels: `topic`, `consumer_group`, `service`, `team`, and `partition`. The
`service` and `team` labels come from the service identity you attach with the
builder's optional `.service(name, team)` method; with none set they are empty
strings, which are valid label values that form their own series.

Counters (the OpenMetrics encoder appends the `_total` suffix, so the exposed
names end `_total`):

| Metric | Meaning |
|---|---|
| `llingr_engine_processed_total` | Every message processed. Increments once per message, unconditionally |
| `llingr_engine_errored_total` | The process handler returned an error (framework trait bit 0) |
| `llingr_engine_panicked_total` | The process handler panicked (framework trait bit 1) |
| `llingr_engine_dead_lettered_total` | The message was routed to the dead-letter handler (framework trait bit 2) |
| `llingr_engine_duplicate_total` | The message was a redelivery (framework trait bit 4) |
| `llingr_engine_used_overflow_total` | The message dispatched via the guard-channel overflow path during worker acquisition (framework trait bit 5) |

The five condition counters each increment independently, so more than one can
fire for a single message (a message that errored and then dead-lettered
increments both plus `processed_total`). This is the only place framework trait
bits surface. The other framework bits (CommitBuffered, Orphaned,
FirstAfterRebalance) have no metric, and the application trait bits you set
(positions 10 to 63) surface nowhere: they are not counters and not labels. See
`docs/processing.md` for the full trait-bit picture and the consequence that
custom bits are effectively write-only in this crate.

Gauges (set on every message, including to zero):

| Metric | Meaning |
|---|---|
| `llingr_engine_queue_depth` | Current per-partition queue depth |
| `llingr_engine_current_offset` | The offset currently being processed on the partition |

Latency histograms (values in seconds, observed only when the measured duration
is strictly positive):

| Metric | Meaning | Buckets |
|---|---|---|
| `llingr_engine_process_duration_seconds` | Time spent in the process handler | Exponential, 1ms doubling over 15 buckets (1ms to about 16.4s) |
| `llingr_engine_dead_letter_duration_seconds` | Time spent in the dead-letter handler | Same as process: 1ms doubling over 15 buckets |
| `llingr_engine_queue_wait_duration_seconds` | Time a message waited in the queue before processing (process-start minus read time, when both are set and positive) | Exponential, 0.1ms doubling over 18 buckets (0.1ms to about 13.1s) |

The bucket layouts are pinned to the Go engine's own (`ExponentialBuckets(0.001,
2, 15)` for the process and dead-letter histograms, `ExponentialBuckets(0.0001,
2, 18)` for queue-wait), so the Rust exposition and the Go engine's own reporting
line up bucket for bucket.

## Bandwidth telemetry

Bandwidth telemetry meters the wire traffic and broker topology, off the
message hot path. It is registered on the same shared registry as the
per-message series and activates together with metrics: turning metrics on
(either mode) registers the bandwidth sink too, and it fills in as the broker
adapter reports its per-interval byte counters. The series are named
`llingr_bandwidth_*`.

Per-partition counters (labels `topic`, `consumer_group`, `service`, `team`,
`partition`; the two compression counters add a `compression` label):

| Metric | Meaning |
|---|---|
| `llingr_bandwidth_received_bytes_total` | Bytes received per partition |
| `llingr_bandwidth_transmitted_bytes_total` | Bytes transmitted per partition |
| `llingr_bandwidth_received_messages_total` | Messages received per partition |
| `llingr_bandwidth_compressed_bytes_total` | Compressed (wire) bytes received; zero when compression visibility is unavailable |
| `llingr_bandwidth_uncompressed_bytes_total` | Uncompressed (decompressed) bytes received; zero when unavailable |

Cluster and topology series (labels `topic`, `consumer_group`, `service`,
`team`):

| Metric | Meaning |
|---|---|
| `llingr_bandwidth_broker_count` | Number of brokers in the cluster at last collection |
| `llingr_bandwidth_partition_count` | Number of assigned partitions at last collection |
| `llingr_bandwidth_stats_interval_seconds` | The configured collection cadence, in seconds |
| `llingr_bandwidth_last_collection_timestamp_seconds` | Unix timestamp of the most recent collection |
| `llingr_bandwidth_broker_info` | Broker topology as an info metric (always 1.0), with extra labels `broker_id`, `broker_host`, `broker_port`, `broker_rack` |

Two behaviours mirror the Go engine's bandwidth sink and are worth knowing: each
incoming counter field is treated as a per-interval delta and added to the
running counter (no per-partition prior-value state is kept), and the
`broker_info` series are never deleted, so when the topology changes the old
broker label sets persist alongside the new ones. When compression visibility is
unavailable, the compressed and uncompressed byte counters report zero and the
`compression` label reads `unknown`.

## A note on labels and cardinality

Every series is labelled by `topic`, `consumer_group`, `service`, `team`, and
(for the per-message and per-partition series) `partition`, so cardinality scales
with your partition count, not with message volume: the counters and gauges are
per-partition, not per-message-key. That keeps the exposition bounded even under
heavy load. The `service` and `team` labels are how fleet tooling routes and
groups telemetry; set the service identity with the builder's
`.service(name, team)` method if you run more than one consumer against a shared
Prometheus.
