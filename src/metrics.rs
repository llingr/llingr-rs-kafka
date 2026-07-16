// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

//! Baked-in Prometheus metrics: activation ([`Metrics`]), the per-message and
//! bandwidth sinks, OpenMetrics rendering, and the built-in scrape endpoint.
//!
//! Everything here is always compiled in (this crate has no features);
//! configuration decides whether any of it runs. Two activation modes, one
//! type:
//!
//! - [`Metrics::serve`]: the crate binds a `host:port` and serves the
//!   OpenMetrics text at a path on its own thread (`tiny_http`, synchronous,
//!   no async runtime).
//! - [`Metrics::registry`]: no server; the caller receives a
//!   [`MetricsHandle`] and mounts [`MetricsHandle::scrape`] output on their
//!   own HTTP stack (axum, actix, anything).
//!
//! Not configuring metrics costs one dormant dependency and nothing at
//! runtime.
//!
//! The sinks are ports of the Go `llingr-metrics-prometheus` module: the
//! per-message series (six counters, two gauges, three latency histograms,
//! names `llingr_engine_*`) and the bandwidth series (five per-partition
//! counters and five topology gauges, names `llingr_bandwidth_*`), with
//! bucket boundaries pinned to the Go sink so dashboards align across the
//! two ecosystems.

use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use llingr_nexus::{
    BandwidthMetrics, BandwidthMetricsHandler, Metrics as MessageMetrics, MetricsHandler,
};
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;
use tiny_http::{Header, Method, Response, Server};

/// The `Content-Type` for the OpenMetrics text exposition, matching what Go's
/// `promhttp` serves with `EnableOpenMetrics` (and what the reference
/// scrapers expect). Use it when mounting [`MetricsHandle::scrape`] output on
/// your own HTTP stack.
pub const OPENMETRICS_CONTENT_TYPE: &str =
    "application/openmetrics-text; version=1.0.0; charset=utf-8";

/// Default Prometheus namespace (mirrors the Go module's `defaultNamespace`).
const NAMESPACE: &str = "llingr";
/// Subsystem for the per-message series (metric names `llingr_engine_*`).
const MESSAGE_SUBSYSTEM: &str = "engine";
/// Subsystem for the bandwidth series (metric names `llingr_bandwidth_*`).
const BANDWIDTH_SUBSYSTEM: &str = "bandwidth";

/// Compression fallback for a partition that reports no algorithm (the Go
/// sink's empty-string to `"unknown"` mapping).
const UNKNOWN_COMPRESSION: &str = "unknown";

/// Render a registry to the OpenMetrics text exposition format.
///
/// Encoding to a `String` cannot fail.
pub(crate) fn scrape(registry: &Registry) -> String {
    let mut buf = String::new();
    prometheus_client::encoding::text::encode(&mut buf, registry)
        .expect("encoding OpenMetrics text to a String is infallible");
    buf
}

// ---------------------------------------------------------------------------
// Activation: the Metrics configuration type and the registry-mode handle
// ---------------------------------------------------------------------------

/// Prometheus metrics activation, passed to the engine builder's
/// `.metrics(...)` hook.
///
/// The capability is always compiled in; constructing one of these is what
/// switches it on. Both modes register the same two sinks (per-message and
/// bandwidth telemetry) on one shared registry:
///
/// ```ignore
/// // Built-in scrape endpoint: tiny_http on its own thread, OpenMetrics
/// // text at the path. No async runtime involved.
/// let engine = builder.metrics(Metrics::serve("0.0.0.0:9464", "/metrics")).build()?;
///
/// // No server: mount the exposition on your own HTTP stack.
/// let (metrics, handle) = Metrics::registry();
/// let engine = builder.metrics(metrics).build()?;
/// // e.g. in an axum route: (OPENMETRICS_CONTENT_TYPE, handle.scrape())
/// ```
///
/// In serve mode the endpoint binds when the engine is built; an unbindable
/// address is a clean build-time error. In registry mode the handle serves
/// an empty exposition until the engine is built.
pub struct Metrics {
    mode: Mode,
}

enum Mode {
    /// Bind `addr` and serve the exposition at `path` on a background thread.
    Serve { addr: String, path: String },
    /// No server: publish the realised registry through the shared slot the
    /// caller's [`MetricsHandle`] reads.
    Registry { slot: Arc<OnceLock<Arc<Registry>>> },
}

impl Metrics {
    /// Serve the metrics on a built-in scrape endpoint: bind `addr` (for
    /// example `"0.0.0.0:9464"`) and answer `GET path` (for example
    /// `"/metrics"`) with the OpenMetrics text, on a dedicated thread.
    ///
    /// The bind happens when the engine is built; a bind failure is a clean
    /// build-time error naming the address. The endpoint serves for the life
    /// of the engine.
    pub fn serve(addr: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            mode: Mode::Serve {
                addr: addr.into(),
                path: path.into(),
            },
        }
    }

    /// Record metrics without serving them: the returned [`MetricsHandle`]
    /// renders the current exposition on demand, for mounting on whatever
    /// HTTP stack the application already runs.
    ///
    /// Before the engine is built the handle serves an empty exposition
    /// (zero series), so a route can be mounted unconditionally.
    pub fn registry() -> (Self, MetricsHandle) {
        let slot = Arc::new(OnceLock::new());
        (
            Self {
                mode: Mode::Registry {
                    slot: Arc::clone(&slot),
                },
            },
            MetricsHandle { slot },
        )
    }

    /// Realise the configuration at engine build time, when the topic, group,
    /// and service identity are known: build the shared registry, register
    /// both sinks, and either bind the built-in endpoint or publish the
    /// registry to the caller's handle.
    ///
    /// Consumed by the engine builder; a serve-mode bind failure surfaces
    /// here as the `io::Error` for the builder to report.
    pub(crate) fn realise(
        self,
        topic: &str,
        consumer_group: &str,
        service: &str,
        team: &str,
    ) -> io::Result<RealisedMetrics> {
        // One registry, both sinks, one endpoint (the shared-registry pattern).
        let mut registry = Registry::default();
        let message_sink = MessageSink::register(
            &mut registry,
            MessageOptions::new()
                .topic(topic)
                .consumer_group(consumer_group)
                .service(service, team),
        );
        let bandwidth_sink = BandwidthSink::register(
            &mut registry,
            BandwidthOptions::new().service(service, team),
        );
        let registry = Arc::new(registry);

        let exporter = match self.mode {
            Mode::Serve { addr, path } => Some(
                serve_exposition(registry, addr.as_str(), path).map_err(|e| {
                    io::Error::other(format!("metrics endpoint failed to bind {addr}: {e}"))
                })?,
            ),
            Mode::Registry { slot } => {
                // A second set() is impossible: realise() consumes self and
                // the engine builds at most once per process.
                let _ = slot.set(registry);
                None
            }
        };

        Ok(RealisedMetrics {
            message_sink,
            bandwidth_sink,
            exporter,
        })
    }
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.mode {
            Mode::Serve { addr, path } => f
                .debug_struct("Metrics")
                .field("mode", &"serve")
                .field("addr", addr)
                .field("path", path)
                .finish(),
            Mode::Registry { .. } => f
                .debug_struct("Metrics")
                .field("mode", &"registry")
                .finish(),
        }
    }
}

/// Handle returned by [`Metrics::registry`]: renders the current OpenMetrics
/// exposition for serving on the application's own HTTP stack.
///
/// Cheap to clone; safe to call from any thread at scrape frequency. Serve
/// the output with the [`OPENMETRICS_CONTENT_TYPE`] content type.
#[derive(Clone)]
pub struct MetricsHandle {
    slot: Arc<OnceLock<Arc<Registry>>>,
}

impl MetricsHandle {
    /// The current exposition as OpenMetrics text. Empty (zero series) until
    /// the engine has been built.
    pub fn scrape(&self) -> String {
        match self.slot.get() {
            Some(registry) => scrape(registry),
            None => String::new(),
        }
    }
}

impl std::fmt::Debug for MetricsHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsHandle")
            .field("realised", &self.slot.get().is_some())
            .finish()
    }
}

/// The build-time product of [`Metrics::realise`]: the two sinks the engine
/// registers as its metrics and bandwidth handlers, plus the running exporter
/// in serve mode (held for the life of the engine; dropping it stops the
/// server thread, which is exactly what a failed build's rollback wants).
pub(crate) struct RealisedMetrics {
    pub(crate) message_sink: MessageSink,
    pub(crate) bandwidth_sink: BandwidthSink,
    pub(crate) exporter: Option<ExporterHandle>,
}

// ---------------------------------------------------------------------------
// Per-message sink (port of the Go messages sink)
// ---------------------------------------------------------------------------

/// The label set shared by every per-message metric, in Go label order.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct MessageLabels {
    topic: String,
    consumer_group: String,
    service: String,
    team: String,
    partition: i64,
}

/// Buckets for the process and dead-letter latency histograms: Go's
/// `prometheus.ExponentialBuckets(0.001, 2, 15)` (1ms * 2^k, k in 0..14).
fn process_buckets() -> impl Iterator<Item = f64> {
    exponential_buckets(0.001, 2.0, 15)
}

/// Buckets for the queue-wait histogram: Go's
/// `prometheus.ExponentialBuckets(0.0001, 2, 18)` (0.1ms * 2^k, k in 0..17).
fn queue_wait_buckets() -> impl Iterator<Item = f64> {
    exponential_buckets(0.0001, 2.0, 18)
}

/// Configuration for a [`MessageSink`].
///
/// The identity fields default to empty strings (valid label values that form
/// their own series, matching Go's nil-`Service` handling).
#[derive(Clone, Debug, Default)]
pub(crate) struct MessageOptions {
    topic: String,
    consumer_group: String,
    service: String,
    team: String,
}

impl MessageOptions {
    /// Start from the defaults (empty identity).
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Set the `topic` label (the single topic this sink's consumer serves).
    pub(crate) fn topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = topic.into();
        self
    }

    /// Set the `consumer_group` label.
    pub(crate) fn consumer_group(mut self, group: impl Into<String>) -> Self {
        self.consumer_group = group.into();
        self
    }

    /// Set the `service` and `team` labels (the service identity).
    pub(crate) fn service(mut self, name: impl Into<String>, team: impl Into<String>) -> Self {
        self.service = name.into();
        self.team = team.into();
        self
    }
}

/// A Prometheus-backed [`MetricsHandler`].
///
/// Records the eleven per-message series (six counters, two gauges, three
/// latency histograms) under `llingr_engine_*`, and never blocks or fails:
/// [`handle`](MetricsHandler::handle) only touches thread-safe atomics.
pub(crate) struct MessageSink {
    topic: String,
    consumer_group: String,
    service: String,
    team: String,

    processed: Family<MessageLabels, Counter>,
    errored: Family<MessageLabels, Counter>,
    panicked: Family<MessageLabels, Counter>,
    dead_lettered: Family<MessageLabels, Counter>,
    duplicate: Family<MessageLabels, Counter>,
    used_overflow: Family<MessageLabels, Counter>,

    queue_depth: Family<MessageLabels, Gauge>,
    current_offset: Family<MessageLabels, Gauge>,

    process_duration: Family<MessageLabels, Histogram>,
    dead_letter_duration: Family<MessageLabels, Histogram>,
    queue_wait_duration: Family<MessageLabels, Histogram>,
}

impl MessageSink {
    /// Register all collectors into `registry` (under the `llingr_engine`
    /// prefix) and return the sink holding the metric handles.
    pub(crate) fn register(registry: &mut Registry, options: MessageOptions) -> Self {
        let processed = Family::<MessageLabels, Counter>::default();
        let errored = Family::<MessageLabels, Counter>::default();
        let panicked = Family::<MessageLabels, Counter>::default();
        let dead_lettered = Family::<MessageLabels, Counter>::default();
        let duplicate = Family::<MessageLabels, Counter>::default();
        let used_overflow = Family::<MessageLabels, Counter>::default();

        let queue_depth = Family::<MessageLabels, Gauge>::default();
        let current_offset = Family::<MessageLabels, Gauge>::default();

        let process_duration = Family::<MessageLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(process_buckets())
        });
        let dead_letter_duration = Family::<MessageLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(process_buckets())
        });
        let queue_wait_duration = Family::<MessageLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(queue_wait_buckets())
        });

        let reg = registry
            .sub_registry_with_prefix(NAMESPACE)
            .sub_registry_with_prefix(MESSAGE_SUBSYSTEM);

        // Counter names omit the `_total` suffix: the OpenMetrics encoder
        // appends it, yielding `processed_total` and friends.
        reg.register(
            "processed",
            "Total number of messages processed",
            processed.clone(),
        );
        reg.register(
            "errored",
            "Total number of messages that resulted in processing errors",
            errored.clone(),
        );
        reg.register(
            "panicked",
            "Total number of messages where processing panicked",
            panicked.clone(),
        );
        reg.register(
            "dead_lettered",
            "Total number of messages sent to dead letter queue",
            dead_lettered.clone(),
        );
        reg.register(
            "duplicate",
            "Total number of duplicate messages detected",
            duplicate.clone(),
        );
        reg.register(
            "used_overflow",
            "Total messages that used guard channel overflow during worker acquisition",
            used_overflow.clone(),
        );
        reg.register(
            "queue_depth",
            "Current queue depth for buffering implementations",
            queue_depth.clone(),
        );
        reg.register(
            "current_offset",
            "Current offset being processed per partition",
            current_offset.clone(),
        );
        reg.register(
            "process_duration_seconds",
            "Time spent processing messages",
            process_duration.clone(),
        );
        reg.register(
            "dead_letter_duration_seconds",
            "Time spent writing to dead letter queue",
            dead_letter_duration.clone(),
        );
        reg.register(
            "queue_wait_duration_seconds",
            "Time messages spent waiting in queue before processing",
            queue_wait_duration.clone(),
        );

        Self {
            topic: options.topic,
            consumer_group: options.consumer_group,
            service: options.service,
            team: options.team,
            processed,
            errored,
            panicked,
            dead_lettered,
            duplicate,
            used_overflow,
            queue_depth,
            current_offset,
            process_duration,
            dead_letter_duration,
            queue_wait_duration,
        }
    }

    fn labels(&self, partition: i32) -> MessageLabels {
        MessageLabels {
            topic: self.topic.clone(),
            consumer_group: self.consumer_group.clone(),
            service: self.service.clone(),
            team: self.team.clone(),
            partition: partition as i64,
        }
    }
}

impl MetricsHandler for MessageSink {
    fn handle(&self, metrics: &MessageMetrics) {
        let labels = self.labels(metrics.partition);
        let traits = metrics.traits;

        // Always count the message; then count each error condition its trait
        // bits report (independent, several may fire on one packet).
        self.processed.get_or_create(&labels).inc();
        if traits.has_process_error() {
            self.errored.get_or_create(&labels).inc();
        }
        if traits.has_process_panic() {
            self.panicked.get_or_create(&labels).inc();
        }
        if traits.has_dead_letter() {
            self.dead_lettered.get_or_create(&labels).inc();
        }
        if traits.has_duplicate() {
            self.duplicate.get_or_create(&labels).inc();
        }
        if traits.has_used_overflow() {
            self.used_overflow.get_or_create(&labels).inc();
        }

        // Gauges overwrite (Set), unconditionally, including zero.
        self.queue_depth
            .get_or_create(&labels)
            .set(metrics.queue_depth as i64);
        self.current_offset
            .get_or_create(&labels)
            .set(metrics.offset);

        // Durations: nanoseconds to fractional seconds, observed only when
        // strictly positive (Go observes `d.Seconds()` when `d > 0`).
        if metrics.process_duration_ns > 0 {
            self.process_duration
                .get_or_create(&labels)
                .observe(metrics.process_duration_ns as f64 / 1e9);
        }
        if metrics.deadletter_duration_ns > 0 {
            self.dead_letter_duration
                .get_or_create(&labels)
                .observe(metrics.deadletter_duration_ns as f64 / 1e9);
        }

        // Queue wait = process start minus read time, recorded only when both
        // timestamps are set (non-zero) and the difference is positive.
        if metrics.read_time_ns != 0 && metrics.process_start_time_ns != 0 {
            let wait_ns = metrics.process_start_time_ns - metrics.read_time_ns;
            if wait_ns > 0 {
                self.queue_wait_duration
                    .get_or_create(&labels)
                    .observe(wait_ns as f64 / 1e9);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Bandwidth sink (port of the Go bandwidth sink)
// ---------------------------------------------------------------------------

/// A float-valued gauge (the Prometheus client models it over an `AtomicU64`).
type FloatGauge = Gauge<f64, AtomicU64>;

/// Labels on the per-partition byte and message counters.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct PartitionLabels {
    topic: String,
    consumer_group: String,
    service: String,
    team: String,
    partition: i64,
}

/// Per-partition labels plus the compression algorithm.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct CompressionLabels {
    topic: String,
    consumer_group: String,
    service: String,
    team: String,
    partition: i64,
    compression: String,
}

/// Labels on the cluster-level topology gauges.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct TopologyLabels {
    topic: String,
    consumer_group: String,
    service: String,
    team: String,
}

/// Labels on the `broker_info` topology metric.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct BrokerInfoLabels {
    topic: String,
    consumer_group: String,
    service: String,
    team: String,
    broker_id: String,
    broker_host: String,
    broker_port: String,
    broker_rack: String,
}

/// Configuration for a [`BandwidthSink`].
///
/// `topic` and `consumer_group` are taken from each packet (which carries
/// them), so only the service identity is configured here.
#[derive(Clone, Debug, Default)]
pub(crate) struct BandwidthOptions {
    service: String,
    team: String,
}

impl BandwidthOptions {
    /// Start from the defaults (empty identity).
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Set the `service` and `team` labels (the service identity).
    pub(crate) fn service(mut self, name: impl Into<String>, team: impl Into<String>) -> Self {
        self.service = name.into();
        self.team = team.into();
        self
    }
}

/// A Prometheus-backed [`BandwidthMetricsHandler`].
///
/// Records five per-partition counters, four cluster gauges and the
/// `broker_info` topology metric under `llingr_bandwidth_*`. The counters
/// treat each incoming field as a per-interval delta and add it to the
/// running counter, exactly like the Go sink; no per-partition prior-value
/// state is kept. Like the Go sink it never deletes stale `broker_info`
/// series: when topology changes, old broker label sets persist alongside
/// the new ones.
pub(crate) struct BandwidthSink {
    service: String,
    team: String,

    received_bytes: Family<PartitionLabels, Counter>,
    transmitted_bytes: Family<PartitionLabels, Counter>,
    received_messages: Family<PartitionLabels, Counter>,
    compressed_bytes: Family<CompressionLabels, Counter>,
    uncompressed_bytes: Family<CompressionLabels, Counter>,

    broker_count: Family<TopologyLabels, Gauge>,
    partition_count: Family<TopologyLabels, Gauge>,
    stats_interval_seconds: Family<TopologyLabels, FloatGauge>,
    last_collection_timestamp_seconds: Family<TopologyLabels, Gauge>,
    broker_info: Family<BrokerInfoLabels, Gauge>,
}

impl BandwidthSink {
    /// Register all collectors into `registry` (under the `llingr_bandwidth`
    /// prefix) and return the sink.
    pub(crate) fn register(registry: &mut Registry, options: BandwidthOptions) -> Self {
        let received_bytes = Family::<PartitionLabels, Counter>::default();
        let transmitted_bytes = Family::<PartitionLabels, Counter>::default();
        let received_messages = Family::<PartitionLabels, Counter>::default();
        let compressed_bytes = Family::<CompressionLabels, Counter>::default();
        let uncompressed_bytes = Family::<CompressionLabels, Counter>::default();

        let broker_count = Family::<TopologyLabels, Gauge>::default();
        let partition_count = Family::<TopologyLabels, Gauge>::default();
        let stats_interval_seconds = Family::<TopologyLabels, FloatGauge>::default();
        let last_collection_timestamp_seconds = Family::<TopologyLabels, Gauge>::default();
        let broker_info = Family::<BrokerInfoLabels, Gauge>::default();

        let reg = registry
            .sub_registry_with_prefix(NAMESPACE)
            .sub_registry_with_prefix(BANDWIDTH_SUBSYSTEM);

        reg.register(
            "received_bytes",
            "Total bytes received by llingr consumer instances",
            received_bytes.clone(),
        );
        reg.register(
            "transmitted_bytes",
            "Total bytes transmitted by llingr consumer instances",
            transmitted_bytes.clone(),
        );
        reg.register(
            "received_messages",
            "Total messages received by llingr consumer instances",
            received_messages.clone(),
        );
        reg.register(
            "compressed_bytes",
            "Total compressed (wire) bytes received; zero when compression visibility is unavailable",
            compressed_bytes.clone(),
        );
        reg.register(
            "uncompressed_bytes",
            "Total uncompressed (decompressed) bytes received; zero when compression visibility is unavailable",
            uncompressed_bytes.clone(),
        );
        reg.register(
            "broker_count",
            "Number of brokers in the cluster at last collection",
            broker_count.clone(),
        );
        reg.register(
            "partition_count",
            "Number of assigned partitions at last collection",
            partition_count.clone(),
        );
        reg.register(
            "stats_interval_seconds",
            "Configured collection cadence in seconds",
            stats_interval_seconds.clone(),
        );
        reg.register(
            "last_collection_timestamp_seconds",
            "Unix timestamp of the most recent bandwidth collection",
            last_collection_timestamp_seconds.clone(),
        );
        reg.register(
            "broker_info",
            "Broker topology at last collection (info metric, always 1.0)",
            broker_info.clone(),
        );

        Self {
            service: options.service,
            team: options.team,
            received_bytes,
            transmitted_bytes,
            received_messages,
            compressed_bytes,
            uncompressed_bytes,
            broker_count,
            partition_count,
            stats_interval_seconds,
            last_collection_timestamp_seconds,
            broker_info,
        }
    }

    fn compression_labels(
        &self,
        metrics: &BandwidthMetrics,
        p: &llingr_nexus::PartitionBandwidth,
    ) -> CompressionLabels {
        let compression = if p.compression.is_empty() {
            UNKNOWN_COMPRESSION.to_string()
        } else {
            p.compression.clone()
        };
        CompressionLabels {
            topic: metrics.topic.clone(),
            consumer_group: metrics.consumer_group.clone(),
            service: self.service.clone(),
            team: self.team.clone(),
            partition: p.id as i64,
            compression,
        }
    }
}

impl BandwidthMetricsHandler for BandwidthSink {
    fn handle(&self, metrics: &BandwidthMetrics) {
        let topology = TopologyLabels {
            topic: metrics.topic.clone(),
            consumer_group: metrics.consumer_group.clone(),
            service: self.service.clone(),
            team: self.team.clone(),
        };

        // Cluster-level gauges (Set), unconditionally.
        self.broker_count
            .get_or_create(&topology)
            .set(metrics.brokers.len() as i64);
        self.partition_count
            .get_or_create(&topology)
            .set(metrics.partitions.len() as i64);
        self.stats_interval_seconds
            .get_or_create(&topology)
            .set(metrics.stats_interval_ms as f64 / 1000.0);
        // Unix epoch seconds of the collection, only when the timestamp is set.
        if metrics.ts_unix_ns != 0 {
            self.last_collection_timestamp_seconds
                .get_or_create(&topology)
                .set(metrics.ts_unix_ns / 1_000_000_000);
        }

        // One info series per broker; value is always 1. Stale series from a
        // previous topology are deliberately left in place (never deleted).
        for broker in &metrics.brokers {
            let labels = BrokerInfoLabels {
                topic: metrics.topic.clone(),
                consumer_group: metrics.consumer_group.clone(),
                service: self.service.clone(),
                team: self.team.clone(),
                broker_id: broker.id.clone(),
                broker_host: broker.host.clone(),
                broker_port: broker.port.clone(),
                broker_rack: broker.rack.clone(),
            };
            self.broker_info.get_or_create(&labels).set(1);
        }

        // Per-partition counters: each field is a per-interval delta, added to
        // the running counter only when strictly positive.
        for p in &metrics.partitions {
            let labels = PartitionLabels {
                topic: metrics.topic.clone(),
                consumer_group: metrics.consumer_group.clone(),
                service: self.service.clone(),
                team: self.team.clone(),
                partition: p.id as i64,
            };
            if p.received_bytes > 0 {
                self.received_bytes
                    .get_or_create(&labels)
                    .inc_by(p.received_bytes as u64);
            }
            if p.transmitted_bytes > 0 {
                self.transmitted_bytes
                    .get_or_create(&labels)
                    .inc_by(p.transmitted_bytes as u64);
            }
            if p.received_message_count > 0 {
                self.received_messages
                    .get_or_create(&labels)
                    .inc_by(p.received_message_count as u64);
            }
            if p.compressed_bytes > 0 {
                self.compressed_bytes
                    .get_or_create(&self.compression_labels(metrics, p))
                    .inc_by(p.compressed_bytes as u64);
            }
            if p.uncompressed_bytes > 0 {
                self.uncompressed_bytes
                    .get_or_create(&self.compression_labels(metrics, p))
                    .inc_by(p.uncompressed_bytes as u64);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Built-in scrape endpoint (port of the Go-side exporter donor)
// ---------------------------------------------------------------------------

/// How often the serve loop wakes to check for a stop request. Bounds the
/// shutdown latency without busy-looping.
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// A running scrape endpoint. Dropping it (or calling [`stop`](Self::stop))
/// shuts the server down and joins its thread.
pub(crate) struct ExporterHandle {
    stop: Arc<AtomicBool>,
    server: Arc<Server>,
    join: Option<JoinHandle<()>>,
    local_addr: Option<SocketAddr>,
}

impl ExporterHandle {
    /// The address the server actually bound to. Useful when the caller passed
    /// port `0` to let the OS choose a free port (tests do; production
    /// addresses are explicit).
    #[cfg(test)]
    pub(crate) fn local_addr(&self) -> Option<SocketAddr> {
        self.local_addr
    }

    /// Stop the server and wait for its thread to finish.
    #[cfg(test)]
    pub(crate) fn stop(mut self) {
        self.shutdown();
    }

    fn shutdown(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        // Wake the accept loop so it observes the flag promptly instead of
        // blocking until the next poll timeout.
        self.server.unblock();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl Drop for ExporterHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl std::fmt::Debug for ExporterHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `tiny_http::Server` is not `Debug`; surface just the bound address.
        f.debug_struct("ExporterHandle")
            .field("local_addr", &self.local_addr)
            .finish_non_exhaustive()
    }
}

/// Bind `addr` and serve `registry` as OpenMetrics text at `path` on a
/// background thread.
///
/// A `GET` at `path` returns the current exposition; any other method or path
/// gets a `404`/`405`. The returned [`ExporterHandle`] owns the server thread:
/// keep it alive for as long as metrics should be exposed.
///
/// # Errors
///
/// Returns an error if the address cannot be bound.
pub(crate) fn serve_exposition(
    registry: Arc<Registry>,
    addr: impl ToSocketAddrs,
    path: impl Into<String>,
) -> io::Result<ExporterHandle> {
    let path = path.into();
    let server = Server::http(addr).map_err(|e| io::Error::other(e.to_string()))?;
    let server = Arc::new(server);
    let local_addr = server.server_addr().to_ip();

    let stop = Arc::new(AtomicBool::new(false));
    let content_type =
        Header::from_bytes(&b"Content-Type"[..], OPENMETRICS_CONTENT_TYPE.as_bytes())
            .expect("static content-type header is valid");

    let thread_server = Arc::clone(&server);
    let thread_stop = Arc::clone(&stop);
    let join = thread::spawn(move || {
        while !thread_stop.load(Ordering::SeqCst) {
            let request = match thread_server.recv_timeout(POLL_INTERVAL) {
                Ok(Some(request)) => request,
                Ok(None) => continue, // timed out; re-check the stop flag
                Err(_) => break,      // server closed
            };

            // Strip any query string before matching the path. All branches
            // produce the same `Response` type so they share one `respond`.
            let request_path = request.url().split('?').next().unwrap_or("");
            let response = if request.method() != &Method::Get {
                Response::from_string(String::new()).with_status_code(405)
            } else if request_path == path {
                Response::from_string(scrape(&registry)).with_header(content_type.clone())
            } else {
                Response::from_string(String::new()).with_status_code(404)
            };
            let _ = request.respond(response);
        }
    });

    Ok(ExporterHandle {
        stop,
        server,
        join: Some(join),
        local_addr,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod message_tests {
    use super::*;
    use llingr_nexus::Traits;

    fn base_metrics() -> MessageMetrics {
        MessageMetrics {
            traits: Traits::none(),
            queue_depth: 0,
            partition: 0,
            offset: 0,
            process_duration_ns: 0,
            deadletter_duration_ns: 0,
            read_time_ns: 0,
            process_start_time_ns: 0,
            watermark_advance_time_ns: 0,
        }
    }

    fn options() -> MessageOptions {
        MessageOptions::new()
            .topic("orders")
            .consumer_group("grp")
            .service("svc", "team")
    }

    fn sink_and_registry() -> (MessageSink, Registry) {
        let mut registry = Registry::default();
        let sink = MessageSink::register(&mut registry, options());
        (sink, registry)
    }

    #[test]
    fn processed_always_counts_with_identity_labels() {
        let (sink, registry) = sink_and_registry();
        sink.handle(&MessageMetrics {
            partition: 3,
            ..base_metrics()
        });
        let text = scrape(&registry);

        assert!(text.contains("llingr_engine_processed_total{"), "{text}");
        assert!(text.contains("topic=\"orders\""), "{text}");
        assert!(text.contains("consumer_group=\"grp\""), "{text}");
        assert!(text.contains("service=\"svc\""), "{text}");
        assert!(text.contains("team=\"team\""), "{text}");
        assert!(text.contains("partition=\"3\""), "{text}");
        // A message with no error traits touches none of the error counters.
        assert!(!text.contains("errored_total{"), "{text}");
        assert!(!text.contains("panicked_total{"), "{text}");
    }

    #[test]
    fn trait_bits_drive_error_counters() {
        let (sink, registry) = sink_and_registry();
        // Bits: 0 process error, 1 panic, 2 dead-letter, 4 duplicate,
        // 5 used-overflow. Bit 3 (commit-buffered) maps to no metric.
        let raw = (1 << 0) | (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4) | (1 << 5);
        sink.handle(&MessageMetrics {
            traits: Traits::from_raw(raw),
            ..base_metrics()
        });
        let text = scrape(&registry);

        for name in [
            "processed_total",
            "errored_total",
            "panicked_total",
            "dead_lettered_total",
            "duplicate_total",
            "used_overflow_total",
        ] {
            assert!(
                text.contains(&format!("llingr_engine_{name}{{")),
                "missing {name} in\n{text}"
            );
        }
    }

    #[test]
    fn gauges_and_histograms_record_conversions() {
        let (sink, registry) = sink_and_registry();
        sink.handle(&MessageMetrics {
            queue_depth: 7,
            offset: 42,
            process_duration_ns: 2_000_000,    // 0.002 s
            deadletter_duration_ns: 4_000_000, // 0.004 s
            read_time_ns: 1_000_000,
            process_start_time_ns: 3_000_000, // wait 0.002 s
            ..base_metrics()
        });
        let text = scrape(&registry);

        assert!(text.contains("llingr_engine_queue_depth{"), "{text}");
        assert!(text.contains("} 7"), "queue_depth value\n{text}");
        assert!(text.contains("llingr_engine_current_offset{"), "{text}");
        assert!(text.contains("} 42"), "offset value\n{text}");

        assert!(
            text.contains("llingr_engine_process_duration_seconds_sum{"),
            "{text}"
        );
        assert!(
            text.contains("llingr_engine_process_duration_seconds_count{"),
            "{text}"
        );
        assert!(
            text.contains("llingr_engine_dead_letter_duration_seconds_count{"),
            "{text}"
        );
        assert!(
            text.contains("llingr_engine_queue_wait_duration_seconds_count{"),
            "{text}"
        );
    }

    #[test]
    fn zero_durations_are_not_observed() {
        let (sink, registry) = sink_and_registry();
        sink.handle(&base_metrics()); // all durations zero, timestamps zero
        let text = scrape(&registry);
        // Histograms exist as declared families but record no observations.
        assert!(!text.contains("process_duration_seconds_count{"), "{text}");
        assert!(
            !text.contains("queue_wait_duration_seconds_count{"),
            "{text}"
        );
    }

    /// Pin the exact histogram bucket boundaries to the Go sink's
    /// ExponentialBuckets(0.001, 2, 15) and (0.0001, 2, 18): a silent change
    /// here would corrupt dashboards that align Rust and Go consumers.
    #[test]
    fn histogram_buckets_match_go_boundaries() {
        let (sink, registry) = sink_and_registry();
        sink.handle(&MessageMetrics {
            process_duration_ns: 1, // land in the lowest bucket
            read_time_ns: 1,
            process_start_time_ns: 2,
            ..base_metrics()
        });
        let text = scrape(&registry);

        // process_duration: first, a middle, and the last finite bound.
        for le in ["0.001", "0.256", "16.384"] {
            assert!(
                text.contains(&format!(
                    "llingr_engine_process_duration_seconds_bucket{{le=\"{le}\""
                )),
                "missing process bucket le={le} in\n{text}"
            );
        }
        // queue_wait: first and last finite bound of the 18-bucket series.
        for le in ["0.0001", "13.1072"] {
            assert!(
                text.contains(&format!(
                    "llingr_engine_queue_wait_duration_seconds_bucket{{le=\"{le}\""
                )),
                "missing queue_wait bucket le={le} in\n{text}"
            );
        }
        // No spurious 16th/19th finite bucket beyond the Go boundary.
        assert!(
            !text.contains("process_duration_seconds_bucket{le=\"32.768\""),
            "{text}"
        );
        assert!(
            !text.contains("queue_wait_duration_seconds_bucket{le=\"26.2144\""),
            "{text}"
        );
    }

    /// Negative durations (a defensive case for clock/FFI anomalies) must not
    /// be observed, exactly like the Go sink's `d > 0` guard, and a process
    /// start EARLIER than the read time must not record a negative wait.
    #[test]
    fn negative_durations_and_waits_are_not_observed() {
        let (sink, registry) = sink_and_registry();
        sink.handle(&MessageMetrics {
            process_duration_ns: -5,
            deadletter_duration_ns: -5,
            read_time_ns: 10,
            process_start_time_ns: 4, // wait would be negative
            ..base_metrics()
        });
        let text = scrape(&registry);
        assert!(!text.contains("process_duration_seconds_count{"), "{text}");
        assert!(
            !text.contains("dead_letter_duration_seconds_count{"),
            "{text}"
        );
        assert!(
            !text.contains("queue_wait_duration_seconds_count{"),
            "{text}"
        );
    }
}

#[cfg(test)]
mod bandwidth_tests {
    use super::*;
    use llingr_nexus::{BrokerInfo, PartitionBandwidth};

    fn broker(id: &str, host: &str) -> BrokerInfo {
        BrokerInfo {
            id: id.to_string(),
            host: host.to_string(),
            port: "9092".to_string(),
            rack: String::new(),
        }
    }

    fn partition(id: i32) -> PartitionBandwidth {
        PartitionBandwidth {
            ts_unix_ns: 0,
            received_bytes: 0,
            transmitted_bytes: 0,
            received_message_count: 0,
            compressed_bytes: 0,
            uncompressed_bytes: 0,
            id,
            leader: String::new(),
            compression: String::new(),
        }
    }

    fn packet(brokers: Vec<BrokerInfo>, partitions: Vec<PartitionBandwidth>) -> BandwidthMetrics {
        BandwidthMetrics {
            ts_unix_ns: 1_700_000_000_000_000_000, // 1_700_000_000 s
            stats_interval_ms: 5_000,              // 5 s
            metrics_id: "id-1".to_string(),
            topic: "orders".to_string(),
            consumer_group: "grp".to_string(),
            brokers,
            partitions,
        }
    }

    fn sink_and_registry() -> (BandwidthSink, Registry) {
        let mut registry = Registry::default();
        let sink = BandwidthSink::register(
            &mut registry,
            BandwidthOptions::new().service("svc", "team"),
        );
        (sink, registry)
    }

    #[test]
    fn counters_gauges_and_broker_info() {
        let (sink, registry) = sink_and_registry();
        let p = PartitionBandwidth {
            received_bytes: 1000,
            transmitted_bytes: 500,
            received_message_count: 10,
            compressed_bytes: 300,
            uncompressed_bytes: 900,
            compression: "lz4".to_string(),
            ..partition(0)
        };
        sink.handle(&packet(vec![broker("1", "h1"), broker("2", "h2")], vec![p]));
        let text = scrape(&registry);

        assert!(
            text.contains("llingr_bandwidth_received_bytes_total{"),
            "{text}"
        );
        assert!(
            text.contains("llingr_bandwidth_transmitted_bytes_total{"),
            "{text}"
        );
        assert!(
            text.contains("llingr_bandwidth_received_messages_total{"),
            "{text}"
        );
        assert!(text.contains("compression=\"lz4\""), "{text}");
        // Topology gauges.
        assert!(text.contains("llingr_bandwidth_broker_count{"), "{text}");
        assert!(text.contains("llingr_bandwidth_partition_count{"), "{text}");
        assert!(
            text.contains("llingr_bandwidth_stats_interval_seconds{"),
            "{text}"
        );
        assert!(
            text.contains("llingr_bandwidth_last_collection_timestamp_seconds{"),
            "{text}"
        );
        // Info metric: one series per broker, value 1.
        assert!(text.contains("broker_host=\"h1\""), "{text}");
        assert!(text.contains("broker_host=\"h2\""), "{text}");
        assert!(text.contains("topic=\"orders\""), "{text}");
    }

    #[test]
    fn empty_compression_falls_back_to_unknown() {
        let (sink, registry) = sink_and_registry();
        let p = PartitionBandwidth {
            compressed_bytes: 128,
            compression: String::new(),
            ..partition(0)
        };
        sink.handle(&packet(vec![], vec![p]));
        let text = scrape(&registry);
        assert!(text.contains("compression=\"unknown\""), "{text}");
    }

    #[test]
    fn counters_accumulate_deltas() {
        let (sink, registry) = sink_and_registry();
        let make = || {
            packet(
                vec![],
                vec![PartitionBandwidth {
                    received_bytes: 1000,
                    ..partition(0)
                }],
            )
        };
        sink.handle(&make());
        sink.handle(&make());
        let text = scrape(&registry);
        // Two 1000-byte deltas accumulate to 2000 on the running counter.
        assert!(
            text.contains("} 2000"),
            "expected accumulated 2000 in\n{text}"
        );
    }

    #[test]
    fn broker_info_retains_stale_series_on_topology_change() {
        let (sink, registry) = sink_and_registry();
        sink.handle(&packet(vec![broker("1", "h1"), broker("2", "h2")], vec![]));
        // Broker 2 moves host; broker 1 unchanged.
        sink.handle(&packet(
            vec![broker("1", "h1"), broker("2", "h2-new")],
            vec![],
        ));
        let text = scrape(&registry);
        // Old and new broker-2 label sets both persist (never deleted): three
        // distinct broker_info series across the two collections.
        assert!(
            text.contains("broker_host=\"h2\""),
            "old series gone\n{text}"
        );
        assert!(
            text.contains("broker_host=\"h2-new\""),
            "new series missing\n{text}"
        );
        let count = text.matches("llingr_bandwidth_broker_info{").count();
        assert_eq!(
            count, 3,
            "expected 3 broker_info series, got {count}\n{text}"
        );
    }

    #[test]
    fn zero_valued_partition_fields_are_skipped() {
        let (sink, registry) = sink_and_registry();
        sink.handle(&packet(vec![], vec![partition(0)])); // all counters zero
        let text = scrape(&registry);
        assert!(!text.contains("received_bytes_total{"), "{text}");
        assert!(!text.contains("compressed_bytes_total{"), "{text}");
    }

    /// The collection-timestamp gauge is set only when the packet carries a
    /// timestamp (Go: `!metrics.Ts.IsZero()`), and the interval gauge converts
    /// milliseconds to seconds.
    #[test]
    fn timestamp_zero_is_skipped_and_interval_converts() {
        let (sink, registry) = sink_and_registry();
        let mut p = packet(vec![], vec![]);
        p.ts_unix_ns = 0;
        sink.handle(&p);
        let text = scrape(&registry);
        assert!(
            !text.contains("last_collection_timestamp_seconds{"),
            "zero ts must not set the gauge\n{text}"
        );
        // 5000 ms configured in packet() renders as 5.0 seconds.
        assert!(
            text.contains("llingr_bandwidth_stats_interval_seconds{"),
            "{text}"
        );
        let line = text
            .lines()
            .find(|l| l.starts_with("llingr_bandwidth_stats_interval_seconds{"))
            .expect("interval series present");
        assert!(line.trim_end().ends_with(" 5.0"), "expected 5.0: {line}");

        // And with a real timestamp the gauge is whole Unix seconds.
        let p2 = packet(vec![], vec![]);
        sink.handle(&p2);
        let text2 = scrape(&registry);
        let ts_line = text2
            .lines()
            .find(|l| l.starts_with("llingr_bandwidth_last_collection_timestamp_seconds{"))
            .expect("timestamp series present after non-zero ts");
        assert!(
            ts_line.trim_end().ends_with(" 1700000000"),
            "expected epoch seconds 1700000000: {ts_line}"
        );
    }
}

#[cfg(test)]
mod exporter_tests {
    use super::*;
    use llingr_nexus::Traits;
    use std::io::{Read, Write};
    use std::net::TcpStream;

    fn http_get(addr: SocketAddr, path: &str) -> String {
        let mut stream = TcpStream::connect(addr).expect("connect");
        let request =
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).expect("write");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("read");
        response
    }

    #[test]
    fn serves_metrics_at_the_configured_path() {
        let mut registry = Registry::default();
        let sink = MessageSink::register(
            &mut registry,
            MessageOptions::new().topic("orders").consumer_group("grp"),
        );
        sink.handle(&MessageMetrics {
            traits: Traits::none(),
            queue_depth: 0,
            partition: 1,
            offset: 0,
            process_duration_ns: 0,
            deadletter_duration_ns: 0,
            read_time_ns: 0,
            process_start_time_ns: 0,
            watermark_advance_time_ns: 0,
        });

        let handle =
            serve_exposition(Arc::new(registry), "127.0.0.1:0", "/telemetry").expect("bind");
        let addr = handle.local_addr().expect("bound addr");

        let ok = http_get(addr, "/telemetry");
        assert!(ok.contains("200 OK"), "status line: {ok}");
        assert!(ok.contains(OPENMETRICS_CONTENT_TYPE), "content type: {ok}");
        assert!(ok.contains("llingr_engine_processed_total{"), "body: {ok}");

        let missing = http_get(addr, "/metrics");
        assert!(missing.contains("404"), "unexpected: {missing}");

        handle.stop();
    }

    #[test]
    fn query_strings_match_and_non_get_is_rejected() {
        let registry = Registry::default();
        let handle = serve_exposition(Arc::new(registry), "127.0.0.1:0", "/metrics").expect("bind");
        let addr = handle.local_addr().expect("bound addr");

        // A scraper appending a query string still hits the path.
        let with_query = http_get(addr, "/metrics?format=openmetrics");
        assert!(with_query.contains("200 OK"), "query string: {with_query}");

        // Non-GET methods get 405.
        let mut stream = TcpStream::connect(addr).expect("connect");
        stream
            .write_all(b"POST /metrics HTTP/1.1\r\nHost: l\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .expect("write");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("read");
        assert!(response.contains("405"), "POST response: {response}");

        handle.stop();
    }
}

#[cfg(test)]
mod activation_tests {
    use super::*;
    use llingr_nexus::Traits;
    use std::io::{Read, Write};
    use std::net::TcpStream;

    fn record_one_message(sink: &MessageSink, partition: i32) {
        sink.handle(&MessageMetrics {
            traits: Traits::none(),
            queue_depth: 0,
            partition,
            offset: 0,
            process_duration_ns: 0,
            deadletter_duration_ns: 0,
            read_time_ns: 0,
            process_start_time_ns: 0,
            watermark_advance_time_ns: 0,
        });
    }

    /// Serve mode: realise binds a real port and both sinks feed the one
    /// endpoint (message and bandwidth series share the registry).
    #[test]
    fn serve_mode_binds_and_exposes_both_sinks() {
        let realised = Metrics::serve("127.0.0.1:0", "/metrics")
            .realise("orders", "grp", "svc", "team")
            .expect("bind succeeds on an OS-assigned port");
        let exporter = realised.exporter.expect("serve mode returns an exporter");
        let addr = exporter.local_addr().expect("bound addr");

        record_one_message(&realised.message_sink, 4);

        let mut stream = TcpStream::connect(addr).expect("connect");
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: l\r\nConnection: close\r\n\r\n")
            .expect("write");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("read");
        assert!(response.contains("200 OK"), "{response}");
        assert!(
            response.contains("llingr_engine_processed_total{"),
            "{response}"
        );

        exporter.stop();
    }

    /// Serve mode: an unbindable address is a clean io::Error, for the engine
    /// builder to surface at build time.
    #[test]
    fn serve_mode_unbindable_address_is_an_error() {
        // Port 1 requires privileges; binding it as a normal user fails.
        let result = Metrics::serve("127.0.0.1:1", "/metrics").realise("t", "g", "", "");
        assert!(result.is_err(), "binding port 1 must fail");
    }

    /// Registry mode: the handle serves an empty exposition before realise
    /// and the live registry afterwards; no server is started.
    #[test]
    fn registry_mode_handle_goes_live_at_realise() {
        let (metrics, handle) = Metrics::registry();
        assert_eq!(handle.scrape(), "", "empty exposition before realise");

        let realised = metrics
            .realise("orders", "grp", "svc", "team")
            .expect("registry mode cannot fail to bind");
        assert!(
            realised.exporter.is_none(),
            "registry mode starts no server"
        );

        record_one_message(&realised.message_sink, 2);
        let text = handle.scrape();
        assert!(
            text.contains("llingr_engine_processed_total{"),
            "handle serves the live registry: {text}"
        );
        assert!(text.contains("partition=\"2\""), "{text}");
    }
}
