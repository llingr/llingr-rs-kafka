// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

//! Engine configuration and the bridge config JSON.
//!
//! [`BrokerConfig`] is internal plumbing: the public surface is the engine
//! builder's `.brokers()` / `.consumer_group()` methods and the
//! [`Options`](crate::Options) builder (whose entries land here), collected
//! into the JSON document the Go bridge parses. [`DemuxConfig`] is public
//! API, mirroring Go's `config.DemuxConfig`: the thirteen validated engine
//! tunables, all optional over production defaults.

use std::time::Duration;

use llingr_nexus::AdapterOptions;

// ---------------------------------------------------------------------------
// BrokerConfig (internal)
// ---------------------------------------------------------------------------

/// Broker connection and Kafka client configuration, assembled by the engine
/// builder: brokers, consumer group, and the librdkafka-style option pairs
/// emitted by [`Options`](crate::Options).
///
/// Deliberately no `Debug`: the option pairs may carry credentials
/// (`sasl.password`, ...). Log the [`Options`](crate::Options) builder
/// instead; its `Debug` output redacts secrets.
#[derive(Clone, Default)]
pub(crate) struct BrokerConfig {
    brokers: String,
    consumer_group: String,
    kafka_config: Vec<(String, String)>,
    /// First client-side validation failure (from
    /// [`AdapterOptions::validate`]), surfaced as an error at engine build
    /// time so the fluent builder chain stays infallible.
    deferred_error: Option<String>,
}

impl BrokerConfig {
    /// An empty configuration (no brokers, no group, no options).
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Broker address(es), comma-separated (e.g. "broker1:9092,broker2:9092").
    pub(crate) fn brokers(mut self, brokers: &str) -> Self {
        self.brokers = brokers.to_string();
        self
    }

    /// Consumer group ID.
    pub(crate) fn consumer_group(mut self, group: &str) -> Self {
        self.consumer_group = group.to_string();
        self
    }

    /// Add one librdkafka-style option pair. Repeating a key is allowed and
    /// deterministic: the LAST write wins (the layered-config idiom), applied
    /// across [`kafka_option`](Self::kafka_option) and
    /// [`adapter_options`](Self::adapter_options) in call order.
    ///
    /// Production code feeds pairs in through [`adapter_options`] (the
    /// public escape hatch lives on `Options`); this direct form exists for
    /// the config tests.
    #[cfg(test)]
    pub(crate) fn kafka_option(mut self, key: impl Into<String>, value: impl ToString) -> Self {
        self.kafka_config.push((key.into(), value.to_string()));
        self
    }

    /// Ingest a typed options builder: validate it client-side (a failure is
    /// deferred to engine build time) and append its option entries.
    pub(crate) fn adapter_options(mut self, options: impl AdapterOptions) -> Self {
        if let Err(message) = options.validate() {
            self.deferred_error.get_or_insert(message);
        }
        self.kafka_config.extend(options.entries());
        self
    }

    /// The first client-side validation failure recorded by
    /// [`adapter_options`](Self::adapter_options), if any.
    pub(crate) fn deferred_error(&self) -> Option<&str> {
        self.deferred_error.as_deref()
    }

    /// The configured consumer group, for labelling telemetry that (unlike
    /// the wire config) needs it Rust-side.
    pub(crate) fn consumer_group_value(&self) -> &str {
        &self.consumer_group
    }

    /// The kafka_config pairs with duplicate keys resolved: the LAST value
    /// written wins, so layering (base config, then overrides) behaves the
    /// way Kafka client users expect, and the emitted JSON object never
    /// carries duplicate keys.
    fn deduped_kafka_config(&self) -> Vec<(String, String)> {
        let mut deduped: Vec<(String, String)> = Vec::with_capacity(self.kafka_config.len());
        for (key, value) in &self.kafka_config {
            if let Some(existing) = deduped.iter_mut().find(|(k, _)| k == key) {
                existing.1 = value.clone();
            } else {
                deduped.push((key.clone(), value.clone()));
            }
        }
        deduped
    }
}

// ---------------------------------------------------------------------------
// DemuxConfig
// ---------------------------------------------------------------------------

/// Engine tunables, mirroring Go's `config.DemuxConfig`. Anything left unset
/// receives the engine's production default, and every value is
/// range-validated at build time, reported as a clean error rather than a
/// crash. The defaults are good enough for most situations.
///
/// `Duration`-valued tunables serialise as whole milliseconds; a
/// sub-millisecond remainder rounds up, so a non-zero `Duration` never
/// silently becomes zero.
#[derive(Debug, Clone, Default)]
pub struct DemuxConfig {
    concurrent_keys: Option<u32>,
    per_key_buffer_len: Option<u32>,
    poll_timeout: Option<Duration>,
    auto_commit_interval: Option<Duration>,
    drain_timeout: Option<Duration>,
    await_assignments_timeout: Option<Duration>,
    commit_ingest_channel_len: Option<u32>,
    commit_partition_slice_len: Option<u32>,
    query_timeout: Option<Duration>,
    acquire_worker_timeout_circuit_breaker: Option<Duration>,
    worker_shards_count: Option<u32>,
    rebalance_pause_polling_timeout: Option<Duration>,
    acquire_commit_guard_timeout: Option<Duration>,
}

impl DemuxConfig {
    /// Create a configuration with every tunable unset (engine defaults).
    pub fn new() -> Self {
        Self::default()
    }

    /// Maximum number of concurrent per-key workers (engine default 250, max 5000).
    /// Zero selects the engine default rather than erroring.
    pub fn concurrent_keys(mut self, n: u32) -> Self {
        self.concurrent_keys = Some(n);
        self
    }

    /// Per-worker channel buffer length (engine default 16, max 64).
    /// Zero selects the engine default rather than erroring.
    pub fn per_key_buffer_len(mut self, n: u32) -> Self {
        self.per_key_buffer_len = Some(n);
        self
    }

    /// Broker poll timeout (engine default 100ms, range 20ms-2s). A zero
    /// duration selects the engine default rather than erroring.
    pub fn poll_timeout(mut self, d: Duration) -> Self {
        self.poll_timeout = Some(d);
        self
    }

    /// Offset auto-commit interval (engine default 5s, range 250ms-15s). A zero
    /// duration selects the engine default rather than erroring.
    pub fn auto_commit_interval(mut self, d: Duration) -> Self {
        self.auto_commit_interval = Some(d);
        self
    }

    /// In-flight drain cap on rebalance/shutdown (engine default 20s, range 2s-55s).
    /// A zero duration selects the engine default rather than erroring.
    pub fn drain_timeout(mut self, d: Duration) -> Self {
        self.drain_timeout = Some(d);
        self
    }

    /// How long Subscribe waits for partition assignment (engine default 50s, range 5s-5m).
    /// A zero duration selects the engine default rather than erroring.
    pub fn await_assignments_timeout(mut self, d: Duration) -> Self {
        self.await_assignments_timeout = Some(d);
        self
    }

    /// Commit ingest channel length. An explicit value must be in the range
    /// [1000, 200000]; a value outside that range is a startup error. Left
    /// unset (or set to zero) the length is derived from concurrent_keys.
    pub fn commit_ingest_channel_len(mut self, n: u32) -> Self {
        self.commit_ingest_channel_len = Some(n);
        self
    }

    /// Initial gap-buffer size per partition (engine default 400, range 50-2000).
    /// Zero selects the engine default rather than erroring.
    pub fn commit_partition_slice_len(mut self, n: u32) -> Self {
        self.commit_partition_slice_len = Some(n);
        self
    }

    /// Broker query timeout (engine default 5s, range 1s-10s). A zero duration
    /// selects the engine default rather than erroring.
    pub fn query_timeout(mut self, d: Duration) -> Self {
        self.query_timeout = Some(d);
        self
    }

    /// How long dispatch may wait for a worker before the circuit breaker
    /// fires (engine default 1m, range 15s-15m). A zero duration selects the
    /// engine default rather than erroring.
    pub fn acquire_worker_timeout_circuit_breaker(mut self, d: Duration) -> Self {
        self.acquire_worker_timeout_circuit_breaker = Some(d);
        self
    }

    /// Worker shard count; must be a power of two (engine default 16, max 64).
    /// Zero selects the engine default rather than erroring, but note the
    /// 0-vs-1 cliff: 1 is a startup error, since shards must be a power of two
    /// >= 2.
    pub fn worker_shards_count(mut self, n: u32) -> Self {
        self.worker_shards_count = Some(n);
        self
    }

    /// Polling pause cap during rebalance (engine default 30s, range 10s-10m).
    /// A zero duration selects the engine default rather than erroring.
    pub fn rebalance_pause_polling_timeout(mut self, d: Duration) -> Self {
        self.rebalance_pause_polling_timeout = Some(d);
        self
    }

    /// Commit guard acquisition timeout (engine default 10s, range 100ms-30s).
    /// A zero duration selects the engine default rather than erroring.
    pub fn acquire_commit_guard_timeout(mut self, d: Duration) -> Self {
        self.acquire_commit_guard_timeout = Some(d);
        self
    }

    /// The "demux" JSON object, or None when no tunable is set.
    /// Field names match the Go config.DemuxConfig JSON tags; durations are
    /// millisecond strings parsed by its UnmarshalJSON via time.ParseDuration.
    fn demux_json(&self) -> Option<String> {
        let mut fields: Vec<String> = Vec::new();

        let mut push_int = |name: &str, v: Option<u32>| {
            if let Some(n) = v {
                fields.push(format!("\"{name}\":{n}"));
            }
        };
        push_int("concurrentKeys", self.concurrent_keys);
        push_int("perKeyBufferLen", self.per_key_buffer_len);
        push_int("commitIngestChannelLen", self.commit_ingest_channel_len);
        push_int("commitPartitionSliceLen", self.commit_partition_slice_len);
        push_int("workerShardsCount", self.worker_shards_count);

        // Millisecond granularity, sub-millisecond remainders rounded UP:
        // a non-zero Duration must never silently serialise as "0ms".
        let mut push_duration = |name: &str, v: Option<Duration>| {
            if let Some(dur) = v {
                fields.push(format!(
                    "\"{name}\":\"{}ms\"",
                    llingr_nexus::duration_ms_ceil(dur)
                ));
            }
        };
        push_duration("pollTimeout", self.poll_timeout);
        push_duration("autoCommitInterval", self.auto_commit_interval);
        push_duration("drainTimeout", self.drain_timeout);
        push_duration("awaitAssignmentsTimeout", self.await_assignments_timeout);
        push_duration("queryTimeout", self.query_timeout);
        push_duration(
            "acquireWorkerTimeoutCircuitBreaker",
            self.acquire_worker_timeout_circuit_breaker,
        );
        push_duration(
            "rebalancePausePollingTimeout",
            self.rebalance_pause_polling_timeout,
        );
        push_duration(
            "acquireCommitGuardTimeout",
            self.acquire_commit_guard_timeout,
        );

        if fields.is_empty() {
            return None;
        }
        Some(format!("{{{}}}", fields.join(",")))
    }
}

// ---------------------------------------------------------------------------
// JSON assembly (bridge contract)
// ---------------------------------------------------------------------------

/// Serialise the full configuration to JSON for the Go bridge. Built
/// manually (the shape is the bridge's contract); the adapter is hard-wired
/// to "franz", the only broker composition this crate ships.
pub(crate) fn config_json(
    topic: &str,
    broker: &BrokerConfig,
    demux: &DemuxConfig,
    service: Option<&(String, String)>,
    bandwidth_stats_interval: Option<Duration>,
    bandwidth_flush_interval: Option<Duration>,
) -> String {
    let mut json = String::with_capacity(512);
    json.push('{');

    push_string_field(&mut json, "adapter", "franz");
    json.push(',');
    push_string_field(&mut json, "brokers", &broker.brokers);
    json.push(',');
    push_string_field(&mut json, "topic", topic);
    json.push(',');
    push_string_field(&mut json, "consumer_group", &broker.consumer_group);

    let kafka_config = broker.deduped_kafka_config();
    if !kafka_config.is_empty() {
        json.push_str(",\"kafka_config\":{");
        for (i, (k, v)) in kafka_config.iter().enumerate() {
            if i > 0 {
                json.push(',');
            }
            push_string_field(&mut json, k, v);
        }
        json.push('}');
    }

    if let Some((name, team)) = service {
        json.push_str(",\"service\":{");
        push_string_field(&mut json, "name", name);
        json.push(',');
        push_string_field(&mut json, "team", team);
        json.push('}');
    }

    if let Some(demux) = demux.demux_json() {
        json.push_str(",\"demux\":");
        json.push_str(&demux);
    }

    // Bandwidth intervals, millisecond-granular like the demux durations.
    // Present only when set; the bridge applies engine defaults otherwise.
    if bandwidth_stats_interval.is_some() || bandwidth_flush_interval.is_some() {
        json.push_str(",\"bandwidth\":{");
        let mut first = true;
        if let Some(d) = bandwidth_stats_interval {
            json.push_str(&format!(
                "\"statsInterval\":\"{}ms\"",
                llingr_nexus::duration_ms_ceil(d)
            ));
            first = false;
        }
        if let Some(d) = bandwidth_flush_interval {
            if !first {
                json.push(',');
            }
            json.push_str(&format!(
                "\"flushInterval\":\"{}ms\"",
                llingr_nexus::duration_ms_ceil(d)
            ));
        }
        json.push('}');
    }

    json.push('}');
    json
}

fn push_string_field(buf: &mut String, key: &str, value: &str) {
    buf.push('"');
    json_escape_into(buf, key);
    buf.push_str("\":\"");
    json_escape_into(buf, value);
    buf.push('"');
}

/// Minimal JSON string escaping for the config document.
fn json_escape_into(buf: &mut String, s: &str) {
    for ch in s.chars() {
        match ch {
            '"' => buf.push_str("\\\""),
            '\\' => buf.push_str("\\\\"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            '\t' => buf.push_str("\\t"),
            c if c < '\x20' => {
                buf.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => buf.push(c),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use llingr_nexus::Adapter;

    fn minimal_broker() -> BrokerConfig {
        BrokerConfig::new()
            .brokers("localhost:9092")
            .consumer_group("grp")
    }

    #[test]
    fn minimal_config_json() {
        let json = config_json(
            "orders",
            &minimal_broker(),
            &DemuxConfig::new(),
            None,
            None,
            None,
        );
        assert_eq!(
            json,
            r#"{"adapter":"franz","brokers":"localhost:9092","topic":"orders","consumer_group":"grp"}"#
        );
    }

    #[test]
    fn full_config_json() {
        let broker = BrokerConfig::new()
            .brokers("b1:9092,b2:9092")
            .consumer_group("grp")
            .kafka_option("auto.offset.reset", "earliest");
        let demux = DemuxConfig::new()
            .concurrent_keys(500)
            .poll_timeout(Duration::from_millis(150))
            .drain_timeout(Duration::from_secs(30))
            .worker_shards_count(32);
        let service = ("order-service".to_string(), "payments".to_string());
        let json = config_json("orders", &broker, &demux, Some(&service), None, None);

        assert!(json.contains(r#""adapter":"franz""#));
        assert!(json.contains(r#""kafka_config":{"auto.offset.reset":"earliest"}"#));
        assert!(json.contains(r#""service":{"name":"order-service","team":"payments"}"#));
        assert!(json.contains(r#""demux":{"#));
        assert!(json.contains(r#""concurrentKeys":500"#));
        assert!(json.contains(r#""pollTimeout":"150ms""#));
        assert!(json.contains(r#""drainTimeout":"30000ms""#));
        assert!(json.contains(r#""workerShardsCount":32"#));
    }

    /// Every one of the thirteen DemuxConfig tunables must serialise under
    /// EXACTLY the Go config.DemuxConfig JSON tag. A typo in any tag compiles,
    /// passes every other test, and silently reverts that tunable to the
    /// engine default in production (Go ignores unknown keys).
    #[test]
    fn all_thirteen_demux_tags_match_go() {
        let demux = DemuxConfig::new()
            .concurrent_keys(500)
            .per_key_buffer_len(32)
            .poll_timeout(Duration::from_millis(150))
            .auto_commit_interval(Duration::from_secs(5))
            .drain_timeout(Duration::from_secs(30))
            .await_assignments_timeout(Duration::from_secs(50))
            .commit_ingest_channel_len(2000)
            .commit_partition_slice_len(400)
            .query_timeout(Duration::from_secs(5))
            .acquire_worker_timeout_circuit_breaker(Duration::from_secs(60))
            .worker_shards_count(32)
            .rebalance_pause_polling_timeout(Duration::from_secs(30))
            .acquire_commit_guard_timeout(Duration::from_secs(10));
        let json = config_json("t", &minimal_broker(), &demux, None, None, None);

        let expected = [
            r#""concurrentKeys":500"#,
            r#""perKeyBufferLen":32"#,
            r#""pollTimeout":"150ms""#,
            r#""autoCommitInterval":"5000ms""#,
            r#""drainTimeout":"30000ms""#,
            r#""awaitAssignmentsTimeout":"50000ms""#,
            r#""commitIngestChannelLen":2000"#,
            r#""commitPartitionSliceLen":400"#,
            r#""queryTimeout":"5000ms""#,
            r#""acquireWorkerTimeoutCircuitBreaker":"60000ms""#,
            r#""workerShardsCount":32"#,
            r#""rebalancePausePollingTimeout":"30000ms""#,
            r#""acquireCommitGuardTimeout":"10000ms""#,
        ];
        for tag in expected {
            assert!(json.contains(tag), "missing or renamed tag {tag} in {json}");
        }
    }

    /// The escaper's remaining branches: tab, carriage return, and the
    /// generic control-character \u escape (an unescaped control char is
    /// malformed JSON and would fail engine init).
    #[test]
    fn json_escaping_control_characters() {
        let broker = minimal_broker().kafka_option("key", "a\tb\rc\u{1}d");
        let json = config_json("t", &broker, &DemuxConfig::new(), None, None, None);
        let expected = "a\\tb\\rc\\u0001d";
        assert!(json.contains(expected), "{json}");
    }

    #[test]
    fn json_escaping() {
        let broker = BrokerConfig::new()
            .brokers("localhost:9092")
            .consumer_group("group\\with\\backslashes")
            .kafka_option("key", "value\nwith\nnewlines");
        let json = config_json(
            "topic-with-\"quotes\"",
            &broker,
            &DemuxConfig::new(),
            None,
            None,
            None,
        );
        assert!(json.contains(r#"topic-with-\"quotes\""#));
        assert!(json.contains(r#"group\\with\\backslashes"#));
        assert!(json.contains(r#"value\nwith\nnewlines"#));
    }

    #[test]
    fn adapter_options_applies_entries() {
        struct FakeOptions;
        impl AdapterOptions for FakeOptions {
            fn adapter(&self) -> Adapter {
                Adapter::Franz
            }
            fn entries(&self) -> Vec<(String, String)> {
                vec![("client.id".to_string(), "app-1".to_string())]
            }
        }

        // By value and by reference both work (blanket impl in nexus).
        let broker = minimal_broker()
            .adapter_options(&FakeOptions)
            .adapter_options(FakeOptions);
        let json = config_json("t", &broker, &DemuxConfig::new(), None, None, None);
        assert!(json.contains(r#""client.id":"app-1""#));
    }

    #[test]
    fn duplicate_kafka_options_last_write_wins() {
        let broker = minimal_broker()
            .kafka_option("session.timeout.ms", 6000)
            .kafka_option("session.timeout.ms", 9000);
        let json = config_json("t", &broker, &DemuxConfig::new(), None, None, None);
        assert!(json.contains(r#""session.timeout.ms":"9000""#), "{json}");
        assert!(
            !json.contains(r#""session.timeout.ms":"6000""#),
            "duplicate key must be deduped: {json}"
        );
        assert_eq!(json.matches("session.timeout.ms").count(), 1);
    }

    #[test]
    fn last_write_wins_across_kafka_option_and_adapter_options() {
        struct FakeOptions;
        impl AdapterOptions for FakeOptions {
            fn adapter(&self) -> Adapter {
                Adapter::Franz
            }
            fn entries(&self) -> Vec<(String, String)> {
                vec![("client.id".to_string(), "from-options".to_string())]
            }
        }

        // adapter_options called after kafka_option: its value wins.
        let broker = minimal_broker()
            .kafka_option("client.id", "from-raw")
            .adapter_options(FakeOptions);
        let json = config_json("t", &broker, &DemuxConfig::new(), None, None, None);
        assert!(json.contains(r#""client.id":"from-options""#), "{json}");
        assert!(!json.contains("from-raw"));
    }

    #[test]
    fn kafka_option_accepts_numbers_and_bools() {
        let broker = minimal_broker()
            .kafka_option("fetch.min.bytes", 1024)
            .kafka_option("enable.partition.eof", false);
        let json = config_json("t", &broker, &DemuxConfig::new(), None, None, None);
        assert!(json.contains(r#""fetch.min.bytes":"1024""#));
        assert!(json.contains(r#""enable.partition.eof":"false""#));
    }

    #[test]
    fn adapter_options_validation_failure_is_deferred() {
        struct BrokenOptions;
        impl AdapterOptions for BrokenOptions {
            fn adapter(&self) -> Adapter {
                Adapter::Franz
            }
            fn entries(&self) -> Vec<(String, String)> {
                Vec::new()
            }
            fn validate(&self) -> Result<(), String> {
                Err("conflicting configuration".to_string())
            }
        }

        let broker = minimal_broker().adapter_options(BrokenOptions);
        assert_eq!(broker.deferred_error(), Some("conflicting configuration"));
    }

    /// Engine timing options are millisecond-granular: a sub-millisecond
    /// Duration must round UP, never silently serialise as "0ms".
    #[test]
    fn sub_millisecond_duration_rounds_up() {
        let demux = DemuxConfig::new().poll_timeout(Duration::from_micros(500));
        let json = config_json("t", &minimal_broker(), &demux, None, None, None);
        assert!(json.contains(r#""pollTimeout":"1ms""#), "{json}");
    }

    /// The engine builder consumes its configs; clones must serialise
    /// identically, including a recorded deferred error, so a retry after a
    /// transient startup failure starts from the same configuration.
    #[test]
    fn clone_preserves_config_for_retry() {
        let broker = minimal_broker().kafka_option("client.id", "app-1");
        let demux = DemuxConfig::new()
            .concurrent_keys(500)
            .poll_timeout(Duration::from_millis(150));
        let service = ("svc".to_string(), "team".to_string());
        assert_eq!(
            config_json(
                "t",
                &broker.clone(),
                &demux.clone(),
                Some(&service),
                None,
                None
            ),
            config_json("t", &broker, &demux, Some(&service), None, None),
        );
        assert_eq!(broker.clone().deferred_error(), broker.deferred_error());
    }

    /// Bandwidth intervals appear only when set, with millisecond strings.
    #[test]
    fn bandwidth_intervals_serialise_when_set() {
        let json = config_json(
            "t",
            &minimal_broker(),
            &DemuxConfig::new(),
            None,
            Some(Duration::from_secs(5)),
            Some(Duration::from_secs(15)),
        );
        assert!(
            json.contains(r#""bandwidth":{"statsInterval":"5000ms","flushInterval":"15000ms"}"#),
            "{json}"
        );

        let stats_only = config_json(
            "t",
            &minimal_broker(),
            &DemuxConfig::new(),
            None,
            Some(Duration::from_secs(1)),
            None,
        );
        assert!(
            stats_only.contains(r#""bandwidth":{"statsInterval":"1000ms"}"#),
            "{stats_only}"
        );

        let none = config_json(
            "t",
            &minimal_broker(),
            &DemuxConfig::new(),
            None,
            None,
            None,
        );
        assert!(!none.contains("bandwidth"));
    }

    #[test]
    fn no_demux_object_when_no_tunables() {
        let json = config_json(
            "t",
            &minimal_broker(),
            &DemuxConfig::new(),
            None,
            None,
            None,
        );
        assert!(!json.contains("demux"));
    }
}
