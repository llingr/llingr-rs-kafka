// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial
//
// Example consumer for the llingr-kafka end-to-end proof. It consumes the
// order events the producer published and, in doing so, exercises the whole
// chain: demux engine + franz adapter + the FFI boundary + the log facade +
// the built-in metrics endpoint, against a real broker.
//
// What a clean exit 0 proves:
//   - the crate builds (Go bridge and all) and connects to the broker;
//   - every record's partition key survived producer -> broker -> franz ->
//     engine -> FFI -> here intact (the key == body.orderId invariant);
//   - engine log lines reach stdout through the Rust `log` facade with no
//     wiring beyond installing env_logger;
//   - the metrics endpoint served real per-message counters while it ran.
//
// run() BLOCKS until the engine is stopped, so a monitor thread calls the
// stopper once the expected number of messages have been processed (or on a
// failure, or after a timeout). Exit 0 only on the clean success path; exit 1
// on any dead letter, any invariant violation, or a timeout.

use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use llingr_kafka::{
    AutoOffsetReset, Builder, DeadLetterHandler, Message, Metrics, Options, ProcessHandler,
    ShutdownHandler, Traits,
};
use serde::Deserialize;

/// The one field the consumer needs from the order payload: the orderId that
/// must equal the record key. Unknown fields are ignored, so the producer can
/// add fields without touching this.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderView {
    order_id: String,
}

/// Shared run state: how many messages were processed, and whether any failure
/// (dead letter or invariant violation) was seen. Read by the monitor thread
/// and by the exit decision after run() returns.
struct State {
    processed: AtomicU64,
    failed: AtomicBool,
    expected: u64,
}

/// Processes each order: parse the JSON, assert the record key equals the
/// body orderId, log one line, and count it. A parse failure or a key mismatch
/// returns an error, which routes the message to the dead-letter handler.
struct OrderProcessor(Arc<State>);

impl ProcessHandler for OrderProcessor {
    fn process(&self, msg: &Message) -> Result<Traits, Box<dyn Error>> {
        let order: OrderView = serde_json::from_slice(msg.value().unwrap_or(&[]))
            .map_err(|e| format!("invalid order JSON: {e}"))?;

        // The adapter delivers UTF-8-safe keys, so key_str is Some here; treat
        // an unexpected absence as a mismatch rather than silently passing.
        let key = msg.key_str().unwrap_or("");
        if key != order.order_id {
            self.0.failed.store(true, Ordering::Relaxed);
            return Err(format!(
                "invariant violated: record key {key:?} != body orderId {:?} \
                 (partition {}, offset {})",
                order.order_id,
                msg.partition(),
                msg.offset()
            )
            .into());
        }

        log::info!(
            "orderId={} partition={} offset={}",
            order.order_id,
            msg.partition(),
            msg.offset()
        );
        self.0.processed.fetch_add(1, Ordering::Relaxed);
        Ok(Traits::none())
    }
}

/// Any dead letter is a failure for this example: it logs the reason and marks
/// the run failed, which the monitor thread turns into a stop and the exit
/// decision turns into a non-zero code.
struct DeadLetters(Arc<State>);

impl DeadLetterHandler for DeadLetters {
    fn handle(&self, msg: &Message, error_msg: &str) -> Result<(), Box<dyn Error>> {
        self.0.failed.store(true, Ordering::Relaxed);
        log::error!(
            "DEAD LETTER partition={} offset={} error={error_msg}",
            msg.partition(),
            msg.offset()
        );
        Ok(())
    }
}

/// Logs the engine's shutdown reason. The callback fires exactly once, on a
/// graceful stop or an emergency exit; here it only reports (the exit code is
/// decided on the main thread after run() returns).
struct ShutdownLogger;

impl ShutdownHandler for ShutdownLogger {
    fn handle(&self, reason: &str) {
        log::info!("engine shutdown: {reason}");
    }
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn main() {
    // env_logger is the entire logging setup. Engine lines arrive under target
    // "llingr"; this consumer's own lines use the default (crate) target.
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let brokers = std::env::var("BROKERS").unwrap_or_else(|_| "redpanda:9092".to_string());
    let topic = std::env::var("TOPIC").unwrap_or_else(|_| "orders".to_string());
    let expected: u64 = env_or("EXPECTED", 1000);

    // Security is env-driven and additive: with SASL_USERNAME set, the consumer
    // authenticates with SCRAM-SHA-256 over TLS (sasl_ssl), trusting the CA at
    // TLS_CA_LOCATION. The typed setters compute security.protocol=sasl_ssl from
    // the presence of both a SASL and a TLS setter. Without SASL_USERNAME it
    // connects in plaintext; the compose stack always authenticates.
    let mut options = Options::new().auto_offset_reset(AutoOffsetReset::Earliest);
    if let Ok(username) = std::env::var("SASL_USERNAME") {
        if !username.is_empty() {
            let password = std::env::var("SASL_PASSWORD").unwrap_or_default();
            let ca_path = std::env::var("TLS_CA_LOCATION")
                .unwrap_or_else(|_| "/certs/ca-cert.pem".to_string());
            options = options
                .sasl_scram_sha256(&username, &password)
                .tls_ca_location(&ca_path);
        }
    }

    let state = Arc::new(State {
        processed: AtomicU64::new(0),
        failed: AtomicBool::new(false),
        expected,
    });

    let engine = Builder::new(
        &topic,
        OrderProcessor(state.clone()),
        DeadLetters(state.clone()),
    )
    .brokers(&brokers)
    .consumer_group("orders-example")
    .options(options)
    .metrics(Metrics::serve("0.0.0.0:9464", "/metrics"))
    .shutdown(ShutdownLogger)
    .build();
    let engine = match engine {
        Ok(engine) => engine,
        Err(e) => {
            log::error!("failed to initialise llingr-kafka engine: {e}");
            std::process::exit(1);
        }
    };

    log::info!(
        "consumer started: topic={topic:?} group=\"orders-example\" \
         expecting {expected} messages, metrics on :9464/metrics",
    );

    // The monitor stops the engine when the run is done: expected reached
    // (success), a failure was recorded, or the timeout elapsed. run() blocks
    // until this stop() lands, then returns.
    let stop = engine.stopper();
    let monitor_state = state.clone();
    let monitor = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            if monitor_state.failed.load(Ordering::Relaxed) {
                log::error!("stopping: a failure was recorded (dead letter or invariant)");
                break;
            }
            let processed = monitor_state.processed.load(Ordering::Relaxed);
            if processed >= monitor_state.expected {
                log::info!(
                    "stopping: reached expected {} messages",
                    monitor_state.expected
                );
                break;
            }
            if Instant::now() >= deadline {
                log::error!(
                    "stopping: timed out after 120s with {processed} of {} messages",
                    monitor_state.expected
                );
                break;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        stop();
    });

    if let Err(e) = engine.run() {
        log::error!("engine error: {e}");
        std::process::exit(1);
    }
    let _ = monitor.join();

    let processed = state.processed.load(Ordering::Relaxed);
    let failed = state.failed.load(Ordering::Relaxed);
    if !failed && processed >= expected {
        log::info!("SUCCESS: processed {processed} messages, no dead letters, key invariant held");
        std::process::exit(0);
    }
    log::error!("FAILURE: processed {processed} of {expected} (failed={failed})");
    std::process::exit(1);
}
