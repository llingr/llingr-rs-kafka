// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

//! Engine lifecycle: handler staging and publication, initialisation of the
//! Go bridge, run/stop. The raw C ABI lives in [`crate::ffi`].

use std::error::Error;
use std::ffi::CString;
use std::fmt;
use std::os::raw::{c_char, c_int};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use llingr_nexus::{
    BandwidthMetricsHandler, DeadLetterHandler, LogHandler, MetricsHandler, ProcessHandler,
    ShutdownHandler,
};

use crate::config::{config_json, BrokerConfig, DemuxConfig};
use crate::ffi;
use crate::logging::LogRouter;
use crate::metrics::{ExporterHandle, Metrics};
use crate::options::Options;
use crate::snapshot::Snapshot;
use crate::trampolines;

/// Error type for llingr operations.
#[derive(Debug)]
pub struct LlingrError {
    message: String,
    code: i32,
}

impl LlingrError {
    pub(crate) fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code,
        }
    }

    /// The error code returned by the Go bridge.
    pub fn code(&self) -> i32 {
        self.code
    }
}

impl fmt::Display for LlingrError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "llingr error {}: {}", self.code, self.message)
    }
}

impl Error for LlingrError {}

// ---------------------------------------------------------------------------
// Global handler storage
// ---------------------------------------------------------------------------

pub(crate) struct Handlers {
    // Topic is fixed per consumer as Rust-side config, not a per-message
    // wire field, so the trampolines read it from here to populate
    // Message.topic.
    pub(crate) topic: String,
    pub(crate) process: Box<dyn ProcessHandler>,
    pub(crate) dead_letter: Option<Box<dyn DeadLetterHandler>>,
    pub(crate) metrics: Option<Box<dyn MetricsHandler>>,
    pub(crate) shutdown: Option<Box<dyn ShutdownHandler>>,
    pub(crate) log: Option<Box<dyn LogHandler>>,
    pub(crate) bandwidth: Option<Box<dyn BandwidthMetricsHandler>>,
    // The built-in scrape endpoint, when Metrics::serve was configured. Held
    // in the leaked handler set purely to keep it alive: it runs for
    // the life of the process like the engine, and a failed build's rollback
    // drops it and stops its server thread. Never read, hence the underscore.
    pub(crate) _prometheus_exporter: Option<ExporterHandle>,
}

// Handlers is published as a raw pointer and handed to the Go worker threads as
// a shared `&'static` (see `handlers()`), which the per-key workers dereference
// concurrently. That is sound only if every field is `Send + Sync`. The
// `AtomicPtr<Handlers>` that stores it is `Send + Sync` for ALL `T`, so nothing
// here forces that invariant; this assertion does, turning a future non-`Sync`
// field (an `Rc`, a `Cell`, a `Send`-but-not-`Sync` handle) into a compile
// error instead of a silent data race across the callback threads.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync + ?Sized>() {}
    assert_send_sync::<Handlers>();
};

/// Init guard: `false` until an engine has been successfully created. Held for
/// the whole of [`Builder::build`], so concurrent constructors serialise
/// and the check-then-init is atomic. A failed init leaves it `false`, so a
/// transient startup error (e.g. broker briefly unreachable) can be retried
/// rather than permanently bricking the process.
static INIT_LOCK: Mutex<bool> = Mutex::new(false);

/// Published handler set, read lock-free from the callback trampolines.
///
/// Publication protocol: `build()` (under INIT_LOCK) leaks the boxed set and
/// release-stores the pointer BEFORE calling `llingr_init`, so Build-time
/// engine logs already reach the `log` facade. If init fails, the pointer is
/// swapped back to null and the box reclaimed; that is sound because the Go
/// engine is not running at that point, so no thread can be inside a
/// trampoline: init-time callbacks happen synchronously on the same thread,
/// inside the `llingr_init` call that has already returned.
///
/// Trampolines acquire-load the pointer; the load pairs with the release
/// store for a clean happens-before edge, and no lock is taken on the
/// message hot path.
pub(crate) static HANDLERS: AtomicPtr<Handlers> = AtomicPtr::new(std::ptr::null_mut());

/// Lock-free view of the published handler set (None before publication).
pub(crate) fn handlers() -> Option<&'static Handlers> {
    let ptr = HANDLERS.load(Ordering::Acquire);
    if ptr.is_null() {
        None
    } else {
        // SAFETY: the box is freed only on the init-failure rollback in
        // `build()`, and that path is sound only because no goroutine can be
        // inside a trampoline at that instant: the engine starts no worker
        // goroutines until `Subscribe` (called from `run()`, after a successful
        // `build()`), and the only trampoline reachable during init is the log
        // callback, which the bridge fires synchronously on the init thread
        // (already returned by the time the rollback runs). Once `run()` starts,
        // the set is never freed and lives for the process lifetime. A future
        // engine that logged from a goroutine spawned during init would break
        // this invariant; see the rollback in `build()`.
        Some(unsafe { &*ptr })
    }
}

/// Capacity for llingr_init's error-text buffer.
const INIT_ERR_CAP: usize = 1024;

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Staged construction of the engine ([`Llingr`]).
///
/// Created by [`Builder::new`] with the topic and the two required handlers,
/// process and dead-letter, mirroring Go's
/// `demux.NewBuilder(topicName, processMessage, writeDeadLetter)`. Chain
/// [`brokers`](Builder::brokers) and [`consumer_group`](Builder::consumer_group),
/// both required, plus any optional configuration, then call
/// [`build`](Builder::build) to initialise the Go bridge and create the
/// broker client. A missing required field is a clean error at build time.
/// Engine logs emitted during startup already flow to the `log` facade.
pub struct Builder {
    topic: String,
    broker: BrokerConfig,
    demux: DemuxConfig,
    service: Option<(String, String)>,
    bandwidth_stats_interval: Option<Duration>,
    bandwidth_flush_interval: Option<Duration>,
    metrics: Option<Metrics>,
    process: Box<dyn ProcessHandler>,
    dead_letter: Box<dyn DeadLetterHandler>,
    shutdown: Option<Box<dyn ShutdownHandler>>,
}

impl Builder {
    /// Start staged construction with the topic and the two required
    /// handlers, mirroring Go's
    /// `demux.NewBuilder(topicName, processMessage, writeDeadLetter)` exactly.
    ///
    /// The dead-letter handler is required: without one, a failed message
    /// would have nowhere to go and would be silently dropped when its
    /// offset commits, breaking the no-dropped-messages invariant. Logging
    /// the message and reason is the bare-minimum implementation; the
    /// recommended one publishes to a real dead-letter destination: a DLQ
    /// topic, a table, or an object store.
    pub fn new(
        topic: &str,
        process: impl ProcessHandler,
        dead_letter: impl DeadLetterHandler,
    ) -> Builder {
        Builder {
            topic: topic.to_string(),
            broker: BrokerConfig::new(),
            demux: DemuxConfig::new(),
            service: None,
            bandwidth_stats_interval: None,
            bandwidth_flush_interval: None,
            metrics: None,
            process: Box::new(process),
            dead_letter: Box::new(dead_letter),
            shutdown: None,
        }
    }

    /// Broker address(es), comma-separated
    /// (e.g. `"broker1:9092,broker2:9092"`). Required.
    pub fn brokers(mut self, brokers: &str) -> Self {
        self.broker = self.broker.brokers(brokers);
        self
    }

    /// Consumer group ID. Required.
    pub fn consumer_group(mut self, group: &str) -> Self {
        self.broker = self.broker.consumer_group(group);
        self
    }

    /// Kafka client configuration: the typed [`Options`] builder covering
    /// offset reset, timeouts, fetch tuning, TLS/SASL security, and the
    /// `kafka_option` string escape hatch. Optional; the defaults connect
    /// to an unauthenticated cluster.
    ///
    /// The options are validated client-side; a failure, such as the same
    /// security key set both via a typed setter and a string pair, is
    /// reported as an error when the engine is built.
    pub fn options(mut self, options: Options) -> Self {
        self.broker = self.broker.adapter_options(options);
        self
    }

    /// Engine settings (Go's `WithDemuxConfig`). Entirely optional: the
    /// engine's production defaults are good enough for most situations.
    pub fn demux(mut self, config: DemuxConfig) -> Self {
        self.demux = config;
        self
    }

    /// Service identity attached to metrics (name, owning team), mirroring
    /// Go's `WithService`. Used by fleet tooling to route and label
    /// telemetry. Optional.
    pub fn service(mut self, name: &str, team: &str) -> Self {
        self.service = Some((name.to_string(), team.to_string()));
        self
    }

    /// Activate the baked-in Prometheus metrics: [`Metrics::serve`] for the
    /// built-in scrape endpoint, or [`Metrics::registry`] to mount the
    /// exposition on your own HTTP stack. Registering metrics also enables
    /// bandwidth telemetry collection (the `llingr_bandwidth_*` series).
    /// Optional; not configuring metrics costs nothing at runtime.
    pub fn metrics(mut self, metrics: Metrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// The broker adapter's bandwidth collection cadence (valid 1s-12h,
    /// default 1 minute). Only meaningful together with
    /// [`metrics`](Self::metrics).
    pub fn bandwidth_stats_interval(mut self, d: Duration) -> Self {
        self.bandwidth_stats_interval = Some(d);
        self
    }

    /// How often the engine's aggregator forwards buffered bandwidth packets
    /// to the metrics sink (Go's `WithBandwidthFlushInterval`; default 60s).
    /// Shorter intervals reduce delivery latency at no cost to message
    /// processing. Only meaningful together with [`metrics`](Self::metrics).
    pub fn bandwidth_flush_interval(mut self, d: Duration) -> Self {
        self.bandwidth_flush_interval = Some(d);
        self
    }

    /// Register an optional shutdown handler. Invoked exactly once, with
    /// the reason, when the consumer exits gracefully or on an emergency
    /// shutdown.
    pub fn shutdown(mut self, handler: impl ShutdownHandler) -> Self {
        self.shutdown = Some(Box::new(handler));
        self
    }

    /// Initialise the engine: publish the handler set, register the FFI
    /// trampolines, and initialise the Go bridge, which creates the broker
    /// client.
    ///
    /// # Errors
    ///
    /// Returns an error if an instance already exists in this process, the
    /// linked engine's ABI does not match, the configuration is invalid,
    /// the metrics endpoint cannot bind, or the broker client cannot be
    /// created. Error text comes from the Go bridge where available. A
    /// failed build can be retried.
    pub fn build(mut self) -> Result<Llingr, LlingrError> {
        // Client-side validation failures recorded during configuration
        // (e.g. a security key set both via typed setters and string pairs)
        // are reported here, before any global state is touched, so a
        // corrected retry starts clean.
        if let Some(message) = self.broker.deferred_error() {
            return Err(LlingrError::new(-5, message.to_string()));
        }

        // Fail fast if the linked engine's ABI does not match what this
        // crate was compiled against: otherwise every callback is silent UB.
        let abi = unsafe { ffi::llingr_abi_version() };
        if abi != ffi::LLINGR_ABI_VERSION {
            return Err(abi_mismatch_error(ffi::LLINGR_ABI_VERSION, abi));
        }

        // Hold the guard across the whole init so the check-then-init is
        // atomic; a second concurrent caller blocks here, then sees
        // `*inited == true` and gets a clean error.
        let mut inited = INIT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        if *inited {
            return Err(LlingrError::new(-1, "only one llingr instance per process"));
        }

        // Reject NUL bytes in config rather than panicking (CString::new).
        let json = config_json(
            &self.topic,
            &self.broker,
            &self.demux,
            self.service.as_ref(),
            self.bandwidth_stats_interval,
            self.bandwidth_flush_interval,
        );
        let c_json = CString::new(json.as_bytes())
            .map_err(|_| LlingrError::new(-2, "configuration contains an interior NUL byte"))?;

        // Realise the metrics activation, if requested: build the shared
        // registry, register both sinks, and bind the built-in endpoint or
        // publish the registry to the caller's handle. This must run before
        // the handler set is snapshotted below, and a bind failure returns
        // here, before anything is published.
        let realised = match self.metrics.take() {
            Some(metrics) => {
                let (service, team) = match &self.service {
                    Some((name, team)) => (name.as_str(), team.as_str()),
                    None => ("", ""),
                };
                Some(
                    metrics
                        .realise(
                            &self.topic,
                            self.broker.consumer_group_value(),
                            service,
                            team,
                        )
                        .map_err(|e| LlingrError::new(-6, e.to_string()))?,
                )
            }
            None => None,
        };

        // Publish the handler set BEFORE llingr_init: Build-time engine logs
        // (licence notice, adapter setup) must reach the `log` facade. The
        // metrics and bandwidth handlers are the Prometheus sinks; the
        // bandwidth trampoline is registered only when metrics are
        // configured, because its registration is the bridge's signal to
        // enable collection on the adapter.
        let bandwidth_enabled = realised.is_some();
        let (metrics_handler, bandwidth_handler, prometheus_exporter) = match realised {
            Some(r) => (
                Some(Box::new(r.message_sink) as Box<dyn MetricsHandler>),
                Some(Box::new(r.bandwidth_sink) as Box<dyn BandwidthMetricsHandler>),
                r.exporter,
            ),
            None => (None, None, None),
        };
        let handler_ptr = Box::into_raw(Box::new(Handlers {
            topic: self.topic,
            process: self.process,
            dead_letter: Some(self.dead_letter),
            metrics: metrics_handler,
            shutdown: self.shutdown,
            log: Some(Box::new(LogRouter::new())),
            bandwidth: bandwidth_handler,
            _prometheus_exporter: prometheus_exporter,
        }));
        let previous = HANDLERS.swap(handler_ptr, Ordering::AcqRel);
        if !previous.is_null() {
            // A previous failed build rolls back and leaves nothing behind,
            // and success sets `inited`; a non-null here means the publish
            // protocol was violated. Restore and refuse.
            HANDLERS.swap(previous, Ordering::AcqRel);
            drop(unsafe { Box::from_raw(handler_ptr) });
            return Err(LlingrError::new(-1, "handler set already published"));
        }

        // Register C callback trampolines.
        unsafe {
            ffi::llingr_on_process(trampolines::process_trampoline);
            ffi::llingr_on_deadletter(trampolines::deadletter_trampoline);
            ffi::llingr_on_metrics(trampolines::metrics_trampoline);
            ffi::llingr_on_shutdown(trampolines::shutdown_trampoline);
            ffi::llingr_on_log(trampolines::log_trampoline);
            if bandwidth_enabled {
                ffi::llingr_on_bandwidth(trampolines::bandwidth_trampoline);
            }
        }

        // Initialise the Go bridge. On failure it reports WHY through the
        // error buffer (adapter errors, invalid options, recovered panics).
        let mut err_buf = [0 as c_char; INIT_ERR_CAP];
        let mut err_len: c_int = 0;
        let rc = unsafe {
            ffi::llingr_init(
                c_json.as_ptr(),
                json.len() as c_int,
                err_buf.as_mut_ptr(),
                INIT_ERR_CAP as c_int,
                &mut err_len,
            )
        };
        if rc != 0 {
            // Roll back the publication so a retry after a transient failure
            // starts clean and `*inited` stays false. Freeing the box here is
            // sound because no goroutine can be inside a trampoline at this
            // point: llingr_init failed, so the engine never reached Subscribe
            // and started no worker goroutines, and any build-time log callback
            // fired synchronously on this thread and has already returned (see
            // the SAFETY note on `handlers`). Leak-on-failure would sidestep
            // that invariant but unbounded-leak a retry loop, so the free stays.
            // Dropping the box also drops a bound scrape endpoint, freeing its
            // port for the retry.
            let published = HANDLERS.swap(std::ptr::null_mut(), Ordering::AcqRel);
            if !published.is_null() {
                drop(unsafe { Box::from_raw(published) });
            }

            return Err(LlingrError::new(
                rc,
                init_failure_text(rc, &err_buf, err_len),
            ));
        }

        *inited = true;
        Ok(Llingr { _private: () })
    }
}

/// The build-time refusal for an engine library whose ABI version does not
/// match this crate. Extracted so the message, quoted by the docs, is unit
/// tested: the branch itself is unreachable against a correctly built
/// library.
pub(crate) fn abi_mismatch_error(expected: c_int, reported: c_int) -> LlingrError {
    LlingrError::new(
        -1,
        format!(
            "llingr ABI mismatch: crate expects {expected}, library reports {reported} \
             (rebuild libllingr to match this crate)"
        ),
    )
}

/// The error text for a failed llingr_init: the bridge's own message from
/// the error buffer when it wrote one, decoded lossily because Go strings
/// have no UTF-8 guarantee across the boundary; otherwise the stable
/// per-code fallback.
/// Extracted so every branch is unit tested without a failing engine.
pub(crate) fn init_failure_text(rc: c_int, err_buf: &[c_char], err_len: c_int) -> String {
    if err_len > 0 {
        let bytes: Vec<u8> = err_buf[..err_len as usize]
            .iter()
            .map(|&c| c as u8)
            .collect();
        String::from_utf8_lossy(&bytes).into_owned()
    } else {
        match rc {
            -1 => "already initialised".to_string(),
            -2 => "invalid configuration JSON".to_string(),
            -3 => "missing required config (brokers, topic, or consumer_group)".to_string(),
            -4 => "failed to create adapter or connect to broker".to_string(),
            -5 => "invalid adapter or engine option".to_string(),
            _ => "unknown initialisation error".to_string(),
        }
    }
}

/// Maps llingr_run's return code onto the run() result. Extracted so that
/// the negative codes, unreachable in a healthy lifecycle, are unit tested.
pub(crate) fn run_result(rc: c_int) -> Result<(), LlingrError> {
    match rc {
        0 => Ok(()),
        -1 => Err(LlingrError::new(rc, "engine not initialised")),
        -2 => Err(LlingrError::new(rc, "subscribe failed")),
        -3 => Err(LlingrError::new(
            rc,
            "engine panicked (recovered at FFI boundary)",
        )),
        _ => Err(LlingrError::new(rc, "unknown runtime error")),
    }
}

/// Fetch the engine snapshot JSON over the FFI. Factored out of the method
/// so the not-initialised path is testable without an engine instance.
pub(crate) fn take_snapshot_json() -> Result<String, LlingrError> {
    let ptr = unsafe { ffi::llingr_take_snapshot() };
    if ptr.is_null() {
        return Err(LlingrError::new(-1, "engine not initialised"));
    }
    // The document is json.Marshal output with control characters escaped,
    // so it is valid UTF-8 with no interior NULs; decode lossily anyway
    // rather than trusting that across the boundary.
    let json = unsafe { std::ffi::CStr::from_ptr(ptr) }
        .to_string_lossy()
        .into_owned();
    unsafe { ffi::llingr_free_string(ptr) };
    Ok(json)
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

/// The llingr engine handle.
///
/// Only one instance may exist per process because the Go runtime is
/// process-global. Attempting to create a second instance will fail.
///
/// # Lifecycle
///
/// Dropping this handle does **not** stop the engine: the Go runtime and its
/// registered handlers outlive the handle and keep running until the process
/// exits or [`stop`](Llingr::stop) is called. Call `stop()` for an orderly
/// drain; see the crate-level "Exit and lifecycle hygiene" section.
pub struct Llingr {
    _private: (), // prevent external construction
}

impl Llingr {
    /// A point-in-time view of the consumer's state, parsed into the typed
    /// [`Snapshot`] document: topic summary, sliding throughput windows with
    /// latency figures, per-partition offset tracking with gap-buffer
    /// depths, guard-channel utilisation, and per-shard worker counts.
    ///
    /// Safe to call from any thread, at any frequency reasonable for an
    /// operational endpoint. For proxying the document verbatim on an HTTP
    /// route, use [`snapshot_json`](Llingr::snapshot_json) instead: it
    /// returns the engine's canonical bytes, identical to what the Go
    /// engine's own snapshot handler serves.
    ///
    /// # Errors
    ///
    /// Returns an error if the engine is not initialised, or if the document
    /// does not parse, which indicates an engine/crate version mismatch.
    pub fn snapshot(&self) -> Result<Snapshot, LlingrError> {
        let json = take_snapshot_json()?;
        Snapshot::from_json(&json)
            .map_err(|e| LlingrError::new(-2, format!("snapshot document did not parse: {e}")))
    }

    /// A point-in-time view of the consumer's state, as the canonical JSON
    /// document the Go engine's own snapshot handler serves: one operational
    /// document across both ecosystems, for mounting verbatim on whatever
    /// HTTP stack the application already runs.
    ///
    /// # Errors
    ///
    /// Returns an error if the engine is not initialised.
    pub fn snapshot_json(&self) -> Result<String, LlingrError> {
        take_snapshot_json()
    }

    /// Returns a closure that triggers graceful shutdown when called.
    ///
    /// The closure is `Send` and can be moved to a signal-watcher thread.
    /// The same restrictions as [`stop`](Llingr::stop) apply: never call it
    /// from a signal handler or from inside a message handler.
    pub fn stopper(&self) -> impl Fn() + Send + 'static {
        || unsafe { ffi::llingr_stop() }
    }

    /// Start consuming messages. Blocks until shutdown or error.
    ///
    /// The engine's poll loop and per-key workers run on Go runtime
    /// goroutines, not this thread; the workers call back into the
    /// registered Rust handlers via FFI. This call subscribes, waits for
    /// the initial partition assignment, then parks the calling thread
    /// until the engine shuts down: it returns `Ok(())` only after a
    /// graceful [`stop`](Llingr::stop) completes its drain, or when an
    /// emergency shutdown terminates the consumer.
    ///
    /// # Errors
    ///
    /// Returns an error if the engine was not initialised, if the initial
    /// partition assignment fails or times out, or if the Go consumer
    /// encounters a fatal error.
    pub fn run(&self) -> Result<(), LlingrError> {
        run_result(unsafe { ffi::llingr_run() })
    }

    /// Signal graceful shutdown.
    ///
    /// This is safe to call from any thread. The engine will drain
    /// in-flight messages, commit offsets, and return from [`run`](Llingr::run).
    /// The call that initiates the shutdown blocks until the drain and final
    /// commit complete, so when it returns the process may exit without
    /// losing acknowledged work. A concurrent second `stop()` returns
    /// immediately while the first is still draining.
    ///
    /// `stop()` only stops a **running** engine: a call that lands before
    /// [`run`](Llingr::run) has started consumption is ignored; there is
    /// nothing to stop, and the later `run()` remains fully stoppable. A
    /// shutdown signal that can arrive during startup should be re-checked
    /// once `run()` is underway, or simply exit the process.
    ///
    /// # Never call from inside a handler
    ///
    /// `stop` drains the workers, and handlers run ON those workers: calling
    /// it from inside a process/dead-letter handler asks the engine to drain
    /// the very worker that is blocked in the call. The drain stalls until
    /// the engine gives up on it after the `drain_timeout` setting, 20s by
    /// default, and that message's completion is discarded as orphaned. To shut down
    /// in response to a message, set a flag or send on a channel from the
    /// handler and call `stop()` from another thread.
    ///
    /// Calling `stop()` from the **shutdown** handler is unnecessary and a
    /// harmless no-op: the engine is already shutting down, and the bridge
    /// recognises the re-entrant call and skips a second shutdown rather than
    /// recursing into it. The process/dead-letter restriction above still
    /// applies.
    ///
    /// # Signal handler safety
    ///
    /// **Do NOT call this from a signal handler.** Go code is never
    /// async-signal-safe: calling any Go exported function from a signal
    /// handler context can deadlock or crash the process. The handler sets an
    /// atomic flag; a normal thread calls `stop()`. Every program under
    /// `examples/auth/` contains the working pattern.
    pub fn stop(&self) {
        unsafe { ffi::llingr_stop() }
    }

    /// Signal emergency shutdown: abandon in-flight work and stop now.
    ///
    /// Unlike [`stop`](Llingr::stop), nothing is drained and no final commit
    /// is made: messages in flight are dropped uncommitted and will be
    /// redelivered after a restart, so downstream consumers must tolerate the
    /// resulting duplicates. The registered shutdown handler receives
    /// `reason` exactly once, an empty string becoming a default description,
    /// and a thread parked in [`run`](Llingr::run) returns once the handler
    /// has run and the broker client has been released.
    ///
    /// Safe to call from any thread, in any lifecycle state, repeatedly:
    /// calls after the first effective one are no-ops, and a call that lands
    /// before [`run`](Llingr::run) still stops the engine, so a subsequent
    /// `run()` fails to subscribe. Because nothing is drained, the
    /// [`stop`](Llingr::stop) restriction on calling from inside a
    /// process/dead-letter handler does not apply here: the engine's
    /// emergency path never waits on the worker the handler runs on.
    ///
    /// The signal-handler rule is unchanged: Go code is never
    /// async-signal-safe, so never call this from a signal handler. Relay
    /// through an atomic flag and a normal thread, exactly as documented on
    /// [`stop`](Llingr::stop).
    pub fn emergency_stop(&self, reason: &str) {
        unsafe {
            ffi::llingr_emergency_stop(reason.as_ptr().cast::<c_char>(), reason.len() as c_int)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use llingr_nexus::{DeadLetterHandler, Message, ProcessHandler, Traits};

    struct NoopProcessor;
    impl ProcessHandler for NoopProcessor {
        fn process(&self, _msg: &Message) -> Result<Traits, Box<dyn std::error::Error>> {
            Ok(Traits::none())
        }
    }

    struct NoopDeadLetter;
    impl DeadLetterHandler for NoopDeadLetter {
        fn handle(
            &self,
            _msg: &Message,
            _error_msg: &str,
        ) -> Result<(), Box<dyn std::error::Error>> {
            Ok(())
        }
    }

    /// The exact Display format is a contract with log/alert tooling that
    /// greps "llingr error <code>: <message>".
    #[test]
    fn llingr_error_display_format_and_code() {
        let err = LlingrError::new(-5, "conflicting configuration");
        assert_eq!(
            err.to_string(),
            "llingr error -5: conflicting configuration"
        );
        assert_eq!(err.code(), -5);
    }

    /// Without an initialised engine the snapshot export returns NULL, which
    /// must be reported as a clean error. This test binary never initialises
    /// the engine, so the outcome is deterministic.
    #[test]
    fn take_snapshot_before_init_is_a_clean_error() {
        let err = match take_snapshot_json() {
            Err(e) => e,
            Ok(json) => panic!("expected an error before init, got snapshot: {json}"),
        };
        assert_eq!(err.code(), -1);
        assert!(err.to_string().contains("not initialised"), "{err}");
    }

    /// Before init there is no engine instance; the emergency-stop export
    /// must return cleanly rather than crash the host. Exercises the real
    /// library through the real FFI, like the snapshot test above.
    #[test]
    fn emergency_stop_before_init_is_a_no_op() {
        let reason = "too early";
        unsafe {
            ffi::llingr_emergency_stop(reason.as_ptr().cast::<c_char>(), reason.len() as c_int)
        };
    }

    /// A deferred config error, here the typed-vs-string security conflict
    /// on Options, fails build() cleanly BEFORE any global state is touched:
    /// no handler publication, no init guard. It therefore cannot brick the
    /// process or interfere with other tests in this binary, and a retry is
    /// possible.
    #[test]
    fn build_surfaces_deferred_config_error() {
        for _ in 0..2 {
            let err = match Builder::new("t", NoopProcessor, NoopDeadLetter)
                .brokers("b:9092")
                .consumer_group("g")
                .options(
                    Options::new()
                        .sasl_plain("u", "p")
                        .kafka_option("sasl.password", "other"),
                )
                .build()
            {
                Err(e) => e,
                Ok(_) => panic!("deferred config error must fail build()"),
            };
            assert_eq!(err.code(), -5);
            assert!(err.to_string().contains("kafka_option"), "{err}");
        }
    }

    /// An unbindable scrape endpoint fails build() with a clean error, code
    /// -6, naming the address, BEFORE handler publication and before the
    /// bridge is initialised, and the failure is retryable. This runs
    /// through the real ABI check against the linked engine on the way.
    #[test]
    fn metrics_bind_failure_is_a_clean_error() {
        for _ in 0..2 {
            // Port 1 requires privileges; binding it as a normal user fails.
            let err = match Builder::new("t", NoopProcessor, NoopDeadLetter)
                .brokers("b:9092")
                .consumer_group("g")
                .metrics(Metrics::serve("127.0.0.1:1", "/metrics"))
                .build()
            {
                Err(e) => e,
                Ok(_) => panic!("binding port 1 must fail build()"),
            };
            assert_eq!(err.code(), -6);
            assert!(err.to_string().contains("127.0.0.1:1"), "{err}");
        }
    }
}
