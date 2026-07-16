// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

//! The llingr concurrent message processing engine with a Kafka broker,
//! batteries included.
//!
//! One pre-baked crate: the `llingr-demux` Go engine and its pure-Go Kafka
//! broker layer (franz-go, speaks Kafka and all Kafka-compatible brokers
//! such as RedPanda and Amazon MSK) compile into your binary as a static
//! c-archive during `cargo build`. Per-key concurrent processing without
//! head-of-line blocking, contiguous offset commit guarantees, engine logs
//! on the process-global [`log`] facade under the target [`LOG_TARGET`],
//! and baked-in Prometheus metrics activated at runtime with [`Metrics`].
//! There are no cargo features and no broker selection.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use llingr_kafka::{AutoOffsetReset, Builder, Message, Metrics, Options, Traits};
//! use llingr_kafka::{DeadLetterHandler, ProcessHandler};
//!
//! struct MyProcessor;
//!
//! impl ProcessHandler for MyProcessor {
//!     fn process(&self, msg: &Message) -> Result<Traits, Box<dyn std::error::Error>> {
//!         // Keys are frequently PII: log coordinates, never key contents.
//!         println!("partition={} offset={}", msg.partition(), msg.offset());
//!         Ok(Traits::none())
//!     }
//! }
//!
//! // Required alongside the processor: failed messages need somewhere to go.
//! // Logging is the bare minimum; a real DLQ (topic, table) is recommended.
//! struct MyDeadLetters;
//!
//! impl DeadLetterHandler for MyDeadLetters {
//!     fn handle(&self, msg: &Message, error_msg: &str) -> Result<(), Box<dyn std::error::Error>> {
//!         eprintln!("dead-letter partition={} offset={} reason={}", msg.partition(), msg.offset(), error_msg);
//!         Ok(())
//!     }
//! }
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let engine = Builder::new("orders", MyProcessor, MyDeadLetters)
//!         .brokers("broker1:9092,broker2:9092")
//!         .consumer_group("orders-svc")
//!         .options(Options::new().auto_offset_reset(AutoOffsetReset::Earliest))
//!         .metrics(Metrics::serve("0.0.0.0:9464", "/metrics"))
//!         .build()?;
//!
//!     let stop = engine.stopper(); // Send closure for signal-watcher threads
//!     # drop(stop);
//!     engine.run()?; // BLOCKS until stop() or an emergency shutdown
//!     Ok(())
//! }
//! ```
//!
//! # Operational constraints
//!
//! Hosting the Go runtime in-process has hard rules. Read these before
//! shipping.
//!
//! - **One engine per process.** The Go runtime is a process-global
//!   singleton. A second [`Builder::build`] returns a clean error.
//!
//! - **The Go runtime initialises at process start.** The statically linked
//!   engine registers a loader initialiser: the Go runtime (and its signal
//!   handlers for SIGSEGV, SIGBUS, SIGFPE, SIGPROF, and SIGURG) comes up
//!   before `main()` runs, not at `build()`. The engine never claims SIGINT
//!   or SIGTERM, so registering handlers for those (for example with
//!   `signal-hook`, which chains to any pre-existing handler) works at any
//!   point. Do not install non-chaining handlers for the fault signals, and
//!   anything touching them must use `SA_ONSTACK`.
//!
//! - **Do NOT call stop() from a signal handler.** Go code is never
//!   async-signal-safe. Use `signal_hook::flag` to set an atomic flag, then
//!   poll it from a normal thread and call [`stop`](Llingr::stop).
//!
//! - **Panics.** Rust panics in handlers are caught at the FFI boundary and
//!   converted to error codes; a panic in the process handler routes the
//!   message to the dead-letter handler with the reason "panic in process
//!   callback". This protection relies on the host being built with
//!   `panic = "unwind"` (the default): under `panic = "abort"`, the first
//!   handler panic aborts the process (the crate's build script warns
//!   loudly when a profile does this). Go panics in engine internals abort
//!   the entire process with no recovery possible.
//!
//! - **Thread budget.** Handlers run on Go runtime threads, and a blocking
//!   handler pins one for its duration. GOMAXPROCS and GODEBUG must be set
//!   as environment variables before process start; setting them from Rust
//!   code has no effect. If you also run tokio or rayon, budget threads
//!   explicitly.
//!
//! - **cargo test.** All tests within a single test binary share one Go
//!   runtime instance. Use `--test-threads=1` or a serialisation crate if
//!   tests depend on engine state.
//!
//! - **musl/Alpine is not supported yet** (upstream Go limitation; the build
//!   script fails with the full explanation and links). Use glibc images
//!   (Debian, Ubuntu); a scratch image works because the binary is static.
//!
//! # Exit and lifecycle hygiene
//!
//! - **Borrowed message data is callback-scoped, and retaining it is worse
//!   than a use-after-free.** The `key`/`value` slices point into a broker
//!   record buffer and a pooled engine work item, both recycled after the
//!   callback returns. A stored slice will later read *a different message's
//!   bytes* (silent wrong data), not necessarily crash. Copy out anything you
//!   need to keep (`to_vec()`, `to_string()`).
//!
//! - **Shutting down: call [`stop`](Llingr::stop), don't just exit.** The
//!   call that initiates shutdown drains in-flight work, commits offsets,
//!   and returns from [`run`](Llingr::run). `std::process::exit` skips the
//!   drain; this is *safe* (uncommitted offsets are redelivered at least
//!   once on restart) but wasteful. Never call `stop()` from inside a
//!   process or dead-letter handler; signal another thread.
//!
//! - **[`emergency_stop`](Llingr::emergency_stop) abandons in-flight work**:
//!   no drain, no final commit, duplicates on restart are expected (the
//!   engine's contract is at-least-once). The shutdown handler receives the
//!   reason exactly once, on graceful and emergency exits alike.
//!
//! - **Liveness is your responsibility.** If a handler wedges, nothing
//!   crashes: `run()` simply blocks. The engine's own resilience covers the
//!   broker side (sustained partition poll failure triggers an emergency
//!   shutdown with the reason after a bail window, ten minutes by default).
//!
//! # Architecture
//!
//! ```text
//! Kafka / RedPanda / MSK
//!     |
//! Go engine, statically linked (libllingr.a)
//!     |  franz-go broker client -> llingr-demux pipeline
//!     |
//! C FFI boundary (ABI v1, checked at build())
//!     |
//! This crate: safe wrapper + llingr-nexus contracts
//!     |
//! Your application (ProcessHandler / DeadLetterHandler)
//! ```

#![deny(missing_docs)]

mod config;
mod engine;
mod ffi;
mod logging;
mod metrics;
mod options;
pub mod snapshot;
mod trampolines;

// Coverage-focused unit tests over pub(crate) internals, kept in their own
// files so the landed test modules stay byte-unmodified (the additive test
// discipline).
#[cfg(test)]
mod engine_coverage_tests;
#[cfg(test)]
mod trampolines_coverage_tests;

pub use config::DemuxConfig;
pub use engine::{Builder, Llingr, LlingrError};
pub use logging::LOG_TARGET;
pub use metrics::{Metrics, MetricsHandle, OPENMETRICS_CONTENT_TYPE};
pub use options::{BalanceStrategy, ClientLogLevel, Options};
pub use snapshot::Snapshot;

// The contract vocabulary lives in, and is versioned by, the published
// `llingr-nexus` crate; it is re-exported here so one `use llingr_kafka::...`
// suffices. Deliberately NOT re-exported: the per-message metrics packet
// type (nexus `Metrics`; this crate's Prometheus integration consumes it
// internally, and the root name `Metrics` belongs to the activation type),
// its `MetricsHandler` trait and the bandwidth equivalents, and the logger
// plumbing (engine logs always flow to the `log` facade).
pub use llingr_nexus::{
    AutoOffsetReset, DeadLetterHandler, Header, Headers, Message, ProcessHandler, ShutdownHandler,
    Timestamp, Traits,
};
