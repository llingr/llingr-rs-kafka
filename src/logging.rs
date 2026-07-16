// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

//! Routes engine log lines into the process-global [`log`] facade.
//!
//! There is deliberately no logger parameter anywhere in this crate's API:
//! the `log` facade is process-global, the application installs env_logger,
//! fern, tracing-log or any other implementation itself, and the engine's
//! lines flow through it under the target [`LOG_TARGET`] with zero wiring.
//! Applications filter or re-route them by target, for example
//! `RUST_LOG=llingr=debug`; `tracing` users get everything via `tracing-log`.

use llingr_nexus::{LogHandler, LogLevel};

/// Log target used for all engine lines, so applications can filter or
/// re-route them (e.g. `RUST_LOG=llingr=debug`).
pub const LOG_TARGET: &str = "llingr";

/// The always-installed [`LogHandler`]: forwards each engine line to the
/// `log` crate macros under [`LOG_TARGET`], mapping the engine's
/// debug/info/warn/error levels one to one.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct LogRouter;

impl LogRouter {
    /// Create the stateless router.
    pub(crate) fn new() -> Self {
        Self
    }
}

impl LogHandler for LogRouter {
    fn log(&self, level: LogLevel, message: &str) {
        match level {
            LogLevel::Debug => log::debug!(target: LOG_TARGET, "{message}"),
            LogLevel::Info => log::info!(target: LOG_TARGET, "{message}"),
            LogLevel::Warn => log::warn!(target: LOG_TARGET, "{message}"),
            LogLevel::Error => log::error!(target: LOG_TARGET, "{message}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static CAPTURED: Mutex<Vec<(log::Level, String, String)>> = Mutex::new(Vec::new());

    struct CapturingLogger;
    impl log::Log for CapturingLogger {
        fn enabled(&self, _: &log::Metadata) -> bool {
            true
        }
        fn log(&self, record: &log::Record) {
            CAPTURED.lock().unwrap().push((
                record.level(),
                record.target().to_string(),
                record.args().to_string(),
            ));
        }
        fn flush(&self) {}
    }

    #[test]
    fn forwards_levels_and_target() {
        log::set_logger(&CapturingLogger).expect("install test logger");
        log::set_max_level(log::LevelFilter::Trace);

        let router = LogRouter::new();
        router.log(LogLevel::Debug, "d");
        router.log(LogLevel::Info, "i");
        router.log(LogLevel::Warn, "w");
        router.log(LogLevel::Error, "e");

        let captured = CAPTURED.lock().unwrap();
        let expect = [
            (log::Level::Debug, "d"),
            (log::Level::Info, "i"),
            (log::Level::Warn, "w"),
            (log::Level::Error, "e"),
        ];
        for (level, message) in expect {
            assert!(
                captured
                    .iter()
                    .any(|(l, t, m)| *l == level && t == LOG_TARGET && m == message),
                "missing {level} {message}"
            );
        }
    }
}
