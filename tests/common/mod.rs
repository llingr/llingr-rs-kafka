// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

//! Shared by the tests/emergency_stop_*.rs programs. Each program is its own
//! process because the engine is process-global: one build() per process. They
//! need a broker, since build() connects to it, so each test is #[ignore]d and
//! `make test-broker` runs them against a throwaway Redpanda.

#![allow(dead_code)] // each program uses a different subset

use std::error::Error;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use llingr_kafka::{Builder, DeadLetterHandler, Message, ProcessHandler, ShutdownHandler, Traits};

/// How long a test waits for something the engine should do promptly
pub const PROMPTLY: Duration = Duration::from_secs(10);

/// How long a test waits for a duplicate that must never arrive
pub const SETTLE: Duration = Duration::from_millis(500);

/// Records in the topic `make test-broker` creates, keyed `key-0` upwards
pub const RECORDS: usize = 200;

/// Every record key in the topic `make test-broker` creates
pub fn all_keys() -> std::collections::BTreeSet<String> {
    (0..RECORDS).map(|i| format!("key-{i}")).collect()
}

/// The broker and topic `make test-broker` provides, and a directory that
/// carries state from one program to the next
pub struct TestBroker {
    pub brokers: String,
    pub topic: String,
    pub state_dir: std::path::PathBuf,
}

impl TestBroker {
    pub fn from_env() -> TestBroker {
        let var = |name: &str| {
            std::env::var(name)
                .unwrap_or_else(|_| panic!("{name} is not set: run `make test-broker`"))
        };
        TestBroker {
            brokers: var("LLINGR_TEST_BROKERS"),
            topic: var("LLINGR_TEST_TOPIC"),
            state_dir: var("LLINGR_TEST_STATE").into(),
        }
    }

    /// An engine for this broker's topic that reports its shutdown to `log`
    pub fn engine(&self, group: &str, process: impl ProcessHandler, log: &ShutdownLog) -> Builder {
        Builder::new(&self.topic, process, Noop)
            .brokers(&self.brokers)
            .consumer_group(group)
            .shutdown(log.clone())
    }
}

/// Processes and dead-letters nothing
pub struct Noop;

impl ProcessHandler for Noop {
    fn process(&self, _msg: &Message) -> Result<Traits, Box<dyn Error>> {
        Ok(Traits::none())
    }
}

impl DeadLetterHandler for Noop {
    fn handle(&self, _msg: &Message, _error_msg: &str) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}

/// Records every shutdown notification, in order
#[derive(Clone, Default)]
pub struct ShutdownLog(Arc<(Mutex<Vec<String>>, Condvar)>);

impl ShutdownLog {
    /// Every reason received once `n` have arrived, or whatever arrived by
    /// `timeout`
    pub fn wait_for(&self, n: usize, timeout: Duration) -> Vec<String> {
        let (reasons, arrived) = &*self.0;
        let deadline = Instant::now() + timeout;
        let mut reasons = reasons.lock().unwrap();
        while reasons.len() < n {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break;
            }
            reasons = arrived.wait_timeout(reasons, left).unwrap().0;
        }
        reasons.clone()
    }

    /// The one reason received, after waiting [`SETTLE`] for a second
    /// notification that would break the exactly-once contract
    pub fn exactly_one(&self) -> String {
        let first = self.wait_for(1, PROMPTLY);
        assert_eq!(
            first.len(),
            1,
            "no shutdown notification within {PROMPTLY:?}"
        );
        std::thread::sleep(SETTLE);
        let all = self.wait_for(usize::MAX, Duration::ZERO);
        assert_eq!(all.len(), 1, "shutdown handler ran more than once: {all:?}");
        all.into_iter().next().unwrap()
    }
}

impl ShutdownHandler for ShutdownLog {
    fn handle(&self, reason: &str) {
        let (reasons, arrived) = &*self.0;
        reasons.lock().unwrap().push(reason.to_string());
        arrived.notify_all();
    }
}
