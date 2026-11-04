// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

//! An empty reason still stops the engine, and the shutdown handler is told
//! the host asked for it rather than receiving an empty description.

mod common;

use std::sync::Arc;

use common::{Noop, ShutdownLog, TestBroker};
use llingr_kafka::EmergencyStop;

#[test]
#[ignore = "needs a broker: make test-broker"]
fn empty_reason_gets_the_default_description() {
    let log = ShutdownLog::default();
    let broker = TestBroker::from_env();
    let engine = broker
        .engine("emergency-stop-empty-reason", Noop, &log)
        .build();
    let stopper: Arc<dyn EmergencyStop> = Arc::new(engine.expect("engine builds"));

    stopper.emergency_stop("");

    let notified = log.exactly_one();
    assert!(
        notified.ends_with("reason: emergency stop requested by host"),
        "{notified}"
    );
}
