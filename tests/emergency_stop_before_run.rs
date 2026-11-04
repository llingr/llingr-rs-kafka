// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

//! An emergency stop through the llingr-nexus trait on a built engine that has
//! not run yet stops it: the shutdown handler is notified once, with the reason
//! intact inside the engine's description, later calls change nothing, and
//! run() refuses to consume.

mod common;

use std::sync::Arc;
use std::time::Instant;

use common::{Noop, ShutdownLog, TestBroker, PROMPTLY, SETTLE};
use llingr_kafka::{EmergencyStop, Llingr};

#[test]
#[ignore = "needs a broker: make test-broker"]
fn emergency_stop_before_run_stops_the_engine() {
    let log = ShutdownLog::default();
    let broker = TestBroker::from_env();
    let engine = broker
        .engine("emergency-stop-before-run", Noop, &log)
        .build();
    let engine: Arc<Llingr> = Arc::new(engine.expect("engine builds"));
    let stopper: Arc<dyn EmergencyStop> = engine.clone();

    // Non-ASCII and quotes: the reason crosses the FFI boundary twice
    let reason = "disk ✗ full: \"/var/lib/orders\"";
    stopper.emergency_stop(reason);
    let notified = log.exactly_one();
    assert!(
        notified.ends_with(&format!("reason: {reason}")),
        "{notified}"
    );

    // Neither path to the engine notifies again
    stopper.emergency_stop("second call through the trait");
    engine.emergency_stop("second call through the inherent method");
    assert_eq!(log.wait_for(2, SETTLE), vec![notified]);

    let started = Instant::now();
    let err = engine.run().expect_err("a stopped engine must not consume");
    assert_eq!(err.code(), -2, "{err}");
    assert!(
        started.elapsed() < PROMPTLY,
        "run() took {:?}: refused only after an assignment timeout, not because it was stopped",
        started.elapsed()
    );
}
