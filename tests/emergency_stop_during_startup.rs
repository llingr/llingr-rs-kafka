// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

//! An emergency stop that lands while run() is still subscribing, before the
//! group has assigned partitions, ends run() promptly and notifies once. The
//! assignment wait is raised well above PROMPTLY, so run() returning cannot be
//! an assignment timeout.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{Noop, ShutdownLog, TestBroker, PROMPTLY};
use llingr_kafka::{DemuxConfig, EmergencyStop, Llingr};

#[test]
#[ignore = "needs a broker: make test-broker"]
fn emergency_stop_during_subscribe_ends_run() {
    let log = ShutdownLog::default();
    let broker = TestBroker::from_env();
    let engine = broker
        .engine("emergency-stop-during-startup", Noop, &log)
        .demux(DemuxConfig::new().await_assignments_timeout(Duration::from_secs(120)))
        .build();
    let engine: Arc<Llingr> = Arc::new(engine.expect("engine builds"));
    let stopper: Arc<dyn EmergencyStop> = engine.clone();

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let runner = Arc::clone(&engine);
    std::thread::spawn(move || done_tx.send(runner.run()).unwrap());

    // A group join takes seconds; this lands inside it
    std::thread::sleep(Duration::from_millis(50));
    let stopped_at = Instant::now();
    stopper.emergency_stop("config service unreachable");

    let result = done_rx.recv_timeout(PROMPTLY).unwrap_or_else(|_| {
        panic!("run() still blocked {PROMPTLY:?} after an emergency stop during subscribe")
    });
    // Either the subscribe is refused, or it completes and run() then
    // returns for the emergency exit; never a hang or another error
    if let Err(err) = result {
        assert_eq!(err.code(), -2, "{err}");
    }
    assert!(stopped_at.elapsed() < PROMPTLY);
    assert!(log
        .exactly_one()
        .ends_with("reason: config service unreachable"));
}
