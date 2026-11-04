// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

//! Emergency stops racing from many threads through one shared
//! `Arc<dyn EmergencyStop>` notify the shutdown handler exactly once, with one
//! of the racing reasons.

mod common;

use std::sync::{Arc, Barrier};

use common::{Noop, ShutdownLog, TestBroker};
use llingr_kafka::{EmergencyStop, Llingr};

const THREADS: usize = 16;

#[test]
#[ignore = "needs a broker: make test-broker"]
fn racing_emergency_stops_notify_once() {
    let log = ShutdownLog::default();
    let broker = TestBroker::from_env();
    let engine = broker
        .engine("emergency-stop-concurrent", Noop, &log)
        .build();
    let engine: Arc<Llingr> = Arc::new(engine.expect("engine builds"));
    let stopper: Arc<dyn EmergencyStop> = engine.clone();

    let start = Arc::new(Barrier::new(THREADS));
    let racers: Vec<_> = (0..THREADS)
        .map(|i| {
            let (stopper, start) = (Arc::clone(&stopper), Arc::clone(&start));
            std::thread::spawn(move || {
                start.wait();
                stopper.emergency_stop(&format!("racer {i}"));
            })
        })
        .collect();
    for racer in racers {
        racer.join().expect("emergency_stop must not panic");
    }

    let notified = log.exactly_one();
    assert!(
        (0..THREADS).any(|i| notified.ends_with(&format!("reason: racer {i}"))),
        "{notified}"
    );
    assert_eq!(
        engine
            .run()
            .expect_err("a stopped engine must not consume")
            .code(),
        -2
    );
}
