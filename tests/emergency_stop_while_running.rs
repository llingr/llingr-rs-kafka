// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

//! A process handler that loses its downstream calls an emergency stop through
//! `Arc<dyn EmergencyStop>` while the engine is consuming: the call returns,
//! run() returns Ok promptly, the shutdown handler is notified once, and no
//! handler runs after run() has returned. The keys processed are saved for
//! emergency_stop_redelivery, which `make test-broker` runs next.

mod common;

use std::error::Error;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use common::{ShutdownLog, TestBroker, PROMPTLY};
use llingr_kafka::{EmergencyStop, Llingr, Message, ProcessHandler, Traits};

// Shared with emergency_stop_redelivery.rs
const GROUP: &str = "emergency-stop-redelivery";
const TRIGGER_AT: usize = 50;
const REASON: &str = "orders database unreachable";

#[derive(Default)]
struct State {
    seen: Mutex<Vec<String>>,
    stopper: OnceLock<Arc<dyn EmergencyStop>>,
    // Set when the emergency_stop call returns, so only if it does
    stopped_at: OnceLock<Instant>,
}

struct Orders(Arc<State>);

impl ProcessHandler for Orders {
    fn process(&self, msg: &Message) -> Result<Traits, Box<dyn Error>> {
        let key = msg
            .key_str()
            .expect("every record has a UTF-8 key")
            .to_string();
        let seen = {
            let mut seen = self.0.seen.lock().unwrap();
            seen.push(key);
            seen.len()
        };
        if seen == TRIGGER_AT {
            self.0
                .stopper
                .get()
                .expect("stopper set before run")
                .emergency_stop(REASON);
            self.0.stopped_at.set(Instant::now()).unwrap();
        }
        Ok(Traits::none())
    }
}

#[test]
#[ignore = "needs a broker: make test-broker"]
fn emergency_stop_from_a_process_handler_stops_a_running_engine() {
    let log = ShutdownLog::default();
    let state = Arc::new(State::default());
    let broker = TestBroker::from_env();
    let engine = broker
        .engine(GROUP, Orders(Arc::clone(&state)), &log)
        .build();
    let engine: Arc<Llingr> = Arc::new(engine.expect("engine builds"));
    let stopper: Arc<dyn EmergencyStop> = engine.clone();
    state.stopper.set(stopper).ok().unwrap();

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let runner = Arc::clone(&engine);
    std::thread::spawn(move || done_tx.send(runner.run()).unwrap());

    let result = done_rx
        .recv_timeout(Duration::from_secs(60))
        .expect("run() still blocked 60s after starting: the emergency stop did not end it");
    let seen_at_return = state.seen.lock().unwrap().len();
    result.expect("run() returns Ok after an emergency shutdown");

    let stopped_at = state
        .stopped_at
        .get()
        .expect("emergency_stop called from the process handler never returned");
    assert!(
        stopped_at.elapsed() < PROMPTLY,
        "run() returned {:?} after the call",
        stopped_at.elapsed()
    );
    // exactly_one() waits SETTLE, time for a late handler to show up in seen
    let notified = log.exactly_one();
    assert!(
        notified.ends_with(&format!("reason: {REASON}")),
        "{notified}"
    );

    let seen = state.seen.lock().unwrap().clone();
    assert_eq!(
        seen.len(),
        seen_at_return,
        "a handler ran after run() returned"
    );
    assert!(
        seen.len() < common::RECORDS,
        "consumed everything: the stop came too late"
    );

    std::fs::write(broker.state_dir.join("seen"), seen.join("\n")).unwrap();
}
