// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

//! After emergency_stop_while_running, the same consumer group resumes and
//! every record is processed by one run or the other: whatever the emergency
//! stop left uncommitted is delivered again, and nothing it committed was
//! skipped. Run by `make test-broker` after that program.

mod common;

use std::collections::BTreeSet;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{all_keys, ShutdownLog, TestBroker};
use llingr_kafka::{Message, ProcessHandler, Traits};

// Shared with emergency_stop_while_running.rs
const GROUP: &str = "emergency-stop-redelivery";

struct Record(Arc<Mutex<BTreeSet<String>>>);

impl ProcessHandler for Record {
    fn process(&self, msg: &Message) -> Result<Traits, Box<dyn Error>> {
        let key = msg
            .key_str()
            .expect("every record has a UTF-8 key")
            .to_string();
        self.0.lock().unwrap().insert(key);
        Ok(Traits::none())
    }
}

#[test]
#[ignore = "needs a broker: make test-broker, after emergency_stop_while_running"]
fn nothing_is_lost_across_an_emergency_stop() {
    let broker = TestBroker::from_env();
    let seen = std::fs::read_to_string(broker.state_dir.join("seen"))
        .unwrap_or_else(|e| panic!("seen: {e}: run emergency_stop_while_running first"));
    let before: BTreeSet<String> = seen.lines().map(str::to_string).collect();
    assert!(
        !before.is_empty() && before != all_keys(),
        "run 1 must have stopped part way"
    );

    let log = ShutdownLog::default();
    let resumed = Arc::new(Mutex::new(BTreeSet::new()));
    let engine = broker
        .engine(GROUP, Record(Arc::clone(&resumed)), &log)
        .build();
    let engine = Arc::new(engine.expect("engine builds"));

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let runner = Arc::clone(&engine);
    std::thread::spawn(move || done_tx.send(runner.run()).unwrap());

    // Stop gracefully, from this thread, once every record has been processed
    // by one run or the other
    let missing = || -> Vec<String> {
        let resumed = resumed.lock().unwrap();
        all_keys()
            .into_iter()
            .filter(|k| !before.contains(k) && !resumed.contains(k))
            .collect()
    };
    let deadline = Instant::now() + Duration::from_secs(60);
    while !missing().is_empty() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    engine.stop();
    done_rx
        .recv_timeout(common::PROMPTLY)
        .expect("run() did not return after stop()")
        .expect("graceful run");

    assert_eq!(
        missing(),
        Vec::<String>::new(),
        "never processed by either run"
    );
    assert!(
        !resumed.lock().unwrap().is_empty(),
        "the resumed run processed nothing"
    );
    assert!(log.exactly_one().contains("graceful"));
}
