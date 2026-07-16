// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial
//
// SASL/SCRAM-SHA-256 over TLS (sasl_ssl). The SASL setter plus a tls_* setter
// is the whole configuration; the typed setters compute
// security.protocol=sasl_ssl from the combination.
//
// Brokers must be configured with a SASL/SCRAM sasl_ssl listener. The
// examples/e2e compose stack runs this same configuration end to end.
//
//   KAFKA_BROKERS=host:9092 KAFKA_TOPIC=orders KAFKA_SASL_USERNAME=svc-orders \
//   KAFKA_SASL_PASSWORD=... KAFKA_TLS_CA=/path/ca.pem \
//   cargo run --example auth_scram_sha256
//
// For the SHA-512 variant, use:
// Options::sasl_scram_sha512(username, password)

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use llingr_kafka::{Builder, DeadLetterHandler, Message, Options, ProcessHandler, Traits};

struct Processor;
impl ProcessHandler for Processor {
    fn process(&self, msg: &Message) -> Result<Traits, Box<dyn std::error::Error>> {
        println!(
            "processed partition={} offset={}",
            msg.partition(),
            msg.offset()
        );
        Ok(Traits::none())
    }
}

struct DeadLetters;
impl DeadLetterHandler for DeadLetters {
    fn handle(&self, msg: &Message, error: &str) -> Result<(), Box<dyn std::error::Error>> {
        eprintln!(
            "dead letter partition={} offset={} error={error}",
            msg.partition(),
            msg.offset()
        );
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "orders".into());
    let group = std::env::var("KAFKA_GROUP").unwrap_or_else(|_| "orders-example".into());
    let username = std::env::var("KAFKA_SASL_USERNAME").unwrap_or_else(|_| "svc-orders".into());
    let password = std::env::var("KAFKA_SASL_PASSWORD")?;
    let ca =
        std::env::var("KAFKA_TLS_CA").unwrap_or_else(|_| "/etc/ssl/certs/cluster-ca.pem".into());

    let options = Options::new()
        .tls_ca_location(&ca)
        .sasl_scram_sha256(&username, &password);

    let engine = Builder::new(&topic, Processor, DeadLetters)
        .brokers(&brokers)
        .consumer_group(&group)
        .options(options)
        .build()?;

    // OS interrupt safety: flag in the handler, stop() on a normal thread.
    let stop = engine.stopper();
    let flag = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGINT, flag.clone())?;
    signal_hook::flag::register(signal_hook::consts::SIGTERM, flag.clone())?;
    std::thread::spawn(move || {
        while !flag.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(100));
        }
        stop();
    });

    engine.run()?;
    Ok(())
}
