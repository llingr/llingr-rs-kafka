// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial
//
// Mutual TLS (mTLS) authentication. The broker CA must be present in issued
// client certificates/keys.
//
// Brokers must be configured with an ssl listener that requires client certificates.
//
//   KAFKA_BROKERS=host:9092 KAFKA_TOPIC=orders KAFKA_TLS_CA=/path/ca.pem \
//   KAFKA_TLS_CLIENT_CERT=/path/client.pem KAFKA_TLS_CLIENT_KEY=/path/client.key \
//   cargo run --example auth_mtls
//
// For inline PEM strings instead of file paths, use:
// Options::tls_client_certificate_pem(certificate_pem, key_pem)

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
    let ca =
        std::env::var("KAFKA_TLS_CA").unwrap_or_else(|_| "/etc/ssl/certs/cluster-ca.pem".into());
    let client_cert = std::env::var("KAFKA_TLS_CLIENT_CERT")?;
    let client_key = std::env::var("KAFKA_TLS_CLIENT_KEY")?;

    // the client certificate is the credential; there is no SASL setter here.
    let options = Options::new()
        .tls_ca_location(&ca)
        .tls_client_certificate(&client_cert, &client_key);

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
