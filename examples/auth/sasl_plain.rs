// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial
//
// SASL/PLAIN: requires a broker that accepts SASL/PLAIN over TLS with a
// publicly trusted certificate, for example Confluent Cloud.
//
// The API key is the username and the API secret is the password, so
// Options::new().tls().sasl_plain(&key, &secret) is the whole configuration
// (tls() trusts the system roots).
//
//   KAFKA_BROKERS=pkc-xxx.eu-west-1.aws.confluent.cloud:9092 KAFKA_TOPIC=orders \
//   KAFKA_API_KEY=... KAFKA_API_SECRET=... \
//   cargo run --example auth_sasl_plain

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
    // Confluent Cloud: API key = username, API secret = password.
    let key = std::env::var("KAFKA_API_KEY")?;
    let secret = std::env::var("KAFKA_API_SECRET")?;

    // tls() trusts the system roots (a public CA); sasl_plain adds the credential.
    let options = Options::new().tls().sasl_plain(&key, &secret);

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
