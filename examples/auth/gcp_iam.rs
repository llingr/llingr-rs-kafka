// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial
//
// GCP IAM: requires a Google Cloud Managed Service for Apache Kafka cluster and
// Application Default Credentials holding managedkafka.clusters.connect.
//
// Options::new().sasl_gcp_iam() selects OAUTHBEARER with the GCP token method
// and enables TLS which Google requires. Credentials are resolved in the client
// using Application Default Credentials: the GOOGLE_APPLICATION_CREDENTIALS key
// file, a gcloud user login, or the GCE/GKE metadata server. No CA setter is
// needed: the listener uses a publicly trusted certificate.
//
//   KAFKA_BROKERS=bootstrap.CLUSTER.REGION.managedkafka.PROJECT.cloud.goog:9092 \
//   KAFKA_TOPIC=orders GOOGLE_APPLICATION_CREDENTIALS=/path/sa-key.json \
//   cargo run --example auth_gcp_iam
//
// Principal nuance: the identity is sent to the broker using:
//     options.gcp_principal(&principal)
//  or the GOOGLE_MANAGED_KAFKA_AUTH_PRINCIPAL environment variable
//  or the service-account email.

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

    // The mechanism enables TLS on its own; credentials come from ADC. By
    // default that is the whole configuration.
    let mut options = Options::new().sasl_gcp_iam();
    // optional: an explicit service-account key file instead of the ADC search.
    if let Ok(key_file) = std::env::var("GCP_CREDENTIALS_FILE") {
        options = options.gcp_credentials_file(&key_file);
    }
    // optional: override the principal (otherwise it falls back to
    // GOOGLE_MANAGED_KAFKA_AUTH_PRINCIPAL and then the service-account email).
    if let Ok(principal) = std::env::var("GCP_PRINCIPAL") {
        options = options.gcp_principal(&principal);
    }

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
