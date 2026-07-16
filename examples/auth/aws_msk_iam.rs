// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial
//
// AWS_MSK_IAM: requires an Amazon MSK cluster with IAM authentication enabled
// and credentials available to the provider chain.
//
// Options::new().sasl_aws_msk_iam() selects the mechanism and enables TLS which
// MSK IAM requires. Credentials are resolved in the client using the AWS SDK's
// default provider chain: environment, shared config/profile, STS assume-role,
// web identity/IRSA, EC2 instance metadata.
//
//   KAFKA_BROKERS=b-1.cluster.xxx.kafka.eu-west-1.amazonaws.com:9098 \
//   KAFKA_TOPIC=orders AWS_REGION=eu-west-1 cargo run --example auth_aws_msk_iam
//
// AWS_REGION nuance: aws_region sets the credential/STS region. The SigV4
// signing region is parsed from the broker hostname. MSK hostnames embed the
// region; AWS_REGION is the fallback for other hostnames.

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
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9098".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "orders".into());
    let group = std::env::var("KAFKA_GROUP").unwrap_or_else(|_| "orders-example".into());
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "eu-west-1".into());

    // The mechanism enables TLS on its own; credentials come from the provider chain.
    let mut options = Options::new().sasl_aws_msk_iam().aws_region(&region);

    // Optional: assume an IAM role on top of the base chain.
    if let Ok(role_arn) = std::env::var("AWS_ROLE_ARN") {
        let session = std::env::var("AWS_ROLE_SESSION_NAME").ok();
        options = options.aws_assume_role(&role_arn, session.as_deref());
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
