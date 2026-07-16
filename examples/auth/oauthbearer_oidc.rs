// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial
//
// SASL/OAUTHBEARER (OIDC): requires an OIDC identity provider and a broker
// configured for OAUTHBEARER.
//
// The engine fetches a token from the OIDC token endpoint with the client
// id/secret, caches it, and refreshes it before expiry. OAUTHBEARER is
// permitted over sasl_plaintext for test clusters, but a bearer token should
// never travel unencrypted, so production configurations add a tls_* setter -
// in this example: Options::new().tls_ca_location(&ca)...
//
//   KAFKA_BROKERS=host:9092 KAFKA_TOPIC=orders KAFKA_TLS_CA=/path/ca.pem \
//   OAUTH_TOKEN_ENDPOINT=https://idp.example.com/oauth2/token \
//   OAUTH_CLIENT_ID=svc-orders OAUTH_CLIENT_SECRET=... \
//   cargo run --example auth_oauthbearer_oidc

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
    let token_endpoint = std::env::var("OAUTH_TOKEN_ENDPOINT")?;
    let client_id = std::env::var("OAUTH_CLIENT_ID")?;
    let client_secret = std::env::var("OAUTH_CLIENT_SECRET")?;

    // OAUTHBEARER does not enable TLS on its own; add a tls_* setter so the token
    // is never sent in the clear.
    let mut options = Options::new().tls_ca_location(&ca).sasl_oauthbearer_oidc(
        &token_endpoint,
        &client_id,
        &client_secret,
    );
    // optional: request a scope/audience from the token endpoint.
    if let Ok(scope) = std::env::var("OAUTH_SCOPE") {
        options = options.oauthbearer_scope(&scope);
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
