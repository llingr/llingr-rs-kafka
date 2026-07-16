// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial
//
// Example producer for the llingr-kafka end-to-end proof. It publishes a fixed
// run of order events to the "orders" topic, which the llingr-kafka consumer
// then processes; between them they exercise the whole demux + franz + FFI
// chain against a real broker.
//
// Pure Rust on rskafka: no librdkafka, no C toolchain, no cmake. That is a
// deliberate choice (it keeps the producer image C-free and statically linkable
// against a scratch base), and it shapes the code below in two ways worth
// stating up front, because rskafka does not mirror librdkafka:
//
//   1. rskafka is partition-explicit. A PartitionClient talks to one partition;
//      there is no client-side key partitioner. So this producer computes the
//      partition itself, as a deterministic hash of the record key modulo the
//      partition count. That is sufficient for this example (every orderId is
//      unique, so any deterministic hash spreads the load across partitions);
//      it is NOT the Java default partitioner and makes no claim to match it.
//
//   2. rskafka's produce() resolves on the broker's ProduceResponse: its
//      Ok value is the broker-assigned offsets, which the broker can only
//      return after it has persisted the batch. Verified against rskafka 0.6
//      (docs.rs) and its source: the ProduceRequest hardcodes acks = -1 (all
//      in-sync replicas), and there is no acks / idempotence knob to configure.
//      So awaiting each produce IS awaiting the acknowledgement. This is
//      at-least-once production; no exactly-once or idempotence is claimed.

use std::collections::BTreeMap;
use std::error::Error;
use std::hash::{Hash, Hasher};

use chrono::Utc;
use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::client::ClientBuilder;
use rskafka::record::Record;
use serde::Serialize;
use uuid::Uuid;

/// One order event. The `orderId` is ALSO carried in the body (not only as the
/// record key) so the consumer can assert the key survived the round trip by
/// comparing the two.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Order {
    order_id: String,
    customer_id: String,
    sku: String,
    quantity: u64,
    unit_price_cents: u64,
    currency: String,
    placed_at: String,
}

/// Deterministic partition for a key: hash modulo the partition count. Any
/// deterministic hash is correct here; std's fixed-key hasher keeps it
/// self-contained without pulling in a hashing crate.
fn partition_for(key: &str, partitions: i32) -> i32 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() % partitions as u64) as i32
}

/// Reads a `usize`/`i32`/`u64`-style env var, falling back to `default` when it
/// is unset or unparsable.
fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let brokers: Vec<String> = std::env::var("BROKERS")
        .unwrap_or_else(|_| "redpanda:9092".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let topic = std::env::var("TOPIC").unwrap_or_else(|_| "orders".to_string());
    let partitions: i32 = env_or("PARTITIONS", 12);
    let count: u64 = env_or("COUNT", 1000);

    log::info!(
        "producing {count} messages to topic {topic:?} ({partitions} partitions) via {}",
        brokers.join(",")
    );

    let client = ClientBuilder::new(brokers).build().await?;

    // One PartitionClient per partition, created up front and reused. Retry on
    // an unknown topic so a race with topic creation self-heals; in the compose
    // stack the topic-init service has already created it before we start.
    let mut partition_clients = Vec::with_capacity(partitions as usize);
    for partition in 0..partitions {
        let pc = client
            .partition_client(topic.clone(), partition, UnknownTopicHandling::Retry)
            .await?;
        partition_clients.push(pc);
    }

    // Plausibly varied, deterministic sample data (no randomness needed beyond
    // the v4 orderId): cycle a few customers and SKUs, wobble quantity/price.
    let customers = ["c-4711", "c-8100", "c-2049", "c-3312", "c-9930"];
    let skus = ["SKU-0042", "SKU-1337", "SKU-2020", "SKU-7777", "SKU-0101"];

    let mut delivered: u64 = 0;
    for i in 0..count {
        let order_id = Uuid::new_v4().to_string();
        let partition = partition_for(&order_id, partitions);

        let order = Order {
            order_id: order_id.clone(),
            customer_id: customers[i as usize % customers.len()].to_string(),
            sku: skus[(i as usize / customers.len()) % skus.len()].to_string(),
            quantity: (i % 5) + 1,
            unit_price_cents: 999 + (i % 50) * 10,
            currency: "GBP".to_string(),
            placed_at: Utc::now().to_rfc3339(),
        };
        let value = serde_json::to_vec(&order)?;

        let record = Record {
            // Record key is the orderId string; the consumer's invariant is
            // key == body.orderId.
            key: Some(order_id.into_bytes()),
            value: Some(value),
            headers: BTreeMap::new(),
            timestamp: Utc::now(),
        };

        // Await the result: rskafka returns the broker-assigned offsets, so this
        // completes only once the broker has acknowledged the batch (acks=all).
        // Any error propagates and exits non-zero (see the `?`).
        partition_clients[partition as usize]
            .produce(vec![record], Compression::NoCompression)
            .await?;
        delivered += 1;
    }

    log::info!("DELIVERED {delivered}/{count}");
    Ok(())
}
