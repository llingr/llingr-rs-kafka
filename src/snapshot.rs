// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

//! Typed view of the engine's snapshot document.
//!
//! The snapshot is a point-in-time view of the consumer's state: topic
//! summary, sliding throughput windows with latency figures, per-partition
//! offset tracking with gap-buffer depths, guard-channel utilisation, and
//! per-shard worker counts. It crosses the FFI as the engine's canonical
//! JSON document, deliberately byte-identical to what the Go engine's own
//! HTTP snapshot handler serves: one operational document across both
//! ecosystems. Applications that proxy the document verbatim, for example
//! on an operational HTTP route, should use
//! [`Llingr::snapshot_json`](crate::Llingr::snapshot_json); this module is
//! for programmatic access.
//!
//! The structs deserialise with serde's default unknown-field tolerance, so
//! a newer engine adding diagnostic fields never breaks an older crate:
//! forward compatibility. They are `#[non_exhaustive]`: fields may be
//! added in step with the engine without a breaking release.
//!
//! Timestamps are UTC strings with millisecond precision in the fixed
//! format `2006-01-02T15:04:05.000Z` (for example `2026-07-16T14:30:05.123Z`).
//! Durations are Go duration strings (for example `1.517ms`, `2m30s`, `0s`);
//! they are kept as strings rather than parsed, because their canonical form
//! is display-oriented diagnostics.

use serde::Deserialize;

/// A point-in-time view of the engine's state, parsed from the canonical
/// snapshot JSON document.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Snapshot {
    /// Top-level engine summary: topic, assignment, lifetime throughput.
    pub summary: Summary,
    /// Sliding-window throughput and latency state.
    pub throughput: Throughput,
    /// Per-partition offset tracking state: the gap-buffer committer's view.
    pub pre_commits: PreCommits,
    /// Guard-channel and commit-ingest utilisation.
    pub concurrency: Concurrency,
    /// Worker population per shard.
    pub shards: Vec<Shard>,
}

impl Snapshot {
    /// Parse the canonical engine snapshot document.
    ///
    /// Unknown fields are tolerated, because a newer engine may add
    /// diagnostics; a missing or malformed known field is an error.
    pub fn from_json(document: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(document)
    }
}

/// Top-level engine summary.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Summary {
    /// The consumed topic.
    pub topic_name: String,
    /// Number of partitions currently assigned to this consumer.
    pub assigned_partition_count: i64,
    /// Messages processed since the engine started.
    pub total_processed: i64,
    /// When the last rebalance completed (UTC, millisecond precision,
    /// format `2006-01-02T15:04:05.000Z`).
    pub last_rebalance_time: String,
}

/// Sliding-window throughput and latency state. The window set covers the
/// most recent buckets (fifteen one-second buckets by default).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Throughput {
    /// Per-bucket message counts, oldest first.
    pub windows: Vec<ThroughputWindow>,
    /// Messages processed since the engine started.
    pub total_processed: i64,
    /// Messages dead-lettered since the engine started.
    pub total_dead_lettered: i64,
    /// Mean handler processing time over the window (Go duration string).
    pub avg_process_duration: String,
    /// Maximum handler processing time over the window (Go duration string).
    pub max_process_duration: String,
    /// Mean end-to-end time (read to watermark advance) over the window
    /// (Go duration string).
    pub avg_end_to_end_duration: String,
    /// Maximum end-to-end time over the window (Go duration string).
    pub max_end_to_end_duration: String,
    /// Minimum end-to-end time over the window (Go duration string).
    pub min_end_to_end_duration: String,
}

/// Message counts for one time bucket of the sliding window.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThroughputWindow {
    /// Bucket start (UTC, millisecond precision).
    pub from_time: String,
    /// Bucket end (UTC, millisecond precision).
    pub to_time: String,
    /// Messages processed in this bucket.
    pub processed_count: u32,
    /// Messages dead-lettered in this bucket.
    pub dead_letter_count: u32,
}

/// Per-partition offset tracking state.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreCommits {
    /// One entry per partition the committer tracks.
    pub partitions: Vec<PartitionOffsets>,
}

/// Offset tracking state for a single partition. Offsets are `-1` when the
/// partition has no recorded position yet.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionOffsets {
    /// The partition number.
    pub partition: i32,
    /// Highest offset committed to the broker.
    pub committed_offset: i64,
    /// Highest contiguously completed offset, ready to commit.
    pub highest_ready_offset: i64,
    /// Highest offset observed on the partition.
    pub max_offset_seen: i64,
    /// Entries currently held in the partition's gap buffer: out-of-order
    /// completions awaiting contiguity.
    pub gap_buffer_depth: i64,
    /// Whether the partition is currently assigned to this consumer.
    pub assigned: bool,
}

/// Guard-channel and commit-ingest utilisation.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Concurrency {
    /// Guard tokens currently held.
    pub guard_active: i64,
    /// Guard channel capacity: the concurrent_keys bound.
    pub guard_capacity: i64,
    /// Overflow tokens currently held.
    pub overflow_active: i64,
    /// Overflow channel capacity.
    pub overflow_capacity: i64,
    /// Commit-ingest channel occupancy.
    pub commit_ingest_active: i64,
    /// Commit-ingest channel capacity.
    #[serde(rename = "commitIngestCapacity")]
    pub commit_ingest_capacity: i64,
}

/// Worker population for a single shard.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Shard {
    /// The shard index.
    pub shard: i64,
    /// Workers currently bound to keys on this shard.
    pub active_workers: i64,
    /// Idle workers parked in the pool, as seen by this shard's snapshot.
    pub pooled_workers: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact document the pinned Go engine emits: generated by
    /// json.Marshal over a fully populated snapshot.Snapshot using
    /// llingr-demux v0.12.0 (demux/metrics/snapshot). Regenerate against the
    /// new pin whenever the engine version changes.
    const CANONICAL_DOCUMENT: &str = r#"{"summary":{"topicName":"orders","assignedPartitionCount":12,"totalProcessed":5000000123,"lastRebalanceTime":"2026-07-16T14:30:05.123Z"},"throughput":{"windows":[{"fromTime":"2026-07-16T14:30:05.123Z","toTime":"2026-07-16T14:30:06.123Z","processedCount":1451,"deadLetterCount":2}],"totalProcessed":5000000123,"totalDeadLettered":7,"avgProcessDuration":"1.517ms","maxProcessDuration":"92ms","avgEndToEndDuration":"3ms","maxEndToEndDuration":"2m30s","minEndToEndDuration":"0s"},"preCommits":{"partitions":[{"partition":3,"committedOffset":9000000001,"highestReadyOffset":9000000005,"maxOffsetSeen":9000000042,"gapBufferDepth":4,"assigned":true},{"partition":7,"committedOffset":-1,"highestReadyOffset":-1,"maxOffsetSeen":-1,"gapBufferDepth":0,"assigned":false}]},"concurrency":{"guardActive":17,"guardCapacity":250,"overflowActive":1,"overflowCapacity":50,"commitIngestActive":42,"commitIngestCapacity":2000},"shards":[{"shard":0,"activeWorkers":5,"pooledWorkers":11},{"shard":1,"activeWorkers":0,"pooledWorkers":16}]}"#;

    #[test]
    fn parses_the_canonical_engine_document() {
        let snapshot = Snapshot::from_json(CANONICAL_DOCUMENT).expect("canonical document parses");

        assert_eq!(snapshot.summary.topic_name, "orders");
        assert_eq!(snapshot.summary.assigned_partition_count, 12);
        assert_eq!(
            snapshot.summary.total_processed, 5_000_000_123,
            "64-bit count intact"
        );
        assert_eq!(
            snapshot.summary.last_rebalance_time,
            "2026-07-16T14:30:05.123Z"
        );

        assert_eq!(snapshot.throughput.windows.len(), 1);
        let window = &snapshot.throughput.windows[0];
        assert_eq!(window.processed_count, 1451);
        assert_eq!(window.dead_letter_count, 2);
        assert_eq!(window.from_time, "2026-07-16T14:30:05.123Z");
        assert_eq!(snapshot.throughput.total_dead_lettered, 7);
        assert_eq!(snapshot.throughput.avg_process_duration, "1.517ms");
        assert_eq!(snapshot.throughput.max_end_to_end_duration, "2m30s");
        assert_eq!(snapshot.throughput.min_end_to_end_duration, "0s");

        assert_eq!(snapshot.pre_commits.partitions.len(), 2);
        let assigned = &snapshot.pre_commits.partitions[0];
        assert_eq!(assigned.partition, 3);
        assert_eq!(assigned.committed_offset, 9_000_000_001);
        assert_eq!(assigned.highest_ready_offset, 9_000_000_005);
        assert_eq!(assigned.max_offset_seen, 9_000_000_042);
        assert_eq!(assigned.gap_buffer_depth, 4);
        assert!(assigned.assigned);
        let unassigned = &snapshot.pre_commits.partitions[1];
        assert_eq!(
            unassigned.committed_offset, -1,
            "-1 marks no recorded position"
        );
        assert!(!unassigned.assigned);

        assert_eq!(snapshot.concurrency.guard_active, 17);
        assert_eq!(snapshot.concurrency.guard_capacity, 250);
        assert_eq!(snapshot.concurrency.commit_ingest_capacity, 2000);

        assert_eq!(snapshot.shards.len(), 2);
        assert_eq!(snapshot.shards[0].active_workers, 5);
        assert_eq!(snapshot.shards[1].pooled_workers, 16);
    }

    /// Forward compatibility: a newer engine adding fields at any level must
    /// not break parsing. Serde ignores unknown fields by default; this pins
    /// that no struct opts into deny_unknown_fields.
    #[test]
    fn tolerates_unknown_fields_at_every_level() {
        let extended = CANONICAL_DOCUMENT
            .replacen(
                r#"{"summary":{"topicName"#,
                r#"{"futureTopLevel":{"a":1},"summary":{"futureSummaryField":"x","topicName"#,
                1,
            )
            .replacen(
                r#"{"partition":3,"#,
                r#"{"futurePartitionField":[1,2],"partition":3,"#,
                1,
            )
            .replacen(
                r#"{"shard":0,"#,
                r#"{"futureShardField":true,"shard":0,"#,
                1,
            );
        let snapshot = Snapshot::from_json(&extended).expect("unknown fields are tolerated");
        assert_eq!(snapshot.summary.topic_name, "orders");
        assert_eq!(snapshot.pre_commits.partitions[0].partition, 3);
        assert_eq!(snapshot.shards[0].active_workers, 5);
    }

    /// A missing known section is an error, not a silent default: a lost
    /// section indicates an engine contract change.
    #[test]
    fn missing_section_is_an_error() {
        let truncated = CANONICAL_DOCUMENT.replacen(r#""summary":"#, r#""renamed":"#, 1);
        assert!(Snapshot::from_json(&truncated).is_err());
    }

    #[test]
    fn malformed_json_is_an_error() {
        assert!(Snapshot::from_json("{not json").is_err());
        assert!(Snapshot::from_json("").is_err());
    }
}
