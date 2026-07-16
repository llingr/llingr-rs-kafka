// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

//! Raw FFI declarations for the C API exported by the Go bridge
//! (`bridge/` in this repository, built as the static `libllingr.a`).

use std::os::raw::{c_char, c_int};

/// One record header delivered to the process/dead-letter callbacks. Field
/// order and types MUST match `llingr_header` in bridge/main.go. `value_len == -1`
/// marks a NULL value (distinct from an empty value, `value_len == 0`). Keys
/// are UTF-8. All pointers are valid only for the duration of the callback.
#[repr(C)]
pub struct HeaderRaw {
    pub key: *const c_char,
    pub key_len: c_int,
    pub value: *const c_char,
    pub value_len: c_int,
}

// All offset / trait / epoch-timestamp fields (ms here, ns in MetricsFn) are
// i64 (matching the int64_t C ABI), never c_long: c_long is 32-bit on LLP64
// (Windows), where epoch values overflow outright and offsets / high trait
// bits truncate. Lengths and partition are genuinely 32-bit, so they stay
// c_int. ts_kind is int8_t (0 not available, 1 create time, 2 log append).
//
// value_len == -1 marks a NULL record value (a tombstone), distinct from an
// empty value (value_len == 0), the same convention `HeaderRaw` uses. Applies
// to both `ProcessFn` and `DeadLetterFn`.
pub type ProcessFn = unsafe extern "C" fn(
    key: *const c_char,
    key_len: c_int,
    value: *const c_char,
    value_len: c_int,
    partition: c_int,
    offset: i64,
    ts_kind: i8,
    ts_millis: i64,
    headers: *const HeaderRaw,
    header_count: c_int,
    traits_out: *mut i64,
    // On a non-zero return, the callback writes up to err_cap bytes of its
    // error text into err_buf and stores the count in err_len_out. The bridge
    // surfaces that as the dead-letter reason.
    err_buf: *mut c_char,
    err_cap: c_int,
    err_len_out: *mut c_int,
) -> c_int;

pub type DeadLetterFn = unsafe extern "C" fn(
    key: *const c_char,
    key_len: c_int,
    value: *const c_char,
    value_len: c_int,
    partition: c_int,
    offset: i64,
    ts_kind: i8,
    ts_millis: i64,
    headers: *const HeaderRaw,
    header_count: c_int,
    error_msg: *const c_char,
    error_len: c_int,
) -> c_int;

pub type MetricsFn = unsafe extern "C" fn(
    traits: i64,
    queue_depth: c_int,
    partition: c_int,
    offset: i64,
    process_duration_ns: i64,
    deadletter_duration_ns: i64,
    read_time_ns: i64,
    process_start_time_ns: i64,
    watermark_advance_time_ns: i64,
);

pub type ShutdownFn = unsafe extern "C" fn(reason: *const c_char, reason_len: c_int);

/// Broker node in a bandwidth packet. All strings are (pointer, length)
/// pairs into C-allocated memory valid only for the duration of the call.
/// Field order and types MUST match `llingr_broker_info` in bridge/main.go.
#[repr(C)]
pub struct BrokerInfoRaw {
    pub id: *const c_char,
    pub id_len: c_int,
    pub host: *const c_char,
    pub host_len: c_int,
    pub port: *const c_char,
    pub port_len: c_int,
    pub rack: *const c_char,
    pub rack_len: c_int,
}

/// Per-partition counters in a bandwidth packet. Field order and types MUST
/// match `llingr_partition_bandwidth` in bridge/main.go. Counters are int64_t
/// (never `long`: 32-bit on LLP64).
#[repr(C)]
pub struct PartitionBandwidthRaw {
    pub ts_unix_ns: i64,
    pub received_bytes: i64,
    pub transmitted_bytes: i64,
    pub received_message_count: i64,
    pub compressed_bytes: i64,
    pub uncompressed_bytes: i64,
    pub id: i32,
    pub leader: *const c_char,
    pub leader_len: c_int,
    pub compression: *const c_char,
    pub compression_len: c_int,
}

/// Bandwidth packet callback: one flushed `nexus.BandwidthMetrics`, flattened
/// into C arrays. Everything is valid only for the duration of the call.
pub type BandwidthFn = unsafe extern "C" fn(
    ts_unix_ns: i64,
    stats_interval_ms: i64,
    metrics_id: *const c_char,
    metrics_id_len: c_int,
    topic: *const c_char,
    topic_len: c_int,
    group: *const c_char,
    group_len: c_int,
    brokers: *const BrokerInfoRaw,
    broker_count: c_int,
    partitions: *const PartitionBandwidthRaw,
    partition_count: c_int,
);

/// Engine log line. level: 0=debug, 1=info, 2=warn, 3=error. msg is NOT
/// NUL-terminated; msg_len bounds it. Valid only for the duration of the call.
pub type LogFn = unsafe extern "C" fn(level: c_int, msg: *const c_char, msg_len: c_int);

extern "C" {
    pub fn llingr_abi_version() -> c_int;
    pub fn llingr_init(
        config_json: *const c_char,
        config_len: c_int,
        err_buf: *mut c_char,
        err_cap: c_int,
        err_len_out: *mut c_int,
    ) -> c_int;
    pub fn llingr_run() -> c_int;
    pub fn llingr_stop();
    /// Trigger the engine's emergency shutdown: abandon in-flight work and
    /// stop now, no drain, no final commit. `reason` is NOT NUL-terminated;
    /// `reason_len` bounds it (a NULL or zero-length reason gets a default
    /// description). Delivered to the shutdown callback exactly once. Safe
    /// in any lifecycle state; a no-op before [`llingr_init`].
    pub fn llingr_emergency_stop(reason: *const c_char, reason_len: c_int);
    pub fn llingr_on_process(cb: ProcessFn);
    pub fn llingr_on_deadletter(cb: DeadLetterFn);
    pub fn llingr_on_metrics(cb: MetricsFn);
    pub fn llingr_on_shutdown(cb: ShutdownFn);
    pub fn llingr_on_log(cb: LogFn);
    pub fn llingr_on_bandwidth(cb: BandwidthFn);
    /// Point-in-time engine state as a C-allocated, NUL-terminated JSON
    /// string (the same document Go's SnapshotHandler serves), or NULL when
    /// the engine is not initialised. The caller owns the string and must
    /// release it with [`llingr_free_string`].
    pub fn llingr_take_snapshot() -> *mut c_char;
    pub fn llingr_free_string(s: *mut c_char);
}

/// FFI contract version the crate is built for. Must equal the `abiVersion`
/// constant in the Go bridge (`bridge/main.go`); the engine builder checks it
/// at startup. Bump both together on any ABI change.
///
/// v1 is the first released contract; the unpublished revisions that
/// preceded it were renumbered away, so the released history starts here.
pub const LLINGR_ABI_VERSION: c_int = 1;

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::{align_of, offset_of, size_of};

    /// The intended contract version, pinned locally. The engine module
    /// separately asserts the LIBRARY reports this value; this test catches a
    /// bump on only one side of a coordinated Rust/Go change.
    #[test]
    fn abi_version_is_v1() {
        assert_eq!(LLINGR_ABI_VERSION, 1);
    }

    /// The three repr(C) structs must match the C layout in bridge/main.go
    /// field for field. The boundary round-trip tests exercise small crafted
    /// cases; these asserts catch a reordered or resized field that happens
    /// to round-trip anyway. Pointer-width dependent: pinned on 64-bit.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn repr_c_layouts_match_the_bridge() {
        // llingr_header: ptr, int, ptr, int (padding after each int).
        assert_eq!(offset_of!(HeaderRaw, key), 0);
        assert_eq!(offset_of!(HeaderRaw, key_len), 8);
        assert_eq!(offset_of!(HeaderRaw, value), 16);
        assert_eq!(offset_of!(HeaderRaw, value_len), 24);
        assert_eq!(size_of::<HeaderRaw>(), 32);
        assert_eq!(align_of::<HeaderRaw>(), 8);

        // llingr_broker_info: four (ptr, int) pairs.
        assert_eq!(offset_of!(BrokerInfoRaw, id), 0);
        assert_eq!(offset_of!(BrokerInfoRaw, id_len), 8);
        assert_eq!(offset_of!(BrokerInfoRaw, host), 16);
        assert_eq!(offset_of!(BrokerInfoRaw, host_len), 24);
        assert_eq!(offset_of!(BrokerInfoRaw, port), 32);
        assert_eq!(offset_of!(BrokerInfoRaw, port_len), 40);
        assert_eq!(offset_of!(BrokerInfoRaw, rack), 48);
        assert_eq!(offset_of!(BrokerInfoRaw, rack_len), 56);
        assert_eq!(size_of::<BrokerInfoRaw>(), 64);

        // llingr_partition_bandwidth: six i64 counters, i32 id (+pad),
        // then two (ptr, int) pairs.
        assert_eq!(offset_of!(PartitionBandwidthRaw, ts_unix_ns), 0);
        assert_eq!(offset_of!(PartitionBandwidthRaw, uncompressed_bytes), 40);
        assert_eq!(offset_of!(PartitionBandwidthRaw, id), 48);
        assert_eq!(offset_of!(PartitionBandwidthRaw, leader), 56);
        assert_eq!(offset_of!(PartitionBandwidthRaw, leader_len), 64);
        assert_eq!(offset_of!(PartitionBandwidthRaw, compression), 72);
        assert_eq!(offset_of!(PartitionBandwidthRaw, compression_len), 80);
        assert_eq!(size_of::<PartitionBandwidthRaw>(), 88);
    }
}
