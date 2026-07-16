// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

//! C callback trampolines, invoked from Go worker goroutines. Every
//! trampoline is wrapped in `catch_unwind` because a Rust panic unwinding
//! out of `extern "C"` aborts the process (RFC 2945). Raw C types come from
//! [`crate::ffi`].

// The `c_int as i32` casts are deliberate: c_int happens to be i32 on every
// supported target, but the FFI signatures are written in C ABI types and
// the conversions to Rust API types are kept explicit.
#![allow(clippy::unnecessary_cast)]

use std::io::Write;
use std::os::raw::{c_char, c_int};
use std::panic;

use crate::ffi::HeaderRaw;
use llingr_nexus::{Header, Headers, LogLevel, Message, Metrics, Timestamp, Traits};

use crate::engine::handlers;

/// Write up to `cap` bytes of `s` into the C buffer `buf`, truncated at a
/// UTF-8 char boundary, storing the byte count in `len_out`. No-op if any
/// pointer is null or `cap <= 0`. Used to hand a callback's error text back to
/// the Go bridge without a cross-allocator transfer (the bridge owns `buf`).
///
/// SAFETY: `buf` must be valid for `cap` bytes and `len_out` for one c_int,
/// for the duration of the call.
unsafe fn write_c_err(buf: *mut c_char, cap: c_int, len_out: *mut c_int, s: &str) {
    if buf.is_null() || len_out.is_null() || cap <= 0 {
        return;
    }
    let cap = cap as usize;
    let mut n = s.len().min(cap);
    while n > 0 && !s.is_char_boundary(n) {
        n -= 1;
    }
    std::ptr::copy_nonoverlapping(s.as_ptr(), buf as *mut u8, n);
    *len_out = n as c_int;
}

/// Print a one-line diagnostic to stderr without ever unwinding. A plain
/// `eprintln!` panics if stderr is closed (broken pipe); in a catch_unwind
/// failure arm that panic would unwind out of `extern "C"` and abort the
/// process, so we use a fallible write and discard the result.
fn report(msg: &str) {
    let _ = writeln!(std::io::stderr(), "{msg}");
}

/// A zero-sized token whose lifetime marks one callback invocation. The borrow
/// helpers below take `&'a CallScope` and hand back borrows with that same `'a`,
/// so a slice into a C buffer is tied at the type level to the single call the
/// Go bridge guarantees it for. A borrow therefore cannot be constructed with a
/// lifetime that outlives the callback (e.g. stashed in a `static` or returned),
/// which would otherwise be a use-after-free the signatures did nothing to stop.
pub(crate) struct CallScope;

/// Drop a caught panic payload without ever unwinding out of the caller. The
/// usual payload (the `String`/`&str` a `panic!` produces) drops cheaply here.
/// A pathological payload whose own `Drop` panics (reachable from safe handler
/// code via `std::panic::panic_any`) is contained by the inner catch and then
/// leaked: letting that second unwind escape the surrounding `extern "C"`
/// trampoline would abort the whole process (RFC 2945), the exact outcome the
/// trampolines' panic containment exists to prevent. The leak happens only on a
/// doubly-pathological payload, never on the normal panic path.
fn drop_panic_payload(payload: Box<dyn std::any::Any + Send>) {
    if let Err(nested) = panic::catch_unwind(panic::AssertUnwindSafe(move || drop(payload))) {
        std::mem::forget(nested);
    }
}

/// Borrow a (pointer, length) pair as a byte slice for the duration of the
/// callback (`_scope`); empty for null/non-positive length.
unsafe fn borrow_bytes(_scope: &CallScope, ptr: *const c_char, len: c_int) -> &[u8] {
    if ptr.is_null() || len <= 0 {
        &[]
    } else {
        std::slice::from_raw_parts(ptr as *const u8, len as usize)
    }
}

/// Map the C timestamp kind (0/1/2) + epoch millis to a nexus `Timestamp`.
fn decode_timestamp(kind: i8, millis: i64) -> Timestamp {
    match kind {
        1 => Timestamp::CreateTime { millis },
        2 => Timestamp::LogAppendTime { millis },
        _ => Timestamp::NotAvailable,
    }
}

/// Borrow the C header array as nexus `Header` views. `value_len < 0` is a null
/// value (distinct from empty). Keys are UTF-8 (an invalid key decodes to "").
/// The returned views borrow the C buffers, valid only for the callback.
///
/// SAFETY: `headers` must point to `count` valid `HeaderRaw` whose string
/// pointers are valid for the duration of the call.
pub(crate) unsafe fn borrow_headers<'a>(
    scope: &'a CallScope,
    headers: *const HeaderRaw,
    count: c_int,
) -> Vec<Header<'a>> {
    if headers.is_null() || count <= 0 {
        return Vec::new();
    }
    std::slice::from_raw_parts(headers, count as usize)
        .iter()
        .map(|h| Header {
            key: std::str::from_utf8(borrow_bytes(scope, h.key, h.key_len)).unwrap_or(""),
            value: if h.value_len < 0 {
                None
            } else {
                Some(borrow_bytes(scope, h.value, h.value_len))
            },
        })
        .collect()
}

/// Process message trampoline: called from Go worker goroutines.
///
/// SAFETY: The Go bridge guarantees that key and value pointers are valid
/// for the duration of this call. The Rust handler borrows them as slices.
pub(crate) unsafe extern "C" fn process_trampoline(
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
    err_buf: *mut c_char,
    err_cap: c_int,
    err_len_out: *mut c_int,
) -> c_int {
    if !err_len_out.is_null() {
        *err_len_out = 0;
    }

    // The closure returns (rc, traits, optional error text). Pointer writes
    // that must not happen on a panic (traits_out, err_buf) are done out
    // here, after the unwind boundary, so a panicking handler can never
    // leave them half-written.
    let result = panic::catch_unwind(|| {
        let Some(h) = handlers() else {
            return (-1, 0, None);
        };

        // `scope` ties every borrow below to this callback invocation, so none
        // can escape into the handler beyond the call.
        let scope = CallScope;

        // SAFETY: the Go bridge guarantees key/value/header pointers are valid
        // for the call. The key is UTF-8-safe (raw if valid, base64 if binary,
        // partition number if absent). value_len == -1 is a null value (a
        // tombstone), delivered as None; 0 is an empty value, delivered as
        // Some(&[]), the same convention the headers use. Topic is fixed per
        // consumer, read from the handler set. The header views borrow the C
        // buffers and live until the callback returns.
        let header_views = unsafe { borrow_headers(&scope, headers, header_count) };
        let msg = Message::new(
            Some(unsafe { borrow_bytes(&scope, key, key_len) }),
            if value_len < 0 {
                None
            } else {
                Some(unsafe { borrow_bytes(&scope, value, value_len) })
            },
            h.topic.as_str(),
            partition as i32,
            offset,
            decode_timestamp(ts_kind, ts_millis),
            Headers::from_slice(&header_views),
        );

        match h.process.process(&msg) {
            Ok(traits) => (0, traits.raw(), None),
            Err(e) => (1, 0, Some(e.to_string())),
        }
    });

    match result {
        Ok((rc, traits, err_text)) => {
            if rc == 0 && !traits_out.is_null() {
                *traits_out = traits as i64;
            }
            if let Some(text) = err_text {
                write_c_err(err_buf, err_cap, err_len_out, &text);
            }
            rc
        }
        Err(payload) => {
            // Drop the payload inside a catch so a panicking Drop cannot unwind
            // out of this extern "C" frame and abort the process.
            drop_panic_payload(payload);
            report("llingr: panic in process callback (caught at FFI boundary)");
            write_c_err(err_buf, err_cap, err_len_out, "panic in process callback");
            1 // route to dead letter
        }
    }
}

/// Dead-letter trampoline: called from Go when ProcessMessage returned an error.
pub(crate) unsafe extern "C" fn deadletter_trampoline(
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
) -> c_int {
    let result = panic::catch_unwind(|| {
        let Some(h) = handlers() else {
            return -1;
        };

        // `scope` ties every borrow below to this callback invocation.
        let scope = CallScope;

        // Go error strings carry no UTF-8 guarantee (they can embed broker
        // bytes), so decode lossily rather than fabricating an invalid &str.
        let error_str =
            String::from_utf8_lossy(unsafe { borrow_bytes(&scope, error_msg, error_len) });

        // SAFETY / field notes: see process_trampoline (value_len == -1 is a
        // null value, delivered as None). Topic comes from the handler set;
        // the header views borrow C buffers for the call.
        let header_views = unsafe { borrow_headers(&scope, headers, header_count) };
        let msg = Message::new(
            Some(unsafe { borrow_bytes(&scope, key, key_len) }),
            if value_len < 0 {
                None
            } else {
                Some(unsafe { borrow_bytes(&scope, value, value_len) })
            },
            h.topic.as_str(),
            partition as i32,
            offset,
            decode_timestamp(ts_kind, ts_millis),
            Headers::from_slice(&header_views),
        );

        let result = match &h.dead_letter {
            Some(dl) => dl.handle(&msg, error_str.as_ref()),
            None => Ok(()),
        };

        match result {
            Ok(()) => 0,
            Err(_) => 1,
        }
    });

    match result {
        Ok(rc) => rc,
        Err(payload) => {
            // Contain a panicking Drop of the payload (see drop_panic_payload).
            drop_panic_payload(payload);
            report("llingr: panic in dead letter callback (caught at FFI boundary)");
            1
        }
    }
}

/// Metrics trampoline: called from Go after each message is processed.
pub(crate) unsafe extern "C" fn metrics_trampoline(
    traits: i64,
    queue_depth: c_int,
    partition: c_int,
    offset: i64,
    process_duration_ns: i64,
    deadletter_duration_ns: i64,
    read_time_ns: i64,
    process_start_time_ns: i64,
    watermark_advance_time_ns: i64,
) {
    if let Err(payload) = panic::catch_unwind(|| {
        let metrics = Metrics {
            traits: Traits::from_raw(traits as u64),
            queue_depth: queue_depth as i32,
            partition: partition as i32,
            offset,
            process_duration_ns,
            deadletter_duration_ns,
            read_time_ns,
            process_start_time_ns,
            watermark_advance_time_ns,
        };

        if let Some(h) = handlers() {
            if let Some(mh) = &h.metrics {
                mh.handle(&metrics);
            }
        }
    }) {
        drop_panic_payload(payload);
    }
}

/// Shutdown trampoline: called from Go when the consumer exits.
pub(crate) unsafe extern "C" fn shutdown_trampoline(reason: *const c_char, reason_len: c_int) {
    if let Err(payload) = panic::catch_unwind(|| {
        let scope = CallScope;
        // Go-supplied reason: decode lossily (no UTF-8 guarantee).
        let reason_str = if reason.is_null() || reason_len <= 0 {
            std::borrow::Cow::Borrowed("unknown")
        } else {
            String::from_utf8_lossy(borrow_bytes(&scope, reason, reason_len))
        };

        if let Some(h) = handlers() {
            if let Some(sh) = &h.shutdown {
                sh.handle(reason_str.as_ref());
            }
        }
    }) {
        drop_panic_payload(payload);
    }
}

/// Bandwidth trampoline: called from Go on each aggregator flush (only
/// registered with the bridge when a bandwidth handler was staged at build
/// time; registration is what enables collection). Off the message hot
/// path, so building an owned [`BandwidthMetrics`] here is fine.
///
/// SAFETY: the Go bridge guarantees every pointer (strings, both arrays and
/// the strings inside their elements) is valid for the duration of the call.
pub(crate) unsafe extern "C" fn bandwidth_trampoline(
    ts_unix_ns: i64,
    stats_interval_ms: i64,
    metrics_id: *const c_char,
    metrics_id_len: c_int,
    topic: *const c_char,
    topic_len: c_int,
    group: *const c_char,
    group_len: c_int,
    brokers: *const crate::ffi::BrokerInfoRaw,
    broker_count: c_int,
    partitions: *const crate::ffi::PartitionBandwidthRaw,
    partition_count: c_int,
) {
    // Decode into owned Rust data BEFORE catch_unwind so no raw pointer
    // logic runs inside the unwind boundary; the decode itself cannot panic
    // (borrow_bytes handles null/negative, from_utf8_lossy never fails).
    let scope = CallScope;
    let owned_str = |ptr, len| String::from_utf8_lossy(borrow_bytes(&scope, ptr, len)).into_owned();

    let brokers = if brokers.is_null() || broker_count <= 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(brokers, broker_count as usize)
            .iter()
            .map(|b| llingr_nexus::BrokerInfo {
                id: owned_str(b.id, b.id_len),
                host: owned_str(b.host, b.host_len),
                port: owned_str(b.port, b.port_len),
                rack: owned_str(b.rack, b.rack_len),
            })
            .collect()
    };
    let partitions = if partitions.is_null() || partition_count <= 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(partitions, partition_count as usize)
            .iter()
            .map(|p| llingr_nexus::PartitionBandwidth {
                ts_unix_ns: p.ts_unix_ns,
                received_bytes: p.received_bytes,
                transmitted_bytes: p.transmitted_bytes,
                received_message_count: p.received_message_count,
                compressed_bytes: p.compressed_bytes,
                uncompressed_bytes: p.uncompressed_bytes,
                id: p.id,
                leader: owned_str(p.leader, p.leader_len),
                compression: owned_str(p.compression, p.compression_len),
            })
            .collect()
    };
    let metrics = llingr_nexus::BandwidthMetrics {
        ts_unix_ns,
        stats_interval_ms,
        metrics_id: owned_str(metrics_id, metrics_id_len),
        topic: owned_str(topic, topic_len),
        consumer_group: owned_str(group, group_len),
        brokers,
        partitions,
    };

    if let Err(payload) = panic::catch_unwind(|| {
        if let Some(h) = handlers() {
            if let Some(bh) = &h.bandwidth {
                bh.handle(&metrics);
            }
        }
    }) {
        drop_panic_payload(payload);
    }
}

/// Log trampoline: called from Go for each engine log line (only routed by
/// the bridge when a log handler was registered at build time).
pub(crate) unsafe extern "C" fn log_trampoline(level: c_int, msg: *const c_char, msg_len: c_int) {
    if let Err(payload) = panic::catch_unwind(|| {
        let scope = CallScope;
        // Go-formatted log text: decode lossily (no UTF-8 guarantee).
        let text = String::from_utf8_lossy(borrow_bytes(&scope, msg, msg_len));

        if let Some(h) = handlers() {
            if let Some(lh) = &h.log {
                lh.log(LogLevel::from_raw(level), text.as_ref());
            }
        }
    }) {
        drop_panic_payload(payload);
    }
}

// ---------------------------------------------------------------------------
// Boundary regression tests
//
// These exercise the FFI trampolines directly with crafted pointers, no broker
// and no engine lifecycle, pinning the review findings so they cannot regress:
//   F1  lossy UTF-8 for Go-origin error/reason strings
//   F2  64-bit offsets/traits/timestamps survive (no c_long truncation) and
//       every metrics argument maps one-to-one onto its Metrics field
//   F5  the process handler's own error text reaches the dead-letter path
//   F7  a panicking handler is contained (no process abort)
//   ABI the crate constant matches the linked library's reported version
//   LOG log lines round-trip with level mapping and lossy decode
//
// The handler set is process-global and published once, so a single set is
// shared by all tests; the test handlers dispatch on the message key, and each
// test uses a unique offset so it can find its own recorded entry concurrently.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod boundary_tests {
    use super::*;
    use crate::engine::{Handlers, HANDLERS};
    use crate::ffi;
    use llingr_nexus::{
        BandwidthMetrics, BandwidthMetricsHandler, DeadLetterHandler, LogHandler, MetricsHandler,
        ProcessHandler, ShutdownHandler,
    };
    use std::error::Error;
    use std::sync::atomic::Ordering;
    use std::sync::{Mutex, Once};

    // Recorders for what the optional handlers observed, keyed by offset.
    #[allow(clippy::type_complexity)]
    static PROC_META_SEEN: Mutex<Vec<(i64, Timestamp, Vec<(String, Option<Vec<u8>>)>)>> =
        Mutex::new(Vec::new());
    static DLQ_SEEN: Mutex<Vec<(i64, String)>> = Mutex::new(Vec::new());
    static VALUE_SEEN: Mutex<Vec<(i64, Option<Vec<u8>>)>> = Mutex::new(Vec::new());
    static METRICS_SEEN: Mutex<Vec<Metrics>> = Mutex::new(Vec::new());
    static SHUTDOWN_SEEN: Mutex<Vec<String>> = Mutex::new(Vec::new());
    static LOG_SEEN: Mutex<Vec<(LogLevel, String)>> = Mutex::new(Vec::new());
    static BANDWIDTH_SEEN: Mutex<Vec<BandwidthMetrics>> = Mutex::new(Vec::new());

    /// A panic payload whose OWN `Drop` panics. Dropping the caught payload
    /// outside `catch_unwind` would run this `Drop`, unwinding a second time out
    /// of the `extern "C"` trampoline and aborting the process; `panic_any`
    /// makes it the payload so the boundary regression test can exercise it.
    struct PanicOnDrop;
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("panic in the panic payload's Drop");
        }
    }

    struct TProc;
    impl ProcessHandler for TProc {
        fn process(&self, msg: &Message) -> Result<Traits, Box<dyn Error>> {
            match msg.key_str() {
                Some("err-marker") => Err("custom failure text".into()),
                Some("panic-marker") => panic!("intentional test panic"),
                Some("panic-any-evil-drop") => std::panic::panic_any(PanicOnDrop),
                Some("traits20") => Ok(Traits::with_bit(20)),
                Some("value-probe") => {
                    VALUE_SEEN
                        .lock()
                        .unwrap()
                        .push((msg.offset(), msg.value().map(<[u8]>::to_vec)));
                    Ok(Traits::none())
                }
                Some("meta-probe") => {
                    let headers = msg
                        .headers()
                        .iter()
                        .map(|h| (h.key.to_string(), h.value.map(<[u8]>::to_vec)))
                        .collect();
                    PROC_META_SEEN
                        .lock()
                        .unwrap()
                        .push((msg.offset(), msg.timestamp(), headers));
                    Ok(Traits::none())
                }
                _ => Ok(Traits::none()),
            }
        }
    }
    struct TDead;
    impl DeadLetterHandler for TDead {
        fn handle(&self, msg: &Message, error_msg: &str) -> Result<(), Box<dyn Error>> {
            // Marker keys drive the trampoline's failure arms, mirroring TProc.
            match msg.key_str() {
                Some("dlq-err-marker") => return Err("dlq write failed".into()),
                Some("dlq-panic-marker") => panic!("intentional dlq panic"),
                _ => {}
            }
            DLQ_SEEN
                .lock()
                .unwrap()
                .push((msg.offset(), error_msg.to_string()));
            Ok(())
        }
    }
    struct TMetrics;
    impl MetricsHandler for TMetrics {
        fn handle(&self, m: &Metrics) {
            // Sentinel offset: panic with an evil-Drop payload, to exercise
            // drop_panic_payload on a VOID trampoline (no rc to signal with).
            if m.offset == -999_999 {
                std::panic::panic_any(PanicOnDrop);
            }
            METRICS_SEEN.lock().unwrap().push(*m);
        }
    }
    struct TShutdown;
    impl ShutdownHandler for TShutdown {
        fn handle(&self, reason: &str) {
            if reason == "trigger-shutdown-panic" {
                panic!("intentional shutdown panic");
            }
            SHUTDOWN_SEEN.lock().unwrap().push(reason.to_string());
        }
    }
    struct TLog;
    impl LogHandler for TLog {
        fn log(&self, level: LogLevel, message: &str) {
            if message.starts_with("trigger-log-panic") {
                panic!("intentional log panic");
            }
            LOG_SEEN.lock().unwrap().push((level, message.to_string()));
        }
    }
    struct TBandwidth;
    impl BandwidthMetricsHandler for TBandwidth {
        fn handle(&self, metrics: &BandwidthMetrics) {
            if metrics.metrics_id == "trigger-bandwidth-panic" {
                panic!("intentional bandwidth panic");
            }
            BANDWIDTH_SEEN.lock().unwrap().push(metrics.clone());
        }
    }

    fn seal() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            let handler_ptr = Box::into_raw(Box::new(Handlers {
                topic: "test-topic".to_string(),
                process: Box::new(TProc),
                dead_letter: Some(Box::new(TDead)),
                metrics: Some(Box::new(TMetrics)),
                shutdown: Some(Box::new(TShutdown)),
                log: Some(Box::new(TLog)),
                bandwidth: Some(Box::new(TBandwidth)),
                _prometheus_exporter: None,
            }));
            HANDLERS.store(handler_ptr, Ordering::Release);
        });
    }

    /// Invoke the process trampoline (no timestamp, no headers); returns
    /// (rc, traits_out, error_text).
    fn call_process(key: &str, value: &[u8], offset: i64) -> (c_int, i64, String) {
        let mut traits_out: i64 = 0;
        let mut err_buf = [0 as c_char; 256];
        let mut err_len: c_int = 0;
        let rc = unsafe {
            process_trampoline(
                key.as_ptr() as *const c_char,
                key.len() as c_int,
                value.as_ptr() as *const c_char,
                value.len() as c_int,
                0,
                offset,
                0, // ts_kind: not available
                0, // ts_millis
                std::ptr::null(),
                0, // header_count
                &mut traits_out,
                err_buf.as_mut_ptr(),
                err_buf.len() as c_int,
                &mut err_len,
            )
        };
        let bytes: Vec<u8> = err_buf[..err_len as usize]
            .iter()
            .map(|&c| c as u8)
            .collect();
        (rc, traits_out, String::from_utf8_lossy(&bytes).into_owned())
    }

    /// Invoke the process trampoline with a timestamp and header array.
    fn call_process_meta(
        key: &str,
        offset: i64,
        ts_kind: i8,
        ts_millis: i64,
        headers: &[ffi::HeaderRaw],
    ) -> c_int {
        let mut traits_out: i64 = 0;
        let mut err_buf = [0 as c_char; 256];
        let mut err_len: c_int = 0;
        let hdr_ptr = if headers.is_empty() {
            std::ptr::null()
        } else {
            headers.as_ptr()
        };
        unsafe {
            process_trampoline(
                key.as_ptr() as *const c_char,
                key.len() as c_int,
                b"v".as_ptr() as *const c_char,
                1,
                0,
                offset,
                ts_kind,
                ts_millis,
                hdr_ptr,
                headers.len() as c_int,
                &mut traits_out,
                err_buf.as_mut_ptr(),
                err_buf.len() as c_int,
                &mut err_len,
            )
        }
    }

    /// A borrowed HeaderRaw over a (key, optional value) pair. value_len == -1
    /// marks a null value.
    fn header_raw(key: &str, value: Option<&[u8]>) -> ffi::HeaderRaw {
        let (vptr, vlen) = match value {
            None => (std::ptr::null(), -1),
            Some(v) => (v.as_ptr() as *const c_char, v.len() as c_int),
        };
        ffi::HeaderRaw {
            key: key.as_ptr() as *const c_char,
            key_len: key.len() as c_int,
            value: vptr,
            value_len: vlen,
        }
    }

    #[test]
    fn process_carries_timestamp_and_headers() {
        seal();
        // Order preserved; a null value (None) stays distinct from an empty
        // value (Some(&[])).
        let headers = [
            header_raw("trace-id", Some(b"abc123")),
            header_raw("content-type", Some(b"application/json")),
            header_raw("tombstone", None),
            header_raw("empty-val", Some(b"")),
        ];
        let offset = 2001;
        let rc = call_process_meta("meta-probe", offset, 1, 1_700_000_000_000, &headers);
        assert_eq!(rc, 0);

        let seen = PROC_META_SEEN.lock().unwrap();
        let (_, ts, hdrs) = seen
            .iter()
            .find(|(o, _, _)| *o == offset)
            .expect("probe recorded its metadata");
        assert_eq!(
            *ts,
            Timestamp::CreateTime {
                millis: 1_700_000_000_000
            },
            "ts_kind=1 maps to CreateTime with the millis intact"
        );
        assert_eq!(hdrs.len(), 4, "all headers delivered in order");
        assert_eq!(hdrs[0], ("trace-id".to_string(), Some(b"abc123".to_vec())));
        assert_eq!(
            hdrs[1],
            (
                "content-type".to_string(),
                Some(b"application/json".to_vec())
            )
        );
        assert_eq!(
            hdrs[2],
            ("tombstone".to_string(), None),
            "value_len == -1 round-trips as a null value"
        );
        assert_eq!(
            hdrs[3],
            ("empty-val".to_string(), Some(Vec::new())),
            "empty value stays Some, distinct from null"
        );
    }

    #[test]
    fn process_without_headers_sees_empty_and_not_available() {
        seal();
        let rc = call_process_meta("meta-probe", 2002, 0, 0, &[]);
        assert_eq!(rc, 0);
        let seen = PROC_META_SEEN.lock().unwrap();
        let (_, ts, hdrs) = seen
            .iter()
            .find(|(o, _, _)| *o == 2002)
            .expect("probe recorded");
        assert_eq!(
            *ts,
            Timestamp::NotAvailable,
            "ts_kind=0 maps to NotAvailable"
        );
        assert!(hdrs.is_empty(), "no headers when count is zero");
    }

    #[test]
    fn f5_process_error_text_is_reported() {
        seal();
        let (rc, _t, err) = call_process("err-marker", b"v", 1001);
        assert_eq!(rc, 1, "an Err return must signal failure");
        assert_eq!(
            err, "custom failure text",
            "handler's own text must be reported, not a synthetic code"
        );
    }

    #[test]
    fn f7_panic_is_contained_and_reported() {
        seal();
        // If F7 regressed (unwind escaping extern "C"), this aborts the test
        // binary instead of returning. Reaching the asserts proves containment.
        let (rc, _t, err) = call_process("panic-marker", b"v", 1002);
        assert_eq!(rc, 1, "a panicking handler routes to dead letter");
        assert_eq!(err, "panic in process callback");
    }

    /// A panic whose PAYLOAD has a panicking `Drop` must also be contained: if
    /// the caught payload were dropped outside `catch_unwind`, that second
    /// unwind would escape the `extern "C"` trampoline and abort the process
    /// (SIGABRT on rustc >= 1.81, UB below it). Reaching the asserts instead of
    /// aborting the test binary proves `drop_panic_payload` contains it.
    #[test]
    fn f7_panicking_drop_payload_is_contained() {
        seal();
        let (rc, _t, err) = call_process("panic-any-evil-drop", b"v", 1009);
        assert_eq!(
            rc, 1,
            "a panicking-Drop payload still routes to dead letter"
        );
        assert_eq!(err, "panic in process callback");
    }

    // The remaining four trampolines are void (no rc), so a panicking handler
    // can only be CONTAINED, never signalled. Each test reaches its final line
    // instead of aborting the binary, which proves containment. The metrics
    // case uses the evil-Drop payload, exercising drop_panic_payload on a void
    // trampoline (the one shape the process test cannot cover).

    #[test]
    fn metrics_panicking_handler_is_contained() {
        seal();
        unsafe {
            metrics_trampoline(0, 0, 0, -999_999, 0, 0, 0, 0, 0);
        }
    }

    #[test]
    fn shutdown_panicking_handler_is_contained() {
        seal();
        let reason = "trigger-shutdown-panic";
        unsafe {
            shutdown_trampoline(reason.as_ptr() as *const c_char, reason.len() as c_int);
        }
    }

    #[test]
    fn log_panicking_handler_is_contained() {
        seal();
        let msg = "trigger-log-panic";
        unsafe {
            log_trampoline(1, msg.as_ptr() as *const c_char, msg.len() as c_int);
        }
    }

    #[test]
    fn bandwidth_panicking_handler_is_contained() {
        seal();
        let id = "trigger-bandwidth-panic";
        unsafe {
            bandwidth_trampoline(
                1,
                1000,
                id.as_ptr() as *const c_char,
                id.len() as c_int,
                std::ptr::null(),
                0,
                std::ptr::null(),
                0,
                std::ptr::null(),
                0,
                std::ptr::null(),
                0,
            );
        }
    }

    #[test]
    fn process_success_sets_custom_traits() {
        seal();
        let (rc, traits, err) = call_process("traits20", b"v", 1003);
        assert_eq!(rc, 0);
        assert!(err.is_empty());
        assert_eq!(traits, 1 << 20, "bit 20 set, framework bits masked");
    }

    /// Invoke the process trampoline with an explicit raw (value ptr, len)
    /// pair, for pinning the value null sentinel.
    fn call_process_raw_value(offset: i64, value: *const c_char, value_len: c_int) -> c_int {
        let key = "value-probe";
        let mut traits_out: i64 = 0;
        let mut err_buf = [0 as c_char; 256];
        let mut err_len: c_int = 0;
        unsafe {
            process_trampoline(
                key.as_ptr() as *const c_char,
                key.len() as c_int,
                value,
                value_len,
                0,
                offset,
                0, // ts_kind: not available
                0, // ts_millis
                std::ptr::null(),
                0, // header_count
                &mut traits_out,
                err_buf.as_mut_ptr(),
                err_buf.len() as c_int,
                &mut err_len,
            )
        }
    }

    fn recorded_value(offset: i64) -> Option<Vec<u8>> {
        VALUE_SEEN
            .lock()
            .unwrap()
            .iter()
            .find(|(o, _)| *o == offset)
            .expect("value probe recorded")
            .1
            .clone()
    }

    /// value_len == -1 is a null value (a tombstone): the handler must see
    /// None, distinct from an empty value. A regression here silently breaks
    /// delete handling on log-compacted topics.
    #[test]
    fn tombstone_value_maps_to_none() {
        seal();
        let rc = call_process_raw_value(3001, std::ptr::null(), -1);
        assert_eq!(rc, 0);
        assert_eq!(recorded_value(3001), None, "value_len == -1 is None");
    }

    /// value_len == 0 is an empty value: Some(&[]), NOT None. The null/empty
    /// distinction must hold in both directions.
    #[test]
    fn empty_value_stays_some_and_distinct_from_null() {
        seal();
        let rc = call_process_raw_value(3002, std::ptr::null(), 0);
        assert_eq!(rc, 0);
        assert_eq!(
            recorded_value(3002),
            Some(Vec::new()),
            "empty value is Some(empty), distinct from a tombstone"
        );
    }

    #[test]
    fn nonempty_value_arrives_byte_identical() {
        seal();
        let payload = b"\x00\xff binary bytes";
        let rc = call_process_raw_value(
            3003,
            payload.as_ptr() as *const c_char,
            payload.len() as c_int,
        );
        assert_eq!(rc, 0);
        assert_eq!(recorded_value(3003), Some(payload.to_vec()));
    }

    /// Every one of the nine metrics arguments must land on ITS OWN Metrics
    /// field. Five are same-typed adjacent i64s, so a transposed pair in the
    /// trampoline's Metrics construction would pass the ABI checks (abi.lock
    /// pins the C signature, not this Rust-side mapping); distinct sentinels
    /// make any swap visible. All i64 sentinels exceed 32 bits, so a c_long
    /// truncation would also fail here (the original F2 property).
    #[test]
    fn f2_metrics_fields_map_one_to_one_and_carry_full_64_bits() {
        seal();
        // App bit 45 plus framework bits 0, 1, 3: the metrics path forwards
        // the FULL bitfield, framework bits included.
        let traits_raw: i64 = ((1u64 << 45) | 0b1011) as i64;
        let offset: i64 = 5_000_000_001;
        unsafe {
            metrics_trampoline(
                traits_raw,
                71, // queue_depth
                72, // partition
                offset,
                5_000_000_002,             // process_duration_ns
                5_000_000_003,             // deadletter_duration_ns
                1_900_000_000_000_000_004, // read_time_ns
                1_900_000_000_000_000_005, // process_start_time_ns
                1_900_000_000_000_000_006, // watermark_advance_time_ns
            );
        }
        let seen = METRICS_SEEN.lock().unwrap();
        let m = seen
            .iter()
            .find(|m| m.offset == offset)
            .expect("metrics packet recorded");
        assert_eq!(
            m.traits.raw_with_framework(),
            (1u64 << 45) | 0b1011,
            "full trait bitfield, framework bits included"
        );
        assert!(m.traits.has_process_error(), "framework bit 0 readable");
        assert!(m.traits.has_process_panic(), "framework bit 1 readable");
        assert!(m.traits.has_commit_buffered(), "framework bit 3 readable");
        assert_eq!(m.queue_depth, 71);
        assert_eq!(m.partition, 72);
        assert_eq!(m.process_duration_ns, 5_000_000_002);
        assert_eq!(m.deadletter_duration_ns, 5_000_000_003);
        assert_eq!(m.read_time_ns, 1_900_000_000_000_000_004);
        assert_eq!(m.process_start_time_ns, 1_900_000_000_000_000_005);
        assert_eq!(m.watermark_advance_time_ns, 1_900_000_000_000_000_006);
    }

    #[test]
    fn f1_deadletter_tolerates_non_utf8_error() {
        seal();
        // Go error strings carry no UTF-8 guarantee; feed raw invalid bytes.
        let bad = [0x66u8, 0x6f, 0x6f, 0xff, 0xfe]; // "foo" + two invalid bytes
        let offset: i64 = 1004;
        unsafe {
            deadletter_trampoline(
                "k".as_ptr() as *const c_char,
                1,
                b"v".as_ptr() as *const c_char,
                1,
                0,
                offset,
                0, // ts_kind
                0, // ts_millis
                std::ptr::null(),
                0, // header_count
                bad.as_ptr() as *const c_char,
                bad.len() as c_int,
            );
        }
        let seen = DLQ_SEEN.lock().unwrap();
        let (_, msg) = seen
            .iter()
            .find(|(o, _)| *o == offset)
            .expect("dead-letter handler ran");
        assert!(msg.starts_with("foo"), "valid prefix preserved");
        assert!(
            msg.contains('\u{FFFD}'),
            "invalid bytes became the replacement char, not UB"
        );
    }

    /// Invoke the dead-letter trampoline with a marker key; returns rc.
    fn call_deadletter(key: &str, offset: i64) -> c_int {
        let err = b"process failed";
        unsafe {
            deadletter_trampoline(
                key.as_ptr() as *const c_char,
                key.len() as c_int,
                b"v".as_ptr() as *const c_char,
                1,
                0,
                offset,
                0, // ts_kind
                0, // ts_millis
                std::ptr::null(),
                0, // header_count
                err.as_ptr() as *const c_char,
                err.len() as c_int,
            )
        }
    }

    /// A panic in the dead-letter handler must be contained at the FFI
    /// boundary (rc 1), never unwind into Go and abort the process. This is
    /// the DLQ analogue of the process-panic containment test.
    #[test]
    fn deadletter_panic_is_contained_at_the_boundary() {
        seal();
        let rc = call_deadletter("dlq-panic-marker", 1005);
        assert_eq!(rc, 1, "panic reports failure, does not unwind");
    }

    /// A dead-letter handler returning Err is reported to the bridge as rc 1
    /// (counted, not retried); Ok is rc 0.
    #[test]
    fn deadletter_error_return_maps_to_rc_1() {
        seal();
        assert_eq!(call_deadletter("dlq-err-marker", 1006), 1);
        assert_eq!(call_deadletter("dlq-ok", 1007), 0);
        let seen = DLQ_SEEN.lock().unwrap();
        assert!(
            seen.iter().any(|(o, _)| *o == 1007),
            "the Ok-path handler actually ran"
        );
    }

    /// ts_kind=2 maps to LogAppendTime (broker ingestion time), distinct from
    /// CreateTime: a 1/2 swap would silently mislabel every record timestamp.
    #[test]
    fn ts_kind_2_maps_to_log_append_time() {
        seal();
        let offset = 7002;
        let rc = call_process_meta("meta-probe", offset, 2, 555, &[]);
        assert_eq!(rc, 0);
        let seen = PROC_META_SEEN.lock().unwrap();
        let (_, ts, _) = seen
            .iter()
            .find(|(o, _, _)| *o == offset)
            .expect("meta probe ran");
        assert_eq!(
            *ts,
            Timestamp::LogAppendTime { millis: 555 },
            "kind 2 is LogAppendTime, not CreateTime"
        );
    }

    #[test]
    fn f1_shutdown_tolerates_non_utf8_reason() {
        seal();
        let bad = [0xffu8, 0xfe];
        unsafe {
            shutdown_trampoline(bad.as_ptr() as *const c_char, bad.len() as c_int);
        }
        let seen = SHUTDOWN_SEEN.lock().unwrap();
        assert!(
            seen.iter().any(|r| r.contains('\u{FFFD}')),
            "lossy decode produced a valid string from invalid reason bytes"
        );
    }

    #[test]
    fn log_round_trip_with_level_mapping() {
        seal();
        let msg = "subscription started: topic=orders";
        unsafe {
            log_trampoline(2, msg.as_ptr() as *const c_char, msg.len() as c_int);
        }
        let seen = LOG_SEEN.lock().unwrap();
        assert!(
            seen.iter().any(|(lv, m)| *lv == LogLevel::Warn && m == msg),
            "warn-level log line must arrive intact"
        );
    }

    #[test]
    fn log_tolerates_non_utf8_and_unknown_level() {
        seal();
        let bad = [0x6cu8, 0x6f, 0x67, 0xff]; // "log" + invalid byte
        unsafe {
            log_trampoline(42, bad.as_ptr() as *const c_char, bad.len() as c_int);
        }
        let seen = LOG_SEEN.lock().unwrap();
        assert!(
            seen.iter().any(|(lv, m)| *lv == LogLevel::Info
                && m.starts_with("log")
                && m.contains('\u{FFFD}')),
            "unknown level maps to Info; invalid bytes decode lossily"
        );
    }

    #[test]
    fn null_pointers_are_handled() {
        seal();
        let (rc, _t, _e) = call_process("", b"", 1005);
        assert_eq!(rc, 0, "empty key/value (null-ish) must not crash");
    }

    #[test]
    fn write_c_err_truncates_on_char_boundary() {
        // "é" is two bytes (0xC3 0xA9). A cap of 2 between them must not split it.
        let s = "aébc"; // bytes: a(1) é(2) b(1) c(1) = 5 bytes
        let mut buf = [0 as c_char; 8];
        let mut len: c_int = 0;
        unsafe { write_c_err(buf.as_mut_ptr(), 2, &mut len, s) };
        let bytes: Vec<u8> = buf[..len as usize].iter().map(|&c| c as u8).collect();
        assert_eq!(len, 1, "truncated back to the char boundary after 'a'");
        assert_eq!(std::str::from_utf8(&bytes).unwrap(), "a");
    }

    #[test]
    fn write_c_err_null_safe() {
        let mut len: c_int = -1;
        unsafe { write_c_err(std::ptr::null_mut(), 16, &mut len, "x") };
        // null buf is a no-op; len_out untouched by the early return path is fine
        // (it is the caller-zeroed value in real use). Just assert no crash.
    }

    /// The bandwidth packet crosses the boundary as flattened C arrays:
    /// verify 64-bit counters survive intact, strings decode (lossily where
    /// invalid), and empty/null arrays are tolerated.
    #[test]
    fn bandwidth_packet_round_trip() {
        seal();
        let broker = ffi::BrokerInfoRaw {
            id: "1".as_ptr() as *const c_char,
            id_len: 1,
            host: "broker-1.cluster.internal".as_ptr() as *const c_char,
            host_len: 25,
            port: "9092".as_ptr() as *const c_char,
            port_len: 4,
            rack: [0x65u8, 0x75, 0xff].as_ptr() as *const c_char, // "eu" + invalid byte
            rack_len: 3,
        };
        let partition = ffi::PartitionBandwidthRaw {
            ts_unix_ns: 1_900_000_000_000_000_001, // > 2^32: pins int64_t, not long
            received_bytes: 5_000_000_002,
            transmitted_bytes: 6_000_000_003,
            received_message_count: 7_000_000_004,
            // Distinct non-zero sentinels: with both at 0 a swapped pair in
            // the trampoline's field mapping would be invisible.
            compressed_bytes: 8_000_000_005,
            uncompressed_bytes: 9_000_000_006,
            id: 3,
            leader: "1".as_ptr() as *const c_char,
            leader_len: 1,
            compression: std::ptr::null(), // adapter without compression visibility
            compression_len: 0,
        };
        unsafe {
            bandwidth_trampoline(
                1_900_000_000_000_000_000,
                60_000,
                "packet-uuid-1".as_ptr() as *const c_char,
                13,
                "orders".as_ptr() as *const c_char,
                6,
                "grp".as_ptr() as *const c_char,
                3,
                &broker,
                1,
                &partition,
                1,
            );
        }
        let seen = BANDWIDTH_SEEN.lock().unwrap();
        let m = seen
            .iter()
            .find(|m| m.metrics_id == "packet-uuid-1")
            .expect("bandwidth handler received the packet");
        assert_eq!(m.ts_unix_ns, 1_900_000_000_000_000_000);
        assert_eq!(m.stats_interval_ms, 60_000);
        assert_eq!(m.topic, "orders");
        assert_eq!(m.consumer_group, "grp");
        assert_eq!(m.brokers.len(), 1);
        assert_eq!(m.brokers[0].id, "1");
        assert_eq!(m.brokers[0].host, "broker-1.cluster.internal");
        assert_eq!(m.brokers[0].port, "9092");
        assert!(
            m.brokers[0].rack.starts_with("eu") && m.brokers[0].rack.contains('\u{FFFD}'),
            "invalid rack bytes decode lossily: {:?}",
            m.brokers[0].rack
        );
        let p = &m.partitions[0];
        assert_eq!(p.ts_unix_ns, 1_900_000_000_000_000_001);
        assert_eq!(p.received_bytes, 5_000_000_002, "64-bit counter intact");
        assert_eq!(p.transmitted_bytes, 6_000_000_003);
        assert_eq!(p.received_message_count, 7_000_000_004);
        assert_eq!(p.compressed_bytes, 8_000_000_005);
        assert_eq!(p.uncompressed_bytes, 9_000_000_006);
        assert_eq!(p.id, 3);
        assert_eq!(p.leader, "1");
        assert_eq!(p.compression, "", "null string decodes to empty");
    }

    /// Empty packet (no brokers, no partitions, null arrays): must not crash.
    #[test]
    fn bandwidth_tolerates_empty_packet() {
        seal();
        unsafe {
            bandwidth_trampoline(
                1,
                1000,
                "empty-packet".as_ptr() as *const c_char,
                12,
                std::ptr::null(),
                0,
                std::ptr::null(),
                0,
                std::ptr::null(),
                0,
                std::ptr::null(),
                0,
            );
        }
        let seen = BANDWIDTH_SEEN.lock().unwrap();
        let m = seen
            .iter()
            .find(|m| m.metrics_id == "empty-packet")
            .expect("empty packet still delivered");
        assert!(m.topic.is_empty() && m.brokers.is_empty() && m.partitions.is_empty());
    }

    #[test]
    fn abi_version_matches_library() {
        // Links and calls into the Go bridge: guards against the crate and the
        // shared library drifting out of ABI sync.
        let lib = unsafe { ffi::llingr_abi_version() };
        assert_eq!(
            lib,
            ffi::LLINGR_ABI_VERSION,
            "rebuild libllingr to match the crate's ABI version"
        );
    }
}
