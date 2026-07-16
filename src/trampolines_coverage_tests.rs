// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

//! Coverage for the trampoline decode helpers (gap report Tier 2): the
//! duplicate-header-key and null-header-value contract asserted directly on
//! `borrow_headers`, deterministic and independent of the process-global
//! handler set the boundary tests own.
//!
//! New coverage in a new file; the landed boundary tests stay byte-unmodified.

use std::os::raw::{c_char, c_int};

use crate::ffi::HeaderRaw;
use crate::trampolines::{borrow_headers, CallScope};

/// A borrowed HeaderRaw over a (key, optional value) pair. value_len == -1
/// marks a null value, the wire convention for a null header.
fn header_raw(key: &[u8], value: Option<&[u8]>) -> HeaderRaw {
    let (value_ptr, value_len) = match value {
        None => (std::ptr::null(), -1),
        Some(v) => (v.as_ptr() as *const c_char, v.len() as c_int),
    };
    HeaderRaw {
        key: key.as_ptr() as *const c_char,
        key_len: key.len() as c_int,
        value: value_ptr,
        value_len,
    }
}

/// DUPLICATE keys are legal in Kafka headers and must all be delivered, in
/// wire order, with their own values; nothing may dedupe or reorder them.
#[test]
fn duplicate_header_keys_are_all_delivered_in_order() {
    let headers = [
        header_raw(b"dup", Some(b"first")),
        header_raw(b"other", Some(b"middle")),
        header_raw(b"dup", Some(b"second")),
    ];
    let scope = CallScope;
    let views = unsafe { borrow_headers(&scope, headers.as_ptr(), headers.len() as c_int) };

    assert_eq!(views.len(), 3, "no deduplication");
    assert_eq!(views[0].key, "dup");
    assert_eq!(views[0].value, Some(&b"first"[..]));
    assert_eq!(views[1].key, "other");
    assert_eq!(views[2].key, "dup", "second duplicate delivered in order");
    assert_eq!(views[2].value, Some(&b"second"[..]));
}

/// A NULL header value (value_len == -1) decodes to None, distinct from an
/// empty value (value_len == 0) which decodes to Some(empty); the null/empty
/// distinction must hold in both directions.
#[test]
fn null_and_empty_header_values_stay_distinct() {
    let headers = [
        header_raw(b"tombstone", None),
        header_raw(b"empty", Some(b"")),
        header_raw(b"filled", Some(b"v")),
    ];
    let scope = CallScope;
    let views = unsafe { borrow_headers(&scope, headers.as_ptr(), headers.len() as c_int) };

    assert_eq!(views[0].value, None, "value_len == -1 is a null value");
    assert_eq!(
        views[1].value,
        Some(&b""[..]),
        "value_len == 0 is an empty value, not null"
    );
    assert_eq!(views[2].value, Some(&b"v"[..]));
}

/// Keys are UTF-8 by the adapter's contract; a header whose key bytes are
/// invalid anyway decodes to "" rather than panicking or fabricating an
/// invalid &str.
#[test]
fn invalid_utf8_header_key_decodes_to_empty() {
    let bad_key: [u8; 3] = [0x64, 0xff, 0xfe]; // "d" + invalid bytes
    let headers = [header_raw(&bad_key, Some(b"v"))];
    let scope = CallScope;
    let views = unsafe { borrow_headers(&scope, headers.as_ptr(), headers.len() as c_int) };
    assert_eq!(views[0].key, "");
    assert_eq!(views[0].value, Some(&b"v"[..]));
}

/// A null array pointer or a non-positive count is an empty header list,
/// never a crash (the trampolines' null-guard arm).
#[test]
fn null_or_empty_header_arrays_decode_to_no_headers() {
    let scope = CallScope;
    assert!(unsafe { borrow_headers(&scope, std::ptr::null(), 4) }.is_empty());

    let headers = [header_raw(b"k", Some(b"v"))];
    assert!(unsafe { borrow_headers(&scope, headers.as_ptr(), 0) }.is_empty());
    assert!(unsafe { borrow_headers(&scope, headers.as_ptr(), -1) }.is_empty());
}
