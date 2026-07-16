// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

//! Coverage for the engine's error-code mappings: the negative codes are
//! unreachable against a healthy engine, so they are exercised through the
//! extracted pure helpers. Every message asserted here is quoted by the
//! docs; a wording change must be deliberate.

use std::os::raw::c_char;

use crate::engine::{abi_mismatch_error, init_failure_text, run_result};

#[test]
fn run_result_maps_every_code_to_its_documented_text() {
    assert!(run_result(0).is_ok());

    let cases = [
        (-1, "engine not initialised"),
        (-2, "subscribe failed"),
        (-3, "engine panicked (recovered at FFI boundary)"),
        (-99, "unknown runtime error"),
        (7, "unknown runtime error"), // positive garbage also maps to the default text
    ];
    for (rc, want) in cases {
        let err = run_result(rc).expect_err("non-zero rc is an error");
        assert_eq!(err.code(), rc);
        assert_eq!(err.to_string(), format!("llingr error {rc}: {want}"));
    }
}

#[test]
fn abi_mismatch_error_text_is_the_documented_shape() {
    let err = abi_mismatch_error(1, 2);
    assert_eq!(err.code(), -1);
    assert_eq!(
        err.to_string(),
        "llingr error -1: llingr ABI mismatch: crate expects 1, library reports 2 \
         (rebuild libllingr to match this crate)"
    );
}

/// With no bridge-written text, each init return code maps to its stable
/// fallback; the codes are the bridge's contract, errAlreadyInit through
/// errBadOption.
#[test]
fn init_failure_fallback_text_per_code() {
    let empty: [c_char; 4] = [0; 4];
    let cases = [
        (-1, "already initialised"),
        (-2, "invalid configuration JSON"),
        (
            -3,
            "missing required config (brokers, topic, or consumer_group)",
        ),
        (-4, "failed to create adapter or connect to broker"),
        (-5, "invalid adapter or engine option"),
        (-6, "unknown initialisation error"),
        (42, "unknown initialisation error"),
    ];
    for (rc, want) in cases {
        assert_eq!(init_failure_text(rc, &empty, 0), want, "rc {rc}");
    }
}

/// When the bridge wrote error text, it wins over the fallback regardless of
/// the code, and only err_len bytes of the buffer are read.
#[test]
fn init_failure_bridge_text_wins_and_is_length_bounded() {
    let text = "franz adapter: broker unreachable";
    let mut buf = [0 as c_char; 64];
    for (i, &b) in text.as_bytes().iter().enumerate() {
        buf[i] = b as c_char;
    }
    assert_eq!(
        init_failure_text(-4, &buf, text.len() as i32),
        text,
        "bridge-written text wins over the fallback"
    );
    assert_eq!(
        init_failure_text(-4, &buf, 5),
        "franz",
        "only err_len bytes are read"
    );
}

/// Go error strings have no UTF-8 guarantee across the boundary: invalid
/// bytes decode lossily to the replacement character, never a panic.
#[test]
fn init_failure_text_decodes_invalid_utf8_lossily() {
    let bytes: [u8; 5] = [0x66, 0x6f, 0x6f, 0xff, 0xfe]; // "foo" + invalid
    let mut buf = [0 as c_char; 8];
    for (i, &b) in bytes.iter().enumerate() {
        buf[i] = b as c_char;
    }
    let text = init_failure_text(-5, &buf, bytes.len() as i32);
    assert!(text.starts_with("foo"), "{text}");
    assert!(
        text.contains('\u{FFFD}'),
        "invalid bytes become the replacement character: {text}"
    );
}
