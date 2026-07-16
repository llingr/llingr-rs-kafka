// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

//! Compile-time proof that the hand-written FFI declarations in `src/ffi.rs`
//! match the C contract the Go bridge exports.
//!
//! `gen` is bindgen's view of the libllingr.h that cgo emits from the
//! authoritative C preamble in `bridge/main.go`. Every check below fails
//! COMPILATION if the two contract copies diverge, so `cargo build` of this
//! crate is the whole test: no binary is linked and nothing runs.
//!
//! Coverage:
//! - callback typedefs: a swapped argument, wrong width, or wrong order in
//!   any of the six `*Fn` types breaks the coercion functions;
//! - passed-by-pointer structs: size, alignment, and every field offset;
//! - exported functions: signature equality via if/else LUB coercion (two
//!   `fn` items only unify when their signatures are identical).
//!
//! The `llingr_on_*` registration functions are deliberately absent from the
//! export checks: bindgen declares their parameter as `Option<fn>` (C
//! function pointers are nullable) while the hand-written declarations take
//! the bare fn type (registration with NULL is meaningless). `Option<extern
//! "C" fn>` is guaranteed ABI-compatible with a nullable C function pointer
//! (the niche optimisation), and the payload type itself is pinned by the
//! coercion checks, so the intentional strictness costs no coverage.

#![allow(dead_code)]
// Both contract copies live in this one crate (the donor layout had the
// hand-written copy in a separate -sys crate), so rustc's
// clashing_extern_declarations lint sees the same symbols declared twice and
// flags exactly the two DOCUMENTED divergences: cgo headers drop `const`
// (llingr_init, llingr_emergency_stop) and bindgen wraps C function pointers
// in Option (llingr_on_*). Those divergences are deliberate and separately
// pinned (the const-normalising shim below, the niche-optimisation note
// above), so the lint is noise here.
#![allow(clashing_extern_declarations)]

use std::os::raw::{c_char, c_int};

// The hand-written contract copy under test, included by path: this dev-only
// crate deliberately does not depend on the llingr-kafka crate (that would
// build the Go engine just to read declarations).
#[path = "../../src/ffi.rs"]
mod ffi;

// Pass A: the structs exactly as bindgen sees them in C (layout checks).
mod gen_types {
    #![allow(
        non_camel_case_types,
        non_snake_case,
        non_upper_case_globals,
        unsafe_op_in_unsafe_fn
    )]
    include!(concat!(env!("OUT_DIR"), "/gen_types.rs"));
}

// Pass B: typedefs and exports, struct names aliased to the hand-written
// types so signatures can unify (see build.rs).
mod gen {
    #![allow(
        non_camel_case_types,
        non_snake_case,
        non_upper_case_globals,
        unsafe_op_in_unsafe_fn
    )]
    #![allow(unused_imports)]
    include!(concat!(env!("OUT_DIR"), "/gen_fns.rs"));
}

// ---------------------------------------------------------------------------
// Callback typedefs: hand-written type must coerce into bindgen's.
// ---------------------------------------------------------------------------

fn process_fn_matches(f: ffi::ProcessFn) -> gen::llingr_process_fn {
    Some(f)
}
fn deadletter_fn_matches(f: ffi::DeadLetterFn) -> gen::llingr_deadletter_fn {
    Some(f)
}
fn metrics_fn_matches(f: ffi::MetricsFn) -> gen::llingr_metrics_fn {
    Some(f)
}
fn shutdown_fn_matches(f: ffi::ShutdownFn) -> gen::llingr_shutdown_fn {
    Some(f)
}
fn log_fn_matches(f: ffi::LogFn) -> gen::llingr_log_fn {
    Some(f)
}
fn bandwidth_fn_matches(f: ffi::BandwidthFn) -> gen::llingr_bandwidth_fn {
    Some(f)
}

// ---------------------------------------------------------------------------
// Passed-by-pointer structs: layout identity, field by field.
// ---------------------------------------------------------------------------

macro_rules! assert_layout {
    ($ours:ty, $theirs:ty, $($field:ident),+ $(,)?) => {
        const _: () = {
            use std::mem::{align_of, offset_of, size_of};
            assert!(size_of::<$ours>() == size_of::<$theirs>());
            assert!(align_of::<$ours>() == align_of::<$theirs>());
            $(assert!(offset_of!($ours, $field) == offset_of!($theirs, $field));)+
        };
    };
}

assert_layout!(
    ffi::HeaderRaw,
    gen_types::llingr_header,
    key,
    key_len,
    value,
    value_len,
);

assert_layout!(
    ffi::BrokerInfoRaw,
    gen_types::llingr_broker_info,
    id,
    id_len,
    host,
    host_len,
    port,
    port_len,
    rack,
    rack_len,
);

assert_layout!(
    ffi::PartitionBandwidthRaw,
    gen_types::llingr_partition_bandwidth,
    ts_unix_ns,
    received_bytes,
    transmitted_bytes,
    received_message_count,
    compressed_bytes,
    uncompressed_bytes,
    id,
    leader,
    leader_len,
    compression,
    compression_len,
);

// ---------------------------------------------------------------------------
// Exported functions: the two `fn` items must LUB-coerce to one pointer type,
// which the compiler only permits for identical signatures.
// ---------------------------------------------------------------------------

macro_rules! assert_same_signature {
    ($name:ident, $ours:path, $theirs:path) => {
        fn $name(pick_ours: bool) {
            let _ = if pick_ours { $ours } else { $theirs };
        }
    };
}

assert_same_signature!(
    check_abi_version,
    ffi::llingr_abi_version,
    gen::llingr_abi_version
);
assert_same_signature!(check_run, ffi::llingr_run, gen::llingr_run);
assert_same_signature!(check_stop, ffi::llingr_stop, gen::llingr_stop);
assert_same_signature!(
    check_take_snapshot,
    ffi::llingr_take_snapshot,
    gen::llingr_take_snapshot
);
assert_same_signature!(
    check_free_string,
    ffi::llingr_free_string,
    gen::llingr_free_string
);

// llingr_init needs const normalisation: cgo-generated headers drop `const`
// (Go has no const pointers), so the header says `char*` where the
// hand-written declaration correctly says `*const c_char`. Constness does
// not exist at the C ABI level. The shim compiles against the hand-written
// declaration (any drift in its arg count, order, or widths breaks the
// body), and the LUB check compares the normalised shape against bindgen's.
unsafe extern "C" fn init_const_normalised(
    config_json: *mut c_char,
    config_len: c_int,
    err_buf: *mut c_char,
    err_cap: c_int,
    err_len_out: *mut c_int,
) -> c_int {
    unsafe { ffi::llingr_init(config_json, config_len, err_buf, err_cap, err_len_out) }
}
assert_same_signature!(check_init, init_const_normalised, gen::llingr_init);
