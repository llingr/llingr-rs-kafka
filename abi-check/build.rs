// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

//! Regenerates the C contract from the header cgo emits alongside the engine
//! archive. Only `llingr_*` items are kept: the Go runtime scaffolding in the
//! header (GoString and friends) is irrelevant to the ABI under test.
//!
//! The bindgen passes below catch a divergence between the two C contract
//! copies. They do NOT, on their own, force the ABI version to move
//! when the contract changes, and their LUB check treats a same-typed argument
//! reorder as identical. The lock mechanism below closes both gaps:
//!
//! - It snapshots the `llingr_*` declaration text from the emitted header
//!   (parameter names included, so a same-typed reorder changes the hash) and
//!   pins the hash in `abi.lock`.
//! - It reads the two version constants (`const abiVersion` in
//!   `bridge/main.go`, `LLINGR_ABI_VERSION` in `src/ffi.rs`) and fails the
//!   build when the contract moved but neither constant did.
//!
//! A change to the FFI contract increments both version constants and
//! regenerates the lock with `UPDATE_ABI_LOCK=1 cargo build`. A pure
//! ABI-meaning change (identical C declarations, new semantics, e.g. a new
//! sentinel value) also increments the constants, with the lock regenerated
//! deliberately.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

/// 64-bit FNV-1a of the given bytes, as lowercase hex. Inlined to avoid adding
/// a hashing dependency: the snapshot only needs a stable, collision-resistant
/// fingerprint, not a cryptographic one.
fn fnv1a_hex(bytes: &[u8]) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

/// Removes C block and line comments. The header is generated ASCII, so byte
/// scanning is safe. Comments are dropped BEFORE splitting on `;` so that a
/// stray semicolon inside a comment cannot fragment a statement, and so that
/// prose edits to the header comments never churn the snapshot.
fn strip_comments(src: &str) -> String {
    let b = src.as_bytes();
    let mut out = String::with_capacity(src.len());
    let mut i = 0;
    while i < b.len() {
        if i + 1 < b.len() && b[i] == b'/' && b[i + 1] == b'*' {
            i += 2;
            while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                i += 1;
            }
            i += 2;
            out.push(' ');
        } else if i + 1 < b.len() && b[i] == b'/' && b[i + 1] == b'/' {
            i += 2;
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
        } else {
            out.push(b[i] as char);
            i += 1;
        }
    }
    out
}

/// The ABI-relevant snapshot of the header: comment-free, whitespace-collapsed
/// statements that mention `llingr_`, in source order. Keeping only `llingr_`
/// statements excludes the cgo boilerplate (GoString and friends), so a Go
/// toolchain upgrade that reshuffles the scaffolding does not churn the hash.
/// Parameter names survive, so a same-typed argument reorder DOES change it.
fn abi_signature(header_src: &str) -> String {
    strip_comments(header_src)
        .split(';')
        .map(|stmt| stmt.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|stmt| stmt.contains("llingr_"))
        .collect::<Vec<_>>()
        .join(";\n")
}

/// Pulls the integer that follows `marker` in `src` (e.g. the number after
/// `const abiVersion =`). Panics with a build-stopping message if the marker
/// is gone or is not followed by digits: a moved or renamed constant must not
/// silently disable the version check.
fn version_after(src: &str, marker: &str, file: &Path) -> u64 {
    let start = src.find(marker).unwrap_or_else(|| {
        panic!(
            "abi-check: could not find `{marker}` in {}. The ABI version constant \
             may have been renamed or moved; update abi-check/build.rs to match.",
            file.display()
        )
    });
    let digits: String = src[start + marker.len()..]
        .trim_start()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    if digits.is_empty() {
        panic!(
            "abi-check: found `{marker}` in {} but no integer followed it.",
            file.display()
        );
    }
    digits.parse().expect("version digits parse as u64")
}

/// Reads `version = N` and `signature = HEX` out of an existing lock file.
fn parse_lock(src: &str) -> Option<(u64, String)> {
    let mut version = None;
    let mut signature = None;
    for line in src.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("version") {
            version = rest
                .trim_start()
                .strip_prefix('=')
                .and_then(|v| v.trim().parse().ok());
        } else if let Some(rest) = line.strip_prefix("signature") {
            signature = rest
                .trim_start()
                .strip_prefix('=')
                .map(|s| s.trim().to_string());
        }
    }
    Some((version?, signature?))
}

/// Locate the cgo-emitted header: LLINGR_HEADER wins; otherwise the first
/// dist/<triple>/libllingr.h that `make engine` produced.
fn find_header(repo_root: &Path) -> PathBuf {
    if let Ok(header) = env::var("LLINGR_HEADER") {
        return PathBuf::from(header);
    }
    let dist = repo_root.join("dist");
    if let Ok(entries) = fs::read_dir(&dist) {
        for entry in entries.flatten() {
            let candidate = entry.path().join("libllingr.h");
            if candidate.exists() {
                return candidate;
            }
        }
    }
    panic!(
        "abi-check: no cgo-emitted header found under {}: run `make engine` first \
         (it builds dist/<target-triple>/libllingr.a with the header beside it), \
         or set LLINGR_HEADER to the header's path",
        dist.display()
    );
}

fn main() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest.join("..");

    let header = find_header(&repo_root);
    if !header.exists() {
        panic!(
            "{} not found: run `make engine` first, or set LLINGR_HEADER",
            header.display()
        );
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    // Pass A: the structs as bindgen genuinely sees them in C, for the
    // layout assertions.
    bindgen::Builder::default()
        .header(header.display().to_string())
        .allowlist_type("llingr_header|llingr_broker_info|llingr_partition_bandwidth")
        .generate()
        .expect("bindgen (types pass) failed on libllingr.h")
        .write_to_file(out_dir.join("gen_types.rs"))
        .expect("writing gen_types.rs failed");

    // Pass B: the callback typedefs and exported functions, with the struct
    // names aliased to the ffi.rs types. Signature checks against this
    // module compare everything EXCEPT the structs' nominal identity (Rust fn
    // types cannot unify across two layout-identical nominal structs); the
    // structs themselves are covered by pass A.
    bindgen::Builder::default()
        .header(header.display().to_string())
        .allowlist_type("llingr_.*_fn")
        .allowlist_function("llingr_.*")
        .blocklist_type("llingr_header|llingr_broker_info|llingr_partition_bandwidth")
        .raw_line("use crate::ffi::HeaderRaw as llingr_header;")
        .raw_line("use crate::ffi::BrokerInfoRaw as llingr_broker_info;")
        .raw_line("use crate::ffi::PartitionBandwidthRaw as llingr_partition_bandwidth;")
        .generate()
        .expect("bindgen (functions pass) failed on libllingr.h")
        .write_to_file(out_dir.join("gen_fns.rs"))
        .expect("writing gen_fns.rs failed");

    // ABI version lock. bindgen proves the two contract copies agree; this
    // proves the version constants moved whenever the declarations did.
    let main_go = repo_root.join("bridge/main.go");
    let ffi_rs = repo_root.join("src/ffi.rs");
    let lock_path = manifest.join("abi.lock");

    let header_src = fs::read_to_string(&header)
        .unwrap_or_else(|e| panic!("abi-check: reading {} failed: {e}", header.display()));
    let signature = fnv1a_hex(abi_signature(&header_src).as_bytes());

    let main_go_src = fs::read_to_string(&main_go)
        .unwrap_or_else(|e| panic!("abi-check: reading {} failed: {e}", main_go.display()));
    let ffi_src = fs::read_to_string(&ffi_rs)
        .unwrap_or_else(|e| panic!("abi-check: reading {} failed: {e}", ffi_rs.display()));

    let go_version = version_after(&main_go_src, "const abiVersion =", &main_go);
    let rust_version = version_after(&ffi_src, "LLINGR_ABI_VERSION: c_int =", &ffi_rs);
    if go_version != rust_version {
        panic!(
            "abi-check: ABI version constants disagree: bridge/main.go declares {go_version} \
             but ffi.rs declares {rust_version}. Set both to the same value."
        );
    }
    let version = go_version;

    let lock_body = format!("version = {version}\nsignature = {signature}\n");
    let regenerate = "UPDATE_ABI_LOCK=1 cargo build";

    if env::var("UPDATE_ABI_LOCK")
        .map(|v| !v.is_empty())
        .unwrap_or(false)
    {
        fs::write(&lock_path, &lock_body)
            .unwrap_or_else(|e| panic!("abi-check: writing {} failed: {e}", lock_path.display()));
        println!(
            "cargo:warning=abi-check: wrote abi.lock (version {version}, signature {signature})"
        );
    } else {
        let locked = fs::read_to_string(&lock_path).unwrap_or_else(|_| {
            panic!(
                "abi-check: {} is missing. Generate it with: {regenerate}",
                lock_path.display()
            )
        });
        let (lock_version, lock_signature) = parse_lock(&locked).unwrap_or_else(|| {
            panic!(
                "abi-check: {} is malformed. Regenerate it with: {regenerate}",
                lock_path.display()
            )
        });

        match (lock_signature == signature, lock_version == version) {
            (true, true) => {}
            (false, true) => panic!(
                "abi-check: the FFI contract changed but the ABI version did not (still \
                 {version}). Increment abiVersion (bridge/main.go) and LLINGR_ABI_VERSION \
                 (src/ffi.rs), then regenerate abi-check/abi.lock with: {regenerate}"
            ),
            (false, false) => panic!(
                "abi-check: the FFI contract changed and the ABI version moved (locked \
                 {lock_version}, now {version}). If this is the intended coordinated change, \
                 regenerate the lock with: {regenerate}"
            ),
            (true, false) => panic!(
                "abi-check: the ABI version changed (locked {lock_version}, now {version}) \
                 but the header signature is byte-identical. A version change with no contract \
                 change is suspicious. Either revert the version, or, if the ABI MEANING \
                 changed without a declaration change (e.g. a new sentinel value), regenerate \
                 the lock deliberately with: {regenerate}"
            ),
        }
    }

    println!("cargo:rerun-if-changed={}", header.display());
    println!("cargo:rerun-if-changed={}", main_go.display());
    println!("cargo:rerun-if-changed={}", ffi_rs.display());
    println!("cargo:rerun-if-changed={}", lock_path.display());
    println!("cargo:rerun-if-env-changed=LLINGR_HEADER");
    println!("cargo:rerun-if-env-changed=UPDATE_ABI_LOCK");
}
