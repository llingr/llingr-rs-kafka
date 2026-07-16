// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

// Builds the Go engine (bridge/) into a static c-archive during `cargo build`
// and links it, so `cargo add llingr-kafka` is the whole integration story on
// a machine with Go and Rust. Behaviour, in order (PLAN.md section 6):
//
//   1. DOCS_RS set        -> emit nothing and return (docs builds have no
//                            network and no Go toolchain).
//   2. LLINGR_LIB_DIR set -> link the prebuilt libllingr.a found there and
//                            skip Go entirely (CI caching, air-gapped hosts,
//                            `make engine` consumers).
//   3. otherwise          -> require Go 1.25+ on PATH, map the cargo target
//                            to GOOS/GOARCH, and `go build` the bridge into
//                            OUT_DIR as a c-archive.
//
// Static c-archive is the ONLY link mode: a single self-contained binary, no
// .so-beside-binary story, and the mode closest to future musl support.
// Docker is NEVER invoked from here: rust-analyzer runs build scripts
// constantly, and CI sandboxes and docs.rs have no daemon; failure messages
// name the Docker remedies instead.

use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    // Every environment variable that changes this script's behaviour MUST
    // be declared here: cargo fingerprints build-script runs and reuses the
    // cached output when nothing it tracks has changed. An untracked
    // variable (the donor prototype omitted DOCS_RS) means a `DOCS_RS=1
    // cargo check` poisons the cache and the next real build silently links
    // nothing.
    println!("cargo:rerun-if-env-changed=LLINGR_LIB_DIR");
    println!("cargo:rerun-if-env-changed=DOCS_RS");
    println!("cargo:rerun-if-env-changed=GOOS");
    println!("cargo:rerun-if-env-changed=GOARCH");
    println!("cargo:rerun-if-env-changed=CC");
    println!("cargo:rerun-if-env-changed=CXX");
    println!("cargo:rerun-if-env-changed=CARGO_CFG_PANIC");

    // The panic-to-dead-letter contract relies on unwinding: every FFI
    // trampoline contains a handler panic with catch_unwind and reports it
    // as a failed message. Under panic = "abort" the abort fires before
    // catch_unwind can run, so the first handler panic kills the whole
    // process instead of dead-lettering. Warn loudly rather than fail:
    // a consumer may knowingly accept process-per-message semantics.
    if std::env::var("CARGO_CFG_PANIC").as_deref() == Ok("abort") {
        println!(
            "cargo::warning=llingr-kafka: this profile sets panic = \"abort\", which DISABLES \
             the panic-to-dead-letter contract: a panicking handler will abort the whole \
             process instead of routing the message to the dead-letter handler."
        );
        println!(
            "cargo::warning=llingr-kafka: remove panic = \"abort\" from the profile \
             (llingr-kafka needs panic = \"unwind\", the default) unless a whole-process \
             abort per handler panic is acceptable."
        );
    }

    // 1. docs.rs: metadata-only build; nothing to compile or link.
    if std::env::var("DOCS_RS").is_ok() {
        return;
    }

    // 2. Prebuilt engine: link it and skip Go entirely.
    if let Ok(dir) = std::env::var("LLINGR_LIB_DIR") {
        let dir = PathBuf::from(&dir);
        // Resolve for a legible message; fall back to the raw path if it does
        // not exist yet (canonicalize fails on a missing dir).
        let resolved = dir.canonicalize().unwrap_or(dir);
        // A warning, not an error: `cargo check` and rust-analyzer must keep
        // working even when LLINGR_LIB_DIR points somewhere the library is
        // not yet built. Naming the resolved path and the expected file turns
        // an opaque `ld: library 'llingr' not found` into a fixable message.
        if !resolved.join("libllingr.a").exists() {
            println!(
                "cargo::warning=LLINGR_LIB_DIR is set to {} but libllingr.a is not there: \
                 linking will fail with `ld: library 'llingr' not found`. Point LLINGR_LIB_DIR \
                 at the directory holding libllingr.a (`make engine` builds it into \
                 dist/<target-triple>/), or unset it to build the engine from source.",
                resolved.display()
            );
        }
        emit_link(&resolved);
        return;
    }

    // 3. Build the engine from bridge/ with the Go toolchain.

    // The musl seam. A statically linked c-archive needs only the
    // golang/go#13492 fix, so this branch is the shortest path to musl once
    // upstream lands it; until then, fail honestly. This message is one of
    // three seams: keep its substance aligned with the Makefile's LIBC guard
    // and docker/Dockerfile, which contain the same canonical text.
    if std::env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("musl") {
        panic!(
            "musl target {} is unsupported: the Go engine c-archive crashes in runtime init \
             on musl (Go assumes glibc's argc/argv/envp .init_array convention; \
             golang/go#13492, fix PR 69325 unmerged), and a dlopen route hits Go's \
             Initial-Exec TLS which musl refuses for dlopen'd libraries (golang/go#48596). \
             Build against glibc (a *-gnu target; the Makefile and docker/Dockerfile \
             default to LIBC=glibc). See docs/internal/MUSL.md",
            std::env::var("TARGET").unwrap_or_default()
        );
    }

    require_go_1_25();

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let bridge = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bridge");
    let bridge = bridge.canonicalize().unwrap_or_else(|_| {
        panic!(
            "engine bridge source not found at {}: the Go bridge ships inside the crate, \
             so this working tree or package is incomplete",
            bridge.display()
        )
    });

    println!("cargo:rerun-if-changed={}", bridge.join("go.sum").display());
    for entry in std::fs::read_dir(&bridge).unwrap().flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "go") {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }

    let goos = match std::env::var("CARGO_CFG_TARGET_OS").as_deref() {
        Ok("linux") => "linux",
        Ok("macos") => "darwin",
        other => panic!("engine build: unsupported target OS {other:?}"),
    };
    let goarch = match std::env::var("CARGO_CFG_TARGET_ARCH").as_deref() {
        Ok("x86_64") => "amd64",
        Ok("aarch64") => "arm64",
        other => panic!("engine build: unsupported target arch {other:?}"),
    };

    // Cross-compilation. cgo compiles the engine's C glue with the host `cc`
    // unless CC names a cross toolchain; on a target != host build that host
    // compiler rejects the foreign arch with an opaque error deep in the cgo
    // step. Catch it here with the actual fix. A caller who already set CC is
    // trusted to have pointed it at the right toolchain.
    //
    // The darwin-to-darwin case (Apple Silicon <-> Intel) needs no external
    // toolchain: Apple clang is a cross compiler within one SDK, selected by
    // -arch, so the build supplies it rather than demanding a CC the
    // toolchain already provides. An explicit CC still wins.
    let user_cc = std::env::var("CC").ok().filter(|s| !s.is_empty());
    let cross = std::env::var("TARGET").ok() != std::env::var("HOST").ok();
    let darwin_to_darwin =
        goos == "darwin" && std::env::var("HOST").is_ok_and(|host| host.ends_with("-apple-darwin"));
    let darwin_cross_cc = if cross && user_cc.is_none() && darwin_to_darwin {
        let arch = if goarch == "amd64" { "x86_64" } else { "arm64" };
        Some(format!("cc -arch {arch}"))
    } else {
        None
    };
    if cross && user_cc.is_none() && darwin_cross_cc.is_none() {
        panic!(
            "engine build: cross-compiling to {} but CC is unset. cgo builds the engine's C \
             glue with the host compiler, which cannot target another architecture. Set CC \
             (and CXX) to a cross toolchain, e.g. CC=aarch64-linux-gnu-gcc \
             CXX=aarch64-linux-gnu-g++, or set LLINGR_LIB_DIR to a prebuilt library.",
            std::env::var("TARGET").unwrap_or_default()
        );
    }

    // netgo: pure-Go DNS, so the archive works on scratch images with no
    // nsswitch machinery. -s -w strips symbol tables from the Go side.
    let mut cmd = Command::new("go");
    cmd.current_dir(&bridge)
        .env("CGO_ENABLED", "1")
        .args(["build", "-tags", "netgo", "-buildmode", "c-archive"])
        .args(["-ldflags", "-s -w"]);
    // Only steer Go's target when the caller has not: an explicit GOOS/GOARCH
    // (a deliberate cross build) must win over the values derived from the
    // cargo target.
    if std::env::var_os("GOOS").is_none() {
        cmd.env("GOOS", goos);
    }
    if std::env::var_os("GOARCH").is_none() {
        cmd.env("GOARCH", goarch);
    }
    // Pass a supplied cross toolchain (or the derived darwin -arch form)
    // through to cgo.
    if let Some(cc) = user_cc.as_deref().or(darwin_cross_cc.as_deref()) {
        cmd.env("CC", cc);
    }
    if let Ok(cxx) = std::env::var("CXX") {
        if !cxx.is_empty() {
            cmd.env("CXX", cxx);
        }
    }
    let status = cmd
        .arg("-o")
        .arg(out_dir.join("libllingr.a"))
        .arg(".")
        .status()
        .expect("failed to run `go build` (is a C toolchain installed for cgo?)");
    assert!(status.success(), "engine `go build` failed");

    emit_link(&out_dir);
}

fn emit_link(dir: &Path) {
    println!("cargo:rustc-link-search=native={}", dir.display());
    println!("cargo:rustc-link-lib=static=llingr");
    // Go runtime C dependencies; after libllingr on the link line.
    println!("cargo:rustc-link-arg=-lpthread");
    println!("cargo:rustc-link-arg=-lm");
    println!("cargo:rustc-link-arg=-ldl");
    // On macOS the Go runtime verifies TLS certificates against the system
    // trust store through CoreFoundation and Security framework calls
    // (crypto/x509's darwin backend); a static archive leaves those symbol
    // references unresolved for the final link, so the frameworks must be
    // named here. Verified empirically: without them the first `cargo test`
    // fails with undefined _CFRelease / _SecTrust* symbols.
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!("cargo:rustc-link-lib=framework=CoreFoundation");
        println!("cargo:rustc-link-lib=framework=Security");
    }
}

fn require_go_1_25() {
    const REMEDIES: &str = "llingr-kafka compiles its Go engine during `cargo build`. Three \
         remedies: (1) install Go 1.25+ from https://go.dev/dl/, (2) build the engine once \
         with `make engine` (uses Docker) and set LLINGR_LIB_DIR=dist/<target-triple>, or \
         (3) build the whole application inside the provided builder image \
         (docker/Dockerfile).";

    let out = Command::new("go")
        .arg("version")
        .output()
        .unwrap_or_else(|_| panic!("Go toolchain not found on PATH. {REMEDIES}"));
    let text = String::from_utf8_lossy(&out.stdout);
    // "go version go1.25.0 linux/arm64"
    let ver = text
        .split_whitespace()
        .nth(2)
        .unwrap_or("")
        .trim_start_matches("go");
    let mut parts = ver.split('.');
    let major: u32 = parts.next().unwrap_or("0").parse().unwrap_or(0);
    let minor: u32 = parts.next().unwrap_or("0").parse().unwrap_or(0);
    assert!(
        major > 1 || (major == 1 && minor >= 25),
        "Go 1.25+ is required for the engine build, found {ver}. {REMEDIES}"
    );
}
