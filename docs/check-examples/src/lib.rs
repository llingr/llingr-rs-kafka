// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

//! Compile-only harness for the llingr-kafka documentation samples.
//!
//! CI proves that every fenced `rust` sample in README.md and docs/*.md
//! compiles against the real llingr-kafka API.
//! build.rs mirrors those samples into a generated markdown file with `no_run`
//! forced (they compile but never run, because the samples call the blocking
//! `engine.run()`); the attribute below hands that file to rustdoc as doctests,
//! and `make docs-check` / the docs-check CI job run `cargo test --doc` to
//! compile them. A sample that stops compiling fails the check.
//!
//! Not published, and not part of the llingr-kafka crate build: it is its own
//! workspace and is excluded from the package.
#![doc = include_str!(concat!(env!("OUT_DIR"), "/samples.md"))]
