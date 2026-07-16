# SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial
#
# Builder image with both toolchains: Go 1.25, a C compiler for cgo, stable
# Rust, make, and libclang for abi-check's bindgen. MODE=docker builds run
# inside it: the repo is bind-mounted at run time and the Makefile re-invokes
# the requested target with MODE=native, so the image never COPYs repo
# sources and holds no build logic of its own.
#
# Debian/glibc, never Alpine: the engine is glibc-only for now; see the LIBC
# branch below and docs/internal/MUSL.md.
# syntax=docker/dockerfile:1

FROM golang:1.25-bookworm

# musl seam, one of three with the Makefile and the *-musl branch in build.rs.
# Keep this message identical to the Makefile's MUSL_MSG.
ARG LIBC=glibc
RUN if [ "$LIBC" != glibc ]; then \
      echo "LIBC=musl is unsupported: the Go engine c-archive crashes in runtime init on musl (Go assumes glibc's argc/argv/envp .init_array convention; golang/go#13492, fix PR 69325 unmerged), and a dlopen route hits Go's Initial-Exec TLS which musl refuses for dlopen'd libraries (golang/go#48596). Build with LIBC=glibc. See docs/internal/MUSL.md." >&2; \
      exit 1; \
    fi

# golang:1.25-bookworm already provides go, gcc, cc, and curl. Add make, the
# in-container entry point, and libclang-dev for abi-check's bindgen over the
# cgo-emitted header.
RUN apt-get update \
 && apt-get install -y --no-install-recommends make libclang-dev \
 && rm -rf /var/lib/apt/lists/*

# Rust stable, minimal profile, plus the two components make lint needs.
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --default-toolchain stable --profile minimal -c rustfmt -c clippy
ENV PATH=/root/.cargo/bin:$PATH

WORKDIR /work
