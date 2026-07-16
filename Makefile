# SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial
#
# Single entry point for building, testing, and exercising llingr-kafka. All the
# incantations (cgo cross flags, buildmode, cache volumes, compose) live here so
# a contributor only needs three knobs:
#
#   MODE    native | docker | auto   where the crate is built (default auto)
#   LIBC    glibc  | musl             target libc (musl is not yet supported)
#   PROFILE release | debug           optimisation level (default release)
#
# MODE=auto builds natively when Go 1.25+, a C compiler, and cargo are all
# present, otherwise it builds inside the Docker builder image (docker/
# Dockerfile.builder). Docker mode re-invokes the SAME make target with
# MODE=native inside that image, so the native and docker paths run identical
# commands and no build logic is duplicated.

MODE    ?= auto
LIBC    ?= glibc
PROFILE ?= release

.DEFAULT_GOAL := help

# --- Argument validation ----------------------------------------------------

ifeq ($(filter $(MODE),native docker auto),)
$(error MODE must be native, docker, or auto (got '$(MODE)'))
endif
ifeq ($(filter $(PROFILE),release debug),)
$(error PROFILE must be release or debug (got '$(PROFILE)'))
endif
ifeq ($(filter $(LIBC),glibc musl),)
$(error LIBC must be glibc or musl (got '$(LIBC)'))
endif

# musl seam (1 of 3; the others are docker/Dockerfile.builder and the *-musl arm
# in build.rs). A static c-archive needs only the first fix, but that fix is
# unmerged, so musl fails here honestly rather than producing a binary that
# segfaults in Go runtime init. Keep this message identical across the three
# seams; see docs/internal/MUSL.md for the flip instructions.
# The '#' in the issue numbers is escaped (\#): an unescaped '#' starts a Make
# comment and would truncate this message mid-sentence.
MUSL_MSG := LIBC=musl is unsupported: the Go engine c-archive crashes in runtime init on musl (Go assumes glibc's argc/argv/envp .init_array convention; golang/go\#13492, fix PR 69325 unmerged), and a dlopen route hits Go's Initial-Exec TLS which musl refuses for dlopen'd libraries (golang/go\#48596). Build with LIBC=glibc. See docs/internal/MUSL.md
ifeq ($(LIBC),musl)
$(error $(MUSL_MSG))
endif

# --- Toolchain detection (immediate, so `auto` resolves once) ----------------

HAVE_GO    := $(shell command -v go >/dev/null 2>&1 && go version 2>/dev/null | awk '{v=$$3; sub(/^go/,"",v); split(v,a,"."); if (a[1]>1 || (a[1]==1 && a[2]>=25)) print "yes"}')
HAVE_CC    := $(shell command -v cc >/dev/null 2>&1 && echo yes)
HAVE_CARGO := $(shell command -v cargo >/dev/null 2>&1 && echo yes)
HAVE_DOCKER:= $(shell command -v docker >/dev/null 2>&1 && echo yes)

ifeq ($(MODE),auto)
ifeq ($(HAVE_GO)/$(HAVE_CC)/$(HAVE_CARGO),yes/yes/yes)
RESOLVED_MODE := native
else
RESOLVED_MODE := docker
endif
else
RESOLVED_MODE := $(MODE)
endif

# --- Derived flags -----------------------------------------------------------

ifeq ($(PROFILE),release)
CARGO_PROFILE_FLAG := --release
GO_LDFLAGS         := -s -w
else
CARGO_PROFILE_FLAG :=
GO_LDFLAGS         :=
endif

# Host target triple, used to name the engine output directory so a prebuilt
# libllingr.a and LLINGR_LIB_DIR line up with what cargo links.
TARGET_TRIPLE := $(shell rustc -vV 2>/dev/null | awk '/^host:/ {print $$2}')

# Coverage measures this crate's own first-party code only. build.rs is a build
# script, not exercised by the test binaries, so it is kept out of the line
# counts; the upstream engine modules live in other repos and the crate's
# dependencies are excluded by cargo-llvm-cov by default. example/,
# docs-examples/ and abi-check/ are separate cargo packages and are not part of
# this coverage run at all.
COVERAGE_IGNORE := --ignore-filename-regex 'build\.rs'

BUILDER_IMAGE      := llingr-rs-kafka-builder:local
GO_CACHE_VOLUME    := llingr-rs-kafka-go
CARGO_CACHE_VOLUME := llingr-rs-kafka-cargo

# The example stack's compose file lives in example/. Running compose with -f
# from the repo root (rather than cd example) keeps the build context
# repo-root-relative, which the consumer image needs to reach Cargo.toml,
# build.rs, src/ and bridge/.
COMPOSE := docker compose -f example/docker-compose.yml

# Re-invoke the current make target ($@) inside the builder image with
# MODE=native. The repo is bind-mounted at /work; the Go and cargo caches are
# named volumes so repeat runs stay incremental. The cargo cache mounts only the
# registry subdirectory, never all of /root/.cargo, so the toolchain in
# /root/.cargo/bin is not shadowed.
define run-in-builder
	@command -v docker >/dev/null 2>&1 || { echo "error: MODE=$(MODE) resolved to docker but docker is not installed. Install Docker, or install Go 1.25+, a C compiler and Rust for a native build."; exit 1; }
	docker build --build-arg LIBC=$(LIBC) -t $(BUILDER_IMAGE) -f docker/Dockerfile.builder docker
	docker run --rm \
		-v "$(CURDIR)":/work -w /work \
		-v $(GO_CACHE_VOLUME):/go \
		-v $(CARGO_CACHE_VOLUME):/root/.cargo/registry \
		-e GOCACHE=/go/.cache/go-build \
		$(BUILDER_IMAGE) \
		make $@ MODE=native LIBC=$(LIBC) PROFILE=$(PROFILE)
endef

.PHONY: toolchains engine build test lint docs-check coverage example example-up example-down example-verify clean help

# --- Targets -----------------------------------------------------------------

toolchains:
	@echo "Toolchain detection:"
	@printf '  go     : '; if command -v go >/dev/null 2>&1; then go version; else echo "not found (need 1.25+)"; fi
	@printf '  cc     : '; if command -v cc >/dev/null 2>&1; then cc --version | head -1; else echo "not found"; fi
	@printf '  cargo  : '; if command -v cargo >/dev/null 2>&1; then cargo --version; else echo "not found"; fi
	@printf '  docker : '; if command -v docker >/dev/null 2>&1; then docker --version; else echo "not found"; fi
	@echo ""
	@echo "  host target triple : $(if $(TARGET_TRIPLE),$(TARGET_TRIPLE),unknown (no rustc))"
	@echo "  MODE=$(MODE) resolves to: $(RESOLVED_MODE)"
	@echo "  LIBC=$(LIBC), PROFILE=$(PROFILE)"

# Build libllingr.a on its own into dist/<triple>/, for LLINGR_LIB_DIR consumers
# and CI caches. Ordinary `make build` does not need this: the crate build.rs
# builds the engine itself.
engine:
ifeq ($(RESOLVED_MODE),docker)
	$(run-in-builder)
else
	@test -n "$(TARGET_TRIPLE)" || { echo "error: could not determine the host target triple (is rustc installed?)"; exit 1; }
	@test -f bridge/go.mod || { echo "error: bridge/go.mod not found; the Go composition root has not landed yet"; exit 1; }
	@mkdir -p dist/$(TARGET_TRIPLE)
	cd bridge && CGO_ENABLED=1 go build -tags netgo -buildmode=c-archive $(if $(GO_LDFLAGS),-ldflags "$(GO_LDFLAGS)",) -o ../dist/$(TARGET_TRIPLE)/libllingr.a .
	@echo "built dist/$(TARGET_TRIPLE)/libllingr.a"
	@echo "link it without rebuilding the engine: LLINGR_LIB_DIR=dist/$(TARGET_TRIPLE) cargo build $(CARGO_PROFILE_FLAG)"
endif

build:
ifeq ($(RESOLVED_MODE),docker)
	$(run-in-builder)
else
	cargo build --locked $(CARGO_PROFILE_FLAG)
endif

test:
ifeq ($(RESOLVED_MODE),docker)
	$(run-in-builder)
else
	cd bridge && go test ./...
	cargo test --locked $(CARGO_PROFILE_FLAG)
	# abi-check regenerates the C contract from the cgo-emitted header, which it
	# discovers at dist/<triple>/libllingr.h. Build the engine first so a fresh
	# clone's `make test` is self-sufficient rather than failing until `make
	# engine` has been run once by hand.
	$(MAKE) engine MODE=native LIBC=$(LIBC) PROFILE=$(PROFILE)
	cd abi-check && cargo build
endif

# Prove a build image (or a freshly provisioned host) can actually build AND
# LINK the engine, not merely that the tools are present. Run it INSIDE the
# environment you are validating; it is native-only by design (it never shells
# out to Docker, or it would be validating the wrong environment). Stronger than
# `toolchains`: it exercises the full chain and names the stage that fails.
# Stage 1 builds the engine archive (Go + cgo + C toolchain). Stage 2 compiles
# and LINKS the crate's test binary against that prebuilt archive (Rust
# toolchain + linker resolving the static engine and -lpthread -lm -ldl); a
# `cargo check` would not link, so a real binary is built. Prints a single
# PROVISIONED / NOT PROVISIONED verdict.
.PHONY: doctor
doctor:
	@echo "== llingr-kafka build-image doctor =="
	@$(MAKE) --no-print-directory toolchains
	@echo ""
	@echo "[1/2] engine: building dist/$(TARGET_TRIPLE)/libllingr.a (Go + cgo + C) ..."
	@$(MAKE) --no-print-directory engine MODE=native LIBC=$(LIBC) PROFILE=$(PROFILE) \
	  || { echo ""; echo "NOT PROVISIONED: stage 'engine' failed. Needs Go 1.25+ and a C compiler (cgo)."; exit 1; }
	@echo ""
	@echo "[2/2] link: compiling and LINKING a test binary against the prebuilt engine ..."
	@LLINGR_LIB_DIR=dist/$(TARGET_TRIPLE) cargo build --tests --locked \
	  || { echo ""; echo "NOT PROVISIONED: stage 'link' failed. Rust toolchain present, and can cc link the static engine (-lpthread -lm -ldl)?"; exit 1; }
	@echo ""
	@echo "PROVISIONED: this environment builds the engine and links it into a Rust binary."

# Identical commands to CI (see .github/workflows/lint.yml).
lint:
ifeq ($(RESOLVED_MODE),docker)
	$(run-in-builder)
else
	cargo fmt --check
	cargo clippy --all-targets --locked -- -D warnings
endif

# Compile (do not run) every fenced `rust` sample in README.md and docs/*.md.
# docs-examples/ mirrors them as no_run doctests (build.rs), so `cargo test
# --doc` compiles them without executing. Identical command to the docs-check CI
# job. NOTE: red until the engine module (Builder/Llingr/DemuxConfig/Options/
# Metrics) lands, because the samples reference those types; the CI wiring in
# pipeline.yml is deliberately left disabled until then.
docs-check:
ifeq ($(RESOLVED_MODE),docker)
	$(run-in-builder)
else
	cd docs-examples && cargo test --doc
endif

# Coverage over the crate's own Rust modules (src/*) and the Go bridge (bridge/*)
# only. NEEDS Go on PATH: the crate's tests build the Go bridge via build.rs.
# Emits coverage-rust.lcov and bridge/coverage-bridge.out plus a human-readable
# summary for each domain. This is the measurement command; the regression gate
# (--fail-under-lines) lives in the CI coverage job, not here.
coverage:
ifeq ($(RESOLVED_MODE),docker)
	$(run-in-builder)
else
	@echo "== Rust coverage (src/*) =="
	cargo llvm-cov --locked $(COVERAGE_IGNORE) --lcov --output-path coverage-rust.lcov
	cargo llvm-cov report $(COVERAGE_IGNORE) --summary-only
	@echo "== Go bridge coverage (bridge/*) =="
	cd bridge && go test -race -coverpkg=./... -coverprofile=coverage-bridge.out ./...
	cd bridge && go tool cover -func=coverage-bridge.out | tail -1
endif

# The example stack always runs via docker compose (it builds the crate,
# including the Go bridge, inside its own images), independent of MODE.
example:
	$(COMPOSE) build

example-up:
	$(COMPOSE) up --build

example-down:
	$(COMPOSE) down -v

# One-shot end-to-end proof: exit 0 means producer -> broker -> franz -> engine
# -> FFI -> Rust worked and the consumer stopped itself.
#
# Do NOT use `up --exit-code-from consumer`: that flag implies
# --abort-on-container-exit, so compose tears the WHOLE stack down (redpanda
# included) the instant the one-shot topic-init exits 0, before the consumer can
# resolve the broker (proven: the consumer then dies with "lookup redpanda: no
# such host"). Instead bring the stack up detached and wait on the consumer
# container by id. Use plain `docker wait <id>`, NOT `docker compose wait`, which
# was observed to return 1 on a known-success run. The logs are printed so the
# run's evidence is captured; `down -v` runs whether the wait succeeded or not,
# and the consumer's exit code is preserved as this target's result.
example-verify:
	$(COMPOSE) up -d --build
	@cid=$$($(COMPOSE) ps -q consumer); \
	  code=$$(docker wait "$$cid"); \
	  $(COMPOSE) logs; \
	  $(COMPOSE) down -v; \
	  echo "consumer exit code: $${code:-<none>}"; \
	  exit $${code:-1}

clean:
	rm -rf dist/ target/ abi-check/target/
	-$(COMPOSE) down -v --rmi local
	-docker rmi $(BUILDER_IMAGE)

help:
	@echo "llingr-kafka build. Variables: MODE=native|docker|auto  LIBC=glibc|musl  PROFILE=release|debug"
	@echo ""
	@echo "  make toolchains     - report go/cc/cargo/docker and what MODE=auto resolves to"
	@echo "  make engine         - build dist/<triple>/libllingr.a alone (for LLINGR_LIB_DIR / CI cache)"
	@echo "  make build          - build the crate (honours MODE/LIBC/PROFILE)"
	@echo "  make test           - bridge go test, cargo test, abi-check build"
	@echo "  make lint           - cargo fmt --check + clippy -D warnings (same as CI)"
	@echo "  make docs-check     - compile (not run) every rust sample in README/docs"
	@echo "  make coverage       - rust (src/*) + go-bridge (bridge/*) coverage; needs Go (tests build the bridge)"
	@echo "  make doctor         - prove this environment can build AND link the engine (PROVISIONED verdict)"
	@echo "  make example        - build both example images"
	@echo "  make example-up     - bring the example stack up (RedPanda + producer + consumer)"
	@echo "  make example-down   - tear the example stack down (-v)"
	@echo "  make example-verify - one-shot E2E: exit 0 proves the full chain"
	@echo "  make clean          - remove dist/, target/, and example/builder images"
	@echo ""
	@echo "  MODE=auto currently resolves to: $(RESOLVED_MODE)"
