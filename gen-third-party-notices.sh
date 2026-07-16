#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial
#
# Generates the THIRD-PARTY-NOTICES file from the Go modules actually compiled
# into the engine archive, so the notices shipped beside an artifact always
# match that artifact exactly and a dependency bump can never leave the notices
# stale.
#
# Why this exists: llingr-kafka links the Go engine statically (go build
# -buildmode=c-archive), so every application binary that embeds this crate also
# embeds the engine's Go dependencies. franz-go and its submodules are
# BSD-3-Clause and require their copyright notice to travel with binary
# distributions; klauspost/compress, pierrec/lz4 and golang.org/x/crypto carry
# their own permissive notices. None of these are visible to Rust-side tooling
# (cargo-deny, FOSSA and SBOM scanners see only the Rust crate graph), so this
# file is the record of what actually ships in the binary. Distribute it
# alongside any binary built from this crate.
#
# The module set is enumerated from the bridge source with `go list -deps`, not
# from `go version -m` of the built library: a c-archive (.a) carries no
# buildinfo that `go version -m` can read (it works only on Go executables and
# c-shared libraries), whereas `go list -deps .` reports precisely the modules
# whose packages are compiled into the archive, and needs no built artifact.
#
# Regenerate after every engine bump (the pinned Go module versions in
# bridge/go.mod change), then commit the refreshed THIRD-PARTY-NOTICES.
#
# Usage: gen-third-party-notices.sh <bridge-dir> <output>
#   e.g. ./gen-third-party-notices.sh bridge THIRD-PARTY-NOTICES
set -euo pipefail

BRIDGE="${1:?usage: gen-third-party-notices.sh <bridge-dir> <output>}"
OUT="${2:?usage: gen-third-party-notices.sh <bridge-dir> <output>}"

command -v go >/dev/null 2>&1 || { echo "error: a Go toolchain is required"; exit 1; }
[ -f "$BRIDGE/go.mod" ] || { echo "error: $BRIDGE/go.mod not found (point at the bridge/ module directory)"; exit 1; }

# Resolve to absolute paths before the cd below.
OUT="$(cd "$(dirname "$OUT")" && pwd)/$(basename "$OUT")"
BRIDGE="$(cd "$BRIDGE" && pwd)"
GOMODCACHE="$(go env GOMODCACHE)"

# Third-party modules whose packages are compiled into the c-archive. `go list
# -deps .` walks every package the bridge's main package imports transitively;
# mapping each to its module and dropping the main module (Main) and the stdlib
# (no .Module) yields the embedded set. The github.com/llingr/* modules are the
# llingr components covered by the repository LICENSE, not third-party.
cd "$BRIDGE"
modules="$(go list -deps -f '{{with .Module}}{{if not .Main}}{{.Path}}@{{.Version}}{{end}}{{end}}' . \
  | grep -v '^$' | grep -v '^github.com/llingr/' | sort -u)"
[ -n "$modules" ] || { echo "error: no embedded third-party modules found for $BRIDGE"; exit 1; }

# Go module cache path escaping: uppercase letters become !lowercase.
escape() {
  printf '%s' "$1" | awk '{
    out = ""
    for (i = 1; i <= length($0); i++) {
      c = substr($0, i, 1)
      if (c ~ /[A-Z]/) out = out "!" tolower(c); else out = out c
    }
    print out
  }'
}

{
  echo "THIRD-PARTY NOTICES"
  echo "==================="
  echo ""
  echo "llingr-kafka statically links the llingr message-processing engine (Go),"
  echo "so every binary built from this crate embeds the Go modules listed below."
  echo "Their licences are reproduced verbatim. GENERATED from the bridge source"
  echo "(go list -deps) by gen-third-party-notices.sh; do not edit by hand. The"
  echo "llingr components (github.com/llingr/*) are covered by the repository"
  echo "LICENSE and are not repeated here. Distribute this file alongside any"
  echo "binary built from this crate."
  echo ""
  echo "Embedded third-party modules:"
  printf '%s\n' "$modules" | sed 's/^/  - /'
} > "$OUT"

emit_section() { # $1 = heading, $2 = file to append verbatim
  {
    echo ""
    echo "==============================================================================="
    echo "$1"
    echo "==============================================================================="
    echo ""
    cat "$2"
  } >> "$OUT"
}

for module_at_version in $modules; do
  module="${module_at_version%@*}"
  version="${module_at_version#*@}"
  dir="$GOMODCACHE/$(escape "$module")@$version"
  [ -d "$dir" ] || { echo "error: $dir not in the module cache (run 'go mod download' in $BRIDGE first)"; exit 1; }

  licence=""
  for name in LICENSE LICENSE.txt LICENSE.md LICENCE COPYING; do
    if [ -f "$dir/$name" ]; then
      licence="$dir/$name"
      break
    fi
  done
  [ -n "$licence" ] || { echo "error: no licence file found for $module in $dir"; exit 1; }
  emit_section "$module  $version" "$licence"

  # Apache-2.0 requires propagating an upstream NOTICE file when one exists.
  if [ -f "$dir/NOTICE" ]; then
    emit_section "$module  $version  (NOTICE)" "$dir/NOTICE"
  fi
done

echo "Wrote $OUT covering:"
printf '%s\n' "$modules" | sed 's/^/  /'
