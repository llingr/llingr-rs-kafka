// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

// Link 1 of the boundary translation chain: these tests
// prove what the Go side WRITES into the C ABI, read back through the same C
// struct layout a consumer of the arena uses. The Rust boundary tests prove
// the read side; the abi-check crate proves the two contract copies match.

package main

import (
	"bytes"
	"testing"
)

func TestMarshalHeadersRoundTrip(t *testing.T) {
	in := []bridgeHeader{
		{key: "trace-id", value: []byte("abc123")},
		{key: "dup", value: []byte("first")},
		{key: "dup", value: []byte("second")},         // duplicate keys allowed, order preserved
		{key: "tombstone", value: nil},                // null value
		{key: "empty-val", value: []byte{}},           // empty value, distinct from null
		{key: "", value: []byte("keyless")},           // empty key tolerated
		{key: "unicode-é", value: []byte{0x00, 0xff}}, // binary value, non-ASCII key
	}

	views, allocated := marshalHeadersView(in)
	if !allocated {
		t.Fatal("non-empty header list must allocate an arena")
	}
	if len(views) != len(in) {
		t.Fatalf("header count: got %d, want %d", len(views), len(in))
	}

	for i, want := range in {
		got := views[i]
		if got.key != want.key {
			t.Errorf("header %d key: got %q, want %q (wire order must be preserved)", i, got.key, want.key)
		}
		switch {
		case want.value == nil && got.value != nil:
			t.Errorf("header %d (%q): null value must survive as null, got %q", i, want.key, got.value)
		case want.value != nil && got.value == nil:
			t.Errorf("header %d (%q): value %q arrived as null", i, want.key, want.value)
		case !bytes.Equal(got.value, want.value):
			t.Errorf("header %d (%q) value: got %q, want %q", i, want.key, got.value, want.value)
		}
	}

	// Null and empty must remain distinguishable after the round trip.
	if views[3].value != nil {
		t.Error("tombstone: null value collapsed to non-null")
	}
	if views[4].value == nil {
		t.Error("empty-val: empty value collapsed to null")
	}
}

func TestMarshalHeadersEmpty(t *testing.T) {
	if _, allocated := marshalHeadersView(nil); allocated {
		t.Error("nil header list must not allocate")
	}
	if _, allocated := marshalHeadersView([]bridgeHeader{}); allocated {
		t.Error("empty header list must not allocate")
	}
}
