// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

// Link 1 of the boundary translation chain: the franz native-record ->
// recordMeta extraction.

package main

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestMapFranzTimestampType(t *testing.T) {
	// kgo.RecordAttrs.TimestampType(): -1 none, 0 create, 1 log append.
	cases := []struct {
		franz int8
		want  int8
	}{
		{-1, tsNotAvailable},
		{0, tsCreateTime},
		{1, tsLogAppendTime},
		{99, tsNotAvailable}, // anything unexpected degrades to not-available
	}
	for _, c := range cases {
		if got := mapFranzTimestampType(c.franz); got != c.want {
			t.Errorf("mapFranzTimestampType(%d): got %d, want %d", c.franz, got, c.want)
		}
	}
}

func TestFranzMeta(t *testing.T) {
	ts := time.UnixMilli(1_700_000_000_000)
	record := &kgo.Record{
		Timestamp: ts,
		// Zero-value Attrs reports TimestampType() == 0 (create time), the
		// common case; the full kind mapping is pinned above, since kgo
		// does not expose an attrs setter to construct the other kinds.
		Headers: []kgo.RecordHeader{
			{Key: "trace-id", Value: []byte("abc")},
			{Key: "null-val", Value: nil},
		},
	}

	meta := franzMeta(record)
	if meta.tsKind != tsCreateTime {
		t.Errorf("tsKind: got %d, want %d (create time)", meta.tsKind, tsCreateTime)
	}
	if meta.tsMillis != 1_700_000_000_000 {
		t.Errorf("tsMillis: got %d, want 1700000000000", meta.tsMillis)
	}
	if len(meta.headers) != 2 {
		t.Fatalf("headers: got %d, want 2", len(meta.headers))
	}
	if meta.headers[0].key != "trace-id" || string(meta.headers[0].value) != "abc" {
		t.Errorf("header 0: got %q=%q", meta.headers[0].key, meta.headers[0].value)
	}
	if meta.headers[1].value != nil {
		t.Errorf("header 1: nil value must stay nil, got %q", meta.headers[1].value)
	}
}

func TestFranzHeadersEmpty(t *testing.T) {
	if got := franzHeaders(nil); got != nil {
		t.Errorf("nil headers: got %v, want nil", got)
	}
	if got := franzHeaders([]kgo.RecordHeader{}); got != nil {
		t.Errorf("empty headers: got %v, want nil", got)
	}
}
