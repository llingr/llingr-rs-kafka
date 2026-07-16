// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package main

import (
	"strings"
	"testing"
	"time"
)

func TestParseBandwidth(t *testing.T) {
	cases := []struct {
		name      string
		cfg       *bandwidthConfig
		wantStats time.Duration
		wantFlush time.Duration
		wantErr   string
	}{
		{name: "absent", cfg: nil},
		{name: "empty", cfg: &bandwidthConfig{}},
		{
			name:      "both set",
			cfg:       &bandwidthConfig{StatsInterval: "5000ms", FlushInterval: "15000ms"},
			wantStats: 5 * time.Second,
			wantFlush: 15 * time.Second,
		},
		{
			name:    "unparseable stats",
			cfg:     &bandwidthConfig{StatsInterval: "fast"},
			wantErr: "invalid bandwidth statsInterval",
		},
		{
			name:    "stats below engine minimum",
			cfg:     &bandwidthConfig{StatsInterval: "500ms"},
			wantErr: "at least 1 second",
		},
		{
			name:    "stats above engine maximum",
			cfg:     &bandwidthConfig{StatsInterval: "13h"},
			wantErr: "must not exceed 12 hours",
		},
		{
			name:    "non-positive flush",
			cfg:     &bandwidthConfig{FlushInterval: "0ms"},
			wantErr: "must be positive",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, berr := parseBandwidth(tc.cfg)
			if tc.wantErr != "" {
				if berr == nil {
					t.Fatalf("expected error containing %q, got none", tc.wantErr)
				}
				if berr.code != errBadOption {
					t.Fatalf("expected errBadOption, got %d", berr.code)
				}
				if !strings.Contains(berr.msg, tc.wantErr) {
					t.Fatalf("error %q does not contain %q", berr.msg, tc.wantErr)
				}
				return
			}
			if berr != nil {
				t.Fatalf("unexpected error: %v", berr)
			}
			if got.statsInterval != tc.wantStats || got.flushInterval != tc.wantFlush {
				t.Fatalf("got (%s, %s), want (%s, %s)",
					got.statsInterval, got.flushInterval, tc.wantStats, tc.wantFlush)
			}
		})
	}
}
