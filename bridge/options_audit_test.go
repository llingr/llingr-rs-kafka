// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

// Coverage for the curated kgo option keys and the llingr.poll.error.*
// adapter options.

package main

import (
	"strings"
	"testing"
	"time"
)

func TestAuditKeysTranslateToOptions(t *testing.T) {
	// Each key that takes a value translates to exactly one kgo option.
	opts, berr := franzKgoOpts(map[string]string{
		"socket.connection.setup.timeout.ms": "5000",
		"connections.max.idle.ms":            "60000",
		"llingr.request.retries":             "10",
		"llingr.retry.timeout.ms":            "45000",
		"llingr.max.concurrent.fetches":      "4",
	})
	if berr != nil {
		t.Fatalf("unexpected error: %v", berr)
	}
	if len(opts) != 5 {
		t.Fatalf("got %d opts, want 5", len(opts))
	}
}

func TestAuditBooleanKeysDefaultRestatementIsANoOp(t *testing.T) {
	// Restating the kgo default is valid config that sets nothing; the
	// non-default value produces exactly one option.
	cases := []struct {
		key        string
		defaultVal string
		activeVal  string
	}{
		{"allow.auto.create.topics", "false", "true"},
		{"check.crcs", "true", "false"},
		{"enable.metrics.push", "true", "false"},
	}
	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			opts, berr := franzKgoOpts(map[string]string{tc.key: tc.defaultVal})
			if berr != nil {
				t.Fatalf("default restatement must be accepted: %v", berr)
			}
			if len(opts) != 0 {
				t.Fatalf("default restatement produced %d opts, want 0", len(opts))
			}

			opts, berr = franzKgoOpts(map[string]string{tc.key: tc.activeVal})
			if berr != nil {
				t.Fatalf("active value must be accepted: %v", berr)
			}
			if len(opts) != 1 {
				t.Fatalf("active value produced %d opts, want 1", len(opts))
			}

			if _, berr := franzKgoOpts(map[string]string{tc.key: "maybe"}); berr == nil {
				t.Fatal("non-boolean value must be rejected")
			}
		})
	}
}

func TestAuditKeysRejectInvalidValues(t *testing.T) {
	for key, bad := range map[string]string{
		"socket.connection.setup.timeout.ms": "0",
		"connections.max.idle.ms":            "-1",
		"llingr.request.retries":             "0",
		"llingr.retry.timeout.ms":            "soon",
		"llingr.max.concurrent.fetches":      "-1",
	} {
		if _, berr := franzKgoOpts(map[string]string{key: bad}); berr == nil {
			t.Fatalf("%s=%q must be rejected", key, bad)
		}
	}
}

func TestUnknownKeyListingIncludesAuditAdditions(t *testing.T) {
	_, berr := franzKgoOpts(map[string]string{"no.such.key": "x"})
	if berr == nil {
		t.Fatal("expected unknown-key error")
	}
	for _, want := range []string{
		"socket.connection.setup.timeout.ms",
		"connections.max.idle.ms",
		"llingr.request.retries",
		"llingr.retry.timeout.ms",
		"llingr.max.concurrent.fetches",
		"allow.auto.create.topics",
		"check.crcs",
		"enable.metrics.push",
	} {
		if !strings.Contains(berr.msg, want) {
			t.Fatalf("supported-keys listing missing %s: %s", want, berr.msg)
		}
	}
}

func TestPopPollErrorOptionsHappyPaths(t *testing.T) {
	cfg := map[string]string{
		"llingr.poll.error.bail.after.ms":   "300000", // 5m, inside [1m, 1h]
		"llingr.poll.error.log.interval.ms": "2000",
		"llingr.poll.error.backoff.ms":      "100",
		"client.id":                         "untouched",
	}
	opts, berr := popPollErrorOptions(cfg)
	if berr != nil {
		t.Fatalf("unexpected error: %v", berr)
	}
	if len(opts) != 3 {
		t.Fatalf("got %d adapter options, want 3", len(opts))
	}
	// The keys were consumed; unrelated keys stay for the option table.
	for _, key := range []string{
		"llingr.poll.error.bail.after.ms",
		"llingr.poll.error.log.interval.ms",
		"llingr.poll.error.backoff.ms",
	} {
		if _, ok := cfg[key]; ok {
			t.Fatalf("%s was not consumed", key)
		}
	}
	if _, ok := cfg["client.id"]; !ok {
		t.Fatal("unrelated key was consumed")
	}
}

func TestPopPollErrorOptionsZeroDisables(t *testing.T) {
	// 0 is the documented disable value for the bail and the backoff.
	opts, berr := popPollErrorOptions(map[string]string{
		"llingr.poll.error.bail.after.ms": "0",
		"llingr.poll.error.backoff.ms":    "0",
	})
	if berr != nil {
		t.Fatalf("zero must be accepted as disable: %v", berr)
	}
	if len(opts) != 2 {
		t.Fatalf("got %d adapter options, want 2", len(opts))
	}
}

func TestPopPollErrorOptionsRejectsOutOfRangeLoudly(t *testing.T) {
	// The adapter would clamp these SILENTLY; the bridge must reject them
	// with the range in the error instead.
	cases := []struct {
		name    string
		pairs   map[string]string
		wantSub string
	}{
		{
			name:    "bail below adapter minimum",
			pairs:   map[string]string{"llingr.poll.error.bail.after.ms": "59999"},
			wantSub: "between 1m0s and 1h0m0s",
		},
		{
			name:    "bail above adapter maximum",
			pairs:   map[string]string{"llingr.poll.error.bail.after.ms": "3600001"},
			wantSub: "between 1m0s and 1h0m0s",
		},
		{
			name:    "backoff above adapter maximum",
			pairs:   map[string]string{"llingr.poll.error.backoff.ms": "5001"},
			wantSub: "at most 5s",
		},
		{
			name:    "log interval must be positive",
			pairs:   map[string]string{"llingr.poll.error.log.interval.ms": "0"},
			wantSub: "positive integer",
		},
		{
			name:    "bail must be an integer",
			pairs:   map[string]string{"llingr.poll.error.bail.after.ms": "10m"},
			wantSub: "non-negative integer",
		},
		{
			name:    "negative backoff rejected",
			pairs:   map[string]string{"llingr.poll.error.backoff.ms": "-1"},
			wantSub: "non-negative integer",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, berr := popPollErrorOptions(tc.pairs)
			if berr == nil {
				t.Fatal("expected an error")
			}
			if berr.code != errBadOption {
				t.Fatalf("code = %d, want errBadOption (%d)", berr.code, errBadOption)
			}
			if !strings.Contains(berr.msg, tc.wantSub) {
				t.Fatalf("error %q does not contain %q", berr.msg, tc.wantSub)
			}
		})
	}
}

func TestPopPollErrorOptionsBoundaryValuesAccepted(t *testing.T) {
	// The exact adapter bounds are legal values, not errors.
	for _, pairs := range []map[string]string{
		{"llingr.poll.error.bail.after.ms": "60000"},   // exactly 1m
		{"llingr.poll.error.bail.after.ms": "3600000"}, // exactly 1h
		{"llingr.poll.error.backoff.ms": "5000"},       // exactly 5s
	} {
		if _, berr := popPollErrorOptions(pairs); berr != nil {
			t.Fatalf("boundary value rejected: %v (%v)", berr, pairs)
		}
	}
}

// The poll-error keys are adapter options, not kgo options: fed through the
// public entry point unpopped they must hit the unknown-key error, so nothing
// silently ignores them if the pop is bypassed.
func TestPollErrorKeysAreNotKgoOptions(t *testing.T) {
	_, berr := franzKgoOpts(map[string]string{"llingr.poll.error.bail.after.ms": "300000"})
	if berr == nil {
		t.Fatal("expected unknown-key error through franzKgoOpts")
	}
}

// The bounds the bridge validates against must equal the adapter's clamp
// bounds; if the adapter changes them on a version update this trips.
func TestPollErrorBoundsMatchAdapterClamps(t *testing.T) {
	if minPollErrorBailAfter != time.Minute {
		t.Fatalf("minPollErrorBailAfter = %s, want 1m", minPollErrorBailAfter)
	}
	if maxPollErrorBailAfter != time.Hour {
		t.Fatalf("maxPollErrorBailAfter = %s, want 1h", maxPollErrorBailAfter)
	}
	if maxPollErrorBackoff != 5*time.Second {
		t.Fatalf("maxPollErrorBackoff = %s, want 5s", maxPollErrorBackoff)
	}
}
