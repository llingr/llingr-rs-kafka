// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package main

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func TestCompiledAdaptersIncludesFranz(t *testing.T) {
	if slices.Contains(compiledAdapters, "franz") {
		return
	}
	t.Fatalf("compiledAdapters missing franz under with_franz: %v", compiledAdapters)
}

func TestReservedOptionKeysAreRejected(t *testing.T) {
	for key, setter := range map[string]string{
		"bootstrap.servers": "brokers()",
		"group.id":          "consumer_group()",
	} {
		berr := reservedOptionError(key)
		if berr == nil {
			t.Fatalf("%s: expected a reserved-key error", key)
		}
		if !strings.Contains(berr.msg, setter) {
			t.Fatalf("%s: error must name the setter %s, got: %s", key, setter, berr.msg)
		}

		// And through the franz entry point.
		if _, berr := franzKgoOpts(map[string]string{key: "x"}); berr == nil {
			t.Fatalf("%s: franzKgoOpts must reject reserved keys", key)
		}
	}

	if berr := reservedOptionError("client.id"); berr != nil {
		t.Fatalf("client.id is not reserved, got: %s", berr.msg)
	}
}

func TestFranzOptionAliasesAndAdditions(t *testing.T) {
	// librdkafka spelling of the fetch wait, and static membership.
	opts, berr := franzKgoOpts(map[string]string{
		"fetch.wait.max.ms": "500",
		"group.instance.id": "orders-0",
	})
	if berr != nil {
		t.Fatalf("unexpected error: %v", berr)
	}
	if len(opts) != 2 {
		t.Fatalf("got %d opts, want 2", len(opts))
	}

	// Both spellings translate to the same option.
	if _, berr := franzKgoOpts(map[string]string{"fetch.max.wait.ms": "500"}); berr != nil {
		t.Fatalf("java spelling must remain supported: %v", berr)
	}

	if _, berr := franzKgoOpts(map[string]string{"group.instance.id": "  "}); berr == nil {
		t.Fatal("blank group.instance.id must be rejected")
	}
}

func TestFranzPartitionAssignmentStrategy(t *testing.T) {
	// Single strategy: accepted.
	if _, berr := franzKgoOpts(map[string]string{"partition.assignment.strategy": "range"}); berr != nil {
		t.Fatalf("single strategy must be accepted: %v", berr)
	}
	// Full compatibility preference list, with spaces and mixed case: accepted.
	if _, berr := franzKgoOpts(map[string]string{
		"partition.assignment.strategy": "Cooperative-Sticky, sticky, roundrobin, range",
	}); berr != nil {
		t.Fatalf("preference list must be accepted: %v", berr)
	}
	// Unknown strategy: clean error listing the supported names.
	_, berr := franzKgoOpts(map[string]string{"partition.assignment.strategy": "zigzag"})
	if berr == nil || !strings.Contains(berr.msg, "cooperative-sticky, sticky, roundrobin, range") {
		t.Fatalf("unknown strategy must be rejected listing the supported names, got: %v", berr)
	}
	// Empty value (e.g. an empty Rust slice): rejected, never a silent no-op.
	if _, berr := franzKgoOpts(map[string]string{"partition.assignment.strategy": ""}); berr == nil {
		t.Fatal("empty strategy list must be rejected")
	}
	// Trailing comma leaves an empty entry: rejected.
	if _, berr := franzKgoOpts(map[string]string{"partition.assignment.strategy": "range,"}); berr == nil {
		t.Fatal("empty entry in the list must be rejected")
	}
}

func TestFranzIsolationLevelAlignment(t *testing.T) {
	// Explicit read_committed: accepted; it restates the bridge default.
	if _, berr := franzKgoOpts(map[string]string{"isolation.level": "read_committed"}); berr != nil {
		t.Fatalf("read_committed must be accepted: %v", berr)
	}
	// read_uncommitted: rejected; aborted transactional records would be
	// processed and committed downstream.
	_, berr := franzKgoOpts(map[string]string{"isolation.level": "read_uncommitted"})
	if berr == nil || !strings.Contains(berr.msg, "read_uncommitted is rejected") {
		t.Fatalf("read_uncommitted must be rejected with the parity rationale, got: %v", berr)
	}
	// Nonsense value: clear error.
	if _, berr := franzKgoOpts(map[string]string{"isolation.level": "sideways"}); berr == nil {
		t.Fatal("invalid isolation.level must be rejected")
	}
}

func TestFranzRackAndMetadataAge(t *testing.T) {
	opts, berr := franzKgoOpts(map[string]string{
		"client.rack":         "eu-west-2a",
		"metadata.max.age.ms": "10000",
	})
	if berr != nil {
		t.Fatalf("unexpected error: %v", berr)
	}
	if len(opts) != 2 {
		t.Fatalf("got %d opts, want 2", len(opts))
	}
	if _, berr := franzKgoOpts(map[string]string{"client.rack": " "}); berr == nil {
		t.Fatal("blank client.rack must be rejected")
	}
}

func TestRebalanceAboveDrainGuard(t *testing.T) {
	seconds := func(s int) time.Duration { return time.Duration(s) * time.Second }
	tests := []struct {
		name      string
		rebalance string // "" = unset (kgo default 60s)
		drain     time.Duration
		wantErr   bool
	}{
		{name: "defaults hold the invariant", rebalance: "", drain: 0, wantErr: false},
		{name: "unset rebalance beats max legal drain", rebalance: "", drain: seconds(55), wantErr: false},
		{name: "explicit rebalance above drain", rebalance: "45000", drain: seconds(40), wantErr: false},
		{name: "explicit rebalance below default drain", rebalance: "15000", drain: 0, wantErr: true},
		{name: "rebalance equal to drain is rejected", rebalance: "20000", drain: 0, wantErr: true},
		{name: "explicit rebalance below raised drain", rebalance: "30000", drain: seconds(40), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := bridgeConfig{KafkaConfig: map[string]string{}}
			cfg.Demux.DrainTimeout = tt.drain
			if tt.rebalance != "" {
				cfg.KafkaConfig["rebalance.timeout.ms"] = tt.rebalance
			}
			berr := checkRebalanceAboveDrain(cfg)
			if tt.wantErr && berr == nil {
				t.Fatal("expected the guard to fire")
			}
			if !tt.wantErr && berr != nil {
				t.Fatalf("guard fired unexpectedly: %v", berr)
			}
			if berr != nil && !strings.Contains(berr.msg, "must exceed the engine drain timeout") {
				t.Fatalf("error must explain the invariant, got: %s", berr.msg)
			}
		})
	}
}

func TestUnknownFranzOptionErrorWording(t *testing.T) {
	_, berr := franzKgoOpts(map[string]string{"no.such.key": "x"})
	if berr == nil {
		t.Fatal("expected unknown-key error")
	}
	if !strings.HasPrefix(berr.msg, "option ") {
		t.Fatalf("error must speak of options, not internal field names: %s", berr.msg)
	}
	if strings.Contains(berr.msg, "kafka_config") {
		t.Fatalf("internal JSON field name leaked into the error: %s", berr.msg)
	}
	// The supported list includes the new additions.
	for _, want := range []string{"group.instance.id", "fetch.wait.max.ms"} {
		if !strings.Contains(berr.msg, want) {
			t.Fatalf("supported-keys listing missing %s: %s", want, berr.msg)
		}
	}
}
