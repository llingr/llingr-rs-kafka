// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package main

import (
	"reflect"
	"strings"
	"testing"

	"github.com/llingr/llingr-demux/demux/config"
)

// A full config with every one of the thirteen demux tunables set (durations
// as strings, counts as numbers), for the "still parses" case and as the base
// the rejection cases mutate.
const fullValidConfigJSON = `{
	"brokers": "localhost:9092",
	"topic": "orders",
	"consumer_group": "grp",
	"demux": {
		"concurrentKeys": 500,
		"perKeyBufferLen": 32,
		"pollTimeout": "150ms",
		"autoCommitInterval": "5s",
		"drainTimeout": "30s",
		"awaitAssignmentsTimeout": "50s",
		"commitIngestChannelLen": 2000,
		"commitPartitionSliceLen": 400,
		"queryTimeout": "5s",
		"acquireWorkerTimeoutCircuitBreaker": "60s",
		"workerShardsCount": 32,
		"rebalancePausePollingTimeout": "30s",
		"acquireCommitGuardTimeout": "10s"
	}
}`

// An unknown top-level key is a startup error: DisallowUnknownFields on the
// strict decoder rejects it as errInvalidJSON naming the field.
func TestParseBridgeConfigRejectsUnknownTopLevelKey(t *testing.T) {
	data := `{"brokers":"b","topic":"t","consumer_group":"g","bogus":1}`
	_, berr := parseBridgeConfig([]byte(data))
	if berr == nil {
		t.Fatal("expected an error for an unknown top-level key")
	}
	if berr.code != errInvalidJSON {
		t.Fatalf("code = %d, want errInvalidJSON (%d)", berr.code, errInvalidJSON)
	}
	if !strings.Contains(berr.msg, "bogus") {
		t.Fatalf("error text must name the unknown key: %s", berr.msg)
	}
}

// An unknown key inside "demux" is not caught by DisallowUnknownFields (the
// custom UnmarshalJSON silently ignores it), so the reflection check must
// reject it as errBadOption naming the key and listing the valid ones.
func TestParseBridgeConfigRejectsUnknownDemuxKey(t *testing.T) {
	data := `{"brokers":"b","topic":"t","consumer_group":"g","demux":{"bogus":1}}`
	_, berr := parseBridgeConfig([]byte(data))
	if berr == nil {
		t.Fatal("expected an error for an unknown demux key")
	}
	if berr.code != errBadOption {
		t.Fatalf("code = %d, want errBadOption (%d)", berr.code, errBadOption)
	}
	if !strings.Contains(berr.msg, "bogus") {
		t.Fatalf("error text must name the unknown demux key: %s", berr.msg)
	}
	// The valid set is listed so the operator can correct the typo; spot-check
	// one canonical tag.
	if !strings.Contains(berr.msg, "concurrentKeys") {
		t.Fatalf("error text must list the valid demux keys: %s", berr.msg)
	}
}

// A valid config with all thirteen demux tags present still parses cleanly.
func TestParseBridgeConfigAcceptsFullDemux(t *testing.T) {
	cfg, berr := parseBridgeConfig([]byte(fullValidConfigJSON))
	if berr != nil {
		t.Fatalf("unexpected error: %v", berr)
	}
	if cfg.Topic != "orders" || cfg.Brokers != "localhost:9092" || cfg.ConsumerGroup != "grp" {
		t.Fatalf("required fields not parsed: %+v", cfg)
	}
	if cfg.Demux.ConcurrentKeys != 500 || cfg.Demux.WorkerShardsCount != 32 {
		t.Fatalf("demux counts not parsed: %+v", cfg.Demux)
	}
	if cfg.Demux.PollTimeout.Milliseconds() != 150 {
		t.Fatalf("demux duration not parsed: %s", cfg.Demux.PollTimeout)
	}
}

// The reflection-derived key set must have exactly the fields
// config.DemuxConfig declares. Pinning the count to thirteen means an engine
// upgrade that adds a tunable trips this test, forcing a deliberate review of
// the bridge's handling rather than a silent gap.
func TestDemuxJSONTagsMatchStruct(t *testing.T) {
	tags := demuxJSONTags()
	const want = 13
	if len(tags) != want {
		t.Fatalf("demuxJSONTags returned %d keys, want %d: %v", len(tags), want, tags)
	}
	// Every declared field carries a json tag that lands in the set, so the
	// count above is not masking a "-" or untagged field.
	structFields := reflect.TypeOf(config.DemuxConfig{}).NumField()
	if structFields != want {
		t.Fatalf("config.DemuxConfig declares %d fields, want %d; the engine changed its tunable set", structFields, want)
	}
}
