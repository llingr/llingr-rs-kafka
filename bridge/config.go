// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-nexus/nexus"
)

// Return codes for llingr_init. Negative and stable: the Rust crate maps them
// to error variants. errCode 0 is success. The accompanying error text (via
// the err_buf out-parameter) carries the human-readable cause.
const (
	errAlreadyInit   = -1
	errInvalidJSON   = -2
	errMissingConfig = -3
	errAdapter       = -4 // adapter construction or broker connection failure
	errBadOption     = -5 // invalid kafka_config/demux option, or recovered panic
)

// bridgeError pairs a stable return code with its human-readable cause.
type bridgeError struct {
	code int
	msg  string
}

func (e *bridgeError) Error() string { return e.msg }

func bridgeErrorf(code int, format string, args ...any) *bridgeError {
	return &bridgeError{code: code, msg: fmt.Sprintf(format, args...)}
}

// bridgeConfig is parsed from JSON provided by the host application.
//
// The "demux" object maps 1:1 onto config.DemuxConfig, whose custom
// UnmarshalJSON accepts duration strings ("30s", "1500ms"). Zero-value fields
// receive engine defaults at Build() time.
type bridgeConfig struct {
	Adapter       string             `json:"adapter"` // "" or "franz": the only broker composition this library ships
	Brokers       string             `json:"brokers"` // comma-separated
	Topic         string             `json:"topic"`
	ConsumerGroup string             `json:"consumer_group"`
	KafkaConfig   map[string]string  `json:"kafka_config"` // librdkafka-style key/value pairs
	Service       *serviceConfig     `json:"service"`      // optional service identity
	Demux         config.DemuxConfig `json:"demux"`        // engine tunables
	Bandwidth     *bandwidthConfig   `json:"bandwidth"`    // optional bandwidth intervals
}

// serviceConfig mirrors nexus.Service (Spec deliberately not exposed over FFI).
type serviceConfig struct {
	Name string `json:"name"`
	Team string `json:"team"`
}

// bandwidthConfig carries the optional bandwidth telemetry intervals as
// duration strings ("5000ms"). Collection itself is enabled by the host
// registering the bandwidth callback, not by this object: absent intervals
// take the engine defaults (1 minute stats cadence, 60s flush).
type bandwidthConfig struct {
	StatsInterval string `json:"statsInterval"`
	FlushInterval string `json:"flushInterval"`
}

// parsedBandwidth is the validated form of bandwidthConfig. Zero durations
// mean "use the default".
type parsedBandwidth struct {
	statsInterval time.Duration
	flushInterval time.Duration
}

// parseBandwidth validates the interval strings. The stats interval is
// checked against the engine's accepted cadence range ([1s, 12h]) up front,
// so a bad value is a clean errBadOption instead of an adapter panic.
func parseBandwidth(cfg *bandwidthConfig) (parsedBandwidth, *bridgeError) {
	var out parsedBandwidth
	if cfg == nil {
		return out, nil
	}
	if cfg.StatsInterval != "" {
		d, err := time.ParseDuration(cfg.StatsInterval)
		if err != nil {
			return out, bridgeErrorf(errBadOption, "invalid bandwidth statsInterval %q: %v", cfg.StatsInterval, err)
		}
		if err := nexus.ValidateBandwidthInterval(d); err != nil {
			return out, bridgeErrorf(errBadOption, "invalid bandwidth statsInterval %q: %v", cfg.StatsInterval, err)
		}
		out.statsInterval = d
	}
	if cfg.FlushInterval != "" {
		d, err := time.ParseDuration(cfg.FlushInterval)
		if err != nil {
			return out, bridgeErrorf(errBadOption, "invalid bandwidth flushInterval %q: %v", cfg.FlushInterval, err)
		}
		if d <= 0 {
			return out, bridgeErrorf(errBadOption, "invalid bandwidth flushInterval %q: must be positive", cfg.FlushInterval)
		}
		out.flushInterval = d
	}
	return out, nil
}

const adapterFranz = "franz"

// parseBridgeConfig unmarshals and validates the host-supplied JSON.
func parseBridgeConfig(data []byte) (bridgeConfig, *bridgeError) {
	var cfg bridgeConfig
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return cfg, bridgeErrorf(errInvalidJSON, "invalid configuration JSON: %v", err)
	}

	// DisallowUnknownFields rejects unknown keys at the top level and inside
	// the structs decoded by the default unmarshaler (service, bandwidth), but
	// it does NOT reach inside "demux": config.DemuxConfig has a custom
	// UnmarshalJSON that silently ignores unknown keys. Validate the demux
	// object's keys here against the tag set reflected off config.DemuxConfig
	// so a mistyped tunable is a startup error, not a silently defaulted field.
	if berr := validateDemuxKeys(data); berr != nil {
		return cfg, berr
	}

	if cfg.Topic == "" || cfg.Brokers == "" || cfg.ConsumerGroup == "" {
		return cfg, bridgeErrorf(errMissingConfig,
			"missing required config: brokers, topic, and consumer_group must all be set")
	}

	switch cfg.Adapter {
	case "", adapterFranz:
	default:
		return cfg, bridgeErrorf(errBadOption,
			"unknown adapter %q (supported: %q)", cfg.Adapter, adapterFranz)
	}

	return cfg, nil
}

// validateDemuxKeys rejects an unknown key inside the "demux" object. The
// valid set is derived by reflection over config.DemuxConfig's json tags, so
// the bridge never hand-duplicates the tunable list: an engine upgrade that
// renames or adds a field is picked up automatically.
func validateDemuxKeys(data []byte) *bridgeError {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(data, &doc); err != nil {
		// The strict decode in parseBridgeConfig already reported malformed
		// JSON; reaching here means the document parsed as an object.
		return bridgeErrorf(errInvalidJSON, "invalid configuration JSON: %v", err)
	}
	raw, ok := doc["demux"]
	if !ok {
		return nil
	}
	var demuxObj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &demuxObj); err != nil {
		return bridgeErrorf(errInvalidJSON, "invalid demux configuration JSON: %v", err)
	}
	valid := demuxJSONTags()
	for key := range demuxObj {
		if _, ok := valid[key]; !ok {
			return bridgeErrorf(errBadOption,
				"unknown demux option %q (valid: %s)", key, strings.Join(sortedKeys(valid), ", "))
		}
	}
	return nil
}

// demuxJSONTags reflects over config.DemuxConfig and collects the json tag
// name of each field (the part before any comma), skipping "-" and untagged
// fields.
func demuxJSONTags() map[string]struct{} {
	tags := make(map[string]struct{})
	t := reflect.TypeOf(config.DemuxConfig{})
	for i := 0; i < t.NumField(); i++ {
		tag := t.Field(i).Tag.Get("json")
		if tag == "" {
			continue
		}
		name := tag
		if idx := strings.IndexByte(tag, ','); idx >= 0 {
			name = tag[:idx]
		}
		if name == "" || name == "-" {
			continue
		}
		tags[name] = struct{}{}
	}
	return tags
}

// sortedKeys returns the keys of a set in a deterministic order, so error
// messages listing the valid options are stable.
func sortedKeys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
