// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

// One broker composition, no build variants: this library carries the
// pure-Go franz adapter (adapter_franz.go) and nothing else. There is no
// adapter selection; a config naming any other adapter fails llingr_init
// with a clean error from parseBridgeConfig.

package main

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/llingr/llingr-demux/demux"
	"github.com/llingr/llingr-demux/demux/metrics/snapshot"
	"github.com/llingr/llingr-nexus/nexus"
)

// compiledAdapters records the broker composition compiled into this
// artifact (adapter_franz.go registers itself in init()). Logged once at
// llingr_init so a deployment's log stream shows what is running.
var compiledAdapters []string

// consumerHandle is the non-generic slice of the consumer the bridge stores:
// lifecycle control and the snapshot are type-parameter-free, so the bridge
// state never carries the concrete record type.
type consumerHandle interface {
	Subscribe() error
	Shutdown() error
	EmergencyShutdown(reason error)
	TakeSnapshot() snapshot.Snapshot
}

// buildConsumer wires the franz consumer (the only broker composition this
// library ships) and returns a running consumer handle plus the adapter's
// broker-release function (its Unsubscribe: leave group, close client), which
// the bridge keeps for the emergency exit path (see emergencyBrokerCleanup).
func buildConsumer(ctx context.Context, cfg bridgeConfig) (consumerHandle, func() error, *bridgeError) {
	return buildFranzConsumer(ctx, cfg)
}

// newBridgeBuilder assembles the demux ConsumerBuilder. valueOf extracts the
// raw message value from the adapter's native payload type and metaOf
// extracts its timestamp and headers; key, partition, and offset come from
// the already-extracted nexus envelope (the adapter guarantees UTF-8-safe
// keys: raw if valid, base64 if binary, partition number if absent).
func newBridgeBuilder[T any](ctx context.Context, cfg bridgeConfig, bw parsedBandwidth, valueOf func(T) []byte, metaOf func(T) recordMeta) *demux.ConsumerBuilder[T] {
	builder := demux.NewBuilder[T](
		cfg.Topic,
		makeProcessMessage[T](valueOf, metaOf),
		makeWriteDeadLetter[T](valueOf, metaOf),
	).WithDemuxConfig(cfg.Demux).
		WithShutdownCallback(shutdownCallback()).
		WithMetricsSink(metricsSinkCallback()).
		WithContext(ctx)

	if cfg.Service != nil {
		builder = builder.WithService(nexus.Service{
			Name: cfg.Service.Name,
			Team: cfg.Service.Team,
		})
	}

	// Route engine logs to the host only when a log callback is registered;
	// otherwise the engine's default (slog to stderr) applies.
	if loadCallbacks().log != nil {
		builder = builder.WithLogger(bridgeLogger{})
	}

	// Bandwidth telemetry: enabled by the host registering the bandwidth
	// callback (the adapters' WithBandwidthInterval is wired by the caller,
	// which owns the concrete adapter type).
	if loadCallbacks().bandwidth != nil {
		builder = builder.WithBandwidthMetricsSink(bandwidthSink())
		if bw.flushInterval > 0 {
			builder = builder.WithBandwidthFlushInterval(bw.flushInterval)
		}
	}

	return builder
}

// asConsumerHandle narrows the adapter's nexus.Consumer to the bridge handle.
// The concrete *demux.Consumer[T] carries TakeSnapshot and EmergencyShutdown
// (the nexus interface does not), so this always succeeds with the real
// engine; the assertion exists to fail loudly rather than panic if that ever
// changes.
func asConsumerHandle(consumer any) (consumerHandle, *bridgeError) {
	handle, ok := consumer.(consumerHandle)
	if !ok {
		return nil, bridgeErrorf(errAdapter,
			"consumer type %T does not implement the bridge handle", consumer)
	}
	return handle, nil
}

// reservedOptionKeys are set through the dedicated config fields and may not
// arrive as client option pairs: a pair would silently override the field
// (the ConfigMap is last-write-wins), which is exactly the kind of quiet
// misconfiguration this bridge exists to prevent.
var reservedOptionKeys = map[string]string{
	"bootstrap.servers": "brokers()",
	"group.id":          "consumer_group()",
}

func reservedOptionError(key string) *bridgeError {
	setter, ok := reservedOptionKeys[key]
	if !ok {
		return nil
	}
	return bridgeErrorf(errBadOption,
		"option %q is reserved: it is set via %s and cannot be overridden as a client option",
		key, setter)
}

func millisOption(key, value string) (time.Duration, error) {
	ms, err := strconv.Atoi(value)
	if err != nil || ms <= 0 {
		return 0, bridgeErrorf(errBadOption, "%s must be a positive integer of milliseconds, got %q", key, value)
	}
	return time.Duration(ms) * time.Millisecond, nil
}

// nonNegativeMillisOption is millisOption for keys where 0 is meaningful
// (it disables the behaviour rather than selecting a default).
func nonNegativeMillisOption(key, value string) (time.Duration, error) {
	ms, err := strconv.Atoi(value)
	if err != nil || ms < 0 {
		return 0, bridgeErrorf(errBadOption, "%s must be a non-negative integer of milliseconds, got %q", key, value)
	}
	return time.Duration(ms) * time.Millisecond, nil
}

// boolOption parses a strict "true"/"false" (lowercased, trimmed).
func boolOption(key, value string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, bridgeErrorf(errBadOption, "%s must be \"true\" or \"false\", got %q", key, value)
	}
}

// asBridgeError narrows the error interface the option helpers return (their
// concrete type is always *bridgeError) without losing the stable code.
func asBridgeError(err error) *bridgeError {
	if berr, ok := err.(*bridgeError); ok {
		return berr
	}
	return bridgeErrorf(errBadOption, "%v", err)
}

func int32Option(key, value string) (int32, error) {
	n, err := strconv.ParseInt(value, 10, 32)
	if err != nil || n <= 0 {
		return 0, bridgeErrorf(errBadOption, "%s must be a positive 32-bit integer, got %q", key, value)
	}
	return int32(n), nil
}
