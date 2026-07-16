// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package main

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/llingr/llingr-adapter-franz/franzadapter"
	"github.com/llingr/llingr-demux/demux"
	"github.com/llingr/llingr-nexus/nexus"
	"github.com/twmb/franz-go/pkg/kgo"
)

func init() {
	compiledAdapters = append(compiledAdapters, "franz")
}

// The adapter refuses to wire a consumer that does not satisfy
// nexus.EmergencyShutdowner (CreateConsumer rejects it), and its poll-error
// escalation reaches the host through that contract. This pin turns any
// engine downgrade below the contract into a build failure here, rather
// than a runtime init error surfacing on every host.
var _ nexus.EmergencyShutdowner = (*demux.Consumer[*kgo.Record])(nil)

// Defaults the rebalance/drain guard reasons about. Keep in sync with the
// upstream sources named in the comments; both are pinned by go.mod, so a
// version bump is the moment to re-check.
const (
	// kgo's default rebalance timeout (franz-go pkg/kgo/config.go).
	kgoDefaultRebalanceTimeout = 60 * time.Second
	// The engine's default DrainTimeout (llingr-demux demux/config).
	engineDefaultDrainTimeout = 20 * time.Second
)

// checkRebalanceAboveDrain enforces the drain-coordination invariant on the
// franz path: the group rebalance timeout must EXCEED the engine's drain
// timeout. During a rebalance the engine drains in-flight work and commits
// before acknowledging the revoke; a rebalance timeout at or below the drain
// budget can evict the member mid-drain, losing the final commit and producing
// duplicates on every rebalance. With everything at defaults the invariant
// holds (60s > 20s, and the engine caps DrainTimeout at 55s), so this only
// fires on explicit misconfiguration.
func checkRebalanceAboveDrain(cfg bridgeConfig) *bridgeError {
	drain := cfg.Demux.DrainTimeout
	if drain <= 0 {
		drain = engineDefaultDrainTimeout
	}

	rebalance := kgoDefaultRebalanceTimeout
	if raw, ok := cfg.KafkaConfig["rebalance.timeout.ms"]; ok {
		d, err := millisOption("rebalance.timeout.ms", raw)
		if err != nil {
			// Unparseable: the option table reports it with full context.
			return nil
		}
		rebalance = d
	}

	if rebalance <= drain {
		return bridgeErrorf(errBadOption,
			"rebalance.timeout.ms (%s) must exceed the engine drain timeout (%s): the engine "+
				"drains in-flight work during a rebalance before acking the revoke, and a "+
				"rebalance timeout at or below the drain budget can evict the consumer "+
				"mid-drain, causing duplicates",
			rebalance, drain)
	}
	return nil
}

// buildFranzConsumer wires the pure-Go franz adapter (no librdkafka).
func buildFranzConsumer(ctx context.Context, cfg bridgeConfig) (consumerHandle, func() error, *bridgeError) {
	if berr := checkRebalanceAboveDrain(cfg); berr != nil {
		return nil, nil, berr
	}

	bw, berr := parseBandwidth(cfg.Bandwidth)
	if berr != nil {
		return nil, nil, berr
	}

	// llingr.client.log.level is an ADAPTER option (the kgo-internals log
	// bridge verbosity), not a kgo.Opt: consume it before the option table,
	// which treats unknown keys as an init error.
	clientLogLevel, berr := popClientLogLevel(cfg.KafkaConfig)
	if berr != nil {
		return nil, nil, berr
	}

	// The llingr.poll.error.* keys are likewise ADAPTER options (the
	// poll-error resilience knobs), consumed before the option table.
	pollErrorOpts, berr := popPollErrorOptions(cfg.KafkaConfig)
	if berr != nil {
		return nil, nil, berr
	}

	opts, berr := franzKgoOpts(cfg.KafkaConfig)
	if berr != nil {
		return nil, nil, berr
	}

	// OPINIONATED DEFAULT: fetch isolation is read_committed. kgo's own
	// default is read_uncommitted (the wire-protocol default), but under
	// read_uncommitted aborted transactional records would be processed and
	// their offsets committed downstream, which the engine's safety posture
	// refuses. The bridge sets read_committed before user options (an
	// explicit isolation.level=read_committed is accepted and redundant;
	// read_uncommitted is rejected by the option table).
	opts = append([]kgo.Opt{kgo.FetchIsolationLevel(kgo.ReadCommitted())}, opts...)

	builder := newBridgeBuilder[*kgo.Record](ctx, cfg, bw,
		func(r *kgo.Record) []byte { return r.Value },
		franzMeta,
	)

	adapter := franzadapter.NewWithOptions(ctx, cfg.ConsumerGroup, strings.Split(cfg.Brokers, ","), opts...)
	if clientLogLevel != nil {
		adapter = adapter.WithOptions(franzadapter.WithClientLogLevel(*clientLogLevel))
	}
	if len(pollErrorOpts) > 0 {
		adapter = adapter.WithOptions(pollErrorOpts...)
	}
	if loadCallbacks().bandwidth != nil {
		// Zero interval = the adapter's default cadence (1 minute).
		adapter = adapter.WithBandwidthInterval(bw.statsInterval)
	}
	consumer, err := adapter.CreateConsumer(builder)
	if err != nil {
		return nil, nil, bridgeErrorf(errAdapter, "franz adapter: %v", err)
	}
	handle, berr := asConsumerHandle(consumer)
	if berr != nil {
		return nil, nil, berr
	}
	return handle, adapter.Unsubscribe, nil
}

// franzMeta extracts the record timestamp and headers from a franz record.
func franzMeta(r *kgo.Record) recordMeta {
	return recordMeta{
		tsKind:   mapFranzTimestampType(r.Attrs.TimestampType()),
		tsMillis: r.Timestamp.UnixMilli(),
		headers:  franzHeaders(r.Headers),
	}
}

// mapFranzTimestampType maps kgo.RecordAttrs.TimestampType() values (-1 none,
// 0 create, 1 log append) to the bridge's ts_kind scheme (0/1/2).
func mapFranzTimestampType(t int8) int8 {
	switch t {
	case 0:
		return tsCreateTime
	case 1:
		return tsLogAppendTime
	default: // -1: not available
		return tsNotAvailable
	}
}

// franzHeaders converts franz record headers to the bridge representation. A
// nil header value is preserved as a null value (not an empty one).
func franzHeaders(hs []kgo.RecordHeader) []bridgeHeader {
	if len(hs) == 0 {
		return nil
	}
	out := make([]bridgeHeader, len(hs))
	for i, h := range hs {
		out[i] = bridgeHeader{key: h.Key, value: h.Value}
	}
	return out
}

// Poll-error resilience bounds. The adapter clamps out-of-range values
// SILENTLY (franzadapter clampBailAfter/clampBackoff); the bridge instead
// rejects them loudly here, so a configured value never silently changes
// meaning. Keep in sync with the adapter's clamp constants (pinned by
// go.mod; a version bump is the moment to re-check).
const (
	minPollErrorBailAfter = time.Minute
	maxPollErrorBailAfter = time.Hour
	maxPollErrorBackoff   = 5 * time.Second
)

// popPollErrorOptions extracts the llingr.poll.error.* adapter options from
// the config, removing them so the kgo option table (which errors on unknown
// keys) never sees them. All values are integer milliseconds.
//
//   - llingr.poll.error.bail.after.ms: how long a partition may fail
//     continuously before the adapter stops the consumer (emergency
//     shutdown with the reason). 0 disables the bail entirely; otherwise
//     the value must be within [1 minute, 1 hour]. Adapter default: 10m.
//   - llingr.poll.error.log.interval.ms: minimum interval between repeat
//     logs of the same partition error. Must be positive. Default: 1s.
//   - llingr.poll.error.backoff.ms: pause after a broker error with no
//     record, so a failing partition does not spin the poll loop. 0
//     disables; otherwise at most 5 seconds. Default: 25ms.
func popPollErrorOptions(kafkaConfig map[string]string) ([]franzadapter.AdapterOption, *bridgeError) {
	var opts []franzadapter.AdapterOption

	if raw, ok := kafkaConfig["llingr.poll.error.bail.after.ms"]; ok {
		delete(kafkaConfig, "llingr.poll.error.bail.after.ms")
		d, err := nonNegativeMillisOption("llingr.poll.error.bail.after.ms", raw)
		if err != nil {
			return nil, asBridgeError(err)
		}
		if d != 0 && (d < minPollErrorBailAfter || d > maxPollErrorBailAfter) {
			return nil, bridgeErrorf(errBadOption,
				"llingr.poll.error.bail.after.ms must be 0 (disable the bail) or between %s and %s, got %s",
				minPollErrorBailAfter, maxPollErrorBailAfter, d)
		}
		opts = append(opts, franzadapter.WithPollErrorBailAfter(d))
	}

	if raw, ok := kafkaConfig["llingr.poll.error.log.interval.ms"]; ok {
		delete(kafkaConfig, "llingr.poll.error.log.interval.ms")
		d, err := millisOption("llingr.poll.error.log.interval.ms", raw)
		if err != nil {
			return nil, asBridgeError(err)
		}
		opts = append(opts, franzadapter.WithPollErrorLogInterval(d))
	}

	if raw, ok := kafkaConfig["llingr.poll.error.backoff.ms"]; ok {
		delete(kafkaConfig, "llingr.poll.error.backoff.ms")
		d, err := nonNegativeMillisOption("llingr.poll.error.backoff.ms", raw)
		if err != nil {
			return nil, asBridgeError(err)
		}
		if d > maxPollErrorBackoff {
			return nil, bridgeErrorf(errBadOption,
				"llingr.poll.error.backoff.ms must be at most %s (0 disables the backoff), got %s",
				maxPollErrorBackoff, d)
		}
		opts = append(opts, franzadapter.WithPollErrorBackoff(d))
	}

	return opts, nil
}

// popClientLogLevel extracts llingr.client.log.level from the config, removing
// it so the kgo option table (which errors on unknown keys) never sees it.
// Returns nil when the key is absent (the adapter's default, info, applies).
func popClientLogLevel(kafkaConfig map[string]string) (*kgo.LogLevel, *bridgeError) {
	raw, ok := kafkaConfig["llingr.client.log.level"]
	if !ok {
		return nil, nil
	}
	delete(kafkaConfig, "llingr.client.log.level")

	var level kgo.LogLevel
	switch raw {
	case "none":
		level = kgo.LogLevelNone
	case "error":
		level = kgo.LogLevelError
	case "warn":
		level = kgo.LogLevelWarn
	case "info":
		level = kgo.LogLevelInfo
	case "debug":
		level = kgo.LogLevelDebug
	default:
		return nil, bridgeErrorf(errBadOption,
			"llingr.client.log.level %q: want none, error, warn, info, or debug", raw)
	}
	return &level, nil
}

// ---------------------------------------------------------------------------
// franz option translation
// ---------------------------------------------------------------------------

// franzOptionKeys maps the INDEPENDENT librdkafka-style option names to
// kgo.Opt constructors (one key, one option). The same names work for both
// adapters, so an application can switch adapter without rewriting its
// options. Unknown keys are an init error, never a silent no-op.
//
// Security keys (security.protocol, ssl.*, sasl.*) are handled separately by
// franzSecurity (franz_security.go): they are cross-key and assemble into
// kgo.DialTLSConfig / kgo.SASL as one validated unit.
var franzOptionKeys = map[string]func(value string) (kgo.Opt, error){
	"auto.offset.reset": func(v string) (kgo.Opt, error) {
		switch v {
		case "earliest":
			return kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), nil
		case "latest":
			return kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()), nil
		default:
			return nil, bridgeErrorf(errBadOption, "auto.offset.reset must be \"earliest\" or \"latest\", got %q", v)
		}
	},
	"client.id": func(v string) (kgo.Opt, error) {
		return kgo.ClientID(v), nil
	},
	"session.timeout.ms": func(v string) (kgo.Opt, error) {
		d, err := millisOption("session.timeout.ms", v)
		if err != nil {
			return nil, err
		}
		return kgo.SessionTimeout(d), nil
	},
	"heartbeat.interval.ms": func(v string) (kgo.Opt, error) {
		d, err := millisOption("heartbeat.interval.ms", v)
		if err != nil {
			return nil, err
		}
		return kgo.HeartbeatInterval(d), nil
	},
	"rebalance.timeout.ms": func(v string) (kgo.Opt, error) {
		d, err := millisOption("rebalance.timeout.ms", v)
		if err != nil {
			return nil, err
		}
		return kgo.RebalanceTimeout(d), nil
	},
	// Consumer-group assignment strategy, overriding the cooperative-sticky
	// default the adapter sets (user options apply after its base options).
	// The value is a comma-separated preference order; the group coordinator
	// picks the first protocol EVERY member supports, so listing fallbacks
	// (e.g. "cooperative-sticky,sticky,roundrobin,range") lets the binding
	// join mixed-protocol groups mid-migration, mirroring the Go adapter's
	// CompatibilityBalancers. librdkafka accepts the same key with the same
	// names except "sticky" (classic sticky is not implemented there).
	"partition.assignment.strategy": func(v string) (kgo.Opt, error) {
		names := strings.Split(v, ",")
		balancers := make([]kgo.GroupBalancer, 0, len(names))
		for _, raw := range names {
			switch name := strings.ToLower(strings.TrimSpace(raw)); name {
			case "cooperative-sticky":
				balancers = append(balancers, kgo.CooperativeStickyBalancer())
			case "sticky":
				balancers = append(balancers, kgo.StickyBalancer())
			case "roundrobin":
				balancers = append(balancers, kgo.RoundRobinBalancer())
			case "range":
				balancers = append(balancers, kgo.RangeBalancer())
			case "":
				return nil, bridgeErrorf(errBadOption,
					"partition.assignment.strategy %q has an empty entry; use a comma-separated "+
						"preference list of cooperative-sticky, sticky, roundrobin, range", v)
			default:
				return nil, bridgeErrorf(errBadOption,
					"partition.assignment.strategy %q is not supported; use a comma-separated "+
						"preference list of cooperative-sticky, sticky, roundrobin, range", name)
			}
		}
		return kgo.Balancers(balancers...), nil
	},
	"fetch.min.bytes": func(v string) (kgo.Opt, error) {
		n, err := int32Option("fetch.min.bytes", v)
		if err != nil {
			return nil, err
		}
		return kgo.FetchMinBytes(n), nil
	},
	"fetch.max.bytes": func(v string) (kgo.Opt, error) {
		n, err := int32Option("fetch.max.bytes", v)
		if err != nil {
			return nil, err
		}
		return kgo.FetchMaxBytes(n), nil
	},
	"max.partition.fetch.bytes": func(v string) (kgo.Opt, error) {
		n, err := int32Option("max.partition.fetch.bytes", v)
		if err != nil {
			return nil, err
		}
		return kgo.FetchMaxPartitionBytes(n), nil
	},
	"fetch.max.wait.ms": func(v string) (kgo.Opt, error) {
		d, err := millisOption("fetch.max.wait.ms", v)
		if err != nil {
			return nil, err
		}
		return kgo.FetchMaxWait(d), nil
	},
	// librdkafka's spelling of the same setting (Java uses fetch.max.wait.ms,
	// librdkafka uses fetch.wait.max.ms): accept both so string pairs written
	// for librdkafka-based clients port over unchanged.
	"fetch.wait.max.ms": func(v string) (kgo.Opt, error) {
		d, err := millisOption("fetch.wait.max.ms", v)
		if err != nil {
			return nil, err
		}
		return kgo.FetchMaxWait(d), nil
	},
	// Static group membership: restarts within the session timeout rejoin
	// without a rebalance.
	"group.instance.id": func(v string) (kgo.Opt, error) {
		if strings.TrimSpace(v) == "" {
			return nil, bridgeErrorf(errBadOption, "group.instance.id must not be empty")
		}
		return kgo.InstanceID(v), nil
	},
	// Rack-aware fetch-from-follower: the broker serves fetches from the
	// nearest replica when the client declares its rack.
	"client.rack": func(v string) (kgo.Opt, error) {
		if strings.TrimSpace(v) == "" {
			return nil, bridgeErrorf(errBadOption, "client.rack must not be empty")
		}
		return kgo.Rack(v), nil
	},
	"metadata.max.age.ms": func(v string) (kgo.Opt, error) {
		d, err := millisOption("metadata.max.age.ms", v)
		if err != nil {
			return nil, err
		}
		return kgo.MetadataMaxAge(d), nil
	},
	// OPINIONATED: the bridge runs read_committed by default (see
	// buildFranzConsumer) and read_uncommitted is rejected. An explicit
	// read_committed is accepted for config parity.
	"isolation.level": func(v string) (kgo.Opt, error) {
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "read_committed":
			return kgo.FetchIsolationLevel(kgo.ReadCommitted()), nil
		case "read_uncommitted":
			return nil, bridgeErrorf(errBadOption,
				"isolation.level=read_uncommitted is rejected: aborted "+
					"transactional records would be processed and committed downstream")
		default:
			return nil, bridgeErrorf(errBadOption,
				"isolation.level must be \"read_committed\" (read_uncommitted is rejected), got %q", v)
		}
	},
	// Connection establishment timeout, TCP dial plus the TLS handshake
	// when TLS is configured (kgo default 10s). librdkafka's key name.
	"socket.connection.setup.timeout.ms": func(v string) (kgo.Opt, error) {
		d, err := millisOption("socket.connection.setup.timeout.ms", v)
		if err != nil {
			return nil, err
		}
		return kgo.DialTimeout(d), nil
	},
	// Idle connection reap age (kgo default 30s). librdkafka's key name.
	"connections.max.idle.ms": func(v string) (kgo.Opt, error) {
		d, err := millisOption("connections.max.idle.ms", v)
		if err != nil {
			return nil, err
		}
		return kgo.ConnIdleTimeout(d), nil
	},
	// Retry budget for retryable (non-fetch) requests: joins, commits,
	// metadata (kgo default 20). No established librdkafka consumer key,
	// hence the llingr namespace.
	"llingr.request.retries": func(v string) (kgo.Opt, error) {
		n, err := int32Option("llingr.request.retries", v)
		if err != nil {
			return nil, err
		}
		return kgo.RequestRetries(int(n)), nil
	},
	// Upper bound on how long one retryable (non-fetch) request keeps being
	// retried (kgo default: the session timeout for group requests, 30s for
	// the rest). No established librdkafka consumer key.
	"llingr.retry.timeout.ms": func(v string) (kgo.Opt, error) {
		d, err := millisOption("llingr.retry.timeout.ms", v)
		if err != nil {
			return nil, err
		}
		return kgo.RetryTimeout(d), nil
	},
	// Maximum in-flight fetch requests across brokers (kgo default:
	// unlimited, bounded by broker count). Caps fetch memory alongside
	// fetch.max.bytes. Must be at least 1: kgo's 0 (fetch only on poll)
	// and negative (unlimited) sentinels are not exposed, because the
	// engine's poll loop assumes buffered fetching. No established
	// librdkafka key.
	"llingr.max.concurrent.fetches": func(v string) (kgo.Opt, error) {
		n, err := int32Option("llingr.max.concurrent.fetches", v)
		if err != nil {
			return nil, err
		}
		return kgo.MaxConcurrentFetches(int(n)), nil
	},
	// Auto-create the consumed topic on first metadata fetch (kgo default:
	// off, matching the Java consumer default). Java/librdkafka key name.
	"allow.auto.create.topics": func(v string) (kgo.Opt, error) {
		on, err := boolOption("allow.auto.create.topics", v)
		if err != nil {
			return nil, err
		}
		if !on {
			return nil, nil // kgo default; nothing to set
		}
		return kgo.AllowAutoTopicCreation(), nil
	},
	// CRC32 validation of fetched record batches (kgo default: on).
	// "false" disables it, for brokers that do not produce proper CRCs.
	// Java/librdkafka key name.
	"check.crcs": func(v string) (kgo.Opt, error) {
		on, err := boolOption("check.crcs", v)
		if err != nil {
			return nil, err
		}
		if on {
			return nil, nil // kgo default; nothing to set
		}
		return kgo.DisableFetchCRCValidation(), nil
	},
	// KIP-714 client metrics push to the broker (kgo default: on where the
	// broker supports it). "false" opts out. librdkafka key name.
	"enable.metrics.push": func(v string) (kgo.Opt, error) {
		on, err := boolOption("enable.metrics.push", v)
		if err != nil {
			return nil, err
		}
		if on {
			return nil, nil // kgo default; nothing to set
		}
		return kgo.DisableClientMetrics(), nil
	},
}

// franzKgoOpts translates librdkafka-style string pairs into typed kgo.Opts.
// Independent keys translate one-to-one; security keys are collected and
// assembled as one validated unit (see franz_security.go).
func franzKgoOpts(kafkaConfig map[string]string) ([]kgo.Opt, *bridgeError) {
	if len(kafkaConfig) == 0 {
		return nil, nil
	}

	// Deterministic order: identical config always produces identical
	// client options regardless of map iteration order.
	keys := make([]string, 0, len(kafkaConfig))
	for key := range kafkaConfig {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	security := &franzSecurity{}
	opts := make([]kgo.Opt, 0, len(keys))
	for _, key := range keys {
		if berr := reservedOptionError(key); berr != nil {
			return nil, berr
		}
		if security.collect(key, kafkaConfig[key]) {
			continue
		}
		translate, ok := franzOptionKeys[key]
		if !ok {
			return nil, bridgeErrorf(errBadOption,
				"option %q is not supported (supported: %s)",
				key, strings.Join(supportedFranzKeys(), ", "))
		}
		opt, err := translate(kafkaConfig[key])
		if err != nil {
			if berr, ok := err.(*bridgeError); ok {
				return nil, berr
			}
			return nil, bridgeErrorf(errBadOption, "option %q: %v", key, err)
		}
		// A nil opt with no error means the value restates the kgo default
		// (e.g. check.crcs=true): valid config, nothing to set.
		if opt != nil {
			opts = append(opts, opt)
		}
	}

	securityOpts, berr := security.build()
	if berr != nil {
		return nil, berr
	}
	return append(opts, securityOpts...), nil
}

// adapterOptionKeys are consumed by the pop functions BEFORE the option
// table, so a correctly spelled one never reaches the unknown-key path; they
// are listed here so a near-miss typo still shows them as supported.
var adapterOptionKeys = []string{
	"llingr.client.log.level",
	"llingr.poll.error.bail.after.ms",
	"llingr.poll.error.log.interval.ms",
	"llingr.poll.error.backoff.ms",
}

func supportedFranzKeys() []string {
	keys := make([]string, 0,
		len(franzOptionKeys)+len(franzSecurityCollectors)+len(adapterOptionKeys))
	for key := range franzOptionKeys {
		keys = append(keys, key)
	}
	for key := range franzSecurityCollectors {
		keys = append(keys, key)
	}
	keys = append(keys, adapterOptionKeys...)
	sort.Strings(keys)
	return keys
}
