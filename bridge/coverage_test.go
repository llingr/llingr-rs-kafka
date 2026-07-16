// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

// Coverage for the config validation rejection branches, the C-typed
// marshalling seams in coverage_testsupport.go, the dead-letter rc-to-error
// mapping, and the remaining emergency and shutdown paths.

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"strings"
	"testing"

	"github.com/llingr/llingr-demux/demux/metrics/snapshot"
	"github.com/llingr/llingr-nexus/nexus"
)

// tlsState builds a connection state with the given PEM certificate as
// the peer chain (empty PEM = no peer certificates).
func tlsState(t *testing.T, certPEM string) tls.ConnectionState {
	t.Helper()
	if certPEM == "" {
		return tls.ConnectionState{}
	}
	block, _ := pem.Decode([]byte(certPEM))
	if block == nil {
		t.Fatal("certificate PEM did not decode")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	return tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
}

// ---------------------------------------------------------------------------
// Config validation rejection branches
// ---------------------------------------------------------------------------

func TestParseBridgeConfigMissingRequiredFields(t *testing.T) {
	for name, config := range map[string]string{
		"all empty":     `{}`,
		"missing topic": `{"brokers":"b:9092","consumer_group":"g"}`,
		"empty broker":  `{"brokers":"","topic":"t","consumer_group":"g"}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, berr := parseBridgeConfig([]byte(config))
			if berr == nil || berr.code != errMissingConfig {
				t.Fatalf("want errMissingConfig, got %v", berr)
			}
			if !strings.Contains(berr.msg, "brokers, topic, and consumer_group") {
				t.Fatalf("error must name the required fields: %s", berr.msg)
			}
		})
	}
}

func TestParseBridgeConfigRejectsUnknownAdapter(t *testing.T) {
	data := `{"brokers":"b","topic":"t","consumer_group":"g","adapter":"kafka"}`
	_, berr := parseBridgeConfig([]byte(data))
	if berr == nil || berr.code != errBadOption {
		t.Fatalf("want errBadOption, got %v", berr)
	}
	want := `unknown adapter "kafka" (supported: "franz")`
	if berr.msg != want {
		t.Fatalf("error = %q, want %q", berr.msg, want)
	}
}

func TestValidateDemuxKeysRejectionBranches(t *testing.T) {
	// A demux value that is not an object.
	berr := validateDemuxKeys([]byte(`{"demux":"not an object"}`))
	if berr == nil || berr.code != errInvalidJSON {
		t.Fatalf("non-object demux: want errInvalidJSON, got %v", berr)
	}

	// Unparseable document: unreachable through parseBridgeConfig, which
	// strict-decodes first, but the defensive branch still must not lie.
	if berr := validateDemuxKeys([]byte(`{nope`)); berr == nil {
		t.Fatal("malformed document must error")
	}

	// No demux object: nothing to validate.
	if berr := validateDemuxKeys([]byte(`{"topic":"t"}`)); berr != nil {
		t.Fatalf("absent demux must pass, got %v", berr)
	}
}

func TestParseBandwidthUnparseableFlushInterval(t *testing.T) {
	_, berr := parseBandwidth(&bandwidthConfig{FlushInterval: "fast"})
	if berr == nil || berr.code != errBadOption {
		t.Fatalf("want errBadOption, got %v", berr)
	}
	if !strings.Contains(berr.msg, "invalid bandwidth flushInterval") {
		t.Fatalf("error must name the field: %s", berr.msg)
	}
}

// ---------------------------------------------------------------------------
// Option helper rejection branches
// ---------------------------------------------------------------------------

func TestPopClientLogLevelAllArms(t *testing.T) {
	// Absent: nil level, nil error.
	level, berr := popClientLogLevel(map[string]string{})
	if level != nil || berr != nil {
		t.Fatalf("absent key: got (%v, %v), want (nil, nil)", level, berr)
	}

	// Every valid level parses and the key is consumed.
	for _, name := range []string{"none", "error", "warn", "info", "debug"} {
		cfg := map[string]string{"llingr.client.log.level": name}
		level, berr := popClientLogLevel(cfg)
		if berr != nil || level == nil {
			t.Fatalf("%s: got (%v, %v)", name, level, berr)
		}
		if _, ok := cfg["llingr.client.log.level"]; ok {
			t.Fatalf("%s: key was not consumed", name)
		}
	}

	// Invalid level: error naming the accepted values.
	_, berr = popClientLogLevel(map[string]string{"llingr.client.log.level": "loud"})
	if berr == nil || !strings.Contains(berr.msg, "want none, error, warn, info, or debug") {
		t.Fatalf("invalid level: got %v", berr)
	}
}

func TestFranzKgoOptsEmptyAndSecurityErrorPropagation(t *testing.T) {
	// Empty config: nothing to translate.
	opts, berr := franzKgoOpts(nil)
	if opts != nil || berr != nil {
		t.Fatalf("empty config: got (%v, %v)", opts, berr)
	}

	// A security assembly failure propagates through the public entry point.
	_, berr = franzKgoOpts(map[string]string{"security.protocol": "quantum"})
	if berr == nil || !strings.Contains(berr.msg, "unknown security.protocol") {
		t.Fatalf("security error must propagate: %v", berr)
	}
}

func TestTLSConfigRejectionBranches(t *testing.T) {
	caPEM, certPEM, keyPEM := testPEMs(t)

	cases := []struct {
		name    string
		pairs   map[string]string
		wantSub string
	}{
		{
			name: "location pair half set",
			pairs: map[string]string{
				"security.protocol":        "ssl",
				"ssl.certificate.location": "/cert.pem",
			},
			wantSub: "must both be set",
		},
		{
			name: "unreadable certificate files",
			pairs: map[string]string{
				"security.protocol":        "ssl",
				"ssl.certificate.location": "/nonexistent/cert.pem",
				"ssl.key.location":         "/nonexistent/key.pem",
			},
			wantSub: "loading client certificate",
		},
		{
			name: "garbage inline pem pair",
			pairs: map[string]string{
				"security.protocol":   "ssl",
				"ssl.certificate.pem": "not a certificate",
				"ssl.key.pem":         "not a key",
			},
			wantSub: "parsing client certificate",
		},
		{
			name: "garbage ca pem",
			pairs: map[string]string{
				"security.protocol": "ssl",
				"ssl.ca.pem":        "not pem material",
			},
			wantSub: "no parseable PEM certificates",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, berr := collectSecurity(t, tc.pairs).build()
			if berr == nil || !strings.Contains(berr.msg, tc.wantSub) {
				t.Fatalf("want %q, got %v", tc.wantSub, berr)
			}
		})
	}

	// Valid mTLS material still assembles: the happy path the rejection
	// cases surround.
	_, berr := collectSecurity(t, map[string]string{
		"security.protocol":   "ssl",
		"ssl.ca.pem":          caPEM,
		"ssl.certificate.pem": certPEM,
		"ssl.key.pem":         keyPEM,
	}).build()
	if berr != nil {
		t.Fatalf("valid material rejected: %v", berr)
	}
}

// The hostname-verification-off VerifyConnection closure: chain verification
// still runs. No peer certificate is an error, and a chain the roots cannot
// verify for the required usage is an error; nothing panics.
func TestTLSVerifyConnectionClosureRuns(t *testing.T) {
	caPEM, certPEM, _ := testPEMs(t)
	security := collectSecurity(t, map[string]string{
		"security.protocol":                     "ssl",
		"ssl.ca.pem":                            caPEM,
		"ssl.endpoint.identification.algorithm": "none",
	})
	tlsConfig, berr := security.tlsConfig()
	if berr != nil {
		t.Fatalf("unexpected error: %v", berr)
	}
	if tlsConfig.VerifyConnection == nil {
		t.Fatal("expected a custom VerifyConnection")
	}

	if err := tlsConfig.VerifyConnection(tlsState(t, "")); err == nil {
		t.Fatal("no peer certificate must be an error")
	}
	// A client-auth certificate fails default (server-usage) verification:
	// the closure's verify call runs end to end and reports the failure.
	if err := tlsConfig.VerifyConnection(tlsState(t, certPEM)); err == nil {
		t.Fatal("expected a verification error for a client-usage certificate")
	}
}

func TestSaslScramSha512Mechanism(t *testing.T) {
	security := collectSecurity(t, map[string]string{
		"security.protocol": "sasl_plaintext",
		"sasl.mechanism":    "SCRAM-SHA-512",
		"sasl.username":     "u",
		"sasl.password":     "p",
	})
	mechanism, berr := security.saslMechanism()
	if berr != nil || mechanism == nil {
		t.Fatalf("SCRAM-SHA-512 must assemble: (%v, %v)", mechanism, berr)
	}
}

// ---------------------------------------------------------------------------
// C-typed marshalling seams
// ---------------------------------------------------------------------------

func TestWriteInitErrThroughCBuffer(t *testing.T) {
	text, n := writeInitErrView("broker unreachable", 64)
	if text != "broker unreachable" || n != len(text) {
		t.Fatalf("got (%q, %d)", text, n)
	}

	// Truncation backs off to a rune boundary through the real C buffer.
	text, n = writeInitErrView("aébc", 2) // é is two bytes
	if text != "a" || n != 1 {
		t.Fatalf("rune-boundary truncation: got (%q, %d), want (\"a\", 1)", text, n)
	}

	// Nil/zero-capacity guards: no write, no crash.
	writeInitErrNilGuards("ignored")
}

func TestMarshalValueNullEmptyAndBytes(t *testing.T) {
	// nil value: the tombstone sentinel (value_len == -1).
	if data, null := marshalValueView(nil); !null || data != nil {
		t.Fatalf("nil: got (%v, %v), want (nil, true)", data, null)
	}
	// Empty value: length 0, NOT null.
	if data, null := marshalValueView([]byte{}); null || len(data) != 0 {
		t.Fatalf("empty: got (%v, %v), want (empty, false)", data, null)
	}
	// Bytes arrive intact, including non-UTF-8.
	payload := []byte{0x00, 0xff, 'b', 'y', 't', 'e', 's'}
	data, null := marshalValueView(payload)
	if null || string(data) != string(payload) {
		t.Fatalf("bytes: got (%v, %v)", data, null)
	}
}

// ---------------------------------------------------------------------------
// llingr_init negative paths, through real C buffers
// ---------------------------------------------------------------------------

func TestInitRejectsInvalidConfigurations(t *testing.T) {
	cases := []struct {
		name     string
		config   string
		wantCode int
		wantSub  string
	}{
		{"malformed json", `{nope`, errInvalidJSON, "invalid configuration JSON"},
		{"missing required", `{}`, errMissingConfig, "brokers, topic, and consumer_group"},
		{
			"unknown adapter", `{"brokers":"b","topic":"t","consumer_group":"g","adapter":"kafka"}`,
			errBadOption, `unknown adapter "kafka" (supported: "franz")`,
		},
		{
			"unknown demux key", `{"brokers":"b","topic":"t","consumer_group":"g","demux":{"bogus":1}}`,
			errBadOption, "unknown demux option",
		},
		{
			"unknown kafka option", `{"brokers":"b","topic":"t","consumer_group":"g","kafka_config":{"no.such.key":"x"}}`,
			errBadOption, "not supported (supported:",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rc, text := llingrInitView(tc.config)
			if rc != tc.wantCode {
				t.Fatalf("rc = %d, want %d (%s)", rc, tc.wantCode, text)
			}
			if !strings.Contains(text, tc.wantSub) {
				t.Fatalf("error %q does not contain %q", text, tc.wantSub)
			}
		})
	}
}

// An unreachable broker fails init with errAdapter and the adapter's own
// message (the adapter dials during CreateConsumer; 127.0.0.1:1 refuses
// immediately, so this is fast and deterministic). NOTE this also documents
// why llingr_init's panic-recovery branch stays uncovered: the engine validates
// DemuxConfig inside Build, which the adapter only reaches after a
// successful broker dial, so the recover path needs a live broker.
func TestInitSurfacesBrokerConnectFailure(t *testing.T) {
	rc, text := llingrInitView(
		`{"brokers":"127.0.0.1:1","topic":"t","consumer_group":"g"}`)
	if rc != errAdapter {
		t.Fatalf("rc = %d, want errAdapter (%d): %s", rc, errAdapter, text)
	}
	if !strings.Contains(text, "franz adapter:") {
		t.Fatalf("adapter failure must contain the adapter's message: %q", text)
	}
}

func TestInitRejectsSecondInstance(t *testing.T) {
	fake := &lifecycleConsumer{}
	resetBridgeForStop(t, fake) // leaves state initialised until cleanup

	rc, text := llingrInitView(`{"brokers":"b","topic":"t","consumer_group":"g"}`)
	if rc != errAlreadyInit {
		t.Fatalf("rc = %d, want errAlreadyInit (%d): %s", rc, errAlreadyInit, text)
	}
	if !strings.Contains(text, "one llingr instance per process") {
		t.Fatalf("unexpected text: %q", text)
	}
}

// ---------------------------------------------------------------------------
// run/stop error paths
// ---------------------------------------------------------------------------

func TestRunAndStopBeforeInit(t *testing.T) {
	state.mu.Lock()
	state.consumer = nil
	state.mu.Unlock()

	if rc := int(llingr_run()); rc != -1 {
		t.Fatalf("llingr_run before init = %d, want -1", rc)
	}
	// stop before init: returns without touching anything.
	runWithTimeout(t, 5e9, llingr_stop)
}

// ---------------------------------------------------------------------------
// Dead-letter rc-to-error mapping: the escalation seam
// ---------------------------------------------------------------------------

func deadLetterUnderTest() nexus.WriteDeadLetter[[]byte] {
	return makeWriteDeadLetter[[]byte](
		func(v []byte) []byte { return v },
		func([]byte) recordMeta { return recordMeta{} },
	)
}

func deadLetterMessage() *nexus.Message[[]byte] {
	payload := []byte("v")
	return &nexus.Message[[]byte]{Key: "k", Partition: 3, Offset: 42, Payload: &payload}
}

// A dead-letter callback answering 0 is a successful write: no error, and
// the failure reason crossed the boundary intact.
func TestWriteDeadLetterSuccessCarriesReason(t *testing.T) {
	restore := installTestDeadletterCallback(0)
	defer restore()

	err := deadLetterUnderTest()(context.Background(), deadLetterMessage(), errors.New("handler failed: boom"))
	if err != nil {
		t.Fatalf("rc 0 must map to nil, got %v", err)
	}
	if got := recordedDeadletterReason(); got != "handler failed: boom" {
		t.Fatalf("reason across the boundary = %q", got)
	}
}

// A non-zero dead-letter return code maps to the exact error the engine's
// circuit breaker consumes: the seam between the Rust panic containment and
// the engine's failure-to-breaker escalation. The containment, panic to
// rc 1, is proven by the Rust boundary tests; the escalation is proven in
// the demux suite.
func TestWriteDeadLetterFailureCodeBecomesError(t *testing.T) {
	restore := installTestDeadletterCallback(1)
	defer restore()

	err := deadLetterUnderTest()(context.Background(), deadLetterMessage(), errors.New("boom"))
	if err == nil {
		t.Fatal("rc 1 must map to an error")
	}
	want := "dead-letter callback returned error code: 1"
	if err.Error() != want {
		t.Fatalf("error = %q, want %q", err.Error(), want)
	}
}

// With no dead-letter callback registered the write silently discards, by
// contract.
func TestWriteDeadLetterWithoutCallbackDiscards(t *testing.T) {
	previous := loadCallbacks().deadletter
	setCallback(func(set *callbackSet) { set.deadletter = nil })
	defer setCallback(func(set *callbackSet) { set.deadletter = previous })

	if err := deadLetterUnderTest()(context.Background(), deadLetterMessage(), errors.New("x")); err != nil {
		t.Fatalf("nil callback must discard, got %v", err)
	}
}

// A nil reason marshals as an empty string rather than crashing.
func TestWriteDeadLetterNilReason(t *testing.T) {
	restore := installTestDeadletterCallback(0)
	defer restore()

	if err := deadLetterUnderTest()(context.Background(), deadLetterMessage(), nil); err != nil {
		t.Fatalf("nil reason must not fail: %v", err)
	}
	if got := recordedDeadletterReason(); got != "" {
		t.Fatalf("nil reason must arrive empty, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// Shutdown callback marshalling and emergency residuals
// ---------------------------------------------------------------------------

// The registered shutdown callback receives "graceful shutdown" for a nil
// reason and the reason's text otherwise: both marshalling branches.
func TestShutdownCallbackMarshalsReason(t *testing.T) {
	fake := &lifecycleConsumer{}
	resetBridgeForStop(t, fake)
	restore := installTestShutdownCallback()
	defer restore()

	cb := shutdownCallback()
	runWithTimeout(t, 5e9, func() { cb(context.Background(), nil) })
	if got := recordedShutdownReason(); got != "graceful shutdown" {
		t.Fatalf("graceful reason = %q", got)
	}

	resetBridgeForStop(t, fake)
	runWithTimeout(t, 5e9, func() { cb(context.Background(), errors.New("sustained poll failure")) })
	if got := recordedShutdownReason(); got != "sustained poll failure" {
		t.Fatalf("emergency reason = %q", got)
	}
}

// panickingEmergencyConsumer trips the emergencyStop recover branch.
type panickingEmergencyConsumer struct{}

func (panickingEmergencyConsumer) Subscribe() error                { return nil }
func (panickingEmergencyConsumer) Shutdown() error                 { return nil }
func (panickingEmergencyConsumer) EmergencyShutdown(error)         { panic("engine exploded") }
func (panickingEmergencyConsumer) TakeSnapshot() snapshot.Snapshot { return snapshot.Snapshot{} }

func TestEmergencyStopRecoversEnginePanic(t *testing.T) {
	resetBridgeForStop(t, panickingEmergencyConsumer{})
	// Reaching the end proves the FFI-boundary recover contained it.
	runWithTimeout(t, 5e9, func() { emergencyStop("x") })
}

func TestEmergencyBrokerCleanupResidualArms(t *testing.T) {
	fake := &lifecycleConsumer{}

	// Cleanup returns an error: logged, not fatal.
	resetBridgeForStop(t, fake)
	state.mu.Lock()
	state.brokerCleanup = func() error { return errors.New("leave group failed") }
	state.mu.Unlock()
	runWithTimeout(t, 5e9, emergencyBrokerCleanup)

	// Cleanup panics: recovered, not fatal.
	resetBridgeForStop(t, fake)
	state.mu.Lock()
	state.brokerCleanup = func() error { panic("cleanup exploded") }
	state.mu.Unlock()
	runWithTimeout(t, 5e9, emergencyBrokerCleanup)

	// No cleanup and no cancel registered: a no-op.
	resetBridgeForStop(t, fake)
	state.mu.Lock()
	state.brokerCleanup = nil
	state.cancel = nil
	state.mu.Unlock()
	runWithTimeout(t, 5e9, emergencyBrokerCleanup)
}
