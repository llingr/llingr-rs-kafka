// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/metrics/snapshot"
)

// reentrantConsumer reproduces the shutdown-handler reentrancy: the engine
// invokes the host shutdown handler from INSIDE Consumer.Shutdown(), and a
// handler is permitted to call stop(). This fake's Shutdown re-enters
// llingr_stop the way that handler would; reenterTimes drives a handler that
// calls stop() more than once (a loop, or nested).
type reentrantConsumer struct {
	shutdownCalls atomic.Int32
	reenterTimes  int
}

func (c *reentrantConsumer) Subscribe() error                { return nil }
func (c *reentrantConsumer) EmergencyShutdown(error)         {}
func (c *reentrantConsumer) TakeSnapshot() snapshot.Snapshot { return snapshot.Snapshot{} }
func (c *reentrantConsumer) Shutdown() error {
	c.shutdownCalls.Add(1)
	n := c.reenterTimes
	if n == 0 {
		n = 1
	}
	for i := 0; i < n; i++ {
		llingr_stop() // the host shutdown handler calling stop()
	}
	return nil
}

// resetBridgeForStop arranges bridge state as if initialised AND running,
// the stop gate open as llingr_run leaves it, with the given consumer, and
// registers cleanup. runDoneOnce is reset so each test can observe its
// effect; it otherwise fires once per process.
func resetBridgeForStop(t *testing.T, consumer consumerHandle) {
	t.Helper()
	state.mu.Lock()
	state.consumer = consumer
	state.cancel = func() {}
	state.brokerCleanup = nil
	state.runDone = make(chan struct{})
	state.runDoneOnce = sync.Once{}
	state.mu.Unlock()
	state.stopGateOpen.Store(true)

	t.Cleanup(func() {
		state.mu.Lock()
		state.consumer = nil
		state.cancel = nil
		state.brokerCleanup = nil
		state.runDone = nil
		state.runDoneOnce = sync.Once{}
		state.mu.Unlock()
		state.stopGateOpen.Store(false)
	})
}

// runWithTimeout fails the test if fn does not return within d, surfacing a
// deadlock as a failure instead of a hung test binary.
func runWithTimeout(t *testing.T, d time.Duration, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); fn() }()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("timed out after %s: likely a deadlock", d)
	}
}

// Without the stop gate the reentrant stop() calls Shutdown() again and
// recurses until the goroutine stack overflows and the runtime crashes. The
// gate must make the reentrant call a no-op: exactly one Shutdown, and
// runDone closed so run() is released.
func TestShutdownHandlerReentrancyDoesNotRecurse(t *testing.T) {
	fake := &reentrantConsumer{}
	resetBridgeForStop(t, fake)

	runWithTimeout(t, 5*time.Second, llingr_stop)

	if got := fake.shutdownCalls.Load(); got != 1 {
		t.Fatalf("Shutdown called %d times, want exactly 1 (reentrant stop must be a no-op)", got)
	}
	select {
	case <-state.runDone:
	default:
		t.Fatal("runDone was not closed; run() would hang")
	}
}

// A shutdown handler that calls stop() several times, plus many concurrent
// stop() callers, must still drive exactly one Shutdown, close runDone once,
// and never deadlock or double-close. Run under -race to catch data races in
// the stopGateOpen/runDone handling.
func TestConcurrentAndReentrantStop(t *testing.T) {
	fake := &reentrantConsumer{reenterTimes: 3}
	resetBridgeForStop(t, fake)

	runWithTimeout(t, 10*time.Second, func() {
		var wg sync.WaitGroup
		for i := 0; i < 64; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); llingr_stop() }()
		}
		wg.Wait()
	})

	if got := fake.shutdownCalls.Load(); got != 1 {
		t.Fatalf("Shutdown called %d times, want exactly 1 under concurrency+reentrancy", got)
	}
	select {
	case <-state.runDone:
	default:
		t.Fatal("runDone was not closed")
	}
}

// stop() after shutdown has completed must be a safe no-op: no second Shutdown,
// no double-close panic, no hang.
func TestStopAfterShutdownIsNoOp(t *testing.T) {
	fake := &reentrantConsumer{}
	resetBridgeForStop(t, fake)

	runWithTimeout(t, 5*time.Second, func() {
		llingr_stop()
		llingr_stop()
		llingr_stop()
	})
	if got := fake.shutdownCalls.Load(); got != 1 {
		t.Fatalf("Shutdown called %d times across repeated stop(), want 1", got)
	}
}

// lifecycleConsumer tracks Subscribe and optionally parks Shutdown (a drain in
// progress) until blockShutdown is closed. EmergencyShutdown records its
// reason for the host-triggered emergency-stop tests.
type lifecycleConsumer struct {
	subscribed      atomic.Bool
	shutdownCalls   atomic.Int32
	inShutdown      atomic.Bool
	blockShutdown   chan struct{}
	emergencyCalls  atomic.Int32
	emergencyReason atomic.Value // string
}

func (c *lifecycleConsumer) Subscribe() error {
	c.subscribed.Store(true)
	return nil
}
func (c *lifecycleConsumer) EmergencyShutdown(reason error) {
	c.emergencyCalls.Add(1)
	c.emergencyReason.Store(reason.Error())
}
func (c *lifecycleConsumer) TakeSnapshot() snapshot.Snapshot { return snapshot.Snapshot{} }
func (c *lifecycleConsumer) Shutdown() error {
	c.shutdownCalls.Add(1)
	if c.blockShutdown != nil {
		c.inShutdown.Store(true)
		<-c.blockShutdown
		c.inShutdown.Store(false)
	}
	return nil
}

// A stop() that lands BEFORE run() finds the gate closed and must be ignored
// entirely: no Shutdown, runDone untouched. run() then opens the gate, and the
// NEXT stop() must still stop the engine (the premature stop must not have
// consumed the gate, or the engine would be unstoppable).
func TestStopBeforeRunIsIgnoredAndRunStaysStoppable(t *testing.T) {
	fake := &lifecycleConsumer{}
	resetBridgeForStop(t, fake)
	state.stopGateOpen.Store(false) // as before llingr_run: gate closed

	runWithTimeout(t, 5*time.Second, llingr_stop)
	if got := fake.shutdownCalls.Load(); got != 0 {
		t.Fatalf("premature stop drove %d Shutdowns, want 0 (ignored)", got)
	}
	select {
	case <-state.runDone:
		t.Fatal("premature stop closed runDone; run() would return immediately")
	default:
	}

	// run() opens the gate and parks; a stop() now must fully shut down.
	runReturned := make(chan int, 1)
	go func() { runReturned <- int(llingr_run()) }()
	for !fake.subscribed.Load() {
		time.Sleep(time.Millisecond)
	}
	runWithTimeout(t, 5*time.Second, llingr_stop)

	if got := fake.shutdownCalls.Load(); got != 1 {
		t.Fatalf("post-run stop drove %d Shutdowns, want exactly 1", got)
	}
	select {
	case rc := <-runReturned:
		if rc != 0 {
			t.Fatalf("llingr_run returned %d, want 0", rc)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("llingr_run did not return after stop()")
	}
}

// An emergency exit reaches the host through the shutdown callback with a
// non-nil reason and never runs the engine's Unsubscribe cleanup; the bridge
// must release the broker (leave group + close client) itself and cancel the
// context. The engine delivers the callback exactly once and both adapters
// guard Unsubscribe internally, so the bridge holds no once guard of its
// own; what it must still guarantee is that a late host stop() adds nothing.
func TestEmergencyExitReleasesBroker(t *testing.T) {
	fake := &lifecycleConsumer{}
	resetBridgeForStop(t, fake)

	var cleanups, cancels atomic.Int32
	state.mu.Lock()
	state.brokerCleanup = func() error { cleanups.Add(1); return nil }
	state.cancel = func() { cancels.Add(1) }
	state.mu.Unlock()

	cb := shutdownCallback()
	runWithTimeout(t, 5*time.Second, func() { cb(context.Background(), errors.New("sustained poll failure")) })

	if got := cleanups.Load(); got != 1 {
		t.Fatalf("broker cleanup ran %d times, want exactly 1", got)
	}
	if got := cancels.Load(); got != 1 {
		t.Fatalf("cancel ran %d times, want 1", got)
	}
	select {
	case <-state.runDone:
	default:
		t.Fatal("emergency callback did not close runDone")
	}

	// A late host stop() adds nothing: the gate is closed, so it neither
	// drives Shutdown nor re-releases the broker.
	runWithTimeout(t, 5*time.Second, llingr_stop)
	if got := cleanups.Load(); got != 1 {
		t.Fatalf("broker cleanup ran %d times after late stop, want 1", got)
	}
	if got := fake.shutdownCalls.Load(); got != 0 {
		t.Fatalf("stop() after emergency exit drove Shutdown %d times, want 0", got)
	}
}

// A graceful shutdown must NOT take the emergency broker-release path: the
// engine's own drain already left the group and closed the client.
func TestGracefulShutdownSkipsBrokerCleanup(t *testing.T) {
	fake := &callbackInvokingConsumer{}
	resetBridgeForStop(t, fake)

	var cleanups atomic.Int32
	state.mu.Lock()
	state.brokerCleanup = func() error { cleanups.Add(1); return nil }
	state.mu.Unlock()

	runWithTimeout(t, 5*time.Second, llingr_stop)
	if got := fake.shutdownCalls.Load(); got != 1 {
		t.Fatalf("Shutdown called %d times, want 1", got)
	}
	if got := cleanups.Load(); got != 0 {
		t.Fatalf("graceful shutdown ran the emergency broker cleanup %d times, want 0", got)
	}
	select {
	case <-state.runDone:
	default:
		t.Fatal("runDone not closed")
	}
}

// callbackInvokingConsumer models the real engine flow: Shutdown() invokes the
// bridge shutdown callback (reason nil = graceful) and the host handler's
// re-entrant stop() before returning.
type callbackInvokingConsumer struct {
	shutdownCalls atomic.Int32
}

func (c *callbackInvokingConsumer) Subscribe() error                { return nil }
func (c *callbackInvokingConsumer) EmergencyShutdown(error)         {}
func (c *callbackInvokingConsumer) TakeSnapshot() snapshot.Snapshot { return snapshot.Snapshot{} }
func (c *callbackInvokingConsumer) Shutdown() error {
	c.shutdownCalls.Add(1)
	cb := shutdownCallback()
	cb(context.Background(), nil)
	llingr_stop() // the host shutdown handler calling stop()
	return nil
}

// A losing concurrent stop() must give up completely: no Shutdown, no cancel,
// no runDone, all while the winner is still draining. The winner alone cancels
// the context, strictly after its Shutdown returns, so the drain's final
// commit can never be cut short by a racing stop().
func TestConcurrentStopLoserGivesUp(t *testing.T) {
	fake := &lifecycleConsumer{blockShutdown: make(chan struct{})}
	resetBridgeForStop(t, fake)

	var cancels atomic.Int32
	state.mu.Lock()
	state.cancel = func() { cancels.Add(1) }
	state.mu.Unlock()

	winnerDone := make(chan struct{})
	go func() { defer close(winnerDone); llingr_stop() }()
	for !fake.inShutdown.Load() {
		time.Sleep(time.Millisecond)
	}

	// loser: returns immediately, touching nothing
	runWithTimeout(t, 5*time.Second, llingr_stop)
	if got := cancels.Load(); got != 0 {
		t.Fatalf("loser cancelled the context %d times during the winner's drain, want 0", got)
	}
	select {
	case <-state.runDone:
		t.Fatal("loser closed runDone while the winner was still draining")
	default:
	}

	close(fake.blockShutdown)
	select {
	case <-winnerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("winner never returned")
	}
	if got := cancels.Load(); got != 1 {
		t.Fatalf("cancel ran %d times after the winner finished, want exactly 1", got)
	}
	if got := fake.shutdownCalls.Load(); got != 1 {
		t.Fatalf("Shutdown called %d times, want 1", got)
	}
	select {
	case <-state.runDone:
	default:
		t.Fatal("winner did not close runDone")
	}
}

// The host-triggered emergency stop must reach the engine regardless of the
// stop gate: the gate protects graceful Shutdown, while the engine's
// emergency path elects its own single deliverer in any lifecycle state. A
// closed gate (as before run(), or after a shutdown began) must not swallow
// it, the reason must arrive verbatim, and graceful Shutdown must not run.
func TestEmergencyStopForwardsPastClosedGate(t *testing.T) {
	fake := &lifecycleConsumer{}
	resetBridgeForStop(t, fake)
	state.stopGateOpen.Store(false) // as before llingr_run: gate closed

	runWithTimeout(t, 5*time.Second, func() { emergencyStop("host says get out") })

	if got := fake.emergencyCalls.Load(); got != 1 {
		t.Fatalf("EmergencyShutdown called %d times, want 1", got)
	}
	if got := fake.emergencyReason.Load(); got != "host says get out" {
		t.Fatalf("EmergencyShutdown reason %q, want %q", got, "host says get out")
	}
	if got := fake.shutdownCalls.Load(); got != 0 {
		t.Fatalf("emergency stop drove graceful Shutdown %d times, want 0", got)
	}
}

// An empty reason (a NULL or zero-length string from the host) still carries
// a usable description to the shutdown callback.
func TestEmergencyStopDefaultsEmptyReason(t *testing.T) {
	fake := &lifecycleConsumer{}
	resetBridgeForStop(t, fake)

	runWithTimeout(t, 5*time.Second, func() { emergencyStop("") })

	if got := fake.emergencyReason.Load(); got != "emergency stop requested by host" {
		t.Fatalf("EmergencyShutdown reason %q, want the default text", got)
	}
}

// Before llingr_init there is no consumer: the call must return cleanly
// rather than crash the host.
func TestEmergencyStopBeforeInitIsNoOp(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() { emergencyStop("too early") })
}

// The snapshot crosses the FFI as the SAME JSON document the engine's HTTP
// handler serves (snapshot.NewHandler json-encodes the identical struct).
// Pin the top-level structure so a struct rename upstream is caught here.
func TestMarshalSnapshotShape(t *testing.T) {
	data, err := marshalSnapshot(snapshot.Snapshot{})
	if err != nil {
		t.Fatalf("marshalSnapshot: %v", err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal([]byte(data), &doc); err != nil {
		t.Fatalf("snapshot JSON does not parse: %v", err)
	}
	for _, key := range []string{"summary", "throughput", "preCommits", "concurrency", "shards"} {
		if _, ok := doc[key]; !ok {
			t.Fatalf("snapshot JSON missing top-level %q: %s", key, data)
		}
	}
}

// The init-error buffer truncation must never split a UTF-8 rune: the Rust
// side decodes the buffer lossily, so a split rune would decode as U+FFFD
// garbage at the end of an otherwise clean error message. Mirrors the Rust
// boundary test write_c_err_truncates_on_char_boundary.
func TestTruncateToRuneBoundary(t *testing.T) {
	cases := []struct {
		name  string
		msg   string
		limit int
		want  string
	}{
		{"no truncation", "abc", 8, "abc"},
		{"exact fit", "abc", 3, "abc"},
		{"ascii cut", "abcdef", 4, "abcd"},
		{"multibyte preserved", "aébc", 3, "aé"}, // é = 2 bytes
		{"multibyte split backs off", "aébc", 2, "a"},
		{"leading multibyte to zero", "é", 1, ""},
		{"empty", "", 4, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := truncateToRuneBoundary(tc.msg, tc.limit)
			if got := tc.msg[:n]; got != tc.want {
				t.Fatalf("truncateToRuneBoundary(%q, %d) = %q, want %q", tc.msg, tc.limit, got, tc.want)
			}
		})
	}
}
