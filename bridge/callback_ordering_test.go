// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

// Memory-ordering coverage for the callback registration globals: a host may
// register callbacks on one OS thread and initialise/run on another, with
// the two calls ordered only by HOST-side synchronisation (the Rust facade's
// mutex), which the Go memory model cannot see. The tests here reproduce
// exactly that window using a C-side atomic flag: a real hardware edge that
// the Go race detector does not observe. Run under -race, they are the
// machine half of the ordering argument; the bridge's own publication
// mechanism must supply the Go-visible edge for them to pass.

package main

import (
	"runtime"
	"testing"
)

// Registration on one locked OS thread, consumption on another, ordered
// only by the C flag: the bridge's publication must make both the pointer
// VALUE visible (the callback runs) and the access DATA-RACE-FREE (the race
// detector stays quiet).
func TestCrossThreadRegistrationPublishes(t *testing.T) {
	testOrderReset()
	t.Cleanup(clearTestLogCallback)

	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		installTestLogCallbackRaw() // llingr_on_log through the real export
		testOrderSignal()
	}()

	done := make(chan struct{})
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		testOrderAwait() // C-side edge only: invisible to the race detector
		emitLog(logInfo, "cross-thread visibility probe")
		close(done)
	}()
	<-done

	if calls := testLogCalls(); calls != 1 {
		t.Fatalf("registered log callback ran %d times, want 1", calls)
	}
}

// After a successful llingr_init the callback set is sealed: the engine
// reads it per message from running workers, so a late registration is
// ignored (reported on stderr) rather than becoming a live data race.
func TestRegistrationAfterInitIsIgnored(t *testing.T) {
	sealCallbacks()
	t.Cleanup(func() { callbacksSealed.Store(false) })

	before := loadCallbacks().log
	installTestLogCallbackRaw() // llingr_on_log through the real export
	if loadCallbacks().log != before {
		t.Fatal("sealed registration must not change the published set")
	}
}
