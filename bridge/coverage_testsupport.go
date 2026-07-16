// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

// Coverage test support (gap report Tier 1/2): Go test files cannot import
// "C", so the tests that exercise C-typed seams reach them through the
// helpers here. Like testsupport.go, this file is referenced only by tests;
// the linker drops it from shipped builds. The C test callbacks let the
// callback-marshalling code paths run without the Rust side: they record
// what they received into static buffers (single-threaded test use) and
// return a settable code.

package main

/*
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
	const char* key;   int key_len;
	const char* value; int value_len;
} llingr_header;

typedef void (*llingr_log_fn)(int level, const char* msg, int msg_len);

// C-side ordering flag for the cross-thread registration test: a REAL
// hardware release/acquire edge that the Go race detector cannot see,
// standing in for the host-side (Rust) synchronisation the Go memory model
// is blind to.
static _Atomic int test_order_flag;
static _Atomic int test_log_calls;

static void test_order_signal(void) { atomic_store_explicit(&test_order_flag, 1, memory_order_release); }
static void test_order_await(void) { while (!atomic_load_explicit(&test_order_flag, memory_order_acquire)) {} }
static void test_order_reset(void) { atomic_store(&test_order_flag, 0); atomic_store(&test_log_calls, 0); }

static void test_log_cb(int level, const char* msg, int msg_len) {
	(void)level; (void)msg; (void)msg_len;
	atomic_fetch_add(&test_log_calls, 1);
}
static llingr_log_fn get_test_log_cb(void) { return test_log_cb; }
static int get_test_log_calls(void) { return atomic_load(&test_log_calls); }

typedef int (*llingr_deadletter_fn)(
	const char* key, int key_len,
	const char* value, int value_len,
	int partition, int64_t offset,
	int8_t ts_kind, int64_t ts_millis,
	const llingr_header* headers, int header_count,
	const char* error_msg, int error_len
);

typedef void (*llingr_shutdown_fn)(const char* reason, int reason_len);

static int test_deadletter_rc = 0;
static char test_deadletter_reason[256];
static int test_deadletter_reason_len = 0;
static char test_shutdown_reason[256];
static int test_shutdown_reason_len = 0;

static int test_deadletter_cb(
	const char* key, int key_len,
	const char* value, int value_len,
	int partition, int64_t offset,
	int8_t ts_kind, int64_t ts_millis,
	const llingr_header* headers, int header_count,
	const char* error_msg, int error_len) {
	(void)key; (void)key_len; (void)value; (void)value_len;
	(void)partition; (void)offset; (void)ts_kind; (void)ts_millis;
	(void)headers; (void)header_count;
	int n = error_len < 255 ? error_len : 255;
	if (n > 0) memcpy(test_deadletter_reason, error_msg, (size_t)n);
	test_deadletter_reason_len = n;
	return test_deadletter_rc;
}

static void test_shutdown_cb(const char* reason, int reason_len) {
	int n = reason_len < 255 ? reason_len : 255;
	if (n > 0) memcpy(test_shutdown_reason, reason, (size_t)n);
	test_shutdown_reason_len = n;
}

static llingr_deadletter_fn get_test_deadletter_cb(void) { return test_deadletter_cb; }
static llingr_shutdown_fn get_test_shutdown_cb(void) { return test_shutdown_cb; }
static void set_test_deadletter_rc(int rc) { test_deadletter_rc = rc; }
static const char* get_test_deadletter_reason(void) { return test_deadletter_reason; }
static int get_test_deadletter_reason_len(void) { return test_deadletter_reason_len; }
static const char* get_test_shutdown_reason(void) { return test_shutdown_reason; }
static int get_test_shutdown_reason_len(void) { return test_shutdown_reason_len; }
*/
import "C"
import "unsafe"

// installTestDeadletterCallback registers the C test callback as the
// dead-letter target, returning what it should answer and a restore func.
func installTestDeadletterCallback(rc int) func() {
	C.set_test_deadletter_rc(C.int(rc))
	previous := loadCallbacks().deadletter
	setCallback(func(set *callbackSet) { set.deadletter = C.get_test_deadletter_cb() })
	return func() { setCallback(func(set *callbackSet) { set.deadletter = previous }) }
}

// recordedDeadletterReason returns the error text the C test callback last
// received.
func recordedDeadletterReason() string {
	return C.GoStringN(C.get_test_deadletter_reason(), C.get_test_deadletter_reason_len())
}

// installTestShutdownCallback registers the C test callback as the shutdown
// target, returning a restore func.
func installTestShutdownCallback() func() {
	previous := loadCallbacks().shutdown
	setCallback(func(set *callbackSet) { set.shutdown = C.get_test_shutdown_cb() })
	return func() { setCallback(func(set *callbackSet) { set.shutdown = previous }) }
}

// recordedShutdownReason returns the reason the C test callback last received.
func recordedShutdownReason() string {
	return C.GoStringN(C.get_test_shutdown_reason(), C.get_test_shutdown_reason_len())
}

// writeInitErrView runs writeInitErr against a real C buffer of the given
// capacity and decodes what was written.
func writeInitErrView(msg string, capacity int) (string, int) {
	buf := (*C.char)(C.malloc(C.size_t(capacity + 1)))
	defer C.free(unsafe.Pointer(buf))
	var length C.int
	writeInitErr(buf, C.int(capacity), &length, msg)
	return C.GoStringN(buf, length), int(length)
}

// writeInitErrNilGuards exercises writeInitErr's nil/zero-capacity guards;
// it returns normally when the guards hold (no write, no crash).
func writeInitErrNilGuards(msg string) {
	var length C.int = -1
	writeInitErr(nil, 16, &length, msg) // nil buffer
	buf := (*C.char)(C.malloc(16))
	defer C.free(unsafe.Pointer(buf))
	writeInitErr(buf, 0, &length, msg)  // zero capacity
	writeInitErr(buf, 16, nil, msg)     // nil length out
	writeInitErr(buf, -4, &length, msg) // negative capacity
}

// marshalValueView decodes marshalValue's (pointer, length) result back into
// Go, preserving the null/empty distinction.
func marshalValueView(value []byte) (data []byte, null bool) {
	ptr, n := marshalValue(value)
	switch {
	case n < 0:
		return nil, true
	case n == 0:
		return []byte{}, false
	default:
		return C.GoBytes(unsafe.Pointer(ptr), n), false
	}
}

// testOrderSignal/Await/Reset drive the C-side ordering flag (see the
// preamble): a hardware edge invisible to the Go race detector.
func testOrderSignal() { C.test_order_signal() }
func testOrderAwait()  { C.test_order_await() }
func testOrderReset()  { C.test_order_reset() }

// installTestLogCallbackRaw registers the counting C log callback through
// the REAL export, exactly as an FFI host does.
func installTestLogCallbackRaw() { llingr_on_log(C.get_test_log_cb()) }

// testLogCalls reports how many times the counting C log callback ran.
func testLogCalls() int { return int(C.get_test_log_calls()) }

// clearTestLogCallback deregisters the counting C log callback.
func clearTestLogCallback() { llingr_on_log(nil) }

// llingrInitView calls the llingr_init export with real C buffers and decodes
// the return code and error text. Callers must only feed configurations that
// FAIL init: a success would set the process-global consumer state.
func llingrInitView(config string) (int, string) {
	cJSON := C.CString(config)
	defer C.free(unsafe.Pointer(cJSON))
	const capacity = 1024
	buf := (*C.char)(C.malloc(capacity))
	defer C.free(unsafe.Pointer(buf))
	var errLen C.int
	rc := llingr_init(cJSON, C.int(len(config)), buf, capacity, &errLen)
	return int(rc), C.GoStringN(buf, errLen)
}
