// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

// Package main exports a C static library (c-archive) providing FFI bindings
// to llingr-demux, the concurrent ordered message processing engine.
//
// Build with:
//
//	CGO_ENABLED=1 go build -tags netgo -buildmode=c-archive -o libllingr.a .
package main

/*
#include <stdlib.h>
#include <stdint.h>

// All offset, trait, and epoch-timestamp parameters (ms on the message
// callbacks, ns on metrics) are int64_t, never `long`: `long` is 32 bits on
// LLP64 platforms (Windows), where epoch values do not fit at all and
// offsets/trait bits silently truncate. Fixed-width types make the ABI
// identical on every platform.

// One record header. Field order and types are ABI: the Rust HeaderRaw struct
// mirrors this exactly. value_len == -1 signals a NULL value, distinct from an
// empty value (value_len == 0). Keys are UTF-8. All pointers are valid only for
// the duration of the callback.
typedef struct {
	const char* key;   int key_len;
	const char* value; int value_len;
} llingr_header;

// ts_kind classifies the record timestamp: 0 not available, 1 create time
// (producer), 2 log append time (broker). ts_millis is epoch milliseconds,
// meaningful only when ts_kind != 0. headers/header_count carry the record's
// header list (order preserved, keys may repeat); header_count == 0 means none.
//
// value_len == -1 signals a NULL record value (a tombstone), distinct from an
// empty value (value_len == 0), the same convention llingr_header uses. This
// applies to both the process and dead-letter callbacks.

// err_buf/err_cap/err_len_out let the process callback report WHY it failed:
// on a non-zero return the callback writes up to err_cap bytes of its error
// string into err_buf and sets *err_len_out. The bridge then surfaces that
// text as the dead-letter reason, instead of a synthetic "error code N".
typedef int (*llingr_process_fn)(
	const char* key, int key_len,
	const char* value, int value_len,
	int partition, int64_t offset,
	int8_t ts_kind, int64_t ts_millis,
	const llingr_header* headers, int header_count,
	int64_t* traits_out,
	char* err_buf, int err_cap, int* err_len_out
);

typedef int (*llingr_deadletter_fn)(
	const char* key, int key_len,
	const char* value, int value_len,
	int partition, int64_t offset,
	int8_t ts_kind, int64_t ts_millis,
	const llingr_header* headers, int header_count,
	const char* error_msg, int error_len
);

typedef void (*llingr_metrics_fn)(
	int64_t traits, int queue_depth, int partition, int64_t offset,
	int64_t process_duration_ns, int64_t deadletter_duration_ns,
	int64_t read_time_ns, int64_t process_start_time_ns,
	int64_t watermark_advance_time_ns
);

typedef void (*llingr_shutdown_fn)(const char* reason, int reason_len);

// Engine log line. level: 0=debug, 1=info, 2=warn, 3=error. msg is NOT
// NUL-terminated; msg_len bounds it. Valid only for the duration of the call.
typedef void (*llingr_log_fn)(int level, const char* msg, int msg_len);

// Bandwidth telemetry (one flushed nexus.BandwidthMetrics, flattened).
// All strings are (pointer, length) pairs into C-allocated memory, and both
// arrays are C-allocated: everything is valid only for the duration of the
// call. Field order and types are ABI: the Rust #[repr(C)] structs mirror
// them exactly.
typedef struct {
	const char* id;   int id_len;
	const char* host; int host_len;
	const char* port; int port_len;
	const char* rack; int rack_len;
} llingr_broker_info;

typedef struct {
	int64_t ts_unix_ns;
	int64_t received_bytes;
	int64_t transmitted_bytes;
	int64_t received_message_count;
	int64_t compressed_bytes;
	int64_t uncompressed_bytes;
	int32_t id;
	const char* leader;      int leader_len;
	const char* compression; int compression_len;
} llingr_partition_bandwidth;

typedef void (*llingr_bandwidth_fn)(
	int64_t ts_unix_ns,
	int64_t stats_interval_ms,
	const char* metrics_id, int metrics_id_len,
	const char* topic, int topic_len,
	const char* group, int group_len,
	const llingr_broker_info* brokers, int broker_count,
	const llingr_partition_bandwidth* partitions, int partition_count
);

// Trampolines: CGO cannot call C function pointers directly.
// These static C functions forward calls to the registered callbacks.

static int call_process(llingr_process_fn fn,
	const char* key, int key_len,
	const char* value, int value_len,
	int partition, int64_t offset,
	int8_t ts_kind, int64_t ts_millis,
	const llingr_header* headers, int header_count,
	int64_t* traits_out,
	char* err_buf, int err_cap, int* err_len_out) {
	return fn(key, key_len, value, value_len, partition, offset,
		ts_kind, ts_millis, headers, header_count, traits_out,
		err_buf, err_cap, err_len_out);
}

static int call_deadletter(llingr_deadletter_fn fn,
	const char* key, int key_len,
	const char* value, int value_len,
	int partition, int64_t offset,
	int8_t ts_kind, int64_t ts_millis,
	const llingr_header* headers, int header_count,
	const char* error_msg, int error_len) {
	return fn(key, key_len, value, value_len, partition, offset,
		ts_kind, ts_millis, headers, header_count, error_msg, error_len);
}

static void call_metrics(llingr_metrics_fn fn,
	int64_t traits, int queue_depth, int partition, int64_t offset,
	int64_t process_duration_ns, int64_t deadletter_duration_ns,
	int64_t read_time_ns, int64_t process_start_time_ns,
	int64_t watermark_advance_time_ns) {
	fn(traits, queue_depth, partition, offset,
		process_duration_ns, deadletter_duration_ns,
		read_time_ns, process_start_time_ns,
		watermark_advance_time_ns);
}

static void call_shutdown(llingr_shutdown_fn fn, const char* reason, int reason_len) {
	fn(reason, reason_len);
}

static void call_log(llingr_log_fn fn, int level, const char* msg, int msg_len) {
	fn(level, msg, msg_len);
}

static void call_bandwidth(llingr_bandwidth_fn fn,
	int64_t ts_unix_ns, int64_t stats_interval_ms,
	const char* metrics_id, int metrics_id_len,
	const char* topic, int topic_len,
	const char* group, int group_len,
	const llingr_broker_info* brokers, int broker_count,
	const llingr_partition_bandwidth* partitions, int partition_count) {
	fn(ts_unix_ns, stats_interval_ms, metrics_id, metrics_id_len,
		topic, topic_len, group, group_len,
		brokers, broker_count, partitions, partition_count);
}
*/
import "C"
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"
	"unsafe"

	"github.com/llingr/llingr-demux/demux/metrics/snapshot"
	"github.com/llingr/llingr-nexus/nexus"
)

// bridgeState holds the consumer lifecycle state.
// Only one consumer instance per process (Go runtime constraint).
type bridgeState struct {
	mu       sync.Mutex
	consumer consumerHandle
	cancel   context.CancelFunc
	// runDone releases llingr_run. The engine's Subscribe returns once
	// partitions are assigned (the poll loop runs on its own goroutines),
	// so llingr_run parks on this channel to deliver the documented
	// blocks-until-shutdown contract. Closed exactly once, by the shutdown
	// callback (graceful and emergency exits both fire it) and
	// defensively by the one llingr_stop that drove Shutdown.
	runDone     chan struct{}
	runDoneOnce sync.Once
	// stopGateOpen gates the single call to consumer.Shutdown(). The gate
	// starts CLOSED: a stop() arriving before llingr_run has started the
	// engine is ignored, and must be. Reaching Shutdown() there would run
	// the engine's graceful exit and consume its exactly-once shutdown
	// notification before anything had subscribed, orphaning the exit the
	// host actually cares about later. llingr_run OPENS the gate just
	// before Subscribe; the one stop() that closes an open gate is elected
	// the shutdown driver, and only that caller may cancel the bridge
	// context and release runDone, so a losing stop can never cut the
	// winner's drain short. The engine invokes the host shutdown handler
	// from INSIDE Shutdown(), so the shutdown callback also closes the gate
	// before the host handler runs; a handler that calls llingr_stop finds
	// it closed and returns. That re-entrancy is same-goroutine, where a
	// sync.Once would DEADLOCK (Do holds a mutex across f); the
	// non-blocking CAS lets the re-entrant caller return instead.
	stopGateOpen atomic.Bool
	// brokerCleanup releases the broker resources directly (the adapter's
	// Unsubscribe: leave the consumer group with the outcome surfaced, then
	// close the client). Used ONLY after an emergency exit, which
	// bypasses the engine's own Unsubscribe path; see emergencyBrokerCleanup.
	brokerCleanup func() error
}

// initMu serialises llingr_init across concurrent callers WITHOUT holding
// state.mu across the build. The engine emits build-time logs (the licence
// notice, adapter setup) synchronously during buildConsumer, and those reach
// the host log handler; holding state.mu there would deadlock a log handler
// that called back into llingr_stop/llingr_run/llingr_take_snapshot (all of
// which take state.mu). state.mu is taken only for the brief state field
// reads/writes.
var initMu sync.Mutex

var state bridgeState

// callbackSet is the six registered host callback pointers, published as one
// immutable unit. Set before llingr_init() (the log callback must be
// registered before init to capture Build-time engine logs, e.g. the licence
// notice); registration after a successful init is ignored.
type callbackSet struct {
	process    C.llingr_process_fn
	deadletter C.llingr_deadletter_fn
	metrics    C.llingr_metrics_fn
	shutdown   C.llingr_shutdown_fn
	log        C.llingr_log_fn
	bandwidth  C.llingr_bandwidth_fn
}

// Callback publication and its memory-model contract. A host may register on
// one OS thread and init/run on another, ordering the two cgo calls only
// with HOST-side synchronisation (the Rust facade's init lock), which the Go
// memory model cannot see: each cgo entry is serviced by its own goroutine,
// and nothing in the documented model orders one entry's plain writes before
// another's reads (in practice the runtime's cgo entry internals provide the
// barriers, and the race runtime even merges all cgo crossings via
// racecgosync, but neither is a documented contract). The bridge therefore
// supplies its own edge: setters copy-on-write under registrationMu and
// PUBLISH via registeredCallbacks.Store; every reader (the per-message
// closures on engine goroutines, emitLog, the build-time enable checks)
// loads via loadCallbacks. The atomic Store/Load pair is a synchronising
// pair per the Go memory model, so registration is visible to every engine
// goroutine regardless of host threading, with no reliance on the host's
// own ordering.
var (
	// registrationMu serialises the copy-on-write in the llingr_on_* setters
	// (cold path; concurrent setters must not lose each other's field) and
	// the seal handshake.
	registrationMu sync.Mutex
	// callbacksSealed flips on the FIRST SUCCESSFUL llingr_init: from then on
	// engine goroutines read the callbacks per message, so a late
	// registration would be a live change under running workers. Sealing
	// makes it an ignored no-op (reported on stderr) instead. Failed init
	// attempts do not seal, keeping the retry path re-registerable.
	callbacksSealed atomic.Bool
	// registeredCallbacks is the published set; nil means nothing registered.
	registeredCallbacks atomic.Pointer[callbackSet]
)

// emptyCallbacks backs loadCallbacks before any registration.
var emptyCallbacks callbackSet

// loadCallbacks returns the published callback set (never nil). The atomic
// load is the reader half of the publication edge documented above; the
// returned set is immutable (setters publish fresh copies).
func loadCallbacks() *callbackSet {
	if set := registeredCallbacks.Load(); set != nil {
		return set
	}
	return &emptyCallbacks
}

// setCallback applies one registration via copy-on-write, unless the set is
// sealed. Returns whether the registration was applied.
func setCallback(mutate func(*callbackSet)) bool {
	registrationMu.Lock()
	defer registrationMu.Unlock()
	if callbacksSealed.Load() {
		return false
	}
	next := *loadCallbacks()
	mutate(&next)
	registeredCallbacks.Store(&next)
	return true
}

// rejectLateRegistration reports an ignored post-init registration. Outside
// the mutex, and on stderr rather than the log callback: the host is
// misusing the API and its log handler might re-enter registration.
func rejectLateRegistration(name string) {
	fmt.Fprintf(os.Stderr,
		"llingr: %s after successful llingr_init is ignored (register callbacks before init)\n", name)
}

// sealCallbacks closes registration; called once init has succeeded.
func sealCallbacks() {
	registrationMu.Lock()
	callbacksSealed.Store(true)
	registrationMu.Unlock()
}

// abiVersion is the FFI contract version. Bump it on ANY change to an exported
// function signature or callback typedef. The Rust crate checks it at startup
// (llingr_abi_version) and refuses to run against a mismatched library, turning
// a silent ABI-skew memory-safety bug into a clean error.
//
// v1 is the first released contract; the unpublished revisions that preceded
// it were renumbered away, so the released history starts here.
const abiVersion = 1

// errBufCap bounds the error text the process callback can report back per
// failed message. Generous for an error string; truncated cleanly past it.
const errBufCap = 1024

//export llingr_abi_version
func llingr_abi_version() C.int {
	return C.int(abiVersion)
}

// truncateToRuneBoundary returns the largest n <= limit such that msg[:n]
// does not end mid-rune, mirroring the Rust side's write_c_err.
func truncateToRuneBoundary(msg string, limit int) int {
	n := len(msg)
	if n > limit {
		n = limit
		for n > 0 && !utf8.RuneStart(msg[n]) {
			n--
		}
	}
	return n
}

// writeInitErr copies msg into the caller-owned error buffer (truncating at
// errCap, backed off to a UTF-8 boundary so a split rune never garbles the
// text) and stores the byte count. Safe with nil/zero-cap buffers.
func writeInitErr(errBuf *C.char, errCap C.int, errLenOut *C.int, msg string) {
	if errBuf == nil || errLenOut == nil || errCap <= 0 {
		return
	}
	n := truncateToRuneBoundary(msg, int(errCap))
	dst := unsafe.Slice((*byte)(unsafe.Pointer(errBuf)), int(errCap))
	copy(dst, msg[:n])
	*errLenOut = C.int(n)
}

//export llingr_init
func llingr_init(configJSON *C.char, configLen C.int, errBuf *C.char, errCap C.int, errLenOut *C.int) (rc C.int) {
	if errLenOut != nil {
		*errLenOut = 0
	}

	// The engine deliberately panics on invalid configuration (out-of-range
	// DemuxConfig, unsafe librdkafka settings). In-process that is a loud
	// startup failure; across an FFI boundary an unrecovered Go panic kills
	// the host. Convert to a clean error instead. `cancel` is captured so a
	// panic during buildConsumer still releases the context and the goroutines
	// a partial build started, instead of leaking them.
	var cancel context.CancelFunc
	defer func() {
		if r := recover(); r != nil {
			if cancel != nil {
				cancel()
			}
			writeInitErr(errBuf, errCap, errLenOut, fmt.Sprintf("invalid configuration: %v", r))
			rc = C.int(errBadOption)
		}
	}()

	// Serialise concurrent init via initMu (NOT state.mu): buildConsumer runs
	// below without state.mu held, so the engine's synchronous build-time logs
	// cannot deadlock a log handler that re-enters llingr_stop/run/snapshot.
	initMu.Lock()
	defer initMu.Unlock()

	state.mu.Lock()
	alreadyInit := state.consumer != nil
	state.mu.Unlock()
	if alreadyInit {
		writeInitErr(errBuf, errCap, errLenOut, "already initialised (one llingr instance per process)")
		return C.int(errAlreadyInit)
	}

	data := C.GoBytes(unsafe.Pointer(configJSON), configLen)
	cfg, berr := parseBridgeConfig(data)
	if berr != nil {
		writeInitErr(errBuf, errCap, errLenOut, berr.msg)
		return C.int(berr.code)
	}

	// One Info line records the build variant where it matters most: the
	// running deployment's log stream (no-op without a log callback). Emitted
	// before buildConsumer so an adapter-not-compiled error is preceded by
	// the list of adapters that ARE present.
	variant := strings.Join(compiledAdapters, ", ")
	if variant == "" {
		variant = "none (stub-only build)"
	}
	emitLog(logInfo, "libllingr adapters compiled in: %s", variant)

	ctx, c := context.WithCancel(context.Background())
	cancel = c

	// buildConsumer runs under initMu only. Its build-time engine logs reach
	// the host log handler without state.mu held.
	consumer, brokerCleanup, berr := buildConsumer(ctx, cfg)
	if berr != nil {
		cancel()
		writeInitErr(errBuf, errCap, errLenOut, berr.msg)
		return C.int(berr.code)
	}

	state.mu.Lock()
	state.cancel = cancel
	state.consumer = consumer
	state.brokerCleanup = brokerCleanup
	state.runDone = make(chan struct{})
	state.mu.Unlock()
	// Close callback registration: the engine now reads the set per message
	// from its own goroutines, so a later llingr_on_* would be a live change
	// under running workers (see callbacksSealed).
	sealCallbacks()
	// The context now belongs to state; the panic recovery above must not
	// cancel the running engine's context on any later (currently impossible)
	// panic.
	cancel = nil
	return 0
}

// signalRunDone releases a parked llingr_run. Safe to call from any
// goroutine, any number of times, before or after llingr_run parks.
func signalRunDone() {
	state.mu.Lock()
	done := state.runDone
	state.mu.Unlock()
	if done == nil {
		return
	}
	state.runDoneOnce.Do(func() { close(done) })
}

//export llingr_run
func llingr_run() (rc C.int) {
	// Recover panics at the FFI boundary. An unrecovered Go panic calls
	// runtime.exit(2), which kills the entire host process (Rust/Python).
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "llingr: recovered panic in llingr_run: %v\n", r)
			rc = -3 // panic recovered
		}
	}()

	state.mu.Lock()
	consumer := state.consumer
	done := state.runDone
	state.mu.Unlock()

	if consumer == nil {
		return -1 // not initialised
	}

	// Open the stop gate: from here a stop() is honoured. Opened BEFORE
	// Subscribe so a stop() racing startup still stops the engine rather
	// than being lost; a stop() that arrived before this point was ignored
	// (nothing was consuming yet).
	state.stopGateOpen.Store(true)

	// Subscribe returns once the group join completes and partitions are
	// assigned (or errors on the assignment timeout); the poll loop runs on
	// engine goroutines, NOT this thread. Park here until the engine's
	// shutdown callback fires, so llingr_run delivers the documented
	// blocks-until-shutdown contract: a host whose main thread sits in
	// run() keeps consuming until stop() or an emergency shutdown.
	if err := consumer.Subscribe(); err != nil {
		return -2
	}
	<-done
	return 0
}

//export llingr_take_snapshot
func llingr_take_snapshot() (out *C.char) {
	// A point-in-time view of the consumer's state (the engine's
	// Consumer.TakeSnapshot, safe from any goroutine), marshalled to the
	// same JSON document the Go SnapshotHandler serves. Returns NULL when
	// the engine is not initialised or marshalling fails; the caller owns
	// the returned string and must release it with llingr_free_string.
	//
	// Recover panics at the FFI boundary: this export is documented for
	// mounting on an operational HTTP route, where an engine panic during
	// snapshotting must surface as a NULL response, not kill the host.
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "llingr: recovered panic in llingr_take_snapshot: %v\n", r)
			out = nil
		}
	}()

	state.mu.Lock()
	consumer := state.consumer
	state.mu.Unlock()

	if consumer == nil {
		return nil
	}
	data, err := marshalSnapshot(consumer.TakeSnapshot())
	if err != nil {
		fmt.Fprintf(os.Stderr, "llingr: snapshot marshal failed: %v\n", err)
		return nil
	}
	return C.CString(data)
}

// marshalSnapshot renders the snapshot exactly as the engine's HTTP handler
// does (snapshot.NewHandler json-encodes the same struct), so the document a
// Rust application serves is byte-compatible with the Go one.
func marshalSnapshot(snap snapshot.Snapshot) (string, error) {
	data, err := json.Marshal(snap)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

//export llingr_free_string
func llingr_free_string(s *C.char) {
	if s != nil {
		C.free(unsafe.Pointer(s))
	}
}

//export llingr_stop
func llingr_stop() {
	// Recover panics at the FFI boundary (same rationale as llingr_run): an
	// unrecovered Go panic in Shutdown would kill the host process instead
	// of failing the one call.
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "llingr: recovered panic in llingr_stop: %v\n", r)
		}
	}()

	state.mu.Lock()
	consumer := state.consumer
	cancel := state.cancel
	state.mu.Unlock()

	if consumer == nil {
		return
	}

	// Drive the engine's Shutdown at most once: only the stop() that closes
	// an OPEN gate proceeds. Every other caller simply gives up: it arrived
	// before run() opened the gate (nothing consuming yet), after shutdown
	// already began (a lost concurrent race, a re-entrant call from the
	// shutdown handler, or an emergency exit), and it must not cancel
	// the context or touch runDone while the winner may still be draining.
	// See stopGateOpen for why this is a CAS and not a sync.Once.
	if !state.stopGateOpen.CompareAndSwap(true, false) {
		return
	}

	// Only this goroutine backstops runDone (the shutdown callback normally
	// closes it post-drain; this covers Shutdown panicking or returning
	// before invoking the callback) and cancels the bridge context, strictly
	// AFTER Shutdown has finished so the drain's final commit is never cut
	// short. Deferred (LIFO: cancel, then runDone) so a Shutdown panic still
	// releases both.
	defer signalRunDone()
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()
	_ = consumer.Shutdown()
}

//export llingr_emergency_stop
func llingr_emergency_stop(reason *C.char, reasonLen C.int) {
	msg := ""
	if reason != nil && reasonLen > 0 {
		msg = C.GoStringN(reason, reasonLen)
	}
	emergencyStop(msg)
}

// emergencyStop forwards the host's emergency-stop request to the engine:
// abandon in-flight work and stop now, no drain, no final commit. The stop
// gate is deliberately not consulted: the engine's emergency path is safe
// from any lifecycle state and elects its own single deliverer, so the
// bridge forwards unconditionally. The shutdown callback then fires exactly
// once with this reason and runs the usual emergency seam (gate closed,
// broker released, a parked llingr_run released). Before llingr_init there
// is no consumer and the call is a no-op.
func emergencyStop(reason string) {
	// Recover panics at the FFI boundary (same rationale as llingr_stop).
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "llingr: recovered panic in llingr_emergency_stop: %v\n", r)
		}
	}()

	state.mu.Lock()
	consumer := state.consumer
	state.mu.Unlock()

	if consumer == nil {
		return
	}

	if reason == "" {
		reason = "emergency stop requested by host"
	}
	consumer.EmergencyShutdown(errors.New(reason))
}

//export llingr_on_process
func llingr_on_process(cb C.llingr_process_fn) {
	if !setCallback(func(set *callbackSet) { set.process = cb }) {
		rejectLateRegistration("llingr_on_process")
	}
}

//export llingr_on_deadletter
func llingr_on_deadletter(cb C.llingr_deadletter_fn) {
	if !setCallback(func(set *callbackSet) { set.deadletter = cb }) {
		rejectLateRegistration("llingr_on_deadletter")
	}
}

//export llingr_on_metrics
func llingr_on_metrics(cb C.llingr_metrics_fn) {
	if !setCallback(func(set *callbackSet) { set.metrics = cb }) {
		rejectLateRegistration("llingr_on_metrics")
	}
}

//export llingr_on_shutdown
func llingr_on_shutdown(cb C.llingr_shutdown_fn) {
	if !setCallback(func(set *callbackSet) { set.shutdown = cb }) {
		rejectLateRegistration("llingr_on_shutdown")
	}
}

//export llingr_on_log
func llingr_on_log(cb C.llingr_log_fn) {
	if !setCallback(func(set *callbackSet) { set.log = cb }) {
		rejectLateRegistration("llingr_on_log")
	}
}

//export llingr_on_bandwidth
func llingr_on_bandwidth(cb C.llingr_bandwidth_fn) {
	// Registration is the enable signal: when set before llingr_init, the
	// bridge turns on the adapter's bandwidth collection and wires the
	// engine's aggregator to this callback.
	if !setCallback(func(set *callbackSet) { set.bandwidth = cb }) {
		rejectLateRegistration("llingr_on_bandwidth")
	}
}

// Timestamp kinds forwarded as ts_kind. Mirrors the Rust Timestamp enum.
const (
	tsNotAvailable  int8 = 0
	tsCreateTime    int8 = 1
	tsLogAppendTime int8 = 2
)

// bridgeHeader is one record header extracted from the adapter-native payload.
// A nil value is a null header (distinct from an empty, non-nil value).
type bridgeHeader struct {
	key   string
	value []byte
}

// recordMeta is the per-message metadata that lives in the broker-native
// payload (not the nexus envelope): the record timestamp and its headers.
// metaOf extracts it per adapter, mirroring valueOf.
type recordMeta struct {
	tsKind   int8
	tsMillis int64
	headers  []bridgeHeader
}

// marshalHeaders packs headers into ONE C allocation (the llingr_header array
// followed by the key/value bytes its pointers reference), so a message with
// headers costs a single malloc/free rather than one per field. Returns
// (nil, 0, no-op) when there are none. value_len == -1 marks a null value.
//
// The returned pointers reference C memory only; no Go pointer is stored in
// the block, so passing it to C is cgo-legal. The caller must defer the
// cleanup, which frees the block after the synchronous callback returns.
func marshalHeaders(headers []bridgeHeader) (*C.llingr_header, C.int, func()) {
	n := len(headers)
	if n == 0 {
		return nil, 0, func() {}
	}

	structsBytes := n * int(unsafe.Sizeof(C.llingr_header{}))
	total := structsBytes
	for _, h := range headers {
		total += len(h.key)
		if h.value != nil {
			total += len(h.value)
		}
	}

	block := C.malloc(C.size_t(total))
	arr := (*C.llingr_header)(block)
	structs := unsafe.Slice(arr, n)
	bytes := unsafe.Slice((*byte)(unsafe.Add(block, structsBytes)), total-structsBytes)

	// Append size bytes into the arena and return a C pointer to them (nil
	// for empty). `copy` takes a string source directly, so keys are not
	// reallocated.
	cursor := 0
	put := func(size int, copyIn func(dst []byte)) (*C.char, C.int) {
		if size == 0 {
			return nil, 0
		}
		copyIn(bytes[cursor : cursor+size])
		ptr := (*C.char)(unsafe.Pointer(&bytes[cursor]))
		cursor += size
		return ptr, C.int(size)
	}

	for i, h := range headers {
		structs[i].key, structs[i].key_len = put(len(h.key), func(dst []byte) { copy(dst, h.key) })
		if h.value == nil {
			structs[i].value = nil
			structs[i].value_len = -1
		} else {
			structs[i].value, structs[i].value_len = put(len(h.value), func(dst []byte) { copy(dst, h.value) })
		}
	}

	return arr, C.int(n), func() { C.free(block) }
}

// marshalValue prepares the record value for the process and dead-letter
// callbacks. A nil slice is a null value (a tombstone on a compacted topic):
// value_len == -1, mirroring the header convention, so the host can
// distinguish "delete this key" from an empty payload (value_len == 0). The
// returned pointer aliases the slice's backing array; the caller's message
// keeps it alive across the synchronous callback. It is nil for both null
// and empty values.
func marshalValue(value []byte) (*C.char, C.int) {
	if value == nil {
		return nil, -1
	}
	if len(value) == 0 {
		return nil, 0
	}
	return (*C.char)(unsafe.Pointer(&value[0])), C.int(len(value))
}

// makeProcessMessage returns a nexus.ProcessMessage[T] that forwards each
// message to the registered C function pointer.
//
// Key, partition, and offset come from the nexus envelope: both adapters
// guarantee the key is UTF-8-safe (raw if valid UTF-8, base64 if binary,
// partition number if absent), and this respects the adapter's canonical
// extraction rather than re-implementing it per payload type. valueOf pulls
// the raw value bytes from the adapter-native payload; metaOf pulls the
// timestamp and headers from it.
func makeProcessMessage[T any](valueOf func(T) []byte, metaOf func(T) recordMeta) nexus.ProcessMessage[T] {
	return func(_ context.Context, msg *nexus.Message[T]) error {
		fn := loadCallbacks().process
		if fn == nil {
			return errors.New("llingr: no process callback registered")
		}

		keyPtr := C.CString(msg.Key)
		defer C.free(unsafe.Pointer(keyPtr))

		valuePtr, valueLen := marshalValue(valueOf(*msg.Payload))

		meta := metaOf(*msg.Payload)
		hdrPtr, hdrCount, freeHeaders := marshalHeaders(meta.headers)
		defer freeHeaders()

		// Stack buffer the callback writes its error text into on failure. It
		// holds no Go pointers and is not retained past the synchronous call,
		// so passing it to C is cgo-legal (no heap alloc on the hot path).
		var errBuf [errBufCap]C.char
		var errLen C.int

		var traitsOut C.int64_t
		rc := C.call_process(fn,
			keyPtr, C.int(len(msg.Key)),
			valuePtr, valueLen,
			C.int(msg.Partition), C.int64_t(msg.Offset),
			C.int8_t(meta.tsKind), C.int64_t(meta.tsMillis),
			hdrPtr, hdrCount,
			&traitsOut,
			&errBuf[0], C.int(errBufCap), &errLen,
		)

		// Apply custom traits returned by the host callback (bits 10-63)
		if traitsOut != 0 {
			msg.AddCustomTraits(nexus.Traits(traitsOut))
		}

		if rc != 0 {
			// Surface the callback's own error text (rides the engine's
			// reason plumbing to the dead-letter handler). Fall back to a
			// synthetic message only if the callback wrote nothing.
			if errLen > 0 {
				return errors.New(C.GoStringN(&errBuf[0], errLen))
			}
			return fmt.Errorf("process callback returned error code: %d", int(rc))
		}
		return nil
	}
}

// makeWriteDeadLetter returns a nexus.WriteDeadLetter[T] that forwards
// failed messages to the registered C function pointer.
func makeWriteDeadLetter[T any](valueOf func(T) []byte, metaOf func(T) recordMeta) nexus.WriteDeadLetter[T] {
	return func(_ context.Context, msg *nexus.Message[T], reason error) error {
		fn := loadCallbacks().deadletter
		if fn == nil {
			return nil // no dead-letter handler registered, silently discard
		}

		keyPtr := C.CString(msg.Key)
		defer C.free(unsafe.Pointer(keyPtr))

		valuePtr, valueLen := marshalValue(valueOf(*msg.Payload))

		meta := metaOf(*msg.Payload)
		hdrPtr, hdrCount, freeHeaders := marshalHeaders(meta.headers)
		defer freeHeaders()

		reasonStr := ""
		if reason != nil {
			reasonStr = reason.Error()
		}
		reasonPtr := C.CString(reasonStr)
		defer C.free(unsafe.Pointer(reasonPtr))

		rc := C.call_deadletter(fn,
			keyPtr, C.int(len(msg.Key)),
			valuePtr, valueLen,
			C.int(msg.Partition), C.int64_t(msg.Offset),
			C.int8_t(meta.tsKind), C.int64_t(meta.tsMillis),
			hdrPtr, hdrCount,
			reasonPtr, C.int(len(reasonStr)),
		)

		if rc != 0 {
			return fmt.Errorf("dead-letter callback returned error code: %d", int(rc))
		}
		return nil
	}
}

// metricsSinkCallback returns a nexus.MetricsSink that forwards
// per-message metrics to the registered C function pointer.
//
// All timing fields from nexus.Metrics are forwarded as nanoseconds,
// matching the gRPC bridge's callback_metrics_sink.go pattern.
func metricsSinkCallback() nexus.MetricsSink {
	return func(_ nexus.SinkContext, metrics nexus.Metrics) error {
		fn := loadCallbacks().metrics
		if fn == nil {
			return nil // no metrics handler, silently discard
		}

		C.call_metrics(fn,
			C.int64_t(metrics.Traits),
			C.int(metrics.QueueDepth),
			C.int(metrics.Partition),
			C.int64_t(metrics.Offset),
			C.int64_t(metrics.ProcessDuration.Nanoseconds()),
			C.int64_t(metrics.WriteDeadLetterDuration.Nanoseconds()),
			C.int64_t(metrics.ReadTime.UnixNano()),
			C.int64_t(metrics.ProcessStartTime.UnixNano()),
			C.int64_t(metrics.WatermarkAdvanceTime.UnixNano()),
		)

		return nil
	}
}

// bandwidthSink returns a nexus.BandwidthMetricsSink forwarding each flushed
// packet to the registered C callback, flattened into C-allocated arrays.
// C allocation is required by the cgo pointer rules: the structs carry
// string pointers, and Go memory containing Go pointers must not be passed
// to C. The sink runs on the aggregator's flush cadence, well off the
// message hot path, so the per-flush allocations are irrelevant.
//
// The packet's Service field is deliberately not forwarded: the host set the
// service identity on its own builder and already knows it.
func bandwidthSink() nexus.BandwidthMetricsSink {
	return func(topicName string, m nexus.BandwidthMetrics) error {
		fn := loadCallbacks().bandwidth
		if fn == nil {
			return nil
		}

		var frees []unsafe.Pointer
		defer func() {
			for _, p := range frees {
				C.free(p)
			}
		}()
		cstr := func(s string) (*C.char, C.int) {
			p := C.CString(s)
			frees = append(frees, unsafe.Pointer(p))
			return p, C.int(len(s))
		}

		var brokers *C.llingr_broker_info
		if len(m.Brokers) > 0 {
			size := C.size_t(len(m.Brokers)) * C.size_t(unsafe.Sizeof(C.llingr_broker_info{}))
			brokers = (*C.llingr_broker_info)(C.malloc(size))
			frees = append(frees, unsafe.Pointer(brokers))
			out := unsafe.Slice(brokers, len(m.Brokers))
			for i, b := range m.Brokers {
				out[i].id, out[i].id_len = cstr(b.ID)
				out[i].host, out[i].host_len = cstr(b.Host)
				out[i].port, out[i].port_len = cstr(b.Port)
				out[i].rack, out[i].rack_len = cstr(b.Rack)
			}
		}

		var partitions *C.llingr_partition_bandwidth
		if len(m.Partitions) > 0 {
			size := C.size_t(len(m.Partitions)) * C.size_t(unsafe.Sizeof(C.llingr_partition_bandwidth{}))
			partitions = (*C.llingr_partition_bandwidth)(C.malloc(size))
			frees = append(frees, unsafe.Pointer(partitions))
			out := unsafe.Slice(partitions, len(m.Partitions))
			for i, p := range m.Partitions {
				out[i].ts_unix_ns = C.int64_t(p.Ts.UnixNano())
				out[i].received_bytes = C.int64_t(p.ReceivedBytes)
				out[i].transmitted_bytes = C.int64_t(p.TransmittedBytes)
				out[i].received_message_count = C.int64_t(p.ReceivedMessageCount)
				out[i].compressed_bytes = C.int64_t(p.CompressedBytes)
				out[i].uncompressed_bytes = C.int64_t(p.UncompressedBytes)
				out[i].id = C.int32_t(p.ID)
				out[i].leader, out[i].leader_len = cstr(p.Leader)
				out[i].compression, out[i].compression_len = cstr(p.Compression)
			}
		}

		topic := m.TopicName
		if topic == "" {
			topic = topicName
		}
		metricsID, metricsIDLen := cstr(m.BandwidthMetricsID)
		topicPtr, topicLen := cstr(topic)
		groupPtr, groupLen := cstr(m.ConsumerGroup)

		C.call_bandwidth(fn,
			C.int64_t(m.Ts.UnixNano()),
			C.int64_t(m.StatsIntervalDuration.Milliseconds()),
			metricsID, metricsIDLen,
			topicPtr, topicLen,
			groupPtr, groupLen,
			brokers, C.int(len(m.Brokers)),
			partitions, C.int(len(m.Partitions)),
		)
		return nil
	}
}

// shutdownCallback returns a nexus.ShutdownCallback that notifies the
// registered C function pointer of consumer shutdown.
func shutdownCallback() nexus.ShutdownCallback {
	return func(_ context.Context, reason error) {
		// The engine delivers this exactly once, on whichever exit happens
		// first (graceful Shutdown or an emergency exit), from inside its
		// shutdown sequence. Close the stop gate BEFORE running the host
		// handler, so a handler that calls llingr_stop finds it closed and
		// does not re-enter Shutdown() (see llingr_stop).
		state.stopGateOpen.Store(false)

		// Release the parked llingr_run AFTER the host's shutdown handler ran
		// (and, on an emergency exit, after the broker is released), so run()
		// returning is the last event the host observes.
		defer signalRunDone()

		// An emergency exit (non-nil reason) bypasses the engine's
		// Unsubscribe path entirely: nothing has left the consumer group or
		// closed the broker client, and stop() will not drive Shutdown (gate
		// closed). Release the broker here, after the host handler has run.
		// A graceful Shutdown never takes this branch; its drain already
		// released the broker.
		if reason != nil {
			defer emergencyBrokerCleanup()
		}

		fn := loadCallbacks().shutdown
		if fn == nil {
			return
		}

		reasonStr := "graceful shutdown"
		if reason != nil {
			reasonStr = reason.Error()
		}

		reasonPtr := C.CString(reasonStr)
		defer C.free(unsafe.Pointer(reasonPtr))

		C.call_shutdown(fn, reasonPtr, C.int(len(reasonStr)))
	}
}

// emergencyBrokerCleanup leaves the consumer group (outcome surfaced) and
// closes the broker client after an emergency exit, where the engine's
// own Unsubscribe cleanup never ran; without it the client stays connected and
// the group only evicts this member at session timeout. No bridge-side once
// guard: the engine delivers the shutdown notification exactly once, and both
// adapters guard Unsubscribe internally, so an engine-side unsubscribe still
// in flight (a timed-out drain that later completes) cannot double-free. Also
// cancels the bridge context, which no stop() will do on this path (the gate
// is already closed).
func emergencyBrokerCleanup() {
	state.mu.Lock()
	cleanup := state.brokerCleanup
	cancel := state.cancel
	state.mu.Unlock()

	if cancel != nil {
		defer cancel()
	}
	if cleanup == nil {
		return
	}
	// This runs on an engine goroutine (the emergency exit's notifier); a
	// panic here would kill the host process, so contain it like the
	// exported entry points do.
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "llingr: recovered panic in emergency broker cleanup: %v\n", r)
		}
	}()
	if err := cleanup(); err != nil {
		fmt.Fprintf(os.Stderr, "llingr: broker cleanup after emergency shutdown failed: %v\n", err)
	}
}

// Log levels forwarded over llingr_log_fn. Matches the Rust LogLevel enum.
const (
	logDebug = 0
	logInfo  = 1
	logWarn  = 2
	logError = 3
)

// bridgeLogger implements nexus.Logger, forwarding formatted engine log
// lines to the registered C function pointer. Only installed when the host
// registered a log callback (see newBridgeBuilder). Logging is off the
// message hot path, so the CString allocation per line is acceptable.
type bridgeLogger struct{}

func (bridgeLogger) Error(_ context.Context, format string, args ...any) {
	emitLog(logError, format, args...)
}
func (bridgeLogger) Warn(_ context.Context, format string, args ...any) {
	emitLog(logWarn, format, args...)
}
func (bridgeLogger) Info(_ context.Context, format string, args ...any) {
	emitLog(logInfo, format, args...)
}
func (bridgeLogger) Debug(_ context.Context, format string, args ...any) {
	emitLog(logDebug, format, args...)
}

func emitLog(level int, format string, args ...any) {
	fn := loadCallbacks().log
	if fn == nil {
		return
	}
	msg := fmt.Sprintf(format, args...)
	msgPtr := C.CString(msg)
	defer C.free(unsafe.Pointer(msgPtr))
	C.call_log(fn, C.int(level), msgPtr, C.int(len(msg)))
}

// Required for c-shared build mode.
func main() {}
