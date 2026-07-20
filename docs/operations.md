# Operations

Running llingr-kafka in production comes down to starting the engine, shutting
it down cleanly, and understanding the handful of rules that follow from the
fact that a Go runtime lives inside your Rust process. This page covers the run
and stop lifecycle, how to wire signal handling to it, what an emergency stop
costs you, how to read a running consumer's state through snapshots, and the
process-level constraints (one instance per process, thread budgeting, liveness)
you plan around. Where behaviour has a sharp edge, it is called out plainly.

## Starting and stopping: the run loop blocks

`engine.run()` starts consuming and blocks the calling thread until the engine
shuts down. The engine's poll loop and per-key workers run on Go runtime
threads, not on the thread that called `run()`; that thread is simply parked
until shutdown. `run()` returns `Ok(())` only after a graceful stop has
completed its drain and final commit, or when an emergency stop has terminated
the consumer. It returns an error if the engine was never initialised, if the
initial partition assignment fails or times out, or if the engine hits a fatal
error.

Because `run()` blocks, you arrange shutdown before you call it. The engine hands
you a stopper for exactly this: `engine.stopper()` returns a closure that is
`Send + 'static`, so you can move it to another thread and call it to trigger a
graceful shutdown. Get the stopper, hand it to whatever will decide to shut down
(usually a signal-watcher thread), then call `run()`.

`engine.stop()` performs a graceful shutdown: it drains in-flight messages,
commits offsets, and releases the parked `run()` call. It is safe to call from
any thread. The call that initiates the shutdown (the one that wins the internal
stop gate) blocks until the drain and final commit have completed, so when that
call returns the process can exit without losing acknowledged work; a concurrent
second `stop()` returns immediately while the first is still draining, rather
than blocking behind it. Have exactly one place call the stopper, and treat that
call's return as the signal that shutdown is complete. Graceful stop is the
clean path: provided the drain completes within the
`drain_timeout` engine setting (default 20 seconds), a rolling restart produces zero
duplicates, because the engine commits exactly the contiguous work it finished.
The exception is work the drain could not finish in time: a drain timeout
abandons it uncommitted, so it is redelivered at least once on the next start.

Two rules make `stop()` safe to reason about:

- **A `stop()` called before `run()` has started consuming is ignored.**
  There is nothing to stop yet, and the later `run()` remains fully stoppable.
  A shutdown signal that can arrive during startup should be re-checked once
  `run()` is underway, or you can simply exit the process.
- **Never call `stop()` from inside a message handler.** `stop()` drains the
  workers, and your handlers run on those workers, so calling it from a
  `ProcessHandler` or `DeadLetterHandler` asks the engine to drain the very
  worker that is blocked in the call. The drain stalls until the engine gives
  up on it (the `drain_timeout` engine setting, default 20 seconds) and that
  message's completion is discarded. To shut down in response to a message, set
  a flag or send on a channel from the handler and call the stopper from another
  thread. Calling `stop()` from the optional shutdown handler is a harmless
  no-op: the engine is already shutting down and recognises the re-entrant call.

## Signal handling

A production consumer shuts down on `SIGINT` or `SIGTERM`. The rule that governs
how you wire that is absolute: **never call `stop()`, `emergency_stop()`, or any
other engine function from a signal handler.** Go code is not async-signal-safe,
and calling an engine function from a signal-handler context will crash the
process. The correct pattern is the standard one: the signal handler only flips
an atomic flag, and an ordinary thread watches the flag and calls the stopper.
The `signal-hook` crate registers such a flag for you.

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use llingr_kafka::{Builder, Message, Traits, ProcessHandler, DeadLetterHandler};

struct Orders;
impl ProcessHandler for Orders {
    fn process(&self, _msg: &Message) -> Result<Traits, Box<dyn std::error::Error>> {
        Ok(Traits::none())
    }
}
struct DeadLetters;
impl DeadLetterHandler for DeadLetters {
    fn handle(&self, _msg: &Message, _error: &str) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = Builder::new("orders", Orders, DeadLetters)
        .brokers("localhost:9092")
        .consumer_group("orders-svc")
        .build()?;

    // The handler flips this flag; it never touches the engine.
    let shutdown = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&shutdown))?;
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&shutdown))?;

    // An ordinary thread watches the flag and calls the stopper.
    let stop = engine.stopper();
    thread::spawn(move || {
        while !shutdown.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(100));
        }
        stop(); // graceful: drain in-flight work, commit, release run()
    });

    engine.run()?; // blocks here until stop() completes its drain
    Ok(())
}
```

How this coexists with the Go runtime is worth understanding, because the engine
links statically. The Go runtime initialises when your process loads, before
`main` runs, and during that load it installs its own handlers for `SIGSEGV`,
`SIGBUS`, `SIGFPE`, `SIGPROF`, and `SIGURG`. It does not touch `SIGINT` or
`SIGTERM`: neither the engine nor the broker adapter imports Go's `os/signal`, so
Go never claims those two, and registering flags for them (as above, with
`signal-hook`) is conflict-free. `signal-hook` also chains to any pre-existing
handler, so it composes rather than replacing.

Two cautions follow from Go owning the fault signals. Do not install a
non-chaining handler for `SIGSEGV`, `SIGBUS`, or `SIGFPE`: replacing Go's handler
there breaks the runtime's own fault handling. Anything that legitimately must
handle one of them needs the `SA_ONSTACK` flag, because Go's handlers run on an
alternate signal stack. And expect visible `SIGURG` noise in a debugger or under
`strace`: Go uses it for goroutine preemption, and it is entirely normal. The
absolute rule is unchanged throughout: never call an engine function from
signal-handler context.

## Emergency stop: fast, at the cost of duplicates

`engine.emergency_stop(reason: &str)` stops the engine immediately without
draining. Nothing in flight is finished and no final commit is made: messages
being processed at that instant are abandoned uncommitted. On the next start
those messages are redelivered, so **downstream consumers must tolerate
duplicates**. This is not a defect; it is the engine's delivery contract.
llingr-kafka is at-least-once: a message is delivered at least once, and after
an emergency stop some messages are delivered again. Design your processing to
be idempotent (a dedup key, a conditional write, a self-identifying payload) so
a redelivery is harmless.

`emergency_stop` is safe to call from any thread, in any lifecycle state, and
repeatedly: only the first effective call does anything, and later calls are
no-ops. Unlike `stop()`, it is safe to call from inside a `ProcessHandler` or
`DeadLetterHandler`, because the emergency path never waits on the worker your
handler runs on: it abandons that work rather than draining it. The one
unchanged rule is the signal-handler rule above: relay through an atomic flag
and an ordinary thread, never call it from a signal handler.

Reach for `emergency_stop` when continuing is worse than the duplicates a
restart will cost: an unrecoverable dependency, a poison condition you have
detected in a handler, or a supervised environment where a fast exit and
restart beats a slow drain.

## The shutdown callback fires exactly once

Register an optional shutdown handler on the builder to learn why the consumer
stopped. Its `handle(&self, reason: &str)` method is called exactly once, on
either path: a graceful `stop()` passes `reason` as `"graceful shutdown"`, and
an emergency stop passes the `reason` string you supplied to `emergency_stop`
(an empty string becomes a default description). A thread parked in `run()`
returns once the handler has run and the broker client has been released.

```rust
use llingr_kafka::{Builder, ShutdownHandler, Message, Traits,
                   ProcessHandler, DeadLetterHandler};

struct OnShutdown;
impl ShutdownHandler for OnShutdown {
    fn handle(&self, reason: &str) {
        // Exactly once, on graceful stop or emergency exit. Flush your own
        // buffers, emit a final metric, note the reason.
        eprintln!("consumer stopped: {reason}");
    }
}

struct Orders;
impl ProcessHandler for Orders {
    fn process(&self, _msg: &Message) -> Result<Traits, Box<dyn std::error::Error>> {
        Ok(Traits::none())
    }
}
struct DeadLetters;
impl DeadLetterHandler for DeadLetters {
    fn handle(&self, _msg: &Message, _error: &str) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}

fn build() -> Result<(), Box<dyn std::error::Error>> {
    let _engine = Builder::new("orders", Orders, DeadLetters)
        .brokers("localhost:9092")
        .consumer_group("orders-svc")
        .shutdown(OnShutdown)
        .build()?;
    Ok(())
}
```

## Exiting without stopping is safe but wasteful

If the process exits without a graceful `stop()`, whether a crash, a `SIGKILL`,
or `main` returning while `run()` is on another thread, you lose no
acknowledged work. The engine only commits contiguous completed work, so
anything uncommitted is simply redelivered at least once on the next start. It
is safe; it is only wasteful, because that work is done twice. A clean `stop()`
avoids the rework and the duplicates; an abrupt exit trades them for speed.

## Reading a running consumer: snapshots

You can take a point-in-time view of the consumer's internal state at any time,
from any thread, at any frequency reasonable for an operational endpoint. Two
methods return the same underlying document in two shapes:

- `engine.snapshot()` returns a typed `Snapshot` (serde structs over the
  document), for programmatic checks inside the process. A liveness watchdog
  that reads per-partition gap-buffer depth, or an autoscaler reading throughput,
  wants this typed form.
- `engine.snapshot_json()` returns the canonical JSON document as a `String`.
  It is byte-identical to what the Go engine's own HTTP snapshot handler serves,
  so it lines up across the Go and Rust ecosystems and is the right thing to
  proxy verbatim onto an HTTP route.

```rust
# fn demo(engine: &llingr_kafka::Llingr) -> Result<(), Box<dyn std::error::Error>> {
let snap = engine.snapshot()?;       // typed Snapshot, for in-process checks
let body = engine.snapshot_json()?;  // canonical JSON, for serving verbatim
# let _ = (snap, body);
# Ok(())
# }
```

The document summarises the topic, sliding fifteen-second throughput windows
with latency figures, per-partition offset tracking with gap-buffer depths,
guard-channel utilisation, and per-shard worker counts. Mount `snapshot_json()`
on whatever HTTP stack your application already runs to expose it as an
operational endpoint; because both forms come from the engine itself, they never
disagree with what the engine is actually doing.

Both methods return an error if the engine is not yet initialised.

## One instance per process

The Go runtime is process-global, so **only one engine may exist per process**.
A second `build()` in the same process returns an error rather than starting a
second engine. To run more consumers, run more processes; that is also how you
scale a consumer group, by adding more group members.

Two consequences follow from the runtime being resident:

- **The engine keeps running until the process exits.** The engine is linked
  statically into the application binary, its goroutines and garbage collector run for the
  process lifetime, and dropping the `Llingr` handle does not stop them. Use
  `stop()` to shut the engine down, not by dropping the handle.
- **It is not fork-safe.** Do not `fork()` without a following `exec` after
  `build()`, because the Go runtime is multithreaded and a bare fork leaves it
  in an inconsistent state. `std::process::Command` is fine: it does
  fork-plus-exec.

In the default packaging mode the engine links statically and the binary is
self-contained: there is no shared library to place beside it, no `rpath`, and
no `LD_LIBRARY_PATH` to set. That is why it drops cleanly into a `scratch`
container image. In the side-binary mode (`LLINGR_LINK=shared`, see
`docs/building-packaging.md`) the one operational addition is `libllingr.so`
deployed beside the binary; RPATH resolves it, so there is still no
`LD_LIBRARY_PATH` and no system install.

## Liveness

If the poll loop or a handler stalls, nothing crashes: `run()` blocks
indefinitely, because Go's deadlock detector is disarmed when the runtime is
embedded this way. Build your own liveness check. The engine's per-message
metrics are a natural heartbeat; the recommended pattern is a watchdog thread
that exits the process when metrics go quiet for some interval while you know
lag is non-zero, letting your supervisor restart it.

There is one built-in bail worth knowing. Sustained poll errors (as opposed to a
silent stall) are handled by the broker adapter: if polling fails continuously
for ten minutes, because the broker is unreachable or authorisation was
revoked, the adapter logs the errors throughout and then triggers the engine's
emergency shutdown with a reason, on the reasoning that a supervised restart
beats consuming nothing forever. The emergency shutdown behaves like any other: your
shutdown handler fires once with the reason, which takes the form `partition
<topic>[<n>] failing to fetch for 10m0s: <underlying error>`; the logs contain a
matching `stopping consumer after sustained poll failure: <same>` line just
before, and a thread parked in `run()` returns. It does not exit the process for
you; whether to exit is your decision,
and in a supervised deployment exiting so the supervisor restarts you is the
sensible response. The window defaults to ten minutes and is configurable with the
`poll_error_bail_after` option (range [1 minute, 1 hour], or `0` to disable the
bail entirely), documented in `docs/kafka-options.md`. Treat a run of poll-error
logs followed by your shutdown handler firing as
this bail, not a crash.

## Thread and CPU budgeting

The Go runtime sets `GOMAXPROCS` to the CPU count by default, and so do the
common Rust async and data-parallel runtimes. Running both stacks at their
defaults doubles the thread pressure. If you also run tokio or rayon in the same
process, set `GOMAXPROCS` in the environment before the process starts to budget
the Go side explicitly. `GOMAXPROCS` (and `GODEBUG`) are read by the Go runtime
when it loads; setting them from Rust code after start has no effect.

Handler concurrency is also a thread budget. Each handler occupies an
operating-system thread for as long as its `process` call runs, so the
`concurrent_keys` engine setting (default 250) bounds not just per-key concurrency
but the number of OS threads a burst of slow handlers can pin. What matters is
therefore the time each message spends in the handler: keep handlers short, and
size `concurrent_keys` (documented in `docs/configuration.md`) for the
per-message time and the OS-thread cost you can afford. Async I/O is welcome; the
synchronous handler signature already steers you to the right pattern, which is to
drive the async work to completion inside the call (a tokio handle and
`block_on`) rather than spawning and returning. Throughput here comes from
raising `concurrent_keys`, not from a spawn inside a handler; the reasoning is the
completion contract in `docs/processing.md`.

## Panic profile

Handler-panic containment relies on the `unwind` panic profile, which is Rust's
default. When a `ProcessHandler` panics, the engine catches it at the FFI
boundary and turns it into a dead letter, exactly as if the handler had returned
an error. If you build with `panic = "abort"`, that safety net is gone: a
handler panic aborts the whole process instead of dead-lettering one message.
Keep the default `unwind` profile for any build that runs this engine.

## Failure domains at a glance

| Failure | Blast radius |
|---|---|
| Handler returns `Err` | That message is dead-lettered; the pipeline continues |
| Handler panics | Same as `Err`, caught at the FFI boundary (requires `panic = "unwind"`) |
| Dead-letter handler returns `Err` | Emergency shutdown: that offset is not committed, and the work is redelivered on restart |
| Broker unreachable at `build()` | Clean error with the broker client's text; retryable |
| Auth credentials unresolvable at `build()` | Clean startup error, resolved eagerly like the broker dial: the AWS provider chain or the OIDC token fetch failed (see `docs/security.md`) |
| Poll errors for ten minutes straight | The adapter triggers an emergency shutdown: the shutdown handler fires with a reason and `run()` returns (exiting the process is then your decision; see Liveness) |
| Invalid configuration | Clean error at `build()`; the engine's validation panics are recovered and reported as an error, not a crash |
| Go runtime internal panic | Process death, total and immediate; a static-linked runtime owns the process's fate |
