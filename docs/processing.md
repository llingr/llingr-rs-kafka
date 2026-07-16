# Processing messages

You process messages by implementing two traits: `ProcessHandler`, which
receives each message and does your work, and `DeadLetterHandler`, which
receives any message that failed so it is never dropped silently. Both are
required; you pass them to `Builder::new`. This page covers what those handlers
receive, the ordering and delivery guarantees you can rely on, how to make
processing safe under the at-least-once contract, how a handler panic is
contained, and how to attach your own classification bits to a message.

Both handler traits are defined in the shared `llingr-nexus` contract crate and
re-exported at the llingr-kafka root, so a single `use llingr_kafka::...` covers
them. Both are `Send + Sync + 'static`: the engine calls them concurrently from
its worker threads, so anything they capture must be safe to share across
threads.

## The two required handlers

`ProcessHandler::process` is called once for each message. It returns
`Result<Traits, Box<dyn std::error::Error>>`: return `Ok(traits)` on success
(the `Traits` value carries any classification bits you want to attach, covered
below), or return an error to route the message to the dead-letter handler.

```rust
use llingr_kafka::{Message, Traits, ProcessHandler};

// Application trait bits are yours to define; positions 10 to 63.
const VALIDATED: u32 = 10;
const HIGH_VALUE: u32 = 11;

struct Orders;
impl ProcessHandler for Orders {
    fn process(&self, msg: &Message) -> Result<Traits, Box<dyn std::error::Error>> {
        // A null value (None) is a tombstone; handle it however your domain wants.
        let body = msg.value().ok_or("unexpected tombstone")?;
        if body.is_empty() {
            return Err("empty order payload".into()); // routes to the dead-letter handler
        }

        // Do your work here. The bytes are borrowed for this call only (see the
        // borrow rule below); parse and act now, or copy out what you keep.

        // Return application trait bits; framework bits 0 to 9 are masked out.
        let mut traits = Traits::with_bit(VALIDATED);
        if body.len() > 1024 {
            traits = traits.set(HIGH_VALUE);
        }
        Ok(traits)
    }
}
```

`DeadLetterHandler::handle` is called with the failed message and the process
handler's error text whenever `process` returns an error or panics. It exists so
a failed message has somewhere to go before its offset commits: without it, a
failure would be dropped silently when the offset advanced past it. Logging the
message and reason is the bare minimum; a real deployment publishes to a durable
dead-letter store (a DLQ topic, a table, an object store) so the failure can be
inspected and replayed.

```rust
use llingr_kafka::{Message, DeadLetterHandler};

struct DeadLetters;
impl DeadLetterHandler for DeadLetters {
    fn handle(&self, msg: &Message, error: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Persist the failed message and the reason. Returning Ok lets the
        // pipeline continue. Returning Err is a hard stop (see below).
        eprintln!("dead-letter key={} partition={} offset={} reason={}",
            msg.key_str().unwrap_or(""), msg.partition(), msg.offset(), error);
        Ok(())
    }
}
```

## What a message gives you, and the borrow rule

A `Message` is a read-only view of one Kafka record. Its accessors are:

- `key() -> Option<&[u8]>`: the partition key bytes, or `None` for a keyless
  record.
- `key_str() -> Option<&str>`: the key as UTF-8. The broker adapter delivers
  keys UTF-8-safe by construction (the raw key if it is valid UTF-8, base64 if
  it is binary, or the partition number if the record had no key), so on
  delivered records this is always `Some`. Use `key()` for the raw bytes.
- `value() -> Option<&[u8]>` (and its alias `payload()`): the record value, or
  `None` for a null value (a tombstone).
- `value_str() -> Option<&str>`: the value as UTF-8, or `None` when it is null
  or not valid UTF-8. Values are frequently binary, so reach for `value()` and
  parse yourself where the payload is not text.
- `topic() -> &str`, `partition() -> i32`, `offset() -> i64`: the record's
  coordinates.
- `timestamp() -> Timestamp`: an enum that carries both the instant and its
  kind. It is `NotAvailable`, `CreateTime { millis }` (producer-assigned event
  time), or `LogAppendTime { millis }` (broker-assigned ingestion time), where
  `millis` is milliseconds since the Unix epoch (the Kafka wire resolution).
  `timestamp().millis() -> Option<i64>` gives the value regardless of kind.
- `headers() -> Headers`: an ordered, borrowed view of the record's headers.

Kafka headers are a list, not a map: keys may repeat and wire order is preserved
(tracing systems depend on it), and a value may be null. `Headers` reflects
that. `len()` counts duplicates separately, `is_empty()` reports emptiness,
`get(index)` reads by position in wire order, `iter()` walks them in wire order,
and `find(key)` returns the first header matching a key (the usual
Kafka-client first-wins convention). Each `Header` is `{ key: &str, value:
Option<&[u8]> }`, so a null-valued header is `value: None`.

**The borrow rule is the one sharp edge here.** Every slice a `Message` hands
you (`key`, `value`, header keys and values) points into memory owned by the Go
engine, and it is valid only for the duration of the `process` (or `handle`)
call. It is recycled for later messages the moment your handler returns.
Retaining a `&[u8]` or `&str` past the callback is not a crash; it is silently
wrong data. If you need to keep any of it, copy it out inside the handler with
`to_vec()`, `to_owned()`, or `to_string()`. Doing the work synchronously inside
the handler needs no copy; handing bytes to another thread or a queue does.

One distinction worth internalising: empty is not absent. A zero-length key or
value is `Some(b"")` (and `key_str`/`value_str` give `Some("")`), which is
distinct from the `None` of a keyless record or a null (tombstone) value. Delete
handling on compacted topics rides exactly this distinction: a null value
(`value() == None`) marks a tombstone, an empty value (`Some(b"")`) does not.

## Per-key ordering: what is and is not guaranteed

The engine routes every record to a worker by hashing its key, so all records
that share a key go to the same worker and are processed in offset order, one
after another. That is the ordering guarantee: **per key, processing is strictly
ordered.** It is what lets you treat a key (a customer, an account, an order) as
a serial stream even while the consumer runs many keys at once.

What is deliberately not guaranteed is any ordering across different keys. Two
records with different keys may be processed concurrently, on different workers,
in either order, regardless of their offsets or partitions. This is the whole
point: a slow key holds up only itself, not the keys interleaved with it on the
same partition. If your logic needs two records to be ordered relative to each
other, they must share a key.

Offsets still commit in contiguous order underneath this concurrency. When
records complete out of order (a later-offset record on a fast key finishes
before an earlier-offset record on a slow key), the engine holds the completed
offset in its gap-buffer committer and only advances the committed position
across a contiguous run. You never commit past an unfinished record, so a crash
never skips one.

## At-least-once delivery, and how to be safe under it

llingr-kafka delivers each message at least once. In the normal path that means
exactly once, but a message can be delivered again in three situations: a
process exit without a graceful stop (uncommitted work is redelivered on
restart), a rebalance where a partition moves to another consumer before its
in-flight work committed, and an emergency stop that abandons in-flight work.
The engine never drops a message to avoid a duplicate; it prefers redelivery,
because a lost message is unrecoverable and a duplicate is not.

The consequence is a design rule: **make your processing idempotent**, so a
redelivery is harmless. Concretely:

- Key your writes by something stable in the record (an order id, an event id)
  and use a conditional or upsert write, so applying the same record twice
  lands the same final state as applying it once. An `INSERT ... ON CONFLICT DO
  NOTHING`, a `PUT` to a content-addressed key, or a compare-and-set all work.
- Where you cannot make the write itself idempotent, keep a dedupe record: store
  the record's id (or a hash of it) on first processing and skip a record whose
  id you have already seen. The dedupe store must be as durable as the effect
  you are guarding.
- Treat an operation whose outcome you cannot tell apart after an interrupted
  reply (a non-idempotent request, a bare `INSERT`) as not blindly retryable.
  Guard it behind an idempotency key or a read-back check.

A self-identifying payload helps: if the record carries its own id in the body
as well as the key, a downstream consumer can dedupe on that id without trusting
the delivery path.

## The completion contract

The reassuring part first, and it is compiler-backed: if you write ordinary code,
you are already correct here, with nothing extra to do. `process` is a plain
synchronous function, not an `async fn`, and that is deliberate. A synchronous
function cannot suspend, so by construction everything you write in its body has
already happened by the time it returns. Do your work, return `Ok`, and the thing
that `Ok` represents (the work is done) is satisfied automatically. The borrow
checker independently guards the one everyday slip, keeping borrowed message bytes
past the call, by refusing to compile it. So the ordinary reader writing ordinary
code is already correct, verified by the compiler, before reading another word of
this section.

It is worth naming precisely what that `Ok` means, because the rest of the
section turns on it: returning `Ok` tells the engine that two things are already
true, **the work is durably done** and **you have kept nothing that borrows the
message**, and the engine reads it as permission to commit the message's offset.
In the default synchronous path, both are guaranteed for you.

### The one way to lose the guarantee

There is exactly one way to break this, and it is not a subtle mistake you can
stumble into: you have to deliberately step around the design, by handing the
work to another thread or task and returning before it finishes. The
`tokio::spawn`-and-return shape is the archetype:

```rust,ignore
// DO NOT DO THIS. It is an active workaround, not an accident: it compiles,
// runs, and passes every demo while breaking the engine's core invariants.
impl ProcessHandler for Orders {
    fn process(&self, msg: &Message) -> Result<Traits, Box<dyn std::error::Error>> {
        let owned = msg.value().unwrap_or_default().to_vec();
        self.runtime.spawn(async move { write_to_store(&owned).await; }); // WRONG
        Ok(Traits::none()) // returns before the spawned task has done the write
    }
}
```

That `Ok` fires before the write happens, and it takes three of the engine's
invariants with it, which is why the workaround is not merely discouraged but
ruled out:

- **At-least-once delivery becomes silent loss.** Your `Ok` feeds the offset
  committer, so a crash in the window between the spawn and the write loses a
  message the engine has already committed as done. That is worse than a
  duplicate, because it is invisible: duplicates show up in metrics and
  downstream, but a lost message is silence.
- **Per-key ordering is voided, under normal operation.** The worker dispatches
  the key's next message the moment you return, so two spawned tasks for the same
  key race each other with no ordering between them. This is not a rare-rebalance
  edge case; it is every busy key, always.
- **Backpressure and memory bounds vanish.** The engine paces itself on
  completions, so it reads your instant `Ok` as instant completion, keeps polling
  at full speed, and lets the detached tasks pile up without bound. A slow
  downstream that should have slowed the consumer instead fills memory.

If you are ever tempted, this litmus test is the check: **if the process lost
power the instant `process` returned `Ok`, would this message be safe? If not,
you returned too early.**

One nuance on why this needs stating at all, given how much the compiler already
does for you: the borrow checker stops the naive version, because a slice
borrowed from the message cannot escape into a `'static` spawned task, so that
does not compile (the borrow rule from earlier on this page doing its job). What
it cannot see is the `to_vec`-then-spawn shape above, because copying the bytes
out satisfies the borrow checker while still returning too early. That single gap
is the whole reason this contract is written down rather than left entirely to the
compiler.

### Using async without stepping around anything

Async I/O is completely fine; detaching it is the only problem. Do the async work
inside the handler and drive it to completion before returning, so `Ok` still
means the work is done. A tokio runtime handle stored on the handler, with
`block_on` in `process`, is the clean way:

```rust
use llingr_kafka::{Message, Traits, ProcessHandler};
use tokio::runtime::Handle;

struct Orders {
    // A handle to a tokio runtime the application owns and keeps alive.
    runtime: Handle,
}

impl ProcessHandler for Orders {
    fn process(&self, msg: &Message) -> Result<Traits, Box<dyn std::error::Error>> {
        // Copy out what the async work needs; the borrow is valid only for this call.
        let value = msg.value().unwrap_or_default().to_vec();

        // Drive the async write to completion HERE, before returning. block_on is
        // safe because this handler runs on a Go-origin engine thread, never
        // inside an async context, so there is no ambient runtime to deadlock.
        self.runtime.block_on(async move { write_to_store(&value).await })?;

        Ok(Traits::none()) // now Ok truly means the write is durably done
    }
}
# async fn write_to_store(_v: &[u8]) -> Result<(), Box<dyn std::error::Error>> { Ok(()) }
```

`block_on` blocks the engine worker until the write finishes, which is exactly
what you want: that blocking is the backpressure and the ordering doing their
jobs. It is safe here specifically because handlers run on the engine's
Go-origin threads and never inside a tokio worker, so there is no runtime nested
inside a runtime.

### Do not spawn for throughput

The instinct behind spawning is usually throughput, and in this engine that
instinct is aimed at the wrong layer. You do not spawn to go faster here; you
raise `concurrent_keys` (the engine tuning knob, default 250, documented in
`docs/configuration.md`). The per-key workers are the concurrency: the engine
already runs many keys at once, each in order. A spawn inside a handler builds a
second, broken concurrency layer on top of the real one, buying nothing the
engine does not already give you and costing you the three guarantees above.

### The dead-letter handler works the same way

The good news carries straight over: `DeadLetterHandler::handle` is also a plain
synchronous function, so recording the dead letter and returning `Ok` is
naturally correct, with nothing extra to do. That `Ok` means the dead letter is
durably recorded, and the offset advances once the failure has been handled. The
only way to break it is, again, the deliberate workaround: a spawn-and-return
that has not yet written the dead letter advances the offset past a failure you
have not actually persisted, leaving the failed message neither processed nor
recorded anywhere. Record the dead letter, then return `Ok`.

This safety is free precisely because the handler traits are synchronous by
design, and they will stay that way (decided by David, 16 July 2026): there will
be no async handler variant and no runtime-handle parameter on the builder. The
synchronous signature is what makes the correct path the path of least
resistance, so the compiler guarantees the default is right and the only way to
lose that guarantee is to go out of your way to.

## The panic-to-dead-letter contract

If your `ProcessHandler` panics, the engine catches the panic at the FFI
boundary and turns it into a dead letter, exactly as if the handler had returned
an error: the dead-letter handler runs with the reason `panic in process
callback`, and the message's offset is handled like any other failure. A
panicking handler therefore costs you one dead-lettered message, not the process.

This containment relies on the `unwind` panic profile, which is Rust's default.
If you build with `panic = "abort"`, the safety net is gone: a handler panic
aborts the whole process instead of dead-lettering one message. Keep the default
`unwind` profile for any build that runs this engine.

(One precise note for anyone reading trait bits: a Rust handler panic surfaces
as the ProcessError framework flag, the same flag a returned `Err` sets, because
the panic is caught and converted at the boundary. The separate ProcessPanic
flag is reserved for a panic the Go engine recovers inside its own callback,
which would indicate a bug in the bridge rather than in your handler.)

## When the dead-letter handler itself fails

The dead-letter handler is the last line before a message's offset commits, so
its failure is treated as serious. If `DeadLetterHandler::handle` returns an
error, the engine performs an emergency shutdown: that offset is not committed,
and the message is redelivered on the next start. The reasoning is the
no-dropped-messages invariant: a message that can be neither processed nor
dead-lettered cannot have its offset safely advanced, so the engine stops rather
than lose it. Because this is an emergency shutdown, the same duplicate-delivery
consequence described in `docs/operations.md` applies: in-flight work is
abandoned and redelivered.

Keep the dead-letter handler robust and simple for this reason. If it writes to
a store that can be briefly unavailable, give it bounded retries and a fallback
(local disk, stderr) so a transient blip does not escalate into a shutdown.

## Attaching your own classification: the Traits return value

`Traits` is a 64-bit field split into two regions. Bits 0 to 9 are
framework-reserved: the engine sets them to record what happened to a message,
and they are read-only to your code. Bits 10 to 63 are yours, to classify a
message however your domain wants. You attach application bits by returning them
from `process`; there is no trait field on the message, because the message is
an input and your classification is an output.

Set application bits with `Traits::with_bit(n)` (which starts a value with bit
`n` set) and `.set(n)` (which sets another bit and returns the value for
chaining), or combine two `Traits` values with the `|` operator. Return
`Traits::none()` when there is nothing to attach.

```rust
# use llingr_kafka::{Message, Traits, ProcessHandler};
const VALIDATED: u32 = 10;
const PREMIUM: u32 = 11;
const FRAUD_CHECK: u32 = 12;

struct Orders;
impl ProcessHandler for Orders {
    fn process(&self, msg: &Message) -> Result<Traits, Box<dyn std::error::Error>> {
        let _ = msg;
        let mut traits = Traits::with_bit(VALIDATED);
        // ... set PREMIUM / FRAUD_CHECK from your business logic ...
        traits = traits.set(PREMIUM).set(FRAUD_CHECK);
        Ok(traits)
    }
}
```

Attempts to set a framework bit are silently masked, not errors: `with_bit(5)`
or `set(9)` is a no-op and yields no bit, and a position above 63 is ignored the
same way. So you can define your application constants freely from 10 upward
without fear of colliding with a framework flag.

The framework flags the engine sets, and what each means:

| Flag (getter) | Bit | Meaning |
|---|---|---|
| `has_process_error` | 0 | The process handler returned an error (or panicked, caught at the boundary) |
| `has_process_panic` | 1 | The Go engine recovered a panic inside its own callback (a bridge bug, not a Rust handler panic) |
| `has_dead_letter` | 2 | The message was routed to the dead-letter handler |
| `has_commit_buffered` | 3 | The offset was held in the gap-buffer committer rather than committed immediately (it completed out of contiguous order) |
| `has_duplicate` | 4 | The message was redelivered |
| `has_used_overflow` | 5 | Dispatched via the overflow path (a capacity signal) |
| `has_orphaned` | 6 | The work item was orphaned by a rebalance |
| `has_first_after_rebalance` | 7 | The first message processed on its partition after a rebalance assignment |

These framework bits describe each message's fate (was it buffered out of order,
was it a duplicate, did a rebalance touch it). Where they surface for you to
observe is narrower than you might expect, and worth stating plainly, because
this crate feeds metrics internally and has no user-facing per-message metrics
callback. Your only window onto them is the Prometheus output, and it shows five
of the framework conditions as counters: a process error (bit 0), a caught panic
(bit 1), a dead letter (bit 2), a duplicate delivery (bit 4), and an overflow
dispatch (bit 5). Each of those increments independently, so several can fire for
one message, and a processed-total counter increments for every message
regardless. The remaining framework bits (CommitBuffered, Orphaned,
FirstAfterRebalance) have `has_*` getters but no metric, so they do not appear in
the Prometheus output at all.

The sharp edge is the application bits. **The custom bits you set (positions 10
to 63) surface nowhere in this crate's observability: not as counters, not as
labels.** The full 64-bit field does cross the FFI, but the metrics sink reads
only the five framework predicates above, and with no per-message metrics
callback there is nothing else that reads the field. So from your application's
perspective, custom trait bits are effectively write-only today: returning them
is supported and costs nothing, but this crate gives you no way to read them back.
If you need per-message business classification in your telemetry, emit it
yourself from the handler rather than relying on trait bits to carry it. The
exact names of the five framework counters are catalogued in `docs/metrics.md`.
