# Logging

You see the engine's internal activity through the same logger your application
already installed, with no wiring and no logger parameter to pass. llingr-kafka
routes every engine log line into the Rust `log` facade under the target
`llingr`. Whatever you use to consume `log` records (env_logger, fern, or
`tracing` through its compatibility layer) receives the engine's lines
alongside your own, and you filter them by target and level like any other
library's output.

This is a deliberate design choice. The `log` facade is process-global: the
application installs exactly one logger and every `log`-aware library in the
process feeds into it. Adding a logger-handle parameter to the builder would
fight that model, so there is none. You install a logger once, at the top of
`main`, and the engine's lines start flowing.

## What you will see, and at which level

The engine logs its lifecycle and any anomalies: the licence notice at startup,
the subscription starting, rebalances completing, poll retries and errors,
drains nearing their timeout, and an emergency shutdown firing. Each line carries one of four engine severities, mapped onto the
matching `log` level:

| Engine severity | Rust `log` level | Typical lines |
|---|---|---|
| Debug | `log::Level::Debug` | Poll internals, worker lifecycle detail, the default (AGPL) licence notice |
| Info | `log::Level::Info` | Subscription started, rebalance complete, a valid commercial licence's verification line |
| Warn | `log::Level::Warn` | Poll retry, drain nearing its timeout, a licence-token warning (expired or invalid) |
| Error | `log::Level::Error` | Broker errors, emergency shutdown |

Every line is emitted under the target `llingr`, so you can raise, lower, or
silence the engine's verbosity independently of your application's own logging.
Filtering happens on the Rust side, through the facade: the target `llingr` is
the handle you filter on.

Lines originate on Go runtime threads (they arrive as callbacks from the
engine), so your logger must be thread-safe. The mainstream loggers below all
are. The text can occasionally contain non-UTF-8 bytes from the broker; it is
decoded lossily before it reaches the facade, so you never see invalid UTF-8,
only the replacement character where a stray byte was.

## The licence notice, and why you might not see it

The engine logs a licence notice at startup, but its level depends on the
licence state, and the default level is Debug, so with a typical `RUST_LOG=info`
you will not see it. The cases are:

- **No licence token (AGPL mode, the default):** the AGPL notice logs at Debug.
  It is invisible at `RUST_LOG=info`; run with `RUST_LOG=llingr=debug` to see it.
- **A valid commercial token:** a line reading `[VERIFIED] llingr-demux instance
  is licensed to "..."` logs at Info, so it appears at `RUST_LOG=info`.
- **An expired token:** the notice logs at Info, plus a separate Warn line
  carrying the expiry error.
- **A malformed or not-yet-valid token:** the notice logs at Debug, plus a Warn
  line explaining the problem.

The notice reaches your logger through the same `log` facade as every other
engine line, because the crate registers its logging before the engine
initialises, so the startup notice is captured rather than lost. If you are
auditing which licence a build is running under, `RUST_LOG=llingr=debug` is the
setting that surfaces the notice in every case.

## env_logger

Installing env_logger is two lines, and it reads its filter from the `RUST_LOG`
environment variable. Once it is installed, engine lines appear under the
`llingr` target with no further setup:

```rust
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
    env_logger::init(); // reads RUST_LOG; this is the entire wiring

    let engine = Builder::new("orders", Orders, DeadLetters)
        .brokers("localhost:9092")
        .consumer_group("orders-svc")
        .build()?;
    engine.run()?;
    Ok(())
}
```

Control what you see with `RUST_LOG`:

- `RUST_LOG=info` shows your application and the engine at info and above.
- `RUST_LOG=llingr=debug` shows every engine line, including debug, while
  leaving your application's own logging at its default.
- `RUST_LOG=warn,llingr=info` sets a warn floor everywhere but keeps the engine
  at info, a common production setting: quiet application logs, but the engine's
  lifecycle and any warnings still visible.
- `RUST_LOG=llingr=off` silences the engine's lines entirely while your
  application keeps logging.

## tracing (through tracing-log)

If your application is built on `tracing` rather than `log`, bridge the two: the
`tracing-log` crate turns `log` records into `tracing` events, so the engine's
lines become events with the target `llingr` and flow through your `tracing`
subscriber like any other event.

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Capture `log` records (including the engine's) and re-emit them as
    // tracing events. Install this before building the engine.
    tracing_log::LogTracer::init()?;
    // Your normal tracing subscriber; this fmt one honours RUST_LOG-style filters.
    tracing_subscriber::fmt::init();

    // ... build and run the engine exactly as above; engine lines now arrive as
    // tracing events under target = "llingr".
    Ok(())
}
```

Filter them with a `tracing` env filter the same way: a directive of
`llingr=debug` raises the engine target, `llingr=off` silences it. The target
name is identical across the two ecosystems because it is set once, on the
`log` record, before the bridge sees it.

## If you install no logger

llingr-kafka logs through the facade and nothing more; it never writes to stderr
itself. This mirrors how every `log`-based library behaves: if your application
installs no logger, the `log` facade discards all records, the engine's
included, and you see no engine output at all. That is not an error, just the
facade's default. Install any `log`-compatible logger to see the lines.

## Not the same as the Kafka client's log level

The engine logs described here are llingr-demux's own lifecycle logging. The
underlying franz-go Kafka client has its own, separate internal log verbosity,
which you set with the `client_log_level` Kafka option rather than through the
`log` facade. If you are chasing broker-protocol detail (connection handshakes,
metadata refreshes) rather than engine lifecycle, that option is the knob; it is
documented in `docs/kafka-options.md`. The `RUST_LOG=llingr=...` filtering on
this page governs the engine's lines, not the Kafka client's.
