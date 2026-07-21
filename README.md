# gospan

*Embedded span tracing for Go. Zero-dependency core; output to your slog logger or a queryable SQLite file.*

[![Go Reference](https://pkg.go.dev/badge/github.com/akmadian/gospan.svg)](https://pkg.go.dev/github.com/akmadian/gospan)
[![Go Report Card](https://goreportcard.com/badge/github.com/akmadian/gospan)](https://goreportcard.com/report/github.com/akmadian/gospan)
[![CI](https://github.com/akmadian/gospan/actions/workflows/ci.yml/badge.svg)](https://github.com/akmadian/gospan/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

[Quickstart & Examples](#quickstart) | [Overhead](#overhead) | [Configuration](#configuration) | [FAQ](#faq)

gospan shows you where a Go program spends its time. Wrap the slow-looking parts in named spans, which can be nested using the Context object you already use pretty much everywhere. Each span costs two heap allocations and, in actual code, ~2μs - ~2.3× faster than OpenTelemetry's cheapest configuration at a sixth the memory, so you can leave them in your code without worrying about overhead. Spans can go to your slog logger, or to a SQLite db, which you can then query with plain SQL. It's for in-process tracing, not a distributed fleet. No collector, no agent, no CGO. Gospan is crash safe, and won't block (unless you want) or crash your program - error boundaries and degradation paths are strictly enforced and tested.

![gospan-demo: one run, logged live and queried as SQL](docs/demo.jpg)

> **Pre-1.0.** The file format (SPEC §3–§5) is a frozen, cross-version contract; the Go API may still shift before 1.0. Implemented, tested, and validated in Alexandria, a real import pipeline. Testers and contributions welcome — help me find where it falls short.
>
> Common questions — "isn't this just OpenTelemetry?", overload behavior, why SQLite — are answered in the [FAQ](#faq).

## Why

Logs tell you what happened, not where the time went. Metrics need a server. `go tool trace` shows the Go scheduler, not your code. OpenTelemetry is built for tracing across services — overkill when one program is slow and you just want to know why. gospan fills that gap: name the operations you care about and read their timing right where you already look.

## What it's for - uses

Anything with a start and an end:

- **Request latency** — which nested DB query, cache miss, or downstream API call ate the time, grouped by request id.
- **Pipeline backpressure** — the wait on a full worker pool or a semaphore *is* the span; its duration is the stall.
- **Batch-job stages** — how long each step takes, with real p99s across every run in the file.
- **Subprocess & I/O** — wrap an `ffmpeg` or `git` exec, or a big-file parse, in one line.

## Built to leave in

- **Two allocations per span, enforced.** The count is deterministic and CI fails if it drifts; attributes add one allocation total, not one per attribute — they share a slice. Time per span is a few hundred nanoseconds on the hot path, low microseconds end-to-end under real load.
- **Tracing off is ~4ns.** A nil tracer makes every call a no-op (tested, zero allocations) and returns your context unchanged, so you gate construction on an env var and leave every call site alone.
- **Safe to leave in.** Tracing can't block or crash your program: a full buffer drops and counts (or blocks, if you opt in), a panicking sink is recovered, disk errors degrade to drop-and-count. Every drop shows up in `Stats()` — loss is measured, never silent.
- **Zero-dependency core.** The tracer and slog output are pure stdlib — nothing in your `go.mod`. The queryable SQLite file is one optional module (`gospan/sqlite`); it uses a pure-Go driver, so no CGO and no C compiler — about 10 modules land in your binary, all from the driver, and only if you import it.

## How it works

`Start` returns a span and a `context.Context` carrying it; anything
started from that context nests beneath it. `End` stamps the duration and
hands the span to a bounded buffer; a single background goroutine drains
the buffer to the destination.
If the buffer is full, the event is dropped and counted — never blocking
your code — unless you opt into blocking instead.

---
<a name="quickstart">

## Quickstart & Examples

```sh
go get github.com/akmadian/gospan
```

Core is stdlib-only. Hand it your logger — every maintained Go logging
library is or bridges to a `slog.Handler` — and spans arrive as structured
log lines with durations:

```go
func main() {
    tracer, err := gospan.New(gospan.SlogSink(slog.Default()))
    if err != nil { /* construction is the only moment gospan can fail */ }
    defer tracer.Close(context.Background())
    gospan.SetDefault(tracer)
    // ...
}

func handleReport(ctx context.Context, req Request) error {
    ctx, span := gospan.Start(ctx, "build-report", slog.String("user", req.User))
    defer span.End()

    data, err := loadRows(ctx, req) // loadRows' own spans nest via ctx
    if err != nil {
        span.Fail(err) // status = error, or canceled when errors.Is says so
        return err
    }
    return render(ctx, data)
}

func render(ctx context.Context, data Rows) error {
    defer gospan.Track(ctx, "render-pdf")() // one-line leaf span
    // ...
}
```

### Or trace to a file

Add the sqlite module
```sh
go get github.com/akmadian/gospan/sqlite
```

The SQLite sink (pure Go, no CGO) writes one auto-named file per run:

```go
sink, err := sqlite.New("./traces")
if err != nil { /* same rule: errors only at construction */ }
tracer, err := gospan.New(sink)
```

When the run ends, the file is plain SQLite. The built-in `spans_named` view
resolves the names join and the duration for you:

```sql
-- worst durations by span name
SELECT name, COUNT(*), MAX(duration_ns) AS worst_ns
FROM spans_named
WHERE end_ns IS NOT NULL GROUP BY name ORDER BY worst_ns DESC;
```

Ready-made analysis queries — per-name percentiles, slowest spans, failure
triage, effective parallelism — ship in
[sqlite/scripts/](sqlite/scripts/):

```sh
sqlite3 ./traces/gospan-*.sqlite < sqlite/scripts/by-name.sql
```

### Or both

To keep spans out of your logs but still hear about tracing's own
problems, make the file the sink and the logger the complaints channel:

```go
sink, err := sqlite.New("./traces")
if err != nil { /* ... */ }
tracer, err := gospan.New(sink, gospan.WithLogger(slog.Default()))
```

Or send every span to both, with `MultiSink`:

```go
tracer, err := gospan.New(gospan.MultiSink(
    gospan.SlogSink(slog.Default()),
    sink,
))
```

### Live statistics output

`Summary()` reports on your code; `Stats()` on the tracer itself — what
tracing costs you, what it dropped:

```go
report := tracer.Summary()["build-report"]
slog.Info("reports", "p90", report.P90, "count", report.Count, "errors", report.Errors)

health := tracer.Stats()
slog.Info("tracing itself", "dropped", health.Dropped, "cost", health.OverheadPerSpan)
```

### Patterns

The quickstart is the call-tree case: ctx flows down the stack, spans nest
for free. Two more shapes cover most programs.

**Pipeline items** cross goroutines through channels, so the span rides
the item, not the call stack:

```go
type pipelineItem struct {
    ctx context.Context // carries this item's root span
    // ...
}
// intake:      item.ctx, _ = gospan.Start(ctx, "job", slog.String("path", path))
// each stage:  _, span := gospan.Start(item.ctx, "hash"); defer span.End()
// final stage: gospan.FromContext(item.ctx).End()
```

**Waits** — the span is the wait, the numbers are attributes:

```go
_, wait := gospan.Start(ctx, "acquire-budget", slog.Int64("tokens", n))
err := sem.Acquire(ctx, n)
wait.End() // the duration IS the wait time
```

More recipes — subprocess leaves, fan-in batches — in
[docs/DESIGN.md](docs/DESIGN.md) §6.

## What it is — and is not

**Is:**

- In-process span monitoring: no external processes to babysit.
- For anything with a start and an end: request handling, subprocess
  calls, DB queries, semaphore waits, queue time.
- A small destination seam (the `slog.Handler` pattern): slog and SQLite
  in-tree; anything heavier lives in its own module, so no one's build
  pays for a destination they don't use.

**Is not:**

- Distributed tracing — no cross-process propagation; that's
  OpenTelemetry's job.
- A log aggregator, a metrics server, or a durable job queue.

## Guarantees

- Nothing gospan does can crash your program: panics are recovered at
  every public boundary, and the test suite proves it with hostile
  fixtures (panicking sinks, panicking loggers, error types whose methods
  panic).
- `nil` is off: every method on a nil `*Tracer`/`*Span` is a ~4ns no-op.
- After construction, no errors: disk failures degrade to drop-and-count,
  visible in `Stats()`, never an error you must handle.
- Graceful `Close` loses nothing; a hard kill loses at most one flush
  interval (default 1s).

<a name="overhead">

## Overhead

Measured on Apple M1, Go 1.26, medians of 5 runs. The allocation counts are deterministic (ns/op is not) and a function of gospan's architecture — the test suite enforces them as ceilings, so they hold on every machine:

| hot-path op              | time    | allocs | bytes |
|--------------------------|---------|--------|-------|
| `Start` + `End`          | 361 ns  | 2      | 160 B |
| `Start` + `End`, 2 attrs | 393 ns  | 3      | 240 B |
| `Track` leaf             | 369 ns  | 2      | 160 B |
| `SetAttrs`               | 121 ns  | 1      | 48 B  |
| buffer full (dropping)   | 187 ns  | 2      | 160 B |
| nil tracer               | 4.3 ns  | 0      | 0 B   |

Attrs cost one slice regardless of count. For context, the same shapes on
the same machine against the OpenTelemetry SDK (v1.44) in its cheapest
configuration — batch processor, discard exporter:

| OTel op          | time   | allocs | bytes  |
|------------------|--------|--------|--------|
| span start + end | 615 ns | 3      | 944 B  |
| span + 2 attrs   | 893 ns | 8      | 1.4 KB |

A gospan span with attrs runs ~2.3× faster than OTel's floor at a sixth
of the memory, and costs about 2.5 structured log lines (a 2-attr `slog`
line to a discard handler: 144 ns). The write side runs on gospan's
goroutine, never yours: the SQLite sink sustains ~286k spans/sec when a
span starts and ends within one flush interval, ~82k spans/sec with four
attrs per span.

Those are hot-path microbenchmarks — single producer, discard sink, warm cache. Treat them as a floor, not a promise: under real concurrent load (many goroutines contending the buffer, attributes on each span, the SQLite writer running) expect low single-digit microseconds per span end-to-end — Alexandria's import pipeline sees about 2µs. Allocations don't drift like that, which is why they're the number gospan enforces.

Reproduce: `go test -bench . -benchmem ./...` in either module.

## Sinks

```go
type Sink interface {
    WriteBatch(b Batch) error // ≥1 events, in order — always from one goroutine
    Flush() error             // the commit moment, ticked every flush interval
    Close() error
}
```

In-tree: `gospan.SlogSink` (spans into your existing log flow),
`sqlite.New` (a nested module carrying `modernc.org/sqlite`, so core stays
**zero-dependency**), and `gospan.MultiSink` (fan-out). Third-party
destinations belong in third-party modules — the seam is three methods
wide.

<a name="configuration">

## Tuning and Configuration

Defaults are drop-in-and-forget; every knob exists because some workload
disagrees:

```go
tracer, err := gospan.New(
    sink,
    gospan.WithBufferSize(8192),           // event buffer; full = drop and count
    gospan.WithFlushInterval(time.Second), // durability heartbeat: a hard kill loses ≤ this
    gospan.WithBlockingPolicy(),           // block producers instead of dropping
    gospan.WithLogger(logger),             // where gospan complains (rate-limited); default silent
    gospan.WithOverheadSampling(128),      // every Nth span times its own cost; 1 = all
)
```

## Docs

- [docs/DESIGN.md](docs/DESIGN.md) — the conceptual model and architecture
- [docs/SPEC.md](docs/SPEC.md) — API surface, schema, file-format contract
- [docs/DECISIONS.md](docs/DECISIONS.md) — why it is the way it is
  (append-only)
- [docs/DEFERRED.md](docs/DEFERRED.md) — what's consciously not in v1, and
  what would trigger it

<a name="faq">
## FAQ

**Can I leave spans in and turn tracing off?**
Yes. Construct the tracer only when you want it — say, behind an env var — and leave it nil otherwise. Every call on a nil tracer or span is a no-op costing ~4ns and zero allocations, and it returns your context unchanged so nesting still works. It's tested, not just claimed (`TestNilTracerIsOff`).

**What happens under heavy load, when the buffer fills?**
By default gospan drops the event and increments a counter — your code never blocks beyond a single channel send. You see the count in `Stats().Dropped`, so loss is measured, not silent. Prefer correctness over liveness (a batch job where you'd rather slow down than lose spans)? `WithBlockingPolicy()` makes producers wait for space instead. At dev/debug volumes with the default buffer the writer keeps up and you rarely drop at all; if `Dropped` climbs, raise `WithBufferSize` or switch to blocking. Same principle throughout: it degrades, it doesn't fail.

**Isn't this just OpenTelemetry?**
OTel does distributed tracing across services, with a collector and an SDK dependency tree. gospan is the opposite on purpose: one process, zero core dependencies, a plain SQLite file at the end. Need cross-service traces? Use OTel. Need to know where one program's time goes? `go get` and two calls.

**Why are attributes in a separate table? Doesn't querying them need a join?**
Attribute keys are yours to choose and unbounded, so they can't be fixed columns. The one way to put them on the span row is a JSON blob — and that corrupts data: JSON's only number type is float64, so int64 attributes past 2⁵³ (byte counts, nanosecond values) silently round off, and adding an attribute late turns a blind one-row write into read-modify-write on the blob. A key/value table stores each value at its exact type and keeps writes a single upsert. Most queries — durations, percentiles, failures — never touch attributes; the ones that do get a view or a one-line join. The join isn't overhead we forgot to delete. It's the price of arbitrary, correctly-typed, incrementally-writable attributes, and you only pay it when you ask for attrs.

**Where's the UI?**
The file is plain SQLite — the shipped scripts in `sqlite/scripts/` answer the common questions today, and a dedicated viewer is the next milestone. You're never locked out; it's just SQL.

**Is it production-ready?**
It can't crash your program: every public boundary recovers panics, proven with hostile-fixture tests (panicking sinks, loggers, error types). It's pre-1.0 and built for dev, debugging, and shipping as a support artifact — not enterprise distributed production.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

Copyright 2026 Ari Madian
