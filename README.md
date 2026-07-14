# gospan

Embedded, span-based tracing for Go. Wrap the operations you care about in
named spans — nested, timed, attributed — and gospan delivers them to a
destination you choose: your existing structured logger, or a local SQLite
file you can query with SQL. It runs entirely inside your process. No
collector, no agent, no CGO, nothing to deploy.


Currently pre 1.0, implemented and tested but still validating in another personal project. Testers and contributions welcome.

## Why

Logs are great, but they're not structured, and not easily queryable or aggregable without larger more dedicated log browsers. When you want per-operation timing with structure — *this* call, inside *this* request, took *this* long, and here's its p99 across the whole run, or *this* step of *this* pipeline job took *this* long, with p99s across all pipeline runs, ALL in a small, efficient, self contained package — the existing tools each miss:

- **OpenTelemetry** solves distributed tracing: multi-service, network
  hops, collectors. In-process, it's a heavy dependency tree pointed at
  infrastructure you don't want to run.
- **`go tool trace`** operates at the runtime-scheduler level (goroutines,
  GC, syscalls) — unreadable for business-logic diagnosis, by design.
- **Metrics** (Prometheus and friends) usually require a server you have to run and provide an small, but non negligent, API surface for you to maintain.

gospan is the missing middle, for servers, CLIs, batch jobs, and worker
pipelines alike: spans with names you chose, kept inside your process,
readable where you already look.

## How it works

`Start` returns a span and a `context.Context` carrying it; anything
started from that context nests beneath it. `End` stamps the duration and
hands the span to a bounded buffer; a single background goroutine drains
the buffer to the destination.
If the buffer is full, the event is dropped and counted — never blocking
your code — unless you opt into blocking instead.

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

```sh
go get github.com/akmadian/gospan/sqlite
```

The SQLite sink (pure Go, no CGO) writes one auto-named file per run:

```go
sink, err := sqlite.New("./traces")
if err != nil { /* same rule: errors only at construction */ }
tracer, err := gospan.New(sink)
```

When the run ends, the file is plain SQLite:

```sql
-- worst durations by span name
SELECT n.name, COUNT(*), MAX(end_ns - start_ns) AS worst_ns
FROM spans s JOIN names n ON n.id = s.name_id
WHERE end_ns IS NOT NULL GROUP BY n.name ORDER BY worst_ns DESC;
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

## Tuning

Defaults are drop-in-and-forget; every knob exists because some workload
disagrees:

```go
tracer, err := gospan.New(sink,
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
