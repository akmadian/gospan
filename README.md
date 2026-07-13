# gospan

Embedded, span-based tracing for Go programs. Instrument your pipeline with
one-line spans; get a local SQLite file you can open in a zero-server HTML
viewer or query with plain SQL. No collector, no agent, no Prometheus, no
CGO — nothing to run but your own program. Everything is per-process: each
process traces itself into its own file.

> **Status:** design complete, pre-v1. The API and file format below are the
> build contract ([SPEC.md](SPEC.md)); implementation has not started.

## Why

You have a long-running, high-volume concurrent pipeline — worker pools,
channels, semaphores — and you want to know what's slow, where the backpressure
is, and what one specific job did. The existing options don't fit:

- **OpenTelemetry** solves distributed tracing: multi-service, network hops,
  collectors. In-process, it's a heavy dependency tree pointed at
  infrastructure you don't want to run.
- **`go tool trace`** operates at the runtime-scheduler level (goroutines, GC,
  syscalls) — unreadable for business-logic diagnosis, by design.
- **Prometheus/Grafana** want a server scraping your process, and give you
  aggregates, never "show me this one job."

gospan is the missing shape: spans with names you chose, stored in a file,
viewed locally, queried with SQL.

## Quickstart

```go
func main() {
    // Pick a destination: the SQLite file (one auto-named file per run) —
    // or your existing log flow, no file at all (any logger with a
    // slog.Handler bridge): sink := gospan.SlogSink(logger)
    sink, err := sqlite.New("./traces")
    if err != nil { /* construction is the only moment gospan can fail */ }

    t, err := gospan.New(sink)
    if err != nil { /* ...same deal */ }
    defer t.Close(context.Background())
    gospan.SetDefault(t)
    // ...
}

func processAsset(ctx context.Context, a Asset) error {
    ctx, span := gospan.Start(ctx, "process-asset", slog.String("asset", a.ID))
    defer span.End()

    if err := extract(ctx, a); err != nil { // nests automatically via ctx
        span.Fail(err)
        return err
    }
    return nil
}

func resize(ctx context.Context, a Asset) {
    defer gospan.Track(ctx, "resize-image")() // one-line leaf span
    // ...
}
```

When the run ends you have an SQLite file. Drag it into the viewer (a single
static HTML file — waterfall per trace, click a span for its attributes), or
just query it:

```sql
-- p99-ish: worst EXTRACT durations by name
SELECT n.name, COUNT(*), MAX(end_ns - start_ns) AS worst_ns
FROM spans s JOIN names n ON n.id = s.name_id
WHERE end_ns IS NOT NULL GROUP BY n.name ORDER BY worst_ns DESC;
```

Live numbers without waiting for the file:

```go
sum := t.Summary()["extract-frames"]
slog.Info("extract", "p90", sum.P90, "count", sum.Count, "errors", sum.Errors)
```

## What it is / is not

**Is:** in-process span collection → batched writes to a local SQLite file
(WAL, pure-Go driver) → a self-contained HTML viewer and your own SQL.
Semaphore waits, subprocess calls, DB writes, queue waits — anything with a
start and an end is a span. Destinations sit behind a small public sink
interface (the `slog.Handler` pattern): in-tree, the SQLite file (a nested
module — **core itself has zero third-party dependencies**) and a stdlib-only
slog emitter; anything heavier lives out of tree, in its own module, so no
one's build ever pays for a destination they don't use. The viewer is its own
repository, reading trace files and live snapshots against the published
schema contract.

**Is not:** distributed tracing (no cross-process propagation — that's
OpenTelemetry's job), a log aggregator, a metrics server, or a durable job
queue. Any number of processes can each run gospan; each traces itself into
its own file, and we never stitch them into one causal tree.

## Guarantees

- Nothing gospan does can crash your program: panics recovered at every public
  boundary, disk failures degrade to drop-and-count, never an error you must
  handle.
- `nil` is off: every method on a nil `*Tracer`/`*Span` is a no-op. Don't
  construct one in production and instrumentation costs a nil check.
- Graceful shutdown (`Close`) loses nothing; a hard kill loses at most one
  flush interval.
- Honest self-reporting: `Stats()` tells you what tracing dropped and what it
  costs, live.

## Docs

- [DESIGN.md](DESIGN.md) — the conceptual model and architecture
- [SPEC.md](SPEC.md) — API surface, schema, file-format contract
- [DECISIONS.md](DECISIONS.md) — why it is the way it is (append-only)
- [DEFERRED.md](DEFERRED.md) — what's consciously not in v1, and what would
  trigger it
