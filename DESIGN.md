# gospan — Design

What the system is and how it works, conceptually. The exact API and schema
live in [SPEC.md](SPEC.md); the why-not-something-else record lives in
[DECISIONS.md](DECISIONS.md).

## 1. The model

One primitive: the **span** — a named unit of work with a start, an end, a
parent, and a bag of typed attributes. A **trace** is not a record anywhere;
it's the set of spans sharing a `trace_id`, and its root is the span with no
parent. `Start` with no span in the context mints a fresh trace.

The organizing principle, borrowed from distributed tracing and load-bearing
everywhere here:

- **Structural hierarchy is exactly one thing** — trace → parent → child. A
  span has exactly one parent. The hierarchy is a strict tree.
- **Every other dimension is a flat attribute** — job kind, worker ID, asset
  ID, tokens requested, bytes written. Attributes don't multiply structurally;
  they're tags you filter by afterward.

This is what keeps "asset × stage × job type × worker" from becoming an ID
explosion, and it's also the answer to every non-tree shape (see §6, fan-in).

Nothing in the model is goroutine- or channel-specific. A subprocess call, an
HTTP request, a semaphore wait, and a channel receive are all just spans.
Semaphore/budget tracking in particular is deliberately **not** a first-class
concept: an acquire-wait is a child span (`acquire-budget`) whose duration is
the wait and whose attributes (`tokens_requested`, `budget_total`) are the why.

## 2. Scope boundary

Per-process, embedded, post-hoc-first. Explicitly out:

- **Distributed tracing.** No cross-process context propagation over any
  wire, ever. A subprocess constructs its own Tracer and produces its own
  file. (Post-hoc *adoption* of a subprocess's finished file into the
  parent's is a deferred idea, not a rejected one — see DEFERRED.md.)
- **Log aggregation.** We don't ingest, index, or search your logs.
  (Span-scoped log *capture* — logs keyed by span ID — is a deferred feature,
  not a rejected one; see DEFERRED.md.)
- **Metrics.** No counters/gauges API. `Summary()` (per-name duration
  aggregates) is derived from spans we already see — introspection, not a
  metrics system.
- **Durable orchestration.** The trace file is observational exhaust: the
  traced program never reads it back, and deleting it loses diagnostics, never
  state.

## 3. Architecture: the hot path and the one writer

```
caller goroutines                    background
─────────────────                    ──────────
Start/End/SetAttrs ──► bounded channel ──► writer goroutine ──► sink
   (~ns: clock read,     (drop-and-count      (batches, dedupes     (SQLite file |
    slice append,         when full;           attrs, maintains      slog | yours)
    channel send)         optional block)      Summary)
```

- **Producers never touch the destination.** Span lifecycle events go into a
  bounded channel via non-blocking send. Full channel → drop and count
  (default) or block (opt-in), never silently stall.
- **One writer goroutine** drains the channel and delivers batches on a flush
  interval. It is the single owner of the sink, the attribute dedupe
  (last-wins), and the per-name aggregates behind `Summary()`.
- **The destination is a seam — the `slog.Handler` pattern.** `New(sink)`
  mirrors `slog.New(handler)`: `Sink` is a tiny public interface (three
  methods, called only from the writer goroutine — sink authors never handle
  concurrency; struct arguments so fields can be added compatibly). The
  writer serializes and orders; **each sink owns its I/O strategy** via the
  `WriteBatch`/`Flush` split: events are delivered as the queue drains (a
  batch of one is a stream), and `Flush` ticks on the interval as the
  commit/fsync moment. The slog sink emits immediately and no-ops `Flush`;
  the SQLite sink buffers and commits on `Flush` — transaction batching is
  a SQLite strategy, not a writer concern. Want live log lines *and* the
  post-run file? `MultiSink(slog, sqlite)` — fan-out is itself a sink (the
  `io.MultiWriter` pattern), so composition costs the tracer nothing.
  In-tree implementations: the
  **SQLite file** (the flagship — the viewer and the SQL story hang off it;
  nested module, so core stays zero-dependency) and a **slog emitter**
  (stdlib-only; spans land in your existing log flow — any logger with a
  `slog.Handler` bridge, which is all of them — and `Summary()` still works
  with no file at all). Third-party sinks live in third-party modules,
  unsupported by design — Go dependencies flow with imports, so an exotic
  sink existing somewhere adds nothing to anyone else's build. In-tree
  admission is deliberately brutal: general-purpose only, no new
  dependencies, no core changes; vendor/network destinations stay out of
  tree forever.
- **Spans hit the sink at start, not at end** — events are emitted at
  occurrence, so a crashed run keeps every span that was open (the
  9-minutes-into-FFmpeg span you care most about), and a live snapshot always
  shows running work. In the SQLite sink, the span row is inserted with
  `end_ns NULL` and updated in place at end — coalesced to a single complete
  insert when both land in one batch (the common case), so only spans that
  outlive a flush interval pay the second write. "Incomplete" is simply
  `end_ns IS NULL` in a closed file.
- **IDs are tracer-minted** (`atomic.Int64`), not DB auto-increment — a child
  needs its parent's ID while the parent is still in the buffer. Monotonic
  counters keep B-tree inserts append-only. Global uniqueness is solved once
  per file (`file_id` in `meta`), never per span.

## 4. Failure philosophy

**Nothing we do is ever the reason your program goes down.**

- Every public entry recovers panics at the boundary; internal failures are
  counted (`Stats`) and optionally logged (`WithLogger`), never propagated.
- The writer goroutine runs its own recover loop — a bad batch degrades to
  "dropped a batch," not a crashed process.
- Disk full / DB errors degrade to drop-and-count. The only errors a caller
  ever sees are at construction (`gospan.New`, sink constructors like
  `sqlite.New`) — the one moment a human can fix a bad path.
- `nil` is off: all methods on nil receivers are no-ops, so "tracing disabled"
  is "don't construct one." No flags, no build tags.
- Shutdown: `Close` drains and finishes the file (zero loss). SIGKILL/power
  loss lose at most one flush interval — batches are small and frequent so
  the file is always nearly current.

Degradation is silent to control flow but never undiscoverable: `Stats()`
carries `Dropped` and `WriteErrors`, and `WithLogger` gives the tracer a place
to complain (rate-limited) without owning your program's stderr.

## 5. Concurrency contract

Span context rides `context.Context` (the `slog` pattern): `Start` reads the
parent off the ctx and returns a ctx carrying the child. This propagates
correctly across call boundaries and into spawned goroutines for free.

A span's methods (`End`, `Fail`, `SetAttrs`) are **safe to call from a
different goroutine than `Start`** — required, not incidental: in pipeline
code the root span routinely starts in one stage's goroutine and ends in
another's. First `End` wins; mutations after `End` are no-ops.

## 6. Instrumentation patterns (the cookbook)

These are as much the product as the API. Each is a documented recipe, not a
library feature.

**Call-tree code** (the easy case): `Start`/`defer End` at each level; ctx
does the nesting. Zero structural change to instrument.

**Channel pipelines** — ctx follows the call stack, but pipeline items cross
goroutines through channels, so the span context must travel *on the item*:

```go
type pipelineItem struct {
    ctx context.Context // carries this item's root span
    // ...
}
// intake:            item.ctx, _ = tracer.Start(ctx, "asset", slog.String("path", p))
// each stage:        _, span := tracer.Start(item.ctx, "hash"); defer span.End()
// final stage:       gospan.FromContext(item.ctx).End()  // close the root
```

One additive field; no signature changes. (Storing a ctx in a struct is
sanctioned exactly here: it's scoped to the item's lifetime and travels with
it.)

**Semaphore / budget waits** — the wait is a child span; the numbers are
attributes:

```go
_, acq := tracer.Start(ctx, "acquire-budget",
    slog.Int64("tokens_requested", n), slog.Int64("budget_total", cap))
err := sem.Acquire(ctx, n)
acq.End() // duration IS the wait time
```

**Subprocess / leaf calls** — `defer tracer.Track(ctx, "ffmpeg-extract")()`.

**Fan-in (batch commits)** — a batch serving N traces belongs to none of
them. The batch is its own tiny trace (`write-batch`, attrs `count`,
`batch_seq`); each contributing trace records its own wait span carrying the
same `batch_seq`. Structure stays a tree; the many-to-one lives in a shared
attribute. (Span links — OTel's multi-parent mechanism — are deliberately
absent; see DEFERRED.md.)

**Retries** — a retry is not a span state; attempt 2 is a new sibling span
with `slog.Int("attempt", 2)`.

## 7. Storage (the SQLite sink)

Core is stdlib-only; the flagship sink lives in a nested module
(`gospan/sqlite`, own go.mod) so its driver dependency never touches users
who chose another destination. Embedded SQLite via a pure-Go driver
(`modernc.org/sqlite` — no CGO, no C compiler to build). WAL mode: readers
never block the one writer. The schema
(SPEC.md §3) is normalized for density — all-integer span rows, interned span
names, attributes in a side table populated only when used — and STRICT, so
writer bugs become insert errors, not silent junk in trace files.

The deeper reason SQLite over anything else: **the file is the interchange
format.** SQLite compiles to WASM, so a browser can open the file and run real
SQL client-side — which is what makes a zero-server viewer possible at all —
and every user gets ad-hoc SQL against their traces for free. Analysis-side
indexing is legal and encouraged: the live write path maintains exactly one
index; add your own to your copy of a closed file.

Timestamps are absolute unix nanoseconds derived monotonically: `New` anchors
(wall W₀, monotonic M₀) once; every timestamp is `W₀ + (M_now − M₀)`. Within a
file, durations are immune to clock jumps; across files, alignment is only as
good as the machines' wall clocks — stated, not solved.

## 8. Viewer

The viewer is a **separate repository** — producer and consumer are separate
concerns with different lifecycles and toolchains, and this repo stays a lean
Go library. The contract between them is SPEC.md's schema and file-format
sections. The viewer's own toolchain is its own business (a React app is
fine); what's invariant is the *user's* experience: it builds to static
assets, runs zero servers, and reads trace data two ways — a completed
`.sqlite` file via drag-and-drop (WASM SQLite, real SQL client-side), or a
live run by polling a `serve` snapshot URL. What it renders:

- **Waterfall per trace**: rows = spans, x = time, indentation = nesting, bar
  = duration, gaps = waiting. Deliberately not a graph layout — timelines stay
  legible at any span count; force-directed DAGs don't.
- Click a span → its attributes (rendered by `kind`: durations as durations,
  not mystery integers).
- Incomplete spans (`end_ns IS NULL`) rendered visibly different — "no end
  recorded" — never diagnosed into a story the data can't back.

**Live-ish mode** comes from `serve` (in the `gospan/sqlite` module — it's
meaningless without a database file): an HTTP handler exposing a consistent
snapshot of the live DB at `/trace.db` (`VACUUM INTO` — you can't serve a
WAL file's bytes mid-write). Refresh = current state within one flush
interval. True streaming is deliberately out (see DEFERRED.md);
snapshot-on-refresh costs nothing when no client asks and changes no core
design.

## 9. Overhead posture

Measured, not assumed — an instrument that slows the pipeline is lying about
it. The levers, in impact order: batch everything (never a per-span fsync);
non-blocking hot path with explicit drop policy; no marshaling/reflection on
the hot path (typed `slog.Attr` all the way to the writer); cheap monotonic
timestamps. Published benchmark numbers in the README (`go test -bench`) plus
live self-reported cost (`Stats().OverheadPerSpan`, `Dropped`) — "here's what
it's costing you on your hardware right now" beats any static claim.

Benchmarks run in CI on every commit; a performance regression is a blocking
defect, same as a failed test. Concrete SLA numbers (ns/op, allocs/op
ceilings) get defined once the first real benchmarks exist to calibrate
against — published, then enforced.
