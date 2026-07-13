# gospan — Deferred

Live obligations, not rejections (rejections live in
[DECISIONS.md](DECISIONS.md)). Each entry names its trigger — the observable
condition under which it gets built. No trigger fired = not built.

## Span-scoped log capture
A `slog.Handler` that tags records with the current span ID; one `logs` table
keyed by span; a viewer panel ("click a span → its logs"). Additive under a
`schema_version` bump. Boundary stands regardless: correlation-by-span, never
log aggregation/search.
**Trigger:** real diagnosis sessions where attributes prove insufficient and
"what did this span log" is the missing question.

## OTel adapter (`gospan/otelexport`, separate go.mod)
Implements OTel's `SpanExporter`; translates finished OTel spans into our
schema. Makes the whole OTel instrumentation ecosystem (incl. go-instrument's
codegen) a feed for our file + viewer. Core never imports OTel.
**Trigger:** first concrete external ask, or a project that's already
OTel-instrumented wanting the file/viewer story.

## Perfetto / Chrome-trace-format export
~100 lines writing the JSON event format ui.perfetto.dev ingests. Escape
hatch for traces beyond our viewer's comfort (the WASM engine loads files
fully into browser memory).
**Trigger:** a real trace file our viewer handles poorly.

## Live streaming viewer
Push-fed, incrementally updating UI. A genuinely different animal (push
channel, incremental queries, viewer state). The write path already keeps it
additive: rows visible at start, events emitted at occurrence.
**Trigger:** snapshot-on-refresh (`gospan/serve`) demonstrably failing a real
workflow — not "streaming would be cool."

## Codegen directive tool (`//gospan:trace`)
A `go generate`-style tool injecting `Track` calls on annotated functions
(the go-instrument mechanism, emitting our API). Note: the OTel adapter +
go-instrument itself may cover this need for free.
**Trigger:** demand from users with large codebases to instrument; never
before the adapter question is settled.

## Subprocess span adoption
For Go-spawns-Go programs: the parent hands the child a trace context via
environment (say `GOSPAN_PARENT=<file_id>:<trace_id>:<span_id>`, minted by a
`span.SubprocessEnv()` helper); the child's `New` records it in `meta`; after
the child exits, the parent calls `tracer.Adopt(childPath)` and the writer
ingests the child's rows — IDs remapped (that's what per-file `file_id` was
designed for), spans re-parented under the spawning span. Same machine means
shared wall clock, so alignment is honest. Crucially this stays **post-hoc
file ingestion** — no live IPC, no OTLP-shaped wire, so the "no cross-process
propagation over a wire" boundary survives. `sqlite.New(dir)` synergy: child
writes into the same directory, parent adopts what it finds.
**Trigger:** a real workload where a Go binary spawns another *gospan-
instrumented* Go binary. (Alexandria's subprocesses are exiftool/ffmpeg —
not instrumentable — so a `Track` span around the exec covers them.)

## Span links (multi-parent references)
OTel's mechanism for non-tree causality. v1 models fan-in as own-trace +
shared correlation attribute (DESIGN §6).
**Trigger:** a real cross-trace causality question the attribute pattern
cannot answer.

## Sampling
Head-sampling only (decide at trace root, inherit — per-span sampling shreds
trees). Rejected for v1: at motivating-workload volumes, capture everything.
**Trigger:** a measured workload where full capture materially distorts the
program being traced (`Stats.OverheadPerSpan` says so).

## Loose attr sugar (`"key", value, …` pairs)
slog-precedented convenience form. Costs: values box into `any`; odd-count
mistakes fail at runtime.
**Trigger:** sustained verbosity complaints from real usage, not aesthetics.

## Allocation micro-optimizations (`sync.Pool`, etc.)
Span structs are small and short-lived; the GC may simply be fine.
**Trigger:** benchmarks (which v1 ships with) showing allocation pressure
attributable to span creation.

## Attr-key interning
Keys are inline TEXT (deliberate asymmetry with interned span names).
**Trigger:** real trace files measurably bloated by repeated keys.

## Gauge/counter samples table
"Queue depth over time" as sampled values. v1 derives it from open-span
counts (window queries over intervals), which covers the motivating cases.
**Trigger:** a want that span-derived concurrency genuinely cannot express.

## Live snapshot handler (`serve`)
The `http.Handler` exposing a consistent `VACUUM INTO` snapshot of the
live DB at `/trace.db` (D9, D24) — in the `gospan/sqlite` module, since
it is meaningless without a database file. Deferred out of v1: its only
consumer is the viewer's live-ish mode (D26). Snapshots should run on
their own read connection so a slow copy never stalls the writer.
**Trigger:** the viewer repository lands and wants live mode — or a real
mid-run diagnosis need that opening the live WAL file locally cannot
serve.
