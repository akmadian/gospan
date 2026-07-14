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

*Trigger fired 2026-07-13 (Alexandria import-pipeline validation): the want is
process RAM/CPU alongside spans — heap and CPU cannot be derived from spans.
Sketch for the design round: a `samples (ts_ns, metric, value)` table
(additive, `schema_version` bump); one opt-in sampler goroutine
(`WithRuntimeSampling(interval)`) emitting through the same event channel —
same one-writer, same drop policy, same monotonic-derived clock, so samples
JOIN against span time-windows exactly. Read `runtime/metrics` (never
`ReadMemStats` — stop-the-world) + process CPU via `getrusage` deltas; stdlib
only. Process-level and honestly labeled: Go cannot attribute CPU/RSS to a
goroutine, so no fake per-span cost — correlation is a SQL time-window join.
First consumer: calibrating Alexandria's D28 weighted-budget size estimates
against measured heap under concurrent decodes.*

## SQLite sink write-throughput ladder

The sink's ceiling is CPU per SQL statement (pure-Go VM), not disk I/O:
~286k bare spans/sec collapses to ~82k at 4 attrs because statement count
went 1→5 while byte volume barely moved. WAL admits one write transaction
at a time, so parallel flushers on one file only queue on the lock. Batch
size per tx is past its amortization knee at flush-interval sizes — it moves
latency, not the ceiling. The ladder, in order:

1. **Multi-row `VALUES` inserts** (prepared, N rows/statement) — cuts
   per-row VM entries; the only move that changes marginal cost. 2–5×,
   no doctrine cost.
2. **Sink-internal double-buffered commit** — the single-goroutine contract
   governs calls INTO the sink; a private committer goroutine + buffer swap
   overlaps commit with intake, removing the flush-window backpressure spike.
3. **Static shard-by-trace across N files** (own writer each; analysis via
   ATTACH). Near-linear; costs one-file-per-run simplicity. Last resort.

Considered and rejected: *dynamic overflow files* (spill to a second DB at
X% queue, merge back on drain) — insert-at-start/update-at-end gives every
span file affinity for its lifetime, so time-based spill strands end-updates,
and the merge repays the same per-statement cost during recovery; if a
second core isn't driving the second file it's just a fancier buffer.
*JSONB-packed attrs* — parseability is fine (`->>`, json_each); the blocker
is fidelity: JSON's only number is float64, silently corrupting int64 attrs
past 2^53 (byte counts, ns durations), and late `SetAttrs` becomes
read-modify-write on a blob instead of a blind upsert (D5 stands).
*Separate start/end event rows in THIS sink's schema* — loses the write
contest, not just the read one: coalescing writes ONE row for any span that
starts and ends within a flush interval (nearly everything), while an event
log writes two rows always, and correlation becomes a self-join every
reader pays forever (D1 stands for the flagship file format).

Note the seam already permits the other write style wholesale: the core
hands sinks an ordered EVENT stream — materialize-and-coalesce is the
SQLite sink's choice, not the architecture's. A pure append-only sink
(raw binary log, or the Perfetto/Chrome-trace exporter above, whose format
has native Begin/End records) is buildable today as its own module with
zero core changes; it trades update-in-place for writer-maximal throughput
and moves correlation cost to its own readers/converter. That — not
reshaping the SQLite schema — is the escape hatch beyond the ladder.

**Trigger:** before the first consumer ships a sustained multi-pipeline
workload (Alexandria's enrichment engine projects 30–50k spans/sec vs a
~100–150k realistic ceiling at typical attr counts — re-run the sink bench
against its real rate), or `Stats().Dropped > 0` / rising `QueueDepth`
on any real workload.

## Live snapshot handler (`serve`)
The `http.Handler` exposing a consistent `VACUUM INTO` snapshot of the
live DB at `/trace.db` (D9, D24) — in the `gospan/sqlite` module, since
it is meaningless without a database file. Deferred out of v1: its only
consumer is the viewer's live-ish mode (D26). Snapshots should run on
their own read connection so a slow copy never stalls the writer.
**Trigger:** the viewer repository lands and wants live mode — or a real
mid-run diagnosis need that opening the live WAL file locally cannot
serve.

## Block-scoped span sugar (`Within`)
Wrapping a block in a span with clean visual enclosure. Go has no block
syntax to hook, so the shape is a closure wrapper:
`tracer.Within(ctx, "name", func(ctx context.Context) error { … }) error` —
and the genuinely attractive part is not the braces but the error plumbing:
the callback's returned error auto-`Fail`s the span (classified) before
`End`, collapsing the Start/defer/Fail/End quartet to one expression. Costs:
one closure allocation per call (the hot path's alloc ceilings are the
enforced performance claim — sugar must not erode them silently, so it gets
its own documented ceiling), a second way to do what `Start`+`defer` already
does, and awkward plumbing for results beyond `error`. `Track` already
covers the leaf one-liner case.
**Trigger:** recurring real call sites (ours or a user's) where the quartet
is demonstrably noisier than a closure — not aesthetics in the abstract.

## Query ergonomics: shipped views + a generic SQL script library
Two observations from the first real consumer (Alexandria's import
pipeline). First: every human query starts by joining `names` — a
`CREATE VIEW` shipped in the schema (spans ⨝ names, exposing `name`
directly) would erase the boilerplate at zero write-path cost; views are
additive, but the schema is the frozen §3 contract, so it lands as a
deliberate versioned addition, not a casual edit. Second: the analysis
queries that made the trace file sing (per-name exact percentiles,
effective-parallelism = total span work / wall clock, slowest-spans
leaderboard with attr join, failed/incomplete triage) are generic to any
gospan file — only the consumer's stage-gap "queue map" was
domain-specific. A `sqlite/scripts/` directory of .sql files (the
sqlite3-CLI kind, `.mode box` headers) would hand every user the
first-day analysis experience Alexandria hand-rolled in `cmd/dev/sql/`.
**Trigger:** the next consumer (or first external user) reinventing the
same queries — or fold the generic half of Alexandria's kit upstream when
the viewer round starts, since the viewer needs the same canned queries
anyway.
