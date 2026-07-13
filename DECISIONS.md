# gospan — Decisions

Append-only. Each entry: what was decided, why, and what was rejected.
Rejections live here too — a rejection is a decision.

## 2026-07-13 — Founding design round

The original design was drafted in a prior session and reworked top-down in
this one; entries below record the settled positions.

**D1 — Span-row storage with insert-at-start, not an event log, not
insert-at-end.** A span is semantically a start event + an end event; the
question is where pairing happens. An event log (two rows) pushes a self-join
into every duration query forever — readers pay, in a browser, per query. Pure
insert-at-end loses every open span on a crash — including the long-running
one you most needed to see — and cannot represent "incomplete." Chosen: one
row inserted at start (`end_ns` NULL), updated by PK at end, **coalesced to a
single insert when start and end land in the same batch** (the common case —
so the second write is only paid by spans that outlive a flush interval).
Incomplete = NULL end, free; live snapshots see running work, free.

**D2 — IDs are tracer-minted monotonic int64s; uniqueness is per-file, not
per-span.** DB auto-increment can't work: a child needs its parent's ID while
the parent is still in the write buffer. An `atomic.Int64` preserves the
sequential-insert B-tree locality. UUIDs-per-span rejected (4–5× ID storage,
string compares, random inserts scatter the B-tree); one random `file_id` in
`meta` carries all global uniqueness, paid once per run.

**D3 — Attributes are `slog.Attr`.** Stdlib type users already know; typed
values with zero boxing for the common kinds; constructors (`slog.Int`, …)
carry the type information that makes stored attributes SQL-queryable
(`WHERE value > 8` as integers, not strings). Rejected: stringify-anything
(destroys typed queries; allocates per attr), generics (heterogeneous attrs
need a common storage type anyway — that type already exists and is
`slog.Value`), `map[string]any` (boxing, unordered). Loose alternating-pair
sugar (`"k", v, …`) deferred until real verbosity pain.

**D4 — Terminal state: `End()` + `Fail(err)`, auto-classified.** At a typical
call site the caller doesn't know whether an error is a cancellation — the
classification is already in the error value, and `errors.Is` reads it once,
centrally; per-site explicit canceled-vs-failed would mislabel in practice.
`Terminate(err)` rejected as a name: it's an ending verb, and the API already
has one — `End` must stay separate because it lives in `defer` (panic
safety). A canceled span did fail to complete; status `{ok, error, canceled}`
keeps the diagnostic distinction. Retries are not a status: attempt N is a
new sibling span with an `attempt` attribute.

**D5 — Attrs in a side table, not a JSON column on the span row.** Attrs
arrive at different times under insert-at-start (some at Start, some via
SetAttrs later); table rows are `INSERT OR REPLACE`, a JSON blob is
read-merge-rewrite per late attr. Typed cells beat `json_extract` re-parsing
per row per query. `PRIMARY KEY (span_id, key)` makes last-wins a schema
property. Spans without attrs pay nothing.

**D6 — Timestamps: absolute unix-ns, derived monotonically from a per-file
anchor.** `New` captures (wall, monotonic) once; all timestamps are anchor +
monotonic delta. Durations survive NTP jumps; absolute values are as good as
the wall clock was once at `New`. Cross-machine alignment declared
unsupported rather than half-solved.

**D7 — The hierarchy is a strict tree; no span links.** Fan-in (a batch
commit serving N traces) is modeled as its own tiny trace plus a shared
correlation attribute (`batch_seq`) on both sides — the hierarchy-vs-
attributes principle applied, not a workaround. OTel-style span links
(many-to-many side table) deferred behind a real trigger.

**D8 — Positioning: own lightweight core; OTel interop via a future optional
adapter (separate go.mod), not OTel as the core API.** The niche re-check
found the space narrower than first claimed — otel-desktop-viewer and
otel-tui do local trace viewing as a *sidecar process fed over OTLP* — but
still open: nothing is embedded, file-first, zero-server. Building core *on*
the OTel SDK rejected: heavy dependency tree, verbose API, surrendered hot
path. Building an OTLP-network anything rejected: that's the sidecar shape.
The adapter (`SpanExporter` → our SQLite schema) makes the entire OTel
instrumentation ecosystem (incl. go-instrument's codegen) a feed for our file
+ viewer, when demand appears.

**D9 — The custom viewer is v1 core, and there is exactly one viewer
implementation.** The self-contained HTML + WASM-SQLite viewer is the
product's identity (and Perfetto's generic UI can't do our attribute
rendering or future log click-through). The live-ish mode serves a
*snapshot* of the DB file (`gospan/serve`, SQLite backup API) rather than
JSON query endpoints, so file mode and live mode are the same viewer
consuming the same artifact — a server-side query API would fork every
viewer feature into two implementations forever. True streaming rejected for
v1 (different animal: push channel, incremental state); refresh-a-snapshot
is 80% of the value for ~0 design cost.

**D10 — Log capture cut from v1.** The boundary was always "not a log
aggregator"; the narrower span-scoped capture (a `slog.Handler` keying lines
by span ID) is genuinely useful but deferrable — attributes carry most of the
"why was this slow" payload, structured and queryable. One table + a handler,
added under a `schema_version` bump when wanted. Cutting it also dissolves
the opt-in-vs-ambient question by deferral.

**D11 — `Summary()` is in scope; `Stats()` and `Summary()` are separate
methods.** Per-name duration aggregates (count/errors/min/mean/p50/p90/p99)
are derived from spans the writer already sees — introspection, not a metrics
system; maintained lock-free in the single writer goroutine via small
log-bucketed histograms (bounded memory, approximate percentiles; exact
answers live in the file via SQL). Stats answers "how is the tracer doing"
(drops, queue, overhead); Summary answers "how is your code doing." Kept
separate because they're different questions with different audiences.

**D12 — Merge tooling deleted from scope.** Cross-machine merging is
unsupported (no clock story, no comms). Same-machine multi-run analysis is
already native SQLite: `ATTACH` and cross-query — a README paragraph, not a
tool. `file_id` stays in `meta` (one column preserves every future option).

**D13 — One live index; analysis-side indexing.** Every index taxes every
insert on the hottest table. The write path pays for the waterfall query
only (`(trace_id, start_ns)`); post-hoc analysts `CREATE INDEX` on their copy
of a closed file — the artifact is just SQLite, that's legal and instant.

**D14 — Nil is off.** All methods on nil `*Tracer`/`*Span` are no-ops:
instrument unconditionally, enable by constructing. The on/off switch is the
existence of the object, not a flag. Production builds that never call `New`
pay a nil check per site.

**D15 — Pure-Go SQLite driver (`modernc.org/sqlite`), STRICT tables.** CGO
means "install a C compiler to adopt this library" — disqualifying friction.
STRICT (SQLite ≥3.37) turns writer type bugs into insert errors caught in CI
instead of silent junk in someone's archived trace file; `ANY` is the
sanctioned dynamic type for attr values.

**D16 — Validation walk findings (against Alexandria's real pipeline).**
(a) Channel pipelines can't get span context from the goroutine ctx — the
work item carries it (one additive field); this is cookbook pattern #1, the
same requirement every tracer has, and the measure of "integrates without
refactor." (b) `FromContext(ctx) *Span` added to the surface — stages need to
tag the item's root span with only a ctx in hand. (c) Cross-goroutine
`End`/`SetAttrs`/`Fail` promoted from implementation detail to documented
contract — pipeline roots start in one goroutine and end in another
routinely. (d) For convergent job systems with no run identity, trace = one
job execution (root never spans scans); per-entity history is an attribute
query. The trace file is observational exhaust, never read back by the traced
program — no second source of truth.

**D17 — `New` takes a directory; one auto-named file per run** *(same-day
amendment)*. `New(path)` left "the file already exists" undefined (append a
second run into one file? overwrite? refuse?). A directory + per-run
auto-naming (`gospan-<timestamp>-<pid>.sqlite`) deletes the question: no two
runs share a file, `meta` stays one-row-per-run by construction, old runs
accumulate as siblings for `ATTACH`-style comparison, and `Path()` reports
where this run landed.

**D18 — The destination is a small public `Sink` interface; in-tree sinks
are curated brutally** *(same-day amendment)*. The orchestration machinery
(hot path, buffer, writer, Stats/Summary, lifecycle) is destination-agnostic;
only the writer's terminal step varies. Resolved the pluggability fork the
`slog.Handler` way: opinionated fixed frontend, one tiny public backend
interface, two in-tree implementations (SQLite — the flagship the viewer and
SQL story hang off — and a zero-dep slog emitter), unbounded *out-of-tree*
ecosystem that costs us nothing — Go dependencies flow with imports, so a
third-party Kafka sink can exist without any gospan user ever downloading
Kafka anything. Price acknowledged: a public interface is frozen API —
mitigated by minimal method count, struct arguments (fields addable
compatibly), and a single-goroutine delivery contract (sink authors never
handle concurrency). In-tree admission: general-purpose only, zero new deps,
zero core changes; vendor/network destinations out of tree forever. Rejected:
fully-internal seam (locks out legitimate adaptation; contradicts wanting
broad adoption of a thin core) and a public exporter *framework* (the OTel
sprawl this project exists to not be). Build order: slog sink first (proves
the machinery end-to-end with zero deps), SQLite second — sequencing, not a
demotion of the file.

**D19 — `New` takes the Sink; core goes zero-dependency; the SQLite sink is
a nested module** *(same-day amendment)*. With the destination a first-class
seam, `New(dir)` privileged one sink in the front door — replaced by
`New(sink, opts...)`, completing the slog mirror (`slog.New(handler)`). The
directory argument moves to `sqlite.New(dir)` (D17's per-run auto-naming and
`Path()` move with it; paths stay `string` — Go has no path type, `filepath`
operates on strings, and `io/fs.FS` is read-only). Module layout: one repo,
two modules — core with **zero third-party dependencies**, and
`gospan/sqlite` (own go.mod) carrying `modernc.org/sqlite` plus `serve`
(which is meaningless without a database file). Nested module ≠ separate
repo: one issue tracker, atomic docs, two dependency universes — core-only
users never see the driver, not even in go.sum.

**D20 — `Flush` joins the Sink interface; batching belongs to sinks, not the
writer.** A logger wants events now; a database wants amortized commits —
that split is each destination's I/O strategy, not the writer's. The writer
serializes and orders: it delivers events to `WriteBatch` as the queue
drains (a batch of one IS a stream — the stream-vs-batch dichotomy dissolves
at this layer) and ticks `Flush()` on the interval as the commit/fsync
moment. Slog sink: emit in WriteBatch, no-op Flush. SQLite sink: buffer in
WriteBatch; commit and coalesce insert/update pairs in Flush. Three methods,
still called from one goroutine only.

**D21 — The viewer moves to its own repository.** Producer and consumer are
separate concerns: different lifecycles, different toolchains — a viz app's
build tooling has no business in a Go library repo. SPEC's schema/file-format
sections are thereby promoted to a cross-repo contract. This supersedes the
founding docs' "no React" note: that constraint was really about the *user's*
experience (zero servers, open a page, drag a file), which a React app
compiled to static assets preserves exactly; only the dev toolchain changes,
and it now lives elsewhere. The viewer reads completed files (drag-and-drop,
WASM SQLite) and live runs (polling a `serve` snapshot URL) — one app, both
modes.

**D22 — Multi-destination via a `MultiSink` combinator, not a sink list in
the tracer.** The io.MultiWriter pattern: composition is itself a `Sink`, so
`New` stays one-sink, the tracer grows zero machinery, it nests, and it works
with out-of-tree sinks unchanged. Delivery is sequential in the writer
goroutine (ordering preserved, single-goroutine contract intact — no per-sink
goroutines by design; a slow sink backpressures visibly via
`Stats().QueueDepth`); errors are joined (`errors.Join`) so one failing sink
never starves the rest. Forced a contract clarification worth having anyway:
sinks must not retain the delivered `Batch` — the writer reuses buffers, and
under fan-out the same batch visits every sink.

## 2026-07-13 — Implementation round

**D23 — `sqlite.New(dir string) (Sink, error)`; errors surface at
construction, wherever construction happens.** The founding spec pinned
`sqlite.New(dir) Sink` with `gospan.New` as "the one place errors surface" —
but sink construction can genuinely fail (bad directory, unwritable disk,
DDL failure), and that error had nowhere idiomatic to go. Rejected: a
Flush-probe in `gospan.New` (a failed sink stores its error and returns it
from every method; New probes once before starting the writer — zero new API,
but overloads Flush's semantics and hides the failure contract) and an
optional `interface{ Err() error }` (a side contract sink authors must
discover). Chosen: plain Go idiom — the constructor returns the error. The
principle survives restated: errors surface at construction time, never
during operation; there are simply two constructors now. Costs the README
quickstart its one-line nesting.

**D24 — `serve` snapshots via `VACUUM INTO`, not the backup API.** The
founding docs parenthetically named SQLite's backup API as the snapshot
mechanism. `VACUUM INTO` provides the identical observable guarantee — a
consistent point-in-time copy of a live WAL database — as one SQL statement
through plain database/sql, where modernc's backup API requires
raw-connection plumbing into driver internals. Trade accepted: each snapshot
is a full compacting rewrite rather than a page-level copy — mitigated by
the snapshot cache and by snapshots being made only on request; the copy
arrives defragmented as a side effect.
