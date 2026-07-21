# gospan — Spec

The build contract: API surface, semantics, schema, file-format guarantees.
Conceptual rationale lives in [DESIGN.md](DESIGN.md). Once implementation
lands, §1–§2 migrate into godoc; §3–§5 remain the compatibility surface (trace
files outlive library versions).

## 1. Public API (core module)

```go
package gospan

// Construction — errors surface at construction and nowhere else (here, and
// in sink constructors like sqlite.New). New mirrors slog.New: hand it a
// Sink (the destination); the Tracer owns everything upstream of it. After
// construction succeeds, nothing gospan does ever returns an error or panics
// into the caller.
func New(s Sink, opts ...Option) (*Tracer, error)
func SetDefault(t *Tracer)
func Default() *Tracer

type Option func(*config)
func WithBufferSize(n int) Option              // bounded event buffer (default ~8192)
func WithFlushInterval(d time.Duration) Option // Flush() tick cadence (default ~1s)
func WithBlockingPolicy() Option               // block producers when full (default: drop and count)
func WithLogger(l *slog.Logger) Option         // where the tracer complains (rate-limited, Warn); nil = silent
func WithOverheadSampling(every int) Option    // every Nth span times its own cost (default 128; 1 = every span)

// Spans. Start reads the parent span off ctx; no parent → new trace root.
// The returned ctx carries the new span.
func (t *Tracer) Start(ctx context.Context, name string, attrs ...slog.Attr) (context.Context, *Span)
func (t *Tracer) Track(ctx context.Context, name string, attrs ...slog.Attr) func()
func FromContext(ctx context.Context) *Span    // current span, or nil; nil-safe like everything else

func (s *Span) SetAttrs(attrs ...slog.Attr)    // facts learned mid-flight; last write per key wins
func (s *Span) Fail(err error)                 // records why the span didn't succeed
func (s *Span) End()

// Package-level mirrors on the default tracer (the slog.Info pattern).
func Start(ctx context.Context, name string, attrs ...slog.Attr) (context.Context, *Span)
func Track(ctx context.Context, name string, attrs ...slog.Attr) func()

// Introspection — atomics snapshots, cheap enough for a ticker.
func (t *Tracer) Stats() Stats      // tracer self-health
func (t *Tracer) Summary() map[string]SpanSummary // your code's performance, by span name

// Lifecycle. Close drains the buffer, finishes the file, and makes the
// Tracer permanently inert (subsequent calls are no-ops). ctx bounds the wait.
func (t *Tracer) Close(ctx context.Context) error

type Stats struct {
    Started, Completed uint64                          // SPANS, cumulative since New
    Written, Dropped   uint64                          // queue EVENTS (start/end/attr updates, ~2–3 per span), cumulative
    SpansInFlight, TracesInFlight, QueueDepth int      // this instant
    WriteErrors uint64                                  // failed batch commits (degraded, counted)
    OverheadPerSpan time.Duration                       // rolling average tracer-added cost
}

type SpanSummary struct {
    Count, Errors, Canceled uint64
    Min, Max, Mean          time.Duration
    P50, P90, P99           time.Duration // approximate: log-bucketed histogram, small bounded relative error
}
```

**The sink seam** (the `slog.Handler` pattern — field-level details settled
at implementation, shape and contract settled now):

```go
// Sink is the destination seam. All methods are called from the single
// writer goroutine — never concurrently — and take struct arguments so
// fields can be added without breaking implementers.
type Sink interface {
    WriteBatch(b Batch) error // ≥1 events, in order, delivered as the queue drains
    Flush() error             // ticked on the flush interval — the commit/fsync moment
    Close() error
}

func SlogSink(l *slog.Logger) Sink // in-tree, stdlib-only: spans into your log flow
func MultiSink(sinks ...Sink) Sink // fan-out, the io.MultiWriter pattern:
                                   // delivers to every sink sequentially in order,
                                   // joins errors (one failure never starves the rest)
```

The writer serializes and orders; each sink owns its I/O strategy. A "batch"
of one is a stream — under light load events reach the sink near-immediately;
the slog sink emits in `WriteBatch` and no-ops `Flush`; the SQLite sink
buffers in `WriteBatch` and commits (and coalesces insert/update pairs) in
`Flush`. Sinks must not retain the `Batch` (or anything reachable from it)
after the call returns — copy what you keep; the writer reuses buffers, and
under `MultiSink` the same batch visits every sink. A slow sink slows the
sequential fan-out and surfaces as rising `Stats().QueueDepth` — deliberate:
no per-sink goroutines. Non-slog loggers need no sink of their own:
`slog.Logger` is a frontend over `slog.Handler`, and maintained logging
libraries bridge to it (charmbracelet/log implements it natively;
zap/zerolog/logrus ship bridges).

In-tree sinks are SQLite and slog only. Admission policy for a third:
general-purpose (a format or stdlib facility, never a vendor system), zero
new dependencies, zero core changes. Third-party sinks live in third-party
modules and are unsupported here by design. `Stats()` and `Summary()` are
sink-independent.

## 1a. Module layout

One repo, two Go modules, one external viewer repo:

- **`gospan` (core module): zero third-party dependencies — stdlib only.**
  Tracer, Span, Sink, SlogSink, Stats, Summary.
- **`gospan/sqlite` (nested module, own go.mod):** the flagship sink —
  `sqlite.New(dir string, opts ...Option) (*Sink, error)` creates the directory if absent
  (construction is where its errors surface) and, by default, mints one
  auto-named file per run (`gospan-<utc-timestamp>-<pid>.sqlite`; no
  collision semantics exist because no two runs share a file; multi-run
  analysis is ATTACH across siblings; the sink exposes `Path()`, and
  `OpenReadHandle() (*sql.DB, error)` — a fresh SQLite-enforced read-only
  connection (`mode=ro`) for live mid-run queries: WAL readers never block
  the one writer, and the sink stays the file's only writer). `WithName(name,
  overwrite)` swaps the auto-name for a stable, optionally-overwritten path
  (D30). Carries `modernc.org/sqlite`; users who don't import this module
  never see it, not even in go.sum. A live-snapshot **`serve`** handler (an
  `http.Handler` exposing a `VACUUM INTO` snapshot of the live DB at
  `/trace.db`) is **deferred out of v1** (D26): it lands here when the
  viewer's live mode needs it — see DEFERRED.md.
- **The viewer is a separate repository** (producer and consumer have
  different lifecycles and toolchains). It consumes §3–§5 of this spec as a
  cross-repo contract: completed files via drag-and-drop (WASM SQLite), and —
  once the deferred `serve` handler lands — live state by polling its snapshot
  URL. It builds to static assets — the
  zero-server, open-a-page, drag-a-file experience is unchanged.

## 2. Semantics

| Rule | Contract |
|---|---|
| Nil is off | Every method on a nil `*Tracer` or nil `*Span` is a no-op with zero-value returns. Nil-`Start` returns `(ctx, nil)` unchanged. |
| Trace root | `Start` with no span in ctx mints a new `trace_id`; the span has NULL parent. |
| Status | `0 ok · 1 error · 2 canceled`. `Fail(err)` sets error, or canceled when `errors.Is(err, context.Canceled)` / `DeadlineExceeded`; records `err.Error()`; `Fail(nil)` is a no-op; last `Fail` before `End` wins. |
| End | First `End` wins; all mutations after `End` (SetAttrs, Fail, second End) are no-ops. `End` never blocks beyond a channel send. |
| Cross-goroutine | `End`/`Fail`/`SetAttrs` are safe from any goroutine, not just `Start`'s. |
| Attrs | Last write per key wins (enforced by the writer, cheap append at the call site). Values are `slog.Attr`; `Group` attrs are flattened with `.`-joined keys. Oversized values are stored verbatim today; a size cap with a truncation marker is deferred (see DEFERRED.md). |
| Track | Leaf-only by construction: it cannot return a ctx, so nothing nests under it. `defer tracer.Track(ctx, "x")()` — span starts at evaluation, ends when the closure runs. |
| Panic safety | `defer`red `End` runs on panic — the span gets an end time. gospan never recovers the *caller's* panics; it only recovers its own. |
| Incomplete spans | A span with `end_ns IS NULL` in a closed file never received `End` (crash, `os.Exit`, dropped end event). Flagged in the viewer; never diagnosed further. |
| Drop policy | Default: buffer full → event dropped, `Stats.Dropped++`. `WithBlockingPolicy`: producer blocks. A dropped *start* with a surviving *end* (or vice versa) degrades to an incomplete/orphan row — readers must tolerate missing parents and render children at top level. |
| Close | Idempotent. Drains, flushes, checkpoints, stamps nothing further. Tracer is inert afterward. |

## 3. Schema (file format v1)

Owned by the `gospan/sqlite` module; specified here because it is the
cross-repo contract the viewer builds against.

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;   -- bounded loss on power cut, not zero loss

CREATE TABLE meta (
    schema_version INTEGER NOT NULL,   -- 1; future versions migrate or refuse loudly
    file_id        TEXT    NOT NULL,   -- random, minted once at New(); global uniqueness paid once per file
    created_at_ns  INTEGER NOT NULL    -- wall-clock anchor (unix ns)
) STRICT;

CREATE TABLE names (                    -- interned span names: ~dozens of rows
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
) STRICT;

CREATE TABLE spans (
    id        INTEGER PRIMARY KEY,      -- tracer-minted, monotonic
    trace_id  INTEGER NOT NULL,         -- tracer-minted; no traces table exists
    parent_id INTEGER,                  -- NULL = trace root
    name_id   INTEGER NOT NULL REFERENCES names(id),
    start_ns  INTEGER NOT NULL,         -- absolute unix ns (monotonic-derived; see §4)
    end_ns    INTEGER,                  -- NULL = running / incomplete
    status    INTEGER NOT NULL DEFAULT 0,  -- 0 ok · 1 error · 2 canceled
    error     TEXT                      -- populated only by Fail()
) STRICT;

CREATE INDEX spans_by_trace ON spans (trace_id, start_ns);

CREATE TABLE attrs (
    span_id INTEGER NOT NULL,           -- no FK: writer owns integrity; readers tolerate orphans
    key     TEXT    NOT NULL,
    kind    INTEGER NOT NULL,           -- slog.Kind numeric value; viewer renders by kind
    value   ANY,                        -- SQLite cells are natively typed
    PRIMARY KEY (span_id, key)
) STRICT, WITHOUT ROWID;

-- Convenience view (additive; no schema_version bump — D27). Resolves the
-- names join and the derived duration every human query needs; readers that
-- only want the tables ignore it. duration_ns is NULL while end_ns is NULL.
CREATE VIEW spans_named AS
    SELECT s.id, s.trace_id, s.parent_id, n.name,
           s.start_ns, s.end_ns, s.end_ns - s.start_ns AS duration_ns,
           s.status, s.error
    FROM spans s JOIN names n ON n.id = s.name_id;
```

Write path: span row inserted at first flush after `Start` (`end_ns` NULL);
updated by primary key at `End`; coalesced to a single complete insert when
both fall in one batch. Attrs land with whichever flush knows them —
`INSERT OR REPLACE` makes last-wins a schema property.

Deliberate omissions: no index on `name_id` or `parent_id` (analysis-side
`CREATE INDEX` on a closed file is legal and encouraged — the live path pays
for exactly one query shape, the waterfall); no logs table (deferred feature;
`schema_version` bump adds it additively); no FK enforcement.

## 4. Time

`New` captures (wall W₀, monotonic M₀) as a pair; every timestamp is
`W₀ + (M_now − M₀)`, stored as int64 unix nanoseconds (overflows in 2262).

Guaranteed: within one file, `end_ns − start_ns` and inter-span deltas are
monotonic-exact — wall-clock jumps (NTP, VM resume) cannot corrupt them.
Not guaranteed: cross-file alignment. Comparing files from different machines
is unsupported (no time sync, no cross-process anything). Same-machine
multi-run analysis needs no tooling from us: `ATTACH 'run2.sqlite' AS r2` and
cross-query.

## 5. Compatibility promises

- `schema_version` gates everything: a future library version encountering an
  older file migrates it or refuses loudly — never misreads silently.
- The `attrs.kind` column carries `slog.Kind` numeric values as of the Go
  version at v1 release; new kinds may appear, existing values never change
  meaning.
- Status enum values are frozen (`0/1/2`); additions append.
- The viewer must tolerate: NULL `end_ns`, missing parents (render at top
  level, flagged), unknown `kind` values (render raw), additive schema
  objects it does not recognize (a new view like `spans_named`, or — under a
  `schema_version` bump — new tables/columns), and files larger than
  comfortable (degrade, don't crash — the WASM engine loads files fully into
  browser memory; this tool is for single-run and modest multi-run analysis,
  not a warehouse).
