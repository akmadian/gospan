// Package sqlite is gospan's flagship destination: one auto-named SQLite
// file per run, written by the tracer's writer goroutine, readable by the
// viewer, by ATTACH-style multi-run analysis, and by plain SQL. It lives
// in its own module so the driver dependency never touches users who
// chose another destination (D19).
package sqlite

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite" // the pure-Go driver: no CGO, no C compiler to adopt (D15)
)

// schemaVersion gates the file format: a future library version opening an
// older file migrates it or refuses loudly — never misreads silently
// (SPEC §5).
const schemaVersion = 1

// schema is SPEC §3 verbatim — the cross-repo contract the viewer builds
// against. STRICT turns writer type bugs into insert errors caught in CI,
// not silent junk in someone's archived trace file. Deliberate omissions
// (no name_id/parent_id indexes, no FK enforcement, no logs table) are the
// spec's, not oversights: the live path pays for exactly one query shape.
const schema = `
CREATE TABLE meta (
    schema_version INTEGER NOT NULL,
    file_id        TEXT    NOT NULL,
    created_at_ns  INTEGER NOT NULL
) STRICT;

CREATE TABLE names (
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
) STRICT;

CREATE TABLE spans (
    id        INTEGER PRIMARY KEY,
    trace_id  INTEGER NOT NULL,
    parent_id INTEGER,
    name_id   INTEGER NOT NULL REFERENCES names(id),
    start_ns  INTEGER NOT NULL,
    end_ns    INTEGER,
    status    INTEGER NOT NULL DEFAULT 0,
    error     TEXT
) STRICT;

CREATE INDEX spans_by_trace ON spans (trace_id, start_ns);

CREATE TABLE attrs (
    span_id INTEGER NOT NULL,
    key     TEXT    NOT NULL,
    kind    INTEGER NOT NULL,
    value   ANY,
    PRIMARY KEY (span_id, key)
) STRICT, WITHOUT ROWID;

-- spans_named resolves the names join every human query starts with and
-- exposes the derived duration, so ad-hoc SQL and the shipped scripts read
-- "name" and "duration_ns" directly. A view is additive — it changes no
-- table and needs no schema_version bump — but it edits the frozen §3
-- surface, so it is a deliberate versioned addition (D27). duration_ns is
-- NULL while a span is running or incomplete (end_ns IS NULL).
CREATE VIEW spans_named AS
    SELECT s.id, s.trace_id, s.parent_id, n.name,
           s.start_ns, s.end_ns, s.end_ns - s.start_ns AS duration_ns,
           s.status, s.error
    FROM spans s JOIN names n ON n.id = s.name_id;
`

// Sink writes trace events into one SQLite file. Construct with New; it
// implements gospan.Sink, buffering events in WriteBatch and committing
// them in Flush — transaction batching is a SQLite strategy, not a writer
// concern (D20). All state below db/path is touched only by the tracer's
// writer goroutine (the Sink contract), so none of it needs locking.
type Sink struct {
	db   *sql.DB
	path string

	pendingSpans []*spanRow
	pendingByID  map[int64]*spanRow // open pending rows, for start+end coalescing
	pendingAttrs []attrRow

	// nameIDs holds committed interned names only. internedThisFlush holds
	// names inserted inside the in-flight transaction; they are promoted
	// into nameIDs only after the commit succeeds, so a rolled-back flush
	// can never leave nameIDs pointing at a names row that no longer exists.
	nameIDs           map[string]int64
	internedThisFlush map[string]int64
}

// Option configures a Sink at construction. With no options, New writes one
// auto-named file per run; WithName overrides that.
type Option func(*config)

// config is the resolved set of options.
type config struct {
	name      string
	named     bool // WithName was passed — distinguishes an empty name from none
	overwrite bool
}

// WithName writes the run to a file you name, instead of the auto-generated
// gospan-<utc-timestamp>-<pid>.sqlite. A relative name lands inside dir; an
// absolute path is used as-is (dir is then ignored for the file's location).
// The name is taken verbatim — no extension is appended, so include
// ".sqlite" yourself if you want it.
//
// By default a name that already exists is an error at construction, so a
// rerun never silently destroys the previous run's trace. Pass overwrite=true
// to replace it — the old file and its -wal/-shm sidecars are removed first.
// Appending two runs into one file is deliberately not offered: it would fold
// their IDs together, defeating the per-file file_id design (D2).
func WithName(name string, overwrite bool) Option {
	return func(cfg *config) {
		cfg.name = name
		cfg.named = true
		cfg.overwrite = overwrite
	}
}

// New prepares a trace file and returns a Sink ready to write. With no
// options it mints one auto-named file inside dir
// (gospan-<utc-timestamp>-<pid>.sqlite — no two runs ever share a file, so
// old runs accumulate as siblings for ATTACH-style comparison, D17);
// WithName overrides the name and its collision policy. The target directory
// is created if absent. Construction is where every error surfaces (D23): a
// Sink you receive is ready to write.
func New(dir string, options ...Option) (*Sink, error) {
	cfg := config{}
	for _, option := range options {
		option(&cfg)
	}

	path, err := resolvePath(dir, cfg)
	if err != nil {
		return nil, err
	}

	// 0o750, not 0o755: trace files can carry sensitive attribute values
	// (paths, identifiers), so the directory defaults to no world access.
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("gospan/sqlite: creating trace directory: %w", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("gospan/sqlite: opening %s: %w", path, err)
	}
	// One connection, always: the tracer's writer goroutine is the only
	// caller, and a single conn makes PRAGMAs and transactions apply to
	// the session that does the writing.
	db.SetMaxOpenConns(1)

	if err := initialize(db); err != nil {
		// Leave no half-born trace file behind: close, remove, report.
		closeErr := db.Close()
		removeErr := os.Remove(path)
		return nil, fmt.Errorf("gospan/sqlite: initializing %s: %w", path, errors.Join(err, closeErr, removeErr))
	}
	return &Sink{
		db:                db,
		path:              path,
		pendingByID:       make(map[int64]*spanRow),
		nameIDs:           make(map[string]int64),
		internedThisFlush: make(map[string]int64),
	}, nil
}

// resolvePath applies the naming and collision rules and returns the file
// New should open. The auto-name carries all its uniqueness in the name
// itself, so it never collides; a WithName file is checked here, before any
// file is touched, so an unwanted overwrite fails at construction.
func resolvePath(dir string, cfg config) (string, error) {
	if !cfg.named {
		// Nanosecond timestamp plus PID: unique even for two tracers built
		// in the same process in the same second.
		startedAt := time.Now().UTC().Format("20060102T150405.000000000Z")
		return filepath.Join(dir, fmt.Sprintf("gospan-%s-%d.sqlite", startedAt, os.Getpid())), nil
	}

	if cfg.name == "" {
		return "", errors.New("gospan/sqlite: WithName given an empty name")
	}
	path := cfg.name
	if !filepath.IsAbs(path) {
		path = filepath.Join(dir, path)
	}

	info, err := os.Stat(path)
	switch {
	case err == nil:
		if info.IsDir() {
			return "", fmt.Errorf("gospan/sqlite: %s is a directory, not a trace file", path)
		}
		if !cfg.overwrite {
			return "", fmt.Errorf("gospan/sqlite: %s already exists; pass WithName's overwrite=true to replace it", path)
		}
		// Overwrite: remove the file and its write-ahead sidecars, so the
		// replacement never inherits a stale WAL from the run it replaced.
		for _, sidecar := range []string{path, path + "-wal", path + "-shm"} {
			if err := os.Remove(sidecar); err != nil && !os.IsNotExist(err) {
				return "", fmt.Errorf("gospan/sqlite: replacing %s: %w", path, err)
			}
		}
	case !os.IsNotExist(err):
		// Stat failed for a reason other than absence (a permission problem
		// on a path component, say): surface it now, at construction.
		return "", fmt.Errorf("gospan/sqlite: checking %s: %w", path, err)
	}
	return path, nil
}

// initialize applies the pragmas, the schema, and the one-row meta table
// that carries the file's identity.
func initialize(db *sql.DB) error {
	// WAL so readers (the serve snapshot, a curious sqlite3 session) never
	// block the writer; synchronous=NORMAL is the spec'd durability trade:
	// bounded loss on power cut, not zero loss.
	for _, pragma := range []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
	} {
		if _, err := db.Exec(pragma); err != nil {
			return fmt.Errorf("%s: %w", pragma, err)
		}
	}
	if _, err := db.Exec(schema); err != nil {
		return fmt.Errorf("creating schema: %w", err)
	}

	// file_id carries all global uniqueness, paid once per run — span IDs
	// stay small monotonic integers (D2).
	fileID := make([]byte, 16)
	if _, err := rand.Read(fileID); err != nil {
		return fmt.Errorf("minting file_id: %w", err)
	}
	_, err := db.Exec(
		"INSERT INTO meta (schema_version, file_id, created_at_ns) VALUES (?, ?, ?)",
		schemaVersion, hex.EncodeToString(fileID), time.Now().UnixNano(),
	)
	if err != nil {
		return fmt.Errorf("writing meta: %w", err)
	}
	return nil
}

// Path reports where this run's trace file landed.
func (sink *Sink) Path() string {
	return sink.path
}

// Close finishes the file. The tracer calls it exactly once, after a
// final WriteBatch and Flush.
func (sink *Sink) Close() error {
	return sink.db.Close()
}

// OpenReadHandle opens a new, genuinely read-only connection to the live
// trace file, so a caller can query mid-run while the writer goroutine
// keeps its own connection: WAL readers never block the one writer. Each
// call opens a fresh handle; the caller closes it when done. Keep read
// transactions short — a long-lived read transaction pins the WAL against
// checkpointing, growing the -wal file for as long as it is held.
func (sink *Sink) OpenReadHandle() (*sql.DB, error) {
	// mode=ro makes read-only an SQLite-enforced property of the
	// connection (writes fail with SQLITE_READONLY), not a convention the
	// caller is trusted to follow — the sink stays the file's only writer.
	db, err := sql.Open("sqlite", "file:"+sink.path+"?mode=ro")
	if err != nil {
		return nil, fmt.Errorf("gospan/sqlite: opening %s for reading: %w", sink.path, err)
	}
	db.SetMaxOpenConns(1)
	return db, nil
}
