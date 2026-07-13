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
	nameIDs      map[string]int64 // interned span names
}

// New creates dir if absent, mints one auto-named trace file inside it
// (gospan-<utc-timestamp>-<pid>.sqlite — no two runs ever share a file,
// so no collision semantics exist; old runs accumulate as siblings for
// ATTACH-style comparison, D17), and prepares the schema. Construction is
// where every error surfaces (D23): a Sink you receive is ready to write.
func New(dir string) (*Sink, error) {
	// 0o750, not 0o755: trace files can carry sensitive attribute values
	// (paths, identifiers), so the directory defaults to no world access.
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("gospan/sqlite: creating trace directory: %w", err)
	}

	// Nanosecond timestamp plus PID: unique even for two tracers built in
	// the same process in the same second.
	startedAt := time.Now().UTC().Format("20060102T150405.000000000Z")
	path := filepath.Join(dir, fmt.Sprintf("gospan-%s-%d.sqlite", startedAt, os.Getpid()))

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
		db:          db,
		path:        path,
		pendingByID: make(map[int64]*spanRow),
		nameIDs:     make(map[string]int64),
	}, nil
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
