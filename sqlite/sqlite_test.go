package sqlite

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/akmadian/gospan"
)

func newTestSink(t *testing.T) *Sink {
	t.Helper()
	sink, err := New(filepath.Join(t.TempDir(), "traces"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })
	return sink
}

// openForInspection opens a second connection to a sink's file — legal
// under WAL — so tests can read what was written.
func openForInspection(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestNewCreatesDirectoryAndFile(t *testing.T) {
	// The directory is nested and absent — New must create the chain.
	dir := filepath.Join(t.TempDir(), "deeply", "nested", "traces")
	sink, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })

	info, err := os.Stat(sink.Path())
	if err != nil {
		t.Fatalf("the trace file must exist at Path(): %v", err)
	}
	if info.IsDir() {
		t.Fatal("Path() must be a file")
	}
	pattern := fmt.Sprintf(`^gospan-\d{8}T\d{6}\.\d{9}Z-%d\.sqlite$`, os.Getpid())
	if base := filepath.Base(sink.Path()); !regexp.MustCompile(pattern).MatchString(base) {
		t.Errorf("file name %q does not match the spec'd shape %s", base, pattern)
	}
}

func TestNoTwoRunsShareAFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "traces")
	first, err := New(dir)
	if err != nil {
		t.Fatalf("first New: %v", err)
	}
	t.Cleanup(func() { _ = first.Close() })
	second, err := New(dir)
	if err != nil {
		t.Fatalf("second New: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })

	if first.Path() == second.Path() {
		t.Fatalf("two runs share %s — meta would no longer be one row per run", first.Path())
	}
}

func TestNewFailsLoudlyOnUnusableDirectory(t *testing.T) {
	// A file where the directory should be: construction is where errors
	// surface (D23), and nothing half-born may remain.
	base := t.TempDir()
	blocker := filepath.Join(base, "not-a-directory")
	if err := os.WriteFile(blocker, []byte("in the way"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(blocker); err == nil {
		t.Fatal("New must fail when the directory cannot be created")
	}
}

func TestMetaCarriesFileIdentity(t *testing.T) {
	sink := newTestSink(t)
	db := openForInspection(t, sink.Path())

	var version int
	var fileID string
	var createdAtNanos int64
	row := db.QueryRow("SELECT schema_version, file_id, created_at_ns FROM meta")
	if err := row.Scan(&version, &fileID, &createdAtNanos); err != nil {
		t.Fatalf("reading meta: %v", err)
	}
	if version != 1 {
		t.Errorf("schema_version = %d, want 1", version)
	}
	if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(fileID) {
		t.Errorf("file_id = %q, want 32 hex characters (16 random bytes)", fileID)
	}
	if age := time.Since(time.Unix(0, createdAtNanos)); age < 0 || age > time.Minute {
		t.Errorf("created_at_ns is %v old, want just now", age)
	}

	var rows int
	if err := db.QueryRow("SELECT COUNT(*) FROM meta").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Errorf("meta has %d rows, must be exactly 1 per file", rows)
	}
}

func TestSchemaIsStrictAndWAL(t *testing.T) {
	sink := newTestSink(t)
	db := openForInspection(t, sink.Path())

	var journalMode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatal(err)
	}
	if journalMode != "wal" {
		t.Errorf("journal_mode = %q, want wal", journalMode)
	}

	// STRICT is the writer-bug tripwire: a mistyped insert must be an
	// error, never silently coerced junk in an archived trace file (D15).
	_, err := db.Exec(`INSERT INTO names (id, name) VALUES (1, 'work')`)
	if err != nil {
		t.Fatalf("well-typed insert must succeed: %v", err)
	}
	_, err = db.Exec(`INSERT INTO spans (id, trace_id, name_id, start_ns) VALUES (1, 1, 1, 'not-a-number')`)
	if err == nil {
		t.Error("STRICT table accepted a text start_ns — type bugs would become silent junk")
	}

	// The one deliberate index exists; nothing else taxes the insert path.
	var indexCount int
	err = db.QueryRow(
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'spans_by_trace'",
	).Scan(&indexCount)
	if err != nil {
		t.Fatal(err)
	}
	if indexCount != 1 {
		t.Error("the spans_by_trace index (the waterfall query's index) must exist")
	}
}

func TestOpenReadHandle_ReadsLiveAndRefusesWrites(t *testing.T) {
	sink := newTestSink(t)
	// A span written through the sink's own path, mid-run (file not closed).
	if err := sink.WriteBatch(gospan.Batch{Events: []gospan.Event{
		startEvent(1, "work"), endEvent(1, "work"),
	}}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if err := sink.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	db, err := sink.OpenReadHandle()
	if err != nil {
		t.Fatalf("OpenReadHandle: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// Reads see the writer's committed state while the sink is still open.
	var rows int
	if err := db.QueryRow("SELECT COUNT(*) FROM spans").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Errorf("read handle sees %d span rows, want 1", rows)
	}

	// Writes are refused by the connection itself (mode=ro): the sink must
	// remain the file's only writer, enforced, not trusted.
	if _, err := db.Exec(`INSERT INTO names (id, name) VALUES (99, 'intruder')`); err == nil {
		t.Fatal("a write through the read handle must fail — the handle is not read-only")
	}
}
