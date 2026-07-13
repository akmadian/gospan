package sqlite

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/akmadian/gospan"
)

func startEvent(spanID int64, name string, attrs ...slog.Attr) gospan.Event {
	return gospan.Event{
		Kind: gospan.EventStart, SpanID: spanID, TraceID: 1, Name: name,
		StartNS: spanID * 1000, Attrs: attrs,
	}
}

func endEvent(spanID int64, name string) gospan.Event {
	return gospan.Event{
		Kind: gospan.EventEnd, SpanID: spanID, TraceID: 1, Name: name,
		StartNS: spanID * 1000, EndNS: spanID*1000 + 500,
	}
}

func writeAndFlush(t *testing.T, sink *Sink, events ...gospan.Event) {
	t.Helper()
	if err := sink.WriteBatch(gospan.Batch{Events: events}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if err := sink.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

type spanRecord struct {
	endNS   sql.NullInt64
	status  int
	errText sql.NullString
	name    string
	parent  sql.NullInt64
}

func readSpan(t *testing.T, db *sql.DB, spanID int64) spanRecord {
	t.Helper()
	var record spanRecord
	err := db.QueryRow(`
		SELECT s.end_ns, s.status, s.error, n.name, s.parent_id
		FROM spans s JOIN names n ON n.id = s.name_id
		WHERE s.id = ?`, spanID).
		Scan(&record.endNS, &record.status, &record.errText, &record.name, &record.parent)
	if err != nil {
		t.Fatalf("reading span %d: %v", spanID, err)
	}
	return record
}

func TestCoalescedStartEndIsOneCompleteRow(t *testing.T) {
	sink := newTestSink(t)
	writeAndFlush(t, sink, startEvent(1, "work"), endEvent(1, "work"))

	record := readSpan(t, openForInspection(t, sink.Path()), 1)
	if !record.endNS.Valid || record.endNS.Int64 != 1500 {
		t.Errorf("end_ns = %+v, want 1500 — start+end in one flush must coalesce complete", record.endNS)
	}
	if record.name != "work" {
		t.Errorf("name = %q, want work", record.name)
	}
}

func TestSpanOutlivingAFlushIsUpdatedInPlace(t *testing.T) {
	sink := newTestSink(t)
	db := openForInspection(t, sink.Path())

	writeAndFlush(t, sink, startEvent(1, "long-running"))
	if record := readSpan(t, db, 1); record.endNS.Valid {
		t.Fatal("a running span must have NULL end_ns — that IS the live/incomplete signal")
	}

	writeAndFlush(t, sink, endEvent(1, "long-running"))
	if record := readSpan(t, db, 1); !record.endNS.Valid {
		t.Fatal("the end arriving in a later flush must complete the existing row")
	}

	var rows int
	if err := db.QueryRow("SELECT COUNT(*) FROM spans").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Errorf("spans has %d rows, want 1 — the update must hit the same row", rows)
	}
}

func TestOrphanedEndBecomesACompleteRow(t *testing.T) {
	// The start was dropped under buffer pressure; the end event carries
	// Name and StartNS so the file still gets a complete span (SPEC §2).
	sink := newTestSink(t)
	writeAndFlush(t, sink, endEvent(7, "orphan"))

	record := readSpan(t, openForInspection(t, sink.Path()), 7)
	if !record.endNS.Valid || record.name != "orphan" {
		t.Errorf("orphaned end must produce a complete row, got %+v", record)
	}
}

func TestIncompleteSpanSurvivesClose(t *testing.T) {
	// The crash story: a span that never ended is exactly a NULL end_ns
	// in the closed file — present, visible, never diagnosed further.
	dir := t.TempDir()
	sink, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	writeAndFlush(t, sink, startEvent(1, "nine-minutes-into-ffmpeg"))
	if err := sink.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db, err := sql.Open("sqlite", sink.Path())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var endNS sql.NullInt64
	if err := db.QueryRow("SELECT end_ns FROM spans WHERE id = 1").Scan(&endNS); err != nil {
		t.Fatalf("the open span must be in the closed file: %v", err)
	}
	if endNS.Valid {
		t.Error("a span that never ended must keep NULL end_ns")
	}
}

func TestNamesAreInterned(t *testing.T) {
	sink := newTestSink(t)
	writeAndFlush(t, sink,
		startEvent(1, "extract"), endEvent(1, "extract"),
		startEvent(2, "extract"), endEvent(2, "extract"),
		startEvent(3, "resize"), endEvent(3, "resize"),
	)

	var names int
	db := openForInspection(t, sink.Path())
	if err := db.QueryRow("SELECT COUNT(*) FROM names").Scan(&names); err != nil {
		t.Fatal(err)
	}
	if names != 2 {
		t.Errorf("names has %d rows, want 2 — repeated names must intern", names)
	}
}

func TestAttrLastWinsAcrossFlushes(t *testing.T) {
	sink := newTestSink(t)
	writeAndFlush(t, sink, startEvent(1, "work", slog.Int("attempt", 1)))
	writeAndFlush(t, sink, gospan.Event{
		Kind: gospan.EventAttrs, SpanID: 1, TraceID: 1,
		Attrs: []slog.Attr{slog.Int("attempt", 2)},
	})

	db := openForInspection(t, sink.Path())
	var attempt int64
	if err := db.QueryRow("SELECT value FROM attrs WHERE span_id = 1 AND key = 'attempt'").Scan(&attempt); err != nil {
		t.Fatal(err)
	}
	if attempt != 2 {
		t.Errorf("attempt = %d, want 2 — the primary key makes last-wins a schema property", attempt)
	}
	var rows int
	if err := db.QueryRow("SELECT COUNT(*) FROM attrs WHERE span_id = 1").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Errorf("attrs has %d rows for the span, want 1", rows)
	}
}

func TestAttrKindsRoundTrip(t *testing.T) {
	sink := newTestSink(t)
	timestamp := time.Date(2026, 7, 13, 10, 0, 0, 0, time.UTC)
	const bigUnsigned = uint64(1) << 63 // above MaxInt64: the bit-reinterpretation case
	writeAndFlush(t, sink, startEvent(1, "work",
		slog.String("path", "/a.raw"),
		slog.Int64("rows", -42),
		slog.Uint64("huge", bigUnsigned),
		slog.Float64("ratio", 0.5),
		slog.Bool("ok", true),
		slog.Duration("wait", 1500*time.Millisecond),
		slog.Time("seen", timestamp),
		slog.Any("blob", struct{ X int }{7}),
	), endEvent(1, "work"))

	db := openForInspection(t, sink.Path())
	readCell := func(key string) (kind int, value any) {
		t.Helper()
		if err := db.QueryRow("SELECT kind, value FROM attrs WHERE span_id = 1 AND key = ?", key).Scan(&kind, &value); err != nil {
			t.Fatalf("reading attr %q: %v", key, err)
		}
		return kind, value
	}

	if kind, value := readCell("path"); kind != int(slog.KindString) || value != "/a.raw" {
		t.Errorf("path = kind %d value %v", kind, value)
	}
	if kind, value := readCell("rows"); kind != int(slog.KindInt64) || value != int64(-42) {
		t.Errorf("rows = kind %d value %v", kind, value)
	}
	if kind, value := readCell("huge"); kind != int(slog.KindUint64) || uint64(value.(int64)) != bigUnsigned { //nolint:gosec // G115: verifying the deliberate bit round-trip
		t.Errorf("huge = kind %d value %v — bits must round-trip through int64", kind, value)
	}
	if kind, value := readCell("ratio"); kind != int(slog.KindFloat64) || value != 0.5 {
		t.Errorf("ratio = kind %d value %v", kind, value)
	}
	if kind, value := readCell("ok"); kind != int(slog.KindBool) || value != int64(1) {
		t.Errorf("ok = kind %d value %v", kind, value)
	}
	if kind, value := readCell("wait"); kind != int(slog.KindDuration) || value != int64(1500*time.Millisecond) {
		t.Errorf("wait = kind %d value %v — durations store as integer nanoseconds", kind, value)
	}
	if kind, value := readCell("seen"); kind != int(slog.KindTime) || value != timestamp.UnixNano() {
		t.Errorf("seen = kind %d value %v", kind, value)
	}
	if kind, value := readCell("blob"); kind != int(slog.KindAny) || value != "{7}" {
		t.Errorf("blob = kind %d value %v — Any formats to text", kind, value)
	}

	// Typed cells mean typed SQL: the whole point of D3/D5.
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM attrs WHERE key = 'rows' AND value < 0").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Error("integer attrs must be queryable as integers, not strings")
	}
}

func TestGroupAttrsFlattenToDottedKeys(t *testing.T) {
	sink := newTestSink(t)
	writeAndFlush(t, sink, startEvent(1, "work",
		slog.Group("db",
			slog.String("query", "SELECT 1"),
			slog.Group("pool", slog.Int("size", 4)),
		),
	), endEvent(1, "work"))

	db := openForInspection(t, sink.Path())
	for key, want := range map[string]any{
		"db.query":     "SELECT 1",
		"db.pool.size": int64(4),
	} {
		var value any
		if err := db.QueryRow("SELECT value FROM attrs WHERE span_id = 1 AND key = ?", key).Scan(&value); err != nil {
			t.Fatalf("flattened key %q missing: %v", key, err)
		}
		if value != want {
			t.Errorf("%q = %v, want %v", key, value, want)
		}
	}
}

func TestFlushFailureDropsPendingAndReports(t *testing.T) {
	sink := newTestSink(t)
	if err := sink.WriteBatch(gospan.Batch{Events: []gospan.Event{startEvent(1, "work")}}); err != nil {
		t.Fatal(err)
	}
	_ = sink.db.Close() // the disk goes away

	if err := sink.Flush(); err == nil {
		t.Fatal("Flush against a dead database must report — it becomes Stats.WriteErrors")
	}
	if len(sink.pendingSpans) != 0 || len(sink.pendingByID) != 0 {
		t.Error("failed flushes must drop pending, never hoard it against a sick disk")
	}
	if err := sink.Flush(); err != nil {
		t.Errorf("an empty flush after the failure must be a clean no-op, got %v", err)
	}
}

func TestEndToEndThroughTheTracer(t *testing.T) {
	sink, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	tracer, err := gospan.New(sink)
	if err != nil {
		t.Fatalf("gospan.New: %v", err)
	}

	rootCtx, root := tracer.Start(context.Background(), "process-asset", slog.String("path", "/a.raw"))
	_, child := tracer.Start(rootCtx, "hash")
	child.End()
	tracer.Track(rootCtx, "ffmpeg-extract")()
	root.End()
	if err := tracer.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db, err := sql.Open("sqlite", sink.Path())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// The README's waterfall shape: one trace, a root with NULL parent,
	// two children beneath it, everything complete.
	var spans, roots, children, incomplete int
	for query, target := range map[string]*int{
		"SELECT COUNT(*) FROM spans":                             &spans,
		"SELECT COUNT(*) FROM spans WHERE parent_id IS NULL":     &roots,
		"SELECT COUNT(*) FROM spans WHERE parent_id IS NOT NULL": &children,
		"SELECT COUNT(*) FROM spans WHERE end_ns IS NULL":        &incomplete,
	} {
		if err := db.QueryRow(query).Scan(target); err != nil {
			t.Fatal(err)
		}
	}
	if spans != 3 || roots != 1 || children != 2 || incomplete != 0 {
		t.Errorf("spans/roots/children/incomplete = %d/%d/%d/%d, want 3/1/2/0", spans, roots, children, incomplete)
	}
}
