package gospan

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// recordingHandler captures slog records so tests can assert on what
// SlogSink emitted. Locked because the writer goroutine logs while the
// test goroutine reads.
type recordingHandler struct {
	mutex   sync.Mutex
	records []recordedLog
}

type recordedLog struct {
	level   slog.Level
	message string
	attrs   map[string]slog.Value
}

func (handler *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

//nolint:gocritic // hugeParam: slog.Handler's signature, not ours to change
func (handler *recordingHandler) Handle(_ context.Context, record slog.Record) error {
	entry := recordedLog{
		level:   record.Level,
		message: record.Message,
		attrs:   make(map[string]slog.Value, record.NumAttrs()),
	}
	record.Attrs(func(attr slog.Attr) bool {
		entry.attrs[attr.Key] = attr.Value
		return true
	})
	handler.mutex.Lock()
	defer handler.mutex.Unlock()
	handler.records = append(handler.records, entry)
	return nil
}

func (handler *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return handler }
func (handler *recordingHandler) WithGroup(string) slog.Handler      { return handler }

func (handler *recordingHandler) snapshot() []recordedLog {
	handler.mutex.Lock()
	defer handler.mutex.Unlock()
	return append([]recordedLog(nil), handler.records...)
}

func TestSlogSinkEmitsOneRecordPerEnd(t *testing.T) {
	handler := &recordingHandler{}
	tracer, err := New(SlogSink(slog.New(handler)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, span := tracer.Start(context.Background(), "process-asset",
		slog.String("path", "/a.raw"), slog.Int("attempt", 1))
	span.SetAttrs(slog.Int("attempt", 2), slog.Int("bytes", 512)) // attempt overrides
	span.End()
	mustClose(t, tracer)

	records := handler.snapshot()
	if len(records) != 1 {
		t.Fatalf("emitted %d records, want exactly 1 (per span end)", len(records))
	}
	record := records[0]
	if record.message != "process-asset" {
		t.Errorf("message = %q, want the span name", record.message)
	}
	if record.level != slog.LevelInfo {
		t.Errorf("level = %v, want Info for an ok span", record.level)
	}
	for _, key := range []string{"span_id", "trace_id", "duration", "status"} {
		if _, present := record.attrs[key]; !present {
			t.Errorf("record is missing the %q attr", key)
		}
	}
	if got := record.attrs["status"].String(); got != "ok" {
		t.Errorf("status attr = %q, want ok", got)
	}
	if got := record.attrs["attempt"].Int64(); got != 2 {
		t.Errorf("attempt = %d, want 2 — SetAttrs must override Start's value (last write wins)", got)
	}
	if got := record.attrs["bytes"].Int64(); got != 512 {
		t.Errorf("bytes = %d, want 512 — mid-flight attrs must merge in", got)
	}
	if got := record.attrs["path"].String(); got != "/a.raw" {
		t.Errorf("path = %q, want Start's attr preserved", got)
	}
}

func TestSlogSinkLevelsTrackStatus(t *testing.T) {
	tests := []struct {
		name      string
		fail      error
		wantLevel slog.Level
	}{
		{"ok", nil, slog.LevelInfo},
		{"canceled", context.Canceled, slog.LevelWarn},
		{"failed", errors.New("boom"), slog.LevelError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			handler := &recordingHandler{}
			tracer, err := New(SlogSink(slog.New(handler)))
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			_, span := tracer.Start(context.Background(), "work")
			span.Fail(tc.fail)
			span.End()
			mustClose(t, tracer)

			records := handler.snapshot()
			if len(records) != 1 {
				t.Fatalf("emitted %d records, want 1", len(records))
			}
			if records[0].level != tc.wantLevel {
				t.Errorf("level = %v, want %v", records[0].level, tc.wantLevel)
			}
			if tc.fail != nil {
				if _, present := records[0].attrs["error"]; !present {
					t.Error("failed span's record must carry the error attr")
				}
			}
		})
	}
}

func TestSlogSinkOrphanEndStillEmits(t *testing.T) {
	// Unit-level: an end whose start was dropped under buffer pressure
	// must still produce a complete record — end events repeat Name and
	// StartNS exactly for this.
	handler := &recordingHandler{}
	sink := SlogSink(slog.New(handler))
	err := sink.WriteBatch(Batch{Events: []Event{{
		Kind:    EventEnd,
		SpanID:  7,
		TraceID: 7,
		Name:    "orphan",
		StartNS: 1000,
		EndNS:   3500,
		Status:  SpanStatusOK,
	}}})
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	records := handler.snapshot()
	if len(records) != 1 || records[0].message != "orphan" {
		t.Fatalf("orphan end must emit its record, got %v", records)
	}
	if got := records[0].attrs["duration"].Duration(); got != 2500*time.Nanosecond {
		t.Errorf("duration = %v, want 2500ns from the end event's own fields", got)
	}
}

func TestSlogSinkDiscardsOpenSpansOnClose(t *testing.T) {
	handler := &recordingHandler{}
	tracer, err := New(SlogSink(slog.New(handler)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	tracer.Start(context.Background(), "never-ends")
	mustClose(t, tracer)

	if records := handler.snapshot(); len(records) != 0 {
		t.Errorf("a span with no end must emit nothing, got %v", records)
	}
}

func TestMultiSinkFansOutInOrder(t *testing.T) {
	first, second := &captureSink{}, &captureSink{}
	tracer, err := New(MultiSink(first, second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, span := tracer.Start(context.Background(), "work")
	span.End()
	mustClose(t, tracer)

	for name, sink := range map[string]*captureSink{"first": first, "second": second} {
		events := sink.snapshot()
		if len(events) != 2 {
			t.Errorf("%s sink captured %d events, want 2", name, len(events))
		}
		_, flushes, closes := sink.counts()
		if flushes < 1 || closes != 1 {
			t.Errorf("%s sink: flushes = %d closes = %d; fan-out must reach every member", name, flushes, closes)
		}
	}
}

func TestMultiSinkOneFailureNeverStarvesTheRest(t *testing.T) {
	failing, healthy := &errorSink{}, &captureSink{}
	sink := MultiSink(failing, healthy)

	err := sink.WriteBatch(Batch{Events: []Event{{Kind: EventStart, SpanID: 1, TraceID: 1, Name: "work"}}})
	if err == nil {
		t.Error("the failing member's error must surface (joined)")
	}
	if events := healthy.snapshot(); len(events) != 1 {
		t.Errorf("healthy sink captured %d events, want 1 — delivery continues past a failure", len(events))
	}
}

func TestMultiSinkJoinsCloseErrors(t *testing.T) {
	failing, healthy := &closeErrorSink{}, &captureSink{}
	sink := MultiSink(failing, healthy)

	err := sink.Close()
	if !errors.Is(err, errSinkClose) {
		t.Errorf("Close = %v, want the failing member's error joined in", err)
	}
	if _, _, closes := healthy.counts(); closes != 1 {
		t.Error("one member's Close failure must not starve the rest")
	}
}

func TestMultiSinkJoinsFlushErrors(t *testing.T) {
	failing, healthy := &flushErrorSink{}, &captureSink{}
	sink := MultiSink(failing, healthy)

	if err := sink.Flush(); !errors.Is(err, errFlushFailed) {
		t.Errorf("Flush = %v, want the failing member's error joined in", err)
	}
	if _, flushes, _ := healthy.counts(); flushes != 1 {
		t.Error("one member's Flush failure must not starve the rest")
	}
}

func TestSlogSinkNilLoggerFallsBackToDefault(t *testing.T) {
	// The documented contract: SlogSink(nil) uses slog.Default.
	handler := &recordingHandler{}
	previous := slog.Default()
	slog.SetDefault(slog.New(handler))
	t.Cleanup(func() { slog.SetDefault(previous) })

	sink := SlogSink(nil)
	err := sink.WriteBatch(Batch{Events: []Event{{
		Kind: EventEnd, SpanID: 1, TraceID: 1, Name: "work", StartNS: 1, EndNS: 2,
	}}})
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if records := handler.snapshot(); len(records) != 1 {
		t.Errorf("SlogSink(nil) must emit via slog.Default, got %d records", len(records))
	}
}

func TestMultiSinkToleratesNilAndEmpty(t *testing.T) {
	empty := MultiSink()
	if err := empty.WriteBatch(Batch{}); err != nil {
		t.Errorf("empty MultiSink must no-op, got %v", err)
	}

	healthy := &captureSink{}
	sink := MultiSink(nil, healthy, nil)
	if err := sink.WriteBatch(Batch{Events: []Event{{Kind: EventStart, SpanID: 1}}}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if events := healthy.snapshot(); len(events) != 1 {
		t.Error("nil members must be dropped, real members must receive")
	}
}
