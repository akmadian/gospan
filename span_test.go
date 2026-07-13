package gospan

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
)

// onlyEnd closes the tracer and returns the single end event it delivered.
func onlyEnd(t *testing.T, tracer *Tracer, capture *captureSink) Event {
	t.Helper()
	mustClose(t, tracer)
	ends := eventsOfKind(capture.snapshot(), EventEnd)
	if len(ends) != 1 {
		t.Fatalf("captured %d end events, want exactly 1", len(ends))
	}
	return ends[0]
}

func TestEndEmitsCompleteEvent(t *testing.T) {
	tracer, capture := newCaptureTracer(t)
	_, span := tracer.Start(context.Background(), "work")
	span.End()
	mustClose(t, tracer)

	events := capture.snapshot()
	if len(events) != 2 {
		t.Fatalf("captured %d events, want start+end", len(events))
	}
	start, end := events[0], events[1]
	if end.Kind != EventEnd {
		t.Fatalf("Kind = %v, want EventEnd", end.Kind)
	}
	if end.SpanID != start.SpanID || end.TraceID != start.TraceID {
		t.Error("end event must identify the same span and trace as its start")
	}
	if end.Name != "work" || end.StartNS != start.StartNS {
		t.Error("end event must repeat Name and StartNS from the start event")
	}
	if end.EndNS < end.StartNS {
		t.Errorf("EndNS %d precedes StartNS %d", end.EndNS, end.StartNS)
	}
	if end.Status != SpanStatusOK || end.Error != "" {
		t.Errorf("unfailed span: Status = %v, Error = %q; want ok and empty", end.Status, end.Error)
	}
}

func TestFailClassification(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus SpanStatus
		wantError  string
	}{
		{"plain error", errors.New("boom"), SpanStatusError, "boom"},
		{"canceled", context.Canceled, SpanStatusCanceled, "context canceled"},
		{"wrapped canceled", fmt.Errorf("stage: %w", context.Canceled), SpanStatusCanceled, "stage: context canceled"},
		{"deadline exceeded", fmt.Errorf("io: %w", context.DeadlineExceeded), SpanStatusCanceled, "io: context deadline exceeded"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tracer, capture := newCaptureTracer(t)
			_, span := tracer.Start(context.Background(), "work")
			span.Fail(tc.err)
			span.End()

			end := onlyEnd(t, tracer, capture)
			if end.Status != tc.wantStatus {
				t.Errorf("Status = %v, want %v", end.Status, tc.wantStatus)
			}
			if end.Error != tc.wantError {
				t.Errorf("Error = %q, want %q", end.Error, tc.wantError)
			}
		})
	}
}

func TestFailNilIsNoOp(t *testing.T) {
	tracer, capture := newCaptureTracer(t)
	_, span := tracer.Start(context.Background(), "work")
	span.Fail(nil)
	span.End()
	if end := onlyEnd(t, tracer, capture); end.Status != SpanStatusOK {
		t.Errorf("Fail(nil) must not change status, got %v", end.Status)
	}
}

func TestLastFailWins(t *testing.T) {
	tracer, capture := newCaptureTracer(t)
	_, span := tracer.Start(context.Background(), "work")
	span.Fail(errors.New("first"))
	span.Fail(errors.New("second"))
	span.End()
	if end := onlyEnd(t, tracer, capture); end.Error != "second" {
		t.Errorf("Error = %q, want the last Fail's message", end.Error)
	}
}

func TestFirstEndWins(t *testing.T) {
	tracer, capture := newCaptureTracer(t)
	_, span := tracer.Start(context.Background(), "work")
	span.End()
	span.End()
	span.Fail(errors.New("too late"))
	span.SetAttrs(slog.String("k", "v"))
	mustClose(t, tracer)

	events := capture.snapshot()
	ends := eventsOfKind(events, EventEnd)
	if len(ends) != 1 {
		t.Fatalf("captured %d end events, want exactly 1 (first End wins)", len(ends))
	}
	if ends[0].Status != SpanStatusOK || ends[0].Error != "" {
		t.Error("mutations after End must not alter the emitted event")
	}
	if attrs := eventsOfKind(events, EventAttrs); len(attrs) != 0 {
		t.Errorf("SetAttrs after End must emit nothing, got %v", attrs)
	}
}

func TestSetAttrsEmitsDelta(t *testing.T) {
	tracer, capture := newCaptureTracer(t)
	_, span := tracer.Start(context.Background(), "work")
	span.SetAttrs(slog.Int("rows", 42), slog.String("stage", "hash"))
	span.SetAttrs() // empty call emits nothing
	mustClose(t, tracer)

	attrEvents := eventsOfKind(capture.snapshot(), EventAttrs)
	if len(attrEvents) != 1 {
		t.Fatalf("captured %d attrs events, want exactly 1", len(attrEvents))
	}
	if attrEvents[0].SpanID != span.id {
		t.Error("attrs event must identify its span")
	}
	if len(attrEvents[0].Attrs) != 2 {
		t.Errorf("Attrs length = %d, want 2", len(attrEvents[0].Attrs))
	}
}

func TestCrossGoroutineMutationsAreSafe(t *testing.T) {
	tracer, capture := newCaptureTracer(t, WithBufferSize(1024))
	_, span := tracer.Start(context.Background(), "work")

	var waitGroup sync.WaitGroup
	for i := 0; i < 16; i++ {
		waitGroup.Add(3)
		go func() { defer waitGroup.Done(); span.Fail(errors.New("racing")) }()
		go func() { defer waitGroup.Done(); span.SetAttrs(slog.Int("i", 1)) }()
		go func() { defer waitGroup.Done(); span.End() }()
	}
	waitGroup.Wait()
	mustClose(t, tracer)

	if ends := eventsOfKind(capture.snapshot(), EventEnd); len(ends) != 1 {
		t.Errorf("got %d end events, want exactly 1 (first End wins)", len(ends))
	}
}
