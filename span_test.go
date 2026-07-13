package gospan

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
)

// startDrained starts a span and discards its start event, leaving the
// buffer empty for the assertion under test.
func startDrained(t *testing.T, tracer *Tracer, name string) *Span {
	t.Helper()
	_, span := tracer.Start(context.Background(), name)
	nextEvent(t, tracer)
	return span
}

func TestEndEmitsCompleteEvent(t *testing.T) {
	tracer := newTestTracer(t)
	_, span := tracer.Start(context.Background(), "work")
	start := nextEvent(t, tracer)

	span.End()
	end := nextEvent(t, tracer)
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
			tracer := newTestTracer(t)
			span := startDrained(t, tracer, "work")
			span.Fail(tc.err)
			span.End()

			end := nextEvent(t, tracer)
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
	tracer := newTestTracer(t)
	span := startDrained(t, tracer, "work")
	span.Fail(nil)
	span.End()
	if end := nextEvent(t, tracer); end.Status != SpanStatusOK {
		t.Errorf("Fail(nil) must not change status, got %v", end.Status)
	}
}

func TestLastFailWins(t *testing.T) {
	tracer := newTestTracer(t)
	span := startDrained(t, tracer, "work")
	span.Fail(errors.New("first"))
	span.Fail(errors.New("second"))
	span.End()
	if end := nextEvent(t, tracer); end.Error != "second" {
		t.Errorf("Error = %q, want the last Fail's message", end.Error)
	}
}

func TestFirstEndWins(t *testing.T) {
	tracer := newTestTracer(t)
	span := startDrained(t, tracer, "work")
	span.End()
	span.End()
	span.Fail(errors.New("too late"))
	span.SetAttrs(slog.String("k", "v"))

	end := nextEvent(t, tracer)
	if end.Status != SpanStatusOK || end.Error != "" {
		t.Error("mutations after End must not alter the emitted event")
	}
	select {
	case e := <-tracer.events:
		t.Errorf("mutations after End must emit nothing, got %+v", e)
	default:
	}
}

func TestSetAttrsEmitsDelta(t *testing.T) {
	tracer := newTestTracer(t)
	span := startDrained(t, tracer, "work")
	span.SetAttrs(slog.Int("rows", 42), slog.String("stage", "hash"))

	event := nextEvent(t, tracer)
	if event.Kind != EventAttrs {
		t.Fatalf("Kind = %v, want EventAttrs", event.Kind)
	}
	if event.SpanID != span.id {
		t.Error("attrs event must identify its span")
	}
	if len(event.Attrs) != 2 {
		t.Errorf("Attrs length = %d, want 2", len(event.Attrs))
	}

	span.SetAttrs() // empty call emits nothing
	select {
	case e := <-tracer.events:
		t.Errorf("SetAttrs() with no attrs must emit nothing, got %+v", e)
	default:
	}
}

func TestCrossGoroutineMutationsAreSafe(t *testing.T) {
	tracer := newTestTracer(t, WithBufferSize(1024))
	span := startDrained(t, tracer, "work")

	var waitGroup sync.WaitGroup
	for i := 0; i < 16; i++ {
		waitGroup.Add(3)
		go func() { defer waitGroup.Done(); span.Fail(errors.New("racing")) }()
		go func() { defer waitGroup.Done(); span.SetAttrs(slog.Int("i", 1)) }()
		go func() { defer waitGroup.Done(); span.End() }()
	}
	waitGroup.Wait()

	ends := 0
	for {
		select {
		case e := <-tracer.events:
			if e.Kind == EventEnd {
				ends++
			}
		default:
			if ends != 1 {
				t.Errorf("got %d end events, want exactly 1 (first End wins)", ends)
			}
			return
		}
	}
}
