package gospan

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
)

// SpanStatus classifies how a span ended — it never says whether it ended.
// That is carried by the end timestamp alone (in trace files, end_ns IS
// NULL means running or incomplete), so a successful finish is SpanStatusOK
// plus an end time, never a distinct status. The numeric values are
// frozen: they are written into trace files (spans.status) and never
// change meaning; future statuses append (SPEC §5).
type SpanStatus int

const (
	SpanStatusOK       SpanStatus = 0 // ended without a recorded failure
	SpanStatusError    SpanStatus = 1 // Fail recorded a non-cancellation error
	SpanStatusCanceled SpanStatus = 2 // Fail recorded context.Canceled or context.DeadlineExceeded
)

// String returns "ok", "error", or "canceled"; unknown values format as
// "status(N)".
func (status SpanStatus) String() string {
	switch status {
	case SpanStatusOK:
		return "ok"
	case SpanStatusError:
		return "error"
	case SpanStatusCanceled:
		return "canceled"
	default:
		return fmt.Sprintf("status(%d)", int(status))
	}
}

// Span is a named unit of work with a start, an end, a parent, and typed
// attributes. Spans are created by Tracer.Start; a nil *Span is valid and
// every method on it is a no-op. End, Fail, and SetAttrs are safe to call
// from any goroutine, not just Start's.
type Span struct {
	tracer  *Tracer
	id      int64
	traceID int64
	parent  int64
	name    string
	startNS int64

	mutex      sync.Mutex
	ended      bool
	status     SpanStatus
	errMessage string
}

// SetAttrs records facts learned mid-flight. Last write per key wins.
// After End it is a no-op.
func (span *Span) SetAttrs(attrs ...slog.Attr) {
	if span == nil || len(attrs) == 0 {
		return
	}
	defer span.tracer.guard()
	span.mutex.Lock()
	defer span.mutex.Unlock()
	if span.ended {
		return
	}
	span.tracer.send(Event{
		Kind:    EventAttrs,
		SpanID:  span.id,
		TraceID: span.traceID,
		Attrs:   attrs,
	})
}

// Fail records why the span didn't succeed: SpanStatusCanceled when err is
// context.Canceled or context.DeadlineExceeded (via errors.Is),
// SpanStatusError otherwise. Fail(nil) is a no-op; the last Fail before End wins; after End
// it is a no-op. Fail does not end the span — End still must be called.
func (span *Span) Fail(err error) {
	if span == nil || err == nil {
		return
	}
	defer span.tracer.guard()
	status := SpanStatusError
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		status = SpanStatusCanceled
	}
	message := err.Error()
	span.mutex.Lock()
	defer span.mutex.Unlock()
	if span.ended {
		return
	}
	span.status = status
	span.errMessage = message
}

// End finishes the span. The first End wins: every later mutation
// (SetAttrs, Fail, a second End) is a no-op. End never blocks beyond a
// channel send, and is safe in defer even through a panic — the span gets
// an end time.
func (span *Span) End() {
	if span == nil {
		return
	}
	defer span.tracer.guard()
	span.mutex.Lock()
	defer span.mutex.Unlock()
	if span.ended {
		return
	}
	span.ended = true
	span.tracer.send(Event{
		Kind:     EventEnd,
		SpanID:   span.id,
		TraceID:  span.traceID,
		ParentID: span.parent,
		Name:     span.name,
		StartNS:  span.startNS,
		EndNS:    span.tracer.now(),
		Status:   span.status,
		Error:    span.errMessage,
	})
}
