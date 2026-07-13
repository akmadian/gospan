package gospan

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

// errorSink always fails WriteBatch — the permanently sick destination.
type errorSink struct {
	captureSink
}

func (sink *errorSink) WriteBatch(Batch) error {
	return errors.New("disk full")
}

// panicOnceSink panics on its first WriteBatch and behaves afterwards —
// the buggy destination the writer must survive.
type panicOnceSink struct {
	captureSink
	panicked bool // touched only by the writer goroutine
}

func (sink *panicOnceSink) WriteBatch(batch Batch) error {
	if !sink.panicked {
		sink.panicked = true
		panic("sink bug")
	}
	return sink.captureSink.WriteBatch(batch)
}

// closeErrorSink fails only its Close — Tracer.Close must surface it.
type closeErrorSink struct {
	captureSink
}

var errSinkClose = errors.New("close failed")

func (sink *closeErrorSink) Close() error {
	return errSinkClose
}

// flushErrorSink accepts batches but fails every commit — the shape of a
// buffering sink whose disk went away between writes.
type flushErrorSink struct {
	captureSink
}

var errFlushFailed = errors.New("commit failed")

func (sink *flushErrorSink) Flush() error {
	return errFlushFailed
}

func TestWriterDeliversInOrder(t *testing.T) {
	tracer, capture := newCaptureTracer(t)
	const spans = 100
	for i := 0; i < spans; i++ {
		_, span := tracer.Start(context.Background(), "work")
		span.End()
	}
	mustClose(t, tracer)

	events := capture.snapshot()
	if len(events) != 2*spans {
		t.Fatalf("captured %d events, want %d", len(events), 2*spans)
	}
	// All sends came from one goroutine, so delivery must preserve the
	// exact start/end interleaving and monotonic span IDs.
	for i := 0; i < spans; i++ {
		start, end := events[2*i], events[2*i+1]
		if start.Kind != EventStart || end.Kind != EventEnd || start.SpanID != end.SpanID {
			t.Fatalf("event pair %d out of order: %+v then %+v", i, start, end)
		}
		if i > 0 && start.SpanID <= events[2*(i-1)].SpanID {
			t.Fatalf("span IDs not monotonic at pair %d", i)
		}
	}
}

func TestBatchOfOneStreams(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tracer, capture := newCaptureTracer(t)
		tracer.Start(context.Background(), "work")
		// No Close, no flush tick, no elapsed time: once the writer is
		// idle again the event must already be at the sink — under light
		// load a batch of one is a stream.
		synctest.Wait()

		events := capture.snapshot()
		if len(events) != 1 {
			t.Fatalf("captured %d events before any flush/close, want 1", len(events))
		}
		if _, flushes, _ := capture.counts(); flushes != 0 {
			t.Errorf("no flush interval elapsed, yet Flush ran %d times", flushes)
		}
	})
}

func TestFlushTicksOnInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tracer, capture := newCaptureTracer(t, WithFlushInterval(time.Second))
		// Fake time: sleeping advances the bubble clock deterministically
		// through exactly two ticker fires, events or no events.
		time.Sleep(2500 * time.Millisecond)
		synctest.Wait()

		if _, flushes, _ := capture.counts(); flushes != 2 {
			t.Errorf("flushes = %d after 2.5 intervals, want 2", flushes)
		}
		mustClose(t, tracer)
		if _, flushes, _ := capture.counts(); flushes != 3 {
			t.Errorf("Close must add the final flush, got %d total", flushes)
		}
	})
}

func TestCloseDrainsEverything(t *testing.T) {
	tracer, capture := newCaptureTracer(t, WithBufferSize(4096))
	const spans = 500
	for i := 0; i < spans; i++ {
		_, span := tracer.Start(context.Background(), "work")
		span.End()
	}
	mustClose(t, tracer)

	if events := capture.snapshot(); len(events) != 2*spans {
		t.Errorf("captured %d events, want %d — Close must drain the buffer completely", len(events), 2*spans)
	}
	if got := tracer.dropped.Load(); got != 0 {
		t.Errorf("dropped = %d, want 0", got)
	}
	_, flushes, closes := capture.counts()
	if flushes < 1 {
		t.Error("Close must flush the sink after the final drain")
	}
	if closes != 1 {
		t.Errorf("sink Close ran %d times, want exactly 1", closes)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	tracer, capture := newCaptureTracer(t)
	tracer.Start(context.Background(), "work")
	mustClose(t, tracer)
	mustClose(t, tracer)

	if _, _, closes := capture.counts(); closes != 1 {
		t.Errorf("sink Close ran %d times across two Tracer.Close calls, want 1", closes)
	}
}

func TestCloseReturnsSinkCloseError(t *testing.T) {
	tracer, err := New(&closeErrorSink{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := tracer.Close(context.Background()); !errors.Is(got, errSinkClose) {
		t.Errorf("Close = %v, want the sink's close error", got)
	}
}

func TestCloseIsContextBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sink := newGateSink()
		tracer, err := New(sink)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = tracer.Close(context.Background()) })
		t.Cleanup(sink.release)

		tracer.Start(context.Background(), "work")
		synctest.Wait() // writer is now hostage inside WriteBatch

		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		if got := tracer.Close(canceled); !errors.Is(got, context.Canceled) {
			t.Errorf("Close with expired ctx = %v, want context.Canceled", got)
		}

		// The writer finishes in the background once the sink recovers,
		// and the sink is still closed exactly once.
		sink.release()
		synctest.Wait()
		if _, _, closes := sink.counts(); closes != 1 {
			t.Errorf("sink Close ran %d times, want 1", closes)
		}
	})
}

func TestSinkErrorsCountedAndContained(t *testing.T) {
	sink := &errorSink{}
	tracer, err := New(sink)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	tracer.Start(context.Background(), "work")
	mustClose(t, tracer)

	if got := tracer.writeErrors.Load(); got < 1 {
		t.Errorf("writeErrors = %d, want at least 1", got)
	}
	if _, _, closes := sink.counts(); closes != 1 {
		t.Error("a failing WriteBatch must not kill the writer — Close must still reach the sink")
	}
}

func TestFlushErrorsCountedAndContained(t *testing.T) {
	// A failed Flush is where a buffering sink's lost commit becomes
	// visible (the Stats.Written docs hang on this), so it must count as
	// a WriteError without killing the writer.
	sink := &flushErrorSink{}
	tracer, err := New(sink)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	tracer.Start(context.Background(), "work")
	mustClose(t, tracer)

	if got := tracer.Stats().WriteErrors; got < 1 {
		t.Errorf("WriteErrors = %d, want at least 1 for the failed flush", got)
	}
	if events := sink.snapshot(); len(events) != 1 {
		t.Errorf("batch delivery must be unaffected by flush failures, captured %d events", len(events))
	}
	if _, _, closes := sink.counts(); closes != 1 {
		t.Error("a failing Flush must not stop Close from reaching the sink")
	}
}

func TestSinkPanicContained(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sink := &panicOnceSink{}
		tracer, err := New(sink)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		tracer.Start(context.Background(), "lost-to-the-panic")
		synctest.Wait() // first delivery panics; the writer must survive

		tracer.Start(context.Background(), "delivered")
		synctest.Wait()
		mustClose(t, tracer)

		if got := tracer.writeErrors.Load(); got != 1 {
			t.Errorf("writeErrors = %d, want 1 for the recovered panic", got)
		}
		events := sink.snapshot()
		if len(events) != 1 || events[0].Name != "delivered" {
			t.Errorf("writer must keep delivering after a sink panic, captured %v", events)
		}
		if _, _, closes := sink.counts(); closes != 1 {
			t.Errorf("sink Close ran %d times, want 1", closes)
		}
	})
}
