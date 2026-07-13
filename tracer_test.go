package gospan

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// captureSink records everything the writer delivers. Its own mutex makes
// it readable from the test goroutine while the writer runs; per the Sink
// contract it copies what it keeps and never retains the Batch.
type captureSink struct {
	mutex   sync.Mutex
	events  []Event
	batches int
	flushes int
	closes  int
}

func (sink *captureSink) WriteBatch(batch Batch) error {
	sink.mutex.Lock()
	defer sink.mutex.Unlock()
	sink.batches++
	for _, event := range batch.Events {
		copied := event
		copied.Attrs = append([]slog.Attr(nil), event.Attrs...)
		sink.events = append(sink.events, copied)
	}
	return nil
}

func (sink *captureSink) Flush() error {
	sink.mutex.Lock()
	defer sink.mutex.Unlock()
	sink.flushes++
	return nil
}

func (sink *captureSink) Close() error {
	sink.mutex.Lock()
	defer sink.mutex.Unlock()
	sink.closes++
	return nil
}

func (sink *captureSink) snapshot() []Event {
	sink.mutex.Lock()
	defer sink.mutex.Unlock()
	return append([]Event(nil), sink.events...)
}

func (sink *captureSink) counts() (batches, flushes, closes int) {
	sink.mutex.Lock()
	defer sink.mutex.Unlock()
	return sink.batches, sink.flushes, sink.closes
}

// gateSink stalls the writer: WriteBatch blocks until release. Tests use
// it to fill the buffer deterministically (drop/blocking-policy tests) or
// to hold the writer hostage (ctx-bounded Close).
type gateSink struct {
	captureSink
	gate        chan struct{}
	releaseOnce sync.Once
}

func newGateSink() *gateSink {
	return &gateSink{gate: make(chan struct{})}
}

func (sink *gateSink) release() {
	sink.releaseOnce.Do(func() { close(sink.gate) })
}

func (sink *gateSink) WriteBatch(batch Batch) error {
	<-sink.gate
	return sink.captureSink.WriteBatch(batch)
}

// newCaptureTracer builds a tracer over a capture sink and guarantees the
// writer is shut down by test end (Close is idempotent, so tests that
// Close explicitly are unaffected).
func newCaptureTracer(t *testing.T, opts ...Option) (*Tracer, *captureSink) {
	t.Helper()
	capture := &captureSink{}
	tracer, err := New(capture, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = tracer.Close(context.Background()) })
	return tracer, capture
}

func mustClose(t *testing.T, tracer *Tracer) {
	t.Helper()
	if err := tracer.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func eventsOfKind(events []Event, kind EventKind) []Event {
	var matched []Event
	for _, event := range events {
		if event.Kind == kind {
			matched = append(matched, event)
		}
	}
	return matched
}

func TestNewValidation(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Error("New(nil) should error")
	}
	if _, err := New(&captureSink{}, WithBufferSize(0)); err == nil {
		t.Error("WithBufferSize(0) should error")
	}
	if _, err := New(&captureSink{}, WithFlushInterval(-time.Second)); err == nil {
		t.Error("WithFlushInterval(-1s) should error")
	}
	tracer, err := New(&captureSink{}, WithBufferSize(16), WithFlushInterval(time.Millisecond))
	if err != nil {
		t.Errorf("valid options should not error: %v", err)
	}
	mustClose(t, tracer)
}

func TestNilTracerIsOff(t *testing.T) {
	var tracer *Tracer
	ctx := context.Background()

	returned, span := tracer.Start(ctx, "work", slog.String("k", "v"))
	if returned != ctx {
		t.Error("nil-tracer Start must return the input context unchanged")
	}
	if span != nil {
		t.Error("nil-tracer Start must return a nil span")
	}
	// Every method on the nil span is a no-op, not a panic.
	span.SetAttrs(slog.Int("n", 1))
	span.Fail(context.Canceled)
	span.End()

	if closer := tracer.Track(ctx, "leaf"); closer == nil {
		t.Error("nil-tracer Track must still return a callable closer")
	} else {
		closer()
	}
	if err := tracer.Close(ctx); err != nil {
		t.Errorf("nil-tracer Close must be a no-op, got %v", err)
	}
}

func TestFromContext(t *testing.T) {
	if FromContext(context.Background()) != nil {
		t.Error("FromContext without a span must return nil")
	}
	if FromContext(nil) != nil { //nolint:staticcheck // nil-safety is the contract under test
		t.Error("FromContext(nil) must return nil")
	}

	tracer, _ := newCaptureTracer(t)
	ctx, span := tracer.Start(context.Background(), "work")
	if FromContext(ctx) != span {
		t.Error("FromContext must return the span Start put in the context")
	}
}

func TestStartMintsRoot(t *testing.T) {
	tracer, capture := newCaptureTracer(t)
	_, span := tracer.Start(context.Background(), "root", slog.String("path", "/a"))
	mustClose(t, tracer)

	events := capture.snapshot()
	if len(events) != 1 {
		t.Fatalf("captured %d events, want 1", len(events))
	}
	start := events[0]
	if start.Kind != EventStart {
		t.Fatalf("Kind = %v, want EventStart", start.Kind)
	}
	if start.SpanID == 0 || span.id != start.SpanID {
		t.Errorf("SpanID = %d, span.id = %d; want equal and nonzero", start.SpanID, span.id)
	}
	if start.TraceID != start.SpanID {
		t.Errorf("root TraceID = %d, want its own SpanID %d", start.TraceID, start.SpanID)
	}
	if start.ParentID != 0 {
		t.Errorf("root ParentID = %d, want 0", start.ParentID)
	}
	if start.Name != "root" {
		t.Errorf("Name = %q, want %q", start.Name, "root")
	}
	if start.StartNS <= 0 {
		t.Errorf("StartNS = %d, want positive", start.StartNS)
	}
	if len(start.Attrs) != 1 || start.Attrs[0].Key != "path" {
		t.Errorf("Attrs = %v, want the one passed to Start", start.Attrs)
	}
}

func TestStartNestsViaContext(t *testing.T) {
	tracer, capture := newCaptureTracer(t)
	rootCtx, root := tracer.Start(context.Background(), "root")
	childCtx, child := tracer.Start(rootCtx, "child")
	mustClose(t, tracer)

	events := capture.snapshot()
	if len(events) != 2 {
		t.Fatalf("captured %d events, want 2", len(events))
	}
	rootEvent, childEvent := events[0], events[1]
	if childEvent.ParentID != rootEvent.SpanID {
		t.Errorf("child ParentID = %d, want root's SpanID %d", childEvent.ParentID, rootEvent.SpanID)
	}
	if childEvent.TraceID != rootEvent.TraceID {
		t.Errorf("child TraceID = %d, want root's %d", childEvent.TraceID, rootEvent.TraceID)
	}
	if childEvent.StartNS < rootEvent.StartNS {
		t.Error("child StartNS must not precede its parent's")
	}
	if FromContext(childCtx) != child || FromContext(rootCtx) != root {
		t.Error("each context must carry its own span")
	}
}

func TestStartWithNilContext(t *testing.T) {
	tracer, _ := newCaptureTracer(t)
	ctx, span := tracer.Start(nil, "work") //nolint:staticcheck // nil-safety is the contract under test
	if ctx == nil {
		t.Fatal("Start(nil, ...) must return a usable context")
	}
	if FromContext(ctx) != span {
		t.Error("returned context must carry the span")
	}
}

func TestDropPolicyCountsDrops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sink := newGateSink()
		tracer, err := New(sink, WithBufferSize(2))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = tracer.Close(context.Background()) })
		t.Cleanup(sink.release) // LIFO: gate opens before Close drains

		// First event: the writer picks it up and is now stuck inside
		// WriteBatch on the gate; after Wait the buffer is empty again.
		tracer.Start(context.Background(), "work")
		synctest.Wait()

		// Two fill the buffer, three overflow.
		for i := 0; i < 5; i++ {
			tracer.Start(context.Background(), "work")
		}
		if got := tracer.dropped.Load(); got != 3 {
			t.Errorf("dropped = %d, want 3 (5 events into a stalled buffer of 2)", got)
		}
	})
}

func TestBlockingPolicyBlocksUntilDrained(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sink := newGateSink()
		tracer, err := New(sink, WithBufferSize(1), WithBlockingPolicy())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = tracer.Close(context.Background()) })
		t.Cleanup(sink.release)

		tracer.Start(context.Background(), "writer-takes-this")
		synctest.Wait() // writer now stuck in the gate; buffer empty
		tracer.Start(context.Background(), "fills-the-buffer")

		var unblocked bool
		go func() {
			tracer.Start(context.Background(), "blocks")
			unblocked = true
		}()
		synctest.Wait()
		if unblocked {
			t.Fatal("send into a full buffer must block under WithBlockingPolicy")
		}

		sink.release()
		synctest.Wait()
		if !unblocked {
			t.Fatal("blocked producer must resume once the buffer drains")
		}
	})
}

func TestCloseUnblocksBlockedProducer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sink := newGateSink()
		tracer, err := New(sink, WithBufferSize(1), WithBlockingPolicy())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = tracer.Close(context.Background()) })
		t.Cleanup(sink.release)

		tracer.Start(context.Background(), "writer-takes-this")
		synctest.Wait()
		tracer.Start(context.Background(), "fills-the-buffer")

		var unblocked bool
		go func() {
			tracer.Start(context.Background(), "blocks")
			unblocked = true
		}()
		synctest.Wait()

		// Close flips closed and shuts stop; the producer must fall
		// through even though the writer is still hostage to the gate.
		go func() { _ = tracer.Close(context.Background()) }()
		synctest.Wait()
		if !unblocked {
			t.Fatal("Close must unblock a producer stuck on a full buffer")
		}
	})
}

func TestSendAfterCloseIsInert(t *testing.T) {
	tracer, capture := newCaptureTracer(t)
	tracer.Start(context.Background(), "before")
	mustClose(t, tracer)

	tracer.Start(context.Background(), "after")
	if events := capture.snapshot(); len(events) != 1 || events[0].Name != "before" {
		t.Errorf("closed tracer must emit nothing, captured %v", events)
	}
	if got := tracer.dropped.Load(); got != 0 {
		t.Errorf("inert no-ops must not count as drops, dropped = %d", got)
	}
}

func TestTrackIsLeafOnly(t *testing.T) {
	tracer, capture := newCaptureTracer(t)
	rootCtx, root := tracer.Start(context.Background(), "root")

	stop := tracer.Track(rootCtx, "leaf", slog.String("tool", "ffmpeg"))
	// Track returned no context, so a sibling started from the same ctx
	// nests under root, never under the tracked leaf.
	_, _ = tracer.Start(rootCtx, "sibling")
	stop()
	mustClose(t, tracer)

	events := capture.snapshot()
	starts := eventsOfKind(events, EventStart)
	if len(starts) != 3 {
		t.Fatalf("captured %d start events, want 3", len(starts))
	}
	leafStart, siblingStart := starts[1], starts[2]
	if leafStart.ParentID != root.id {
		t.Errorf("Track span ParentID = %d, want the ctx span %d", leafStart.ParentID, root.id)
	}
	if siblingStart.ParentID != root.id {
		t.Errorf("sibling ParentID = %d, want root %d — nothing may nest under a Track span", siblingStart.ParentID, root.id)
	}
	ends := eventsOfKind(events, EventEnd)
	if len(ends) != 1 || ends[0].SpanID != leafStart.SpanID {
		t.Errorf("Track's closer must end exactly the tracked span, got %v", ends)
	}
}

func TestDefaultTracerMirrors(t *testing.T) {
	t.Cleanup(func() { SetDefault(nil) })

	SetDefault(nil)
	if Default() != nil {
		t.Fatal("Default() must be nil until SetDefault")
	}
	ctx := context.Background()
	returned, span := Start(ctx, "work")
	if returned != ctx || span != nil {
		t.Error("package Start without a default must be the nil-tracer no-op")
	}
	Track(ctx, "leaf")() // must not panic

	tracer, capture := newCaptureTracer(t)
	SetDefault(tracer)
	if Default() != tracer {
		t.Fatal("Default() must return the tracer just set")
	}
	_, span = Start(ctx, "work")
	if span == nil {
		t.Error("package Start must mint spans on the default tracer")
	}
	Track(ctx, "leaf")()
	mustClose(t, tracer)

	starts := eventsOfKind(capture.snapshot(), EventStart)
	if len(starts) != 2 || starts[0].Name != "work" || starts[1].Name != "leaf" {
		t.Errorf("package mirrors must emit on the default tracer, got %v", starts)
	}
}

// panickingHandler is the broken user logger — the complaint channel
// itself as a crash vector.
type panickingHandler struct{}

func (panickingHandler) Enabled(context.Context, slog.Level) bool  { return true }
func (panickingHandler) Handle(context.Context, slog.Record) error { panic("buggy user handler") }
func (panickingHandler) WithAttrs([]slog.Attr) slog.Handler        { return panickingHandler{} }
func (panickingHandler) WithGroup(string) slog.Handler             { return panickingHandler{} }

func TestPanickingLoggerNeverKillsTheWriter(t *testing.T) {
	// An erroring sink makes the writer warn; the warn's handler panics.
	// Without warn's self-shield that panic escapes recoverSinkPanic and
	// kills the writer goroutine — and with it, the process.
	sink := &errorSink{}
	tracer, err := New(sink, WithLogger(slog.New(panickingHandler{})))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	tracer.Start(context.Background(), "work")

	// If the writer died, this Close never completes — the timeout turns
	// a hang into a loud failure.
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tracer.Close(closeCtx); err != nil {
		t.Fatalf("Close = %v — the writer must survive a panicking logger", err)
	}
	if got := tracer.Stats().WriteErrors; got < 1 {
		t.Errorf("WriteErrors = %d, want the original sink failure still counted", got)
	}
	if _, _, closes := sink.counts(); closes != 1 {
		t.Error("the sink must still be closed exactly once")
	}
}

func TestPanickingLoggerNeverEscapesGuard(t *testing.T) {
	// The double failure: Fail's user error panics (guard recovers, its
	// recover now spent), then guard's warn hits the panicking handler.
	// Without warn's self-shield the second panic propagates into the
	// caller — this test would die here.
	tracer, err := New(&captureSink{}, WithLogger(slog.New(panickingHandler{})))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = tracer.Close(context.Background()) })

	_, span := tracer.Start(context.Background(), "work")
	span.Fail(panickingError{})
	span.End()
}

func TestWarnIsRateLimited(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		handler := &recordingHandler{}
		capture := &captureSink{}
		tracer, err := New(capture, WithLogger(slog.New(handler)))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = tracer.Close(context.Background()) })

		// Three complaints inside one fake-clock second: exactly one lands.
		tracer.warn("first")
		tracer.warn("second")
		tracer.warn("third")
		if records := handler.snapshot(); len(records) != 1 || records[0].message != "first" {
			t.Fatalf("warn must emit once per second, got %v", records)
		}

		// The next second opens one new slot.
		time.Sleep(1100 * time.Millisecond)
		tracer.warn("fourth")
		tracer.warn("fifth")
		if records := handler.snapshot(); len(records) != 2 || records[1].message != "fourth" {
			t.Fatalf("a new second must admit exactly one more warning, got %v", records)
		}
	})
}

func TestTimestampsAreMonotonic(t *testing.T) {
	tracer, _ := newCaptureTracer(t)
	first := tracer.now()
	second := tracer.now()
	if second < first {
		t.Errorf("now() went backwards: %d then %d", first, second)
	}
	// The wall clock may tick coarser than the monotonic delta (darwin:
	// microseconds vs nanoseconds), so now() can read slightly ahead of a
	// later time.Now() — tolerate sub-millisecond skew in both directions.
	wall := time.Now().UnixNano()
	if diff := wall - second; diff < -int64(time.Millisecond) || diff > int64(time.Minute) {
		t.Errorf("now() = %d is implausibly far from wall clock %d", second, wall)
	}
}
