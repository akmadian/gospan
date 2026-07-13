package gospan

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

// nopSink satisfies New; chunk-2 tests observe the event channel directly,
// so nothing ever drains into it.
type nopSink struct{}

func (nopSink) WriteBatch(Batch) error { return nil }
func (nopSink) Flush() error           { return nil }
func (nopSink) Close() error           { return nil }

func newTestTracer(t *testing.T, opts ...Option) *Tracer {
	t.Helper()
	tracer, err := New(nopSink{}, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return tracer
}

// nextEvent pops the oldest buffered event. All sends in these tests
// happen-before the read, so an empty buffer is a hard failure, not a race.
func nextEvent(t *testing.T, tracer *Tracer) Event {
	t.Helper()
	select {
	case e := <-tracer.events:
		return e
	default:
		t.Fatal("expected a buffered event, buffer is empty")
		return Event{}
	}
}

func TestNewValidation(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Error("New(nil) should error")
	}
	if _, err := New(nopSink{}, WithBufferSize(0)); err == nil {
		t.Error("WithBufferSize(0) should error")
	}
	if _, err := New(nopSink{}, WithFlushInterval(-time.Second)); err == nil {
		t.Error("WithFlushInterval(-1s) should error")
	}
	if _, err := New(nopSink{}, WithBufferSize(16), WithFlushInterval(time.Millisecond)); err != nil {
		t.Errorf("valid options should not error: %v", err)
	}
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
}

func TestFromContext(t *testing.T) {
	if FromContext(context.Background()) != nil {
		t.Error("FromContext without a span must return nil")
	}
	if FromContext(nil) != nil { //nolint:staticcheck // nil-safety is the contract under test
		t.Error("FromContext(nil) must return nil")
	}

	tracer := newTestTracer(t)
	ctx, span := tracer.Start(context.Background(), "work")
	if FromContext(ctx) != span {
		t.Error("FromContext must return the span Start put in the context")
	}
}

func TestStartMintsRoot(t *testing.T) {
	tracer := newTestTracer(t)
	_, span := tracer.Start(context.Background(), "root", slog.String("path", "/a"))

	event := nextEvent(t, tracer)
	if event.Kind != EventStart {
		t.Fatalf("Kind = %v, want EventStart", event.Kind)
	}
	if event.SpanID == 0 || span.id != event.SpanID {
		t.Errorf("SpanID = %d, span.id = %d; want equal and nonzero", event.SpanID, span.id)
	}
	if event.TraceID != event.SpanID {
		t.Errorf("root TraceID = %d, want its own SpanID %d", event.TraceID, event.SpanID)
	}
	if event.ParentID != 0 {
		t.Errorf("root ParentID = %d, want 0", event.ParentID)
	}
	if event.Name != "root" {
		t.Errorf("Name = %q, want %q", event.Name, "root")
	}
	if event.StartNS <= 0 {
		t.Errorf("StartNS = %d, want positive", event.StartNS)
	}
	if len(event.Attrs) != 1 || event.Attrs[0].Key != "path" {
		t.Errorf("Attrs = %v, want the one passed to Start", event.Attrs)
	}
}

func TestStartNestsViaContext(t *testing.T) {
	tracer := newTestTracer(t)
	rootCtx, root := tracer.Start(context.Background(), "root")
	childCtx, child := tracer.Start(rootCtx, "child")

	rootEvent := nextEvent(t, tracer)
	childEvent := nextEvent(t, tracer)
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
	tracer := newTestTracer(t)
	ctx, span := tracer.Start(nil, "work") //nolint:staticcheck // nil-safety is the contract under test
	if ctx == nil {
		t.Fatal("Start(nil, ...) must return a usable context")
	}
	if FromContext(ctx) != span {
		t.Error("returned context must carry the span")
	}
}

func TestDropPolicyCountsDrops(t *testing.T) {
	tracer := newTestTracer(t, WithBufferSize(2))
	for i := 0; i < 5; i++ {
		tracer.Start(context.Background(), "work")
	}
	if got := tracer.dropped.Load(); got != 3 {
		t.Errorf("dropped = %d, want 3 (5 events into a buffer of 2)", got)
	}
}

func TestBlockingPolicyBlocksUntilDrained(t *testing.T) {
	tracer := newTestTracer(t, WithBufferSize(1), WithBlockingPolicy())
	tracer.Start(context.Background(), "fills-the-buffer")

	unblocked := make(chan struct{})
	go func() {
		tracer.Start(context.Background(), "blocks")
		close(unblocked)
	}()

	select {
	case <-unblocked:
		t.Fatal("send into a full buffer must block under WithBlockingPolicy")
	case <-time.After(50 * time.Millisecond):
	}

	<-tracer.events // make room
	select {
	case <-unblocked:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked producer must resume once the buffer drains")
	}
}

func TestStopUnblocksBlockedProducer(t *testing.T) {
	tracer := newTestTracer(t, WithBufferSize(1), WithBlockingPolicy())
	tracer.Start(context.Background(), "fills-the-buffer")

	unblocked := make(chan struct{})
	go func() {
		tracer.Start(context.Background(), "blocks")
		close(unblocked)
	}()
	time.Sleep(50 * time.Millisecond) // let it reach the blocking select

	close(tracer.stop)
	select {
	case <-unblocked:
	case <-time.After(2 * time.Second):
		t.Fatal("closing stop must unblock a blocked producer")
	}
}

func TestSendAfterCloseIsInert(t *testing.T) {
	tracer := newTestTracer(t)
	tracer.closed.Store(true)

	tracer.Start(context.Background(), "work")
	select {
	case e := <-tracer.events:
		t.Errorf("closed tracer must not enqueue events, got %+v", e)
	default:
	}
	if got := tracer.dropped.Load(); got != 0 {
		t.Errorf("inert no-ops must not count as drops, dropped = %d", got)
	}
}

func TestTimestampsAreMonotonic(t *testing.T) {
	tracer := newTestTracer(t)
	first := tracer.now()
	second := tracer.now()
	if second < first {
		t.Errorf("now() went backwards: %d then %d", first, second)
	}
	wall := time.Now().UnixNano()
	if diff := wall - second; diff < 0 || diff > int64(time.Minute) {
		t.Errorf("now() = %d is implausibly far from wall clock %d", second, wall)
	}
}
