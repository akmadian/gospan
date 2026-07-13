package gospan

import (
	"context"
	"testing"
	"testing/synctest"
)

func TestStatsCountersAndGauges(t *testing.T) {
	tracer, _ := newCaptureTracer(t)

	rootCtx, root := tracer.Start(context.Background(), "root")
	_, child := tracer.Start(rootCtx, "child")
	_, secondRoot := tracer.Start(context.Background(), "other-root")

	stats := tracer.Stats()
	if stats.Started != 3 || stats.Completed != 0 {
		t.Errorf("Started/Completed = %d/%d, want 3/0", stats.Started, stats.Completed)
	}
	if stats.SpansInFlight != 3 {
		t.Errorf("SpansInFlight = %d, want 3", stats.SpansInFlight)
	}
	if stats.TracesInFlight != 2 {
		t.Errorf("TracesInFlight = %d, want 2 (two roots open)", stats.TracesInFlight)
	}

	child.End()
	child.End() // second End must not double-decrement
	secondRoot.End()

	stats = tracer.Stats()
	if stats.Completed != 2 {
		t.Errorf("Completed = %d, want 2", stats.Completed)
	}
	if stats.SpansInFlight != 1 {
		t.Errorf("SpansInFlight = %d, want 1 (root still open)", stats.SpansInFlight)
	}
	if stats.TracesInFlight != 1 {
		t.Errorf("TracesInFlight = %d, want 1 — child End must not close the trace", stats.TracesInFlight)
	}

	root.End()
	mustClose(t, tracer)
	stats = tracer.Stats()
	if stats.Written != 6 {
		t.Errorf("Written = %d, want 6 (3 starts + 3 ends, all delivered)", stats.Written)
	}
	if stats.SpansInFlight != 0 || stats.TracesInFlight != 0 {
		t.Errorf("in-flight gauges = %d/%d after everything ended, want 0/0", stats.SpansInFlight, stats.TracesInFlight)
	}
	if stats.Dropped != 0 || stats.WriteErrors != 0 {
		t.Errorf("healthy run: Dropped = %d WriteErrors = %d, want 0/0", stats.Dropped, stats.WriteErrors)
	}
}

func TestStatsQueueDepthAndDropped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sink := newGateSink()
		tracer, err := New(sink, WithBufferSize(2))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = tracer.Close(context.Background()) })
		t.Cleanup(sink.release)

		tracer.Start(context.Background(), "writer-takes-this")
		synctest.Wait() // writer stuck in the gate; buffer empty
		for i := 0; i < 4; i++ {
			tracer.Start(context.Background(), "work")
		}

		stats := tracer.Stats()
		if stats.QueueDepth != 2 {
			t.Errorf("QueueDepth = %d, want 2 (buffer full behind a stalled writer)", stats.QueueDepth)
		}
		if stats.Dropped != 2 {
			t.Errorf("Dropped = %d, want 2", stats.Dropped)
		}
	})
}

func TestStatsWriteErrors(t *testing.T) {
	sink := &errorSink{}
	tracer, err := New(sink)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	tracer.Start(context.Background(), "work")
	mustClose(t, tracer)

	if got := tracer.Stats().WriteErrors; got < 1 {
		t.Errorf("WriteErrors = %d, want at least 1", got)
	}
	if got := tracer.Stats().Written; got != 0 {
		t.Errorf("Written = %d, want 0 — failed batches must not count as written", got)
	}
}

func TestStatsOverheadIsSampled(t *testing.T) {
	tracer, _ := newCaptureTracer(t, WithBufferSize(4096))
	// 300 spans crosses the 1-in-128 sampling cadence at least twice.
	for i := 0; i < 300; i++ {
		_, span := tracer.Start(context.Background(), "work")
		span.End()
	}
	mustClose(t, tracer)

	overhead := tracer.Stats().OverheadPerSpan
	if overhead <= 0 {
		t.Error("OverheadPerSpan must be positive once samples exist")
	}
	// Loose sanity ceiling: a Start+End pair costing more than 1ms would
	// mean the measurement itself is broken.
	if overhead.Milliseconds() >= 1 {
		t.Errorf("OverheadPerSpan = %v, implausibly high", overhead)
	}
}

func TestOverheadSamplingCadence(t *testing.T) {
	if _, err := New(&captureSink{}, WithOverheadSampling(0)); err == nil {
		t.Error("WithOverheadSampling(0) should error")
	}
	if _, err := New(&captureSink{}, WithOverheadSampling(-8)); err == nil {
		t.Error("WithOverheadSampling(-8) should error")
	}

	// One span is enough to seed the average at every=1...
	perSpan, _ := newCaptureTracer(t, WithOverheadSampling(1))
	_, span := perSpan.Start(context.Background(), "work")
	span.End()
	if perSpan.Stats().OverheadPerSpan <= 0 {
		t.Error("every=1 must sample the very first span")
	}

	// ...and is nowhere near the default 1-in-128 cadence's first sample.
	sparse, _ := newCaptureTracer(t)
	_, span = sparse.Start(context.Background(), "work")
	span.End()
	if got := sparse.Stats().OverheadPerSpan; got != 0 {
		t.Errorf("default cadence sampled the first span (overhead %v), want first sample at span 128", got)
	}
}

func TestStatsOnNilTracer(t *testing.T) {
	var tracer *Tracer
	if got := tracer.Stats(); got != (Stats{}) {
		t.Errorf("nil tracer Stats = %+v, want zero value", got)
	}
}
