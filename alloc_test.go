package gospan_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/akmadian/gospan"
)

// TestAllocationCeilings is the enforced half of the overhead posture:
// allocs/op is deterministic (unlike ns/op, which varies by machine), so
// CI can gate on it exactly. These ceilings are the measured values —
// a regression here means an accidental escape or captured closure on
// the hot path. ns/op numbers are published in the README instead.
func TestAllocationCeilings(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("the race detector changes allocation behavior; make check runs this separately without -race")
	}

	tracer, err := gospan.New(discardSink{}, gospan.WithBufferSize(1<<16))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = tracer.Close(context.Background()) })
	ctx := context.Background()

	// The quarter-alloc slack absorbs the writer goroutine's rare
	// amortized allocations (map growth) without letting a real +1
	// regression through.
	assertCeiling := func(name string, ceiling float64, operation func()) {
		t.Helper()
		operation() // warm up: first-use allocations (summary accumulators) land here
		if average := testing.AllocsPerRun(200, operation); average > ceiling+0.25 {
			t.Errorf("%s allocates %.2f/op, ceiling %.0f", name, average, ceiling)
		}
	}

	assertCeiling("StartEnd", 2, func() { // the Span + the context node
		_, span := tracer.Start(ctx, "bench")
		span.End()
	})
	assertCeiling("StartEndWithAttrs", 3, func() { // + one attrs slice, regardless of attr count
		_, span := tracer.Start(ctx, "bench", slog.String("path", "/a"), slog.Int("attempt", 1))
		span.End()
	})
	assertCeiling("Track", 2, func() {
		tracer.Track(ctx, "bench")()
	})

	_, openSpan := tracer.Start(ctx, "bench")
	defer openSpan.End()
	assertCeiling("SetAttrs", 1, func() { // the attrs slice only
		openSpan.SetAttrs(slog.Int("progress", 1))
	})

	var nilTracer *gospan.Tracer
	assertCeiling("NilTracer", 0, func() { // tracing off is free
		_, span := nilTracer.Start(ctx, "bench")
		span.End()
	})
}
