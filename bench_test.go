package gospan_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/akmadian/gospan"
)

// discardSink accepts everything instantly, so benchmarks measure the
// tracer's cost, not a destination's.
type discardSink struct{}

func (discardSink) WriteBatch(gospan.Batch) error { return nil }
func (discardSink) Flush() error                  { return nil }
func (discardSink) Close() error                  { return nil }

// stalledSink never accepts a batch until released — behind it, the
// buffer fills and every send exercises the drop path.
type stalledSink struct {
	gate chan struct{}
}

func (sink stalledSink) WriteBatch(gospan.Batch) error {
	<-sink.gate
	return nil
}
func (sink stalledSink) Flush() error { return nil }
func (sink stalledSink) Close() error { return nil }

func newBenchTracer(b *testing.B, options ...gospan.Option) *gospan.Tracer {
	b.Helper()
	tracer, err := gospan.New(discardSink{}, options...)
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	b.Cleanup(func() { _ = tracer.Close(context.Background()) })
	return tracer
}

// BenchmarkStartEnd is the headline number: one full span lifecycle,
// no attributes, writer draining at full speed.
func BenchmarkStartEnd(b *testing.B) {
	tracer := newBenchTracer(b, gospan.WithBufferSize(1<<16))
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_, span := tracer.Start(ctx, "bench")
		span.End()
	}
}

func BenchmarkStartEndWithAttrs(b *testing.B) {
	tracer := newBenchTracer(b, gospan.WithBufferSize(1<<16))
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_, span := tracer.Start(ctx, "bench",
			slog.String("path", "/a.raw"), slog.Int("attempt", 1))
		span.End()
	}
}

func BenchmarkStartEndNested(b *testing.B) {
	tracer := newBenchTracer(b, gospan.WithBufferSize(1<<16))
	rootCtx, root := tracer.Start(context.Background(), "root")
	defer root.End()
	b.ReportAllocs()
	for b.Loop() {
		_, span := tracer.Start(rootCtx, "child")
		span.End()
	}
}

func BenchmarkTrack(b *testing.B) {
	tracer := newBenchTracer(b, gospan.WithBufferSize(1<<16))
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		tracer.Track(ctx, "bench")()
	}
}

func BenchmarkSetAttrs(b *testing.B) {
	tracer := newBenchTracer(b, gospan.WithBufferSize(1<<16))
	_, span := tracer.Start(context.Background(), "bench")
	defer span.End()
	b.ReportAllocs()
	for b.Loop() {
		span.SetAttrs(slog.Int("progress", 1))
	}
}

// BenchmarkStartEndParallel contends the atomics and the channel from
// every core — the pipeline-shaped load gospan exists for.
func BenchmarkStartEndParallel(b *testing.B) {
	tracer := newBenchTracer(b, gospan.WithBufferSize(1<<16))
	ctx := context.Background()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, span := tracer.Start(ctx, "bench")
			span.End()
		}
	})
}

// BenchmarkStartEndDropping measures the degraded hot path: a stalled
// sink, a full buffer, every event dropped and counted.
func BenchmarkStartEndDropping(b *testing.B) {
	sink := stalledSink{gate: make(chan struct{})}
	tracer, err := gospan.New(sink, gospan.WithBufferSize(8))
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	b.Cleanup(func() {
		close(sink.gate)
		_ = tracer.Close(context.Background())
	})
	ctx := context.Background()
	// Prefill until drops begin so the loop measures pure drop cost.
	for range 16 {
		_, span := tracer.Start(ctx, "prefill")
		span.End()
	}
	b.ReportAllocs()
	for b.Loop() {
		_, span := tracer.Start(ctx, "bench")
		span.End()
	}
}

// BenchmarkNilTracer substantiates the "nil is off costs a nil check"
// claim from the README.
func BenchmarkNilTracer(b *testing.B) {
	var tracer *gospan.Tracer
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_, span := tracer.Start(ctx, "bench")
		span.End()
	}
}
