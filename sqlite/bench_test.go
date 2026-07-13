package sqlite

import (
	"log/slog"
	"testing"

	"github.com/akmadian/gospan"
)

// The sink benchmarks measure throughput, not hot-path latency — the
// writer goroutine pays these costs, never the traced program. They exist
// to price three design claims: coalescing (D1), batch amortization, and
// the per-commit overhead behind the 1s flush heartbeat. ns/op = one
// complete span through WriteBatch+Flush.

const benchBatchSpans = 128

func newBenchSink(b *testing.B) *Sink {
	b.Helper()
	sink, err := New(b.TempDir())
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	b.Cleanup(func() { _ = sink.Close() })
	return sink
}

// BenchmarkCoalescedSpans is the headline: start+end land in the same
// flush window (the common case), one INSERT per span.
func BenchmarkCoalescedSpans(b *testing.B) {
	sink := newBenchSink(b)
	events := make([]gospan.Event, 0, 2*benchBatchSpans)
	var nextSpanID int64
	b.ReportAllocs()
	for b.Loop() {
		events = events[:0]
		for range benchBatchSpans {
			nextSpanID++
			events = append(events, startEvent(nextSpanID, "bench"), endEvent(nextSpanID, "bench"))
		}
		if err := sink.WriteBatch(gospan.Batch{Events: events}); err != nil {
			b.Fatal(err)
		}
		if err := sink.Flush(); err != nil {
			b.Fatal(err)
		}
	}
	reportPerSpan(b, benchBatchSpans)
}

// BenchmarkSplitLifecycleSpans prices the spans that outlive a flush
// interval: the start inserts in one commit, the end upserts in a later
// one — the second write D1's coalescing avoids.
func BenchmarkSplitLifecycleSpans(b *testing.B) {
	sink := newBenchSink(b)
	starts := make([]gospan.Event, 0, benchBatchSpans)
	ends := make([]gospan.Event, 0, benchBatchSpans)
	var nextSpanID int64
	b.ReportAllocs()
	for b.Loop() {
		starts, ends = starts[:0], ends[:0]
		for range benchBatchSpans {
			nextSpanID++
			starts = append(starts, startEvent(nextSpanID, "bench"))
			ends = append(ends, endEvent(nextSpanID, "bench"))
		}
		if err := sink.WriteBatch(gospan.Batch{Events: starts}); err != nil {
			b.Fatal(err)
		}
		if err := sink.Flush(); err != nil {
			b.Fatal(err)
		}
		if err := sink.WriteBatch(gospan.Batch{Events: ends}); err != nil {
			b.Fatal(err)
		}
		if err := sink.Flush(); err != nil {
			b.Fatal(err)
		}
	}
	reportPerSpan(b, benchBatchSpans)
}

// BenchmarkSpansWithAttrs prices the attrs side table: four typed attrs
// per span on top of the coalesced case.
func BenchmarkSpansWithAttrs(b *testing.B) {
	sink := newBenchSink(b)
	events := make([]gospan.Event, 0, 2*benchBatchSpans)
	var nextSpanID int64
	b.ReportAllocs()
	for b.Loop() {
		events = events[:0]
		for range benchBatchSpans {
			nextSpanID++
			events = append(events,
				startEvent(nextSpanID, "bench",
					slog.String("path", "/photos/IMG_0001.RAW"),
					slog.Int("worker", 3),
					slog.Duration("wait", 1500),
					slog.Bool("cached", true)),
				endEvent(nextSpanID, "bench"))
		}
		if err := sink.WriteBatch(gospan.Batch{Events: events}); err != nil {
			b.Fatal(err)
		}
		if err := sink.Flush(); err != nil {
			b.Fatal(err)
		}
	}
	reportPerSpan(b, benchBatchSpans)
}

// BenchmarkTinyBatches prices the stream end of the spectrum: one span
// per commit — what a quiet program's heartbeat flush costs.
func BenchmarkTinyBatches(b *testing.B) {
	sink := newBenchSink(b)
	var nextSpanID int64
	b.ReportAllocs()
	for b.Loop() {
		nextSpanID++
		batch := gospan.Batch{Events: []gospan.Event{
			startEvent(nextSpanID, "bench"), endEvent(nextSpanID, "bench"),
		}}
		if err := sink.WriteBatch(batch); err != nil {
			b.Fatal(err)
		}
		if err := sink.Flush(); err != nil {
			b.Fatal(err)
		}
	}
	reportPerSpan(b, 1)
}

// reportPerSpan rescales ns/op from per-iteration to per-span, and adds
// spans/sec — the number that says when buffer pressure would begin.
func reportPerSpan(b *testing.B, spansPerIteration int) {
	b.Helper()
	perSpan := float64(b.Elapsed().Nanoseconds()) / float64(b.N*spansPerIteration)
	b.ReportMetric(perSpan, "ns/span")
	b.ReportMetric(1e9/perSpan, "spans/sec")
}
