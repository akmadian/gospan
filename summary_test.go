package gospan

import (
	"context"
	"errors"
	"math"
	"testing"
	"testing/synctest"
	"time"
)

func TestHistogramBucketsAreConsistent(t *testing.T) {
	// Indexes must be monotonic in duration and stay within the table.
	previousIndex := -1
	for _, durationNanos := range []int64{0, 1, 2, 7, 8, 9, 15, 16, 100, 1_000, 1_000_000, 1_000_000_000, math.MaxInt64} {
		index := bucketIndexForDuration(durationNanos)
		if index < 0 || index >= histogramBucketCount {
			t.Fatalf("duration %d maps to index %d, outside [0, %d)", durationNanos, index, histogramBucketCount)
		}
		if index < previousIndex {
			t.Fatalf("bucket index went backwards at duration %d", durationNanos)
		}
		previousIndex = index
	}
}

func TestHistogramRelativeErrorBound(t *testing.T) {
	// The documented contract: any duration, reconstructed through its
	// bucket midpoint, lands within 12.5% of the original. Exhaustive at
	// the places the bound can break: every value below 1024, then every
	// bucket's lower edge, midpoint, and upper edge across all octaves —
	// no sampling, no randomness.
	check := func(durationNanos int64) {
		t.Helper()
		reconstructed := int64(durationForBucketIndex(bucketIndexForDuration(durationNanos)))
		relativeError := math.Abs(float64(reconstructed-durationNanos)) / float64(durationNanos)
		if relativeError > 0.125 {
			t.Errorf("duration %d reconstructed as %d: relative error %.4f exceeds 12.5%%",
				durationNanos, reconstructed, relativeError)
		}
	}
	for durationNanos := int64(1); durationNanos < 1024; durationNanos++ {
		check(durationNanos)
	}
	for octave := 3; octave <= 62; octave++ {
		for subBucket := int64(0); subBucket < 4; subBucket++ {
			lowerBound := int64(1)<<octave + subBucket<<(octave-2)
			bucketWidth := int64(1) << (octave - 2)
			check(lowerBound)
			check(lowerBound + bucketWidth/2)
			check(lowerBound + bucketWidth - 1) // tops out at exactly MaxInt64
		}
	}
}

func TestSummaryAccumulatorExactFields(t *testing.T) {
	accumulator := &summaryAccumulator{}
	durations := []int64{100, 300, 200, 400} // ns
	for _, durationNanos := range durations {
		accumulator.record(Event{Kind: EventEnd, Name: "work", StartNS: 0, EndNS: durationNanos})
	}
	accumulator.record(Event{Kind: EventEnd, Name: "work", StartNS: 0, EndNS: 500, Status: SpanStatusError, Error: "boom"})
	accumulator.record(Event{Kind: EventEnd, Name: "work", StartNS: 0, EndNS: 600, Status: SpanStatusCanceled})

	summary := accumulator.snapshot()
	if summary.Count != 6 || summary.Errors != 1 || summary.Canceled != 1 {
		t.Errorf("Count/Errors/Canceled = %d/%d/%d, want 6/1/1", summary.Count, summary.Errors, summary.Canceled)
	}
	if summary.Min != 100 || summary.Max != 600 {
		t.Errorf("Min/Max = %v/%v, want 100ns/600ns (exact)", summary.Min, summary.Max)
	}
	if summary.Mean != 350 {
		t.Errorf("Mean = %v, want 350ns (exact: 2100/6)", summary.Mean)
	}
}

func TestSummaryPercentilesWithinBound(t *testing.T) {
	// 1..1000 microseconds, uniform: true P50/P90/P99 are known, and the
	// histogram's answers must land within the 12.5% contract.
	accumulator := &summaryAccumulator{}
	for microseconds := int64(1); microseconds <= 1000; microseconds++ {
		accumulator.record(Event{Kind: EventEnd, EndNS: microseconds * 1000})
	}
	summary := accumulator.snapshot()

	assertWithinBound := func(name string, got time.Duration, trueValue time.Duration) {
		t.Helper()
		relativeError := math.Abs(float64(got-trueValue)) / float64(trueValue)
		if relativeError > 0.125 {
			t.Errorf("%s = %v, true value %v: relative error %.4f exceeds 12.5%%", name, got, trueValue, relativeError)
		}
	}
	assertWithinBound("P50", summary.P50, 500*time.Microsecond)
	assertWithinBound("P90", summary.P90, 900*time.Microsecond)
	assertWithinBound("P99", summary.P99, 990*time.Microsecond)
}

func TestSummaryThroughTheTracer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tracer, _ := newCaptureTracer(t)

		// Fake time makes these durations exact: 10ms, 20ms, 30ms.
		for _, milliseconds := range []int{10, 20, 30} {
			_, span := tracer.Start(context.Background(), "extract")
			time.Sleep(time.Duration(milliseconds) * time.Millisecond)
			span.End()
		}
		_, failed := tracer.Start(context.Background(), "extract")
		failed.Fail(errors.New("boom"))
		failed.End()
		_, other := tracer.Start(context.Background(), "resize")
		other.End()
		synctest.Wait()

		summaries := tracer.Summary()
		if len(summaries) != 2 {
			t.Fatalf("Summary has %d names, want 2: %v", len(summaries), summaries)
		}
		extract := summaries["extract"]
		if extract.Count != 4 || extract.Errors != 1 {
			t.Errorf("extract Count/Errors = %d/%d, want 4/1", extract.Count, extract.Errors)
		}
		if extract.Min != 0 || extract.Max != 30*time.Millisecond {
			t.Errorf("extract Min/Max = %v/%v, want 0/30ms exactly (fake clock)", extract.Min, extract.Max)
		}
		if resize := summaries["resize"]; resize.Count != 1 {
			t.Errorf("resize Count = %d, want 1", resize.Count)
		}

		// A span still open contributes nothing until it ends.
		if extract.Count+summaries["resize"].Count != 5 {
			t.Error("only ended spans may be aggregated")
		}
		mustClose(t, tracer)
	})
}

func TestSummaryOnNilAndEmptyTracer(t *testing.T) {
	var nilTracer *Tracer
	if summaries := nilTracer.Summary(); summaries != nil {
		t.Errorf("nil tracer Summary = %v, want nil", summaries)
	}

	tracer, _ := newCaptureTracer(t)
	if summaries := tracer.Summary(); len(summaries) != 0 {
		t.Errorf("fresh tracer Summary has %d entries, want 0", len(summaries))
	}
}
