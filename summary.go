package gospan

import (
	"math"
	"math/bits"
	"time"
)

// SpanSummary aggregates every finished span sharing one name — "how is
// your code doing", where Stats answers "how is the tracer doing".
// Count, Errors, Canceled, Min, Max, and Mean are exact; the percentiles
// are approximate, read from a log-bucketed histogram with relative error
// bounded at ~12.5% (four linear sub-buckets per power of two, values
// reported at bucket midpoints). Exact answers live in the trace file —
// one SQL query away — so the in-process copy stays small and lock-free
// on the hot path.
type SpanSummary struct {
	Count    uint64
	Errors   uint64
	Canceled uint64
	Min      time.Duration
	Max      time.Duration
	Mean     time.Duration
	P50      time.Duration
	P90      time.Duration
	P99      time.Duration
}

// Summary returns the per-name aggregates for every span name seen so
// far. It is safe from any goroutine and cheap enough for a ticker; on a
// nil Tracer it returns nil.
func (tracer *Tracer) Summary() map[string]SpanSummary {
	if tracer == nil {
		return nil
	}
	tracer.summaryMutex.Lock()
	defer tracer.summaryMutex.Unlock()
	result := make(map[string]SpanSummary, len(tracer.summaries))
	for name, accumulator := range tracer.summaries {
		result[name] = accumulator.snapshot()
	}
	return result
}

// recordCompletedSpans folds a batch's end events into the per-name
// accumulators. Called by the writer goroutine alongside sink delivery —
// and independently of sink health: the span happened whether or not the
// destination is well. The mutex is writer-vs-Summary only; producers
// never touch it, so the hot path stays lock-free (D11's intent).
func (tracer *Tracer) recordCompletedSpans(batch []Event) {
	// Pre-scan cheaply so batches with no end events skip the lock.
	hasEndEvent := false
	for index := range batch {
		if batch[index].Kind == EventEnd {
			hasEndEvent = true
			break
		}
	}
	if !hasEndEvent {
		return
	}

	tracer.summaryMutex.Lock()
	defer tracer.summaryMutex.Unlock()
	for _, event := range batch {
		if event.Kind != EventEnd {
			continue
		}
		accumulator := tracer.summaries[event.Name]
		if accumulator == nil {
			accumulator = &summaryAccumulator{}
			tracer.summaries[event.Name] = accumulator
		}
		accumulator.record(event)
	}
}

// histogramBucketCount covers every representable duration: indexes 0–6
// hold 1–7ns exactly, then four sub-buckets per octave up to the top of
// int64 (octave 62) — 7 + (62−3+1)×4 = 247 buckets, ~2KB per span name.
const histogramBucketCount = 247

// summaryAccumulator is one span name's running aggregate. Owned by the
// writer goroutine under summaryMutex; never touched by producers.
type summaryAccumulator struct {
	count      uint64
	errors     uint64
	canceled   uint64
	minNanos   int64
	maxNanos   int64
	totalNanos int64
	histogram  [histogramBucketCount]uint64
}

func (accumulator *summaryAccumulator) record(event Event) {
	durationNanos := event.EndNS - event.StartNS
	if durationNanos < 0 {
		durationNanos = 0 // an orphaned end with no real start time
	}
	if accumulator.count == 0 || durationNanos < accumulator.minNanos {
		accumulator.minNanos = durationNanos
	}
	if durationNanos > accumulator.maxNanos {
		accumulator.maxNanos = durationNanos
	}
	accumulator.count++
	accumulator.totalNanos += durationNanos
	accumulator.histogram[bucketIndexForDuration(durationNanos)]++

	switch event.Status {
	case SpanStatusError:
		accumulator.errors++
	case SpanStatusCanceled:
		accumulator.canceled++
	case SpanStatusOK:
	}
}

func (accumulator *summaryAccumulator) snapshot() SpanSummary {
	summary := SpanSummary{
		Count:    accumulator.count,
		Errors:   accumulator.errors,
		Canceled: accumulator.canceled,
		Min:      time.Duration(accumulator.minNanos),
		Max:      time.Duration(accumulator.maxNanos),
	}
	if accumulator.count > 0 {
		// count grows one event at a time and can never approach 2^63,
		// so the divisor conversion cannot overflow.
		summary.Mean = time.Duration(accumulator.totalNanos / int64(accumulator.count)) //nolint:gosec // G115: see above
		summary.P50 = accumulator.percentile(0.50)
		summary.P90 = accumulator.percentile(0.90)
		summary.P99 = accumulator.percentile(0.99)
	}
	return summary
}

// percentile walks the histogram to the bucket containing the requested
// rank (nearest-rank definition) and reports that bucket's midpoint.
func (accumulator *summaryAccumulator) percentile(fraction float64) time.Duration {
	targetRank := uint64(math.Ceil(fraction * float64(accumulator.count)))
	if targetRank < 1 {
		targetRank = 1
	}
	var cumulative uint64
	for index := range accumulator.histogram {
		cumulative += accumulator.histogram[index]
		if cumulative >= targetRank {
			return durationForBucketIndex(index)
		}
	}
	return time.Duration(accumulator.maxNanos) // unreachable if counts are consistent
}

// bucketIndexForDuration maps a duration to its histogram bucket: 1–7ns
// are exact, and beyond that each power of two splits into four linear
// sub-buckets — integer-only math (a bit-length and a shift), no logs or
// floats on the writer path.
func bucketIndexForDuration(durationNanos int64) int {
	if durationNanos < 8 {
		if durationNanos < 1 {
			durationNanos = 1
		}
		return int(durationNanos) - 1 // 1..7 → indexes 0..6, exact
	}
	// durationNanos ≥ 8 past the branch above, so its unsigned view is
	// identical and bits.Len64 reads the true magnitude.
	value := uint64(durationNanos)                // positive by the guard above
	octave := bits.Len64(value) - 1               // floor(log2(value)), 3..62 for any positive int64
	subBucket := int((value >> (octave - 2)) & 3) // masked to 0..3 before converting
	return 7 + (octave-3)*4 + subBucket
}

// durationForBucketIndex is the reverse mapping, reporting the bucket's
// midpoint — the choice that halves the worst-case error to ~12.5%
// (half a sub-bucket over the octave's lower bound).
func durationForBucketIndex(index int) time.Duration {
	if index < 7 {
		return time.Duration(index + 1)
	}
	adjusted := index - 7
	octave := adjusted/4 + 3
	subBucket := adjusted % 4
	// int64 math throughout: octave tops out at 62, so the largest sum is
	// 2^62 + 3·2^60 + 2^59 — comfortably below 2^63. Shift counts are
	// plain ints; Go has accepted signed shift counts since 1.13.
	lowerBound := int64(1)<<octave + int64(subBucket)<<(octave-2)
	bucketWidth := int64(1) << (octave - 2)
	return time.Duration(lowerBound + bucketWidth/2)
}
