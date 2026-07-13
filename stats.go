package gospan

import "time"

// Stats is the tracer's self-health snapshot: what tracing has cost and
// what it has dropped — "how is the tracer doing", where Summary answers
// "how is your code doing". All values come from atomics; calling this on
// a ticker is fine.
type Stats struct {
	// Cumulative since New.
	//
	// Written means accepted by the sink, not durable: a sink that
	// buffers in WriteBatch and commits in Flush (the SQLite sink) can
	// still lose accepted events to a failed commit — that failure is a
	// WriteErrors increment, because durability is the sink's own story
	// and the tracer never guesses at it. The trace file itself is the
	// ground truth of what survived.
	Started   uint64 // spans begun
	Completed uint64 // spans whose End ran
	Written   uint64 // events accepted by the sink
	Dropped   uint64 // events lost to a full buffer

	// This instant.
	SpansInFlight  int // started, not yet ended
	TracesInFlight int // root spans still open
	QueueDepth     int // events buffered, not yet taken by the writer

	// WriteErrors counts failed sink calls — a WriteBatch or Flush that
	// errored or panicked. Flush failures matter as much as batch
	// failures: for a buffering sink the flush IS the commit, so this is
	// where lost durability becomes visible. Degraded operation, counted
	// never propagated.
	WriteErrors uint64

	// OverheadPerSpan is the rolling average tracer-added cost of one
	// span's Start+End pair — what tracing costs you on this hardware,
	// right now. Measured on a 1-in-128 sample by default; tune with
	// WithOverheadSampling.
	OverheadPerSpan time.Duration
}

// Stats returns the current snapshot. On a nil Tracer it is all zeros.
func (tracer *Tracer) Stats() Stats {
	if tracer == nil {
		return Stats{}
	}
	return Stats{
		Started:         tracer.started.Load(),
		Completed:       tracer.completed.Load(),
		Written:         tracer.written.Load(),
		Dropped:         tracer.dropped.Load(),
		SpansInFlight:   int(tracer.spansInFlight.Load()),
		TracesInFlight:  int(tracer.tracesInFlight.Load()),
		QueueDepth:      len(tracer.events),
		WriteErrors:     tracer.writeErrors.Load(),
		OverheadPerSpan: time.Duration(tracer.overheadNS.Load()),
	}
}

// recordOverhead folds one sampled span's full tracer cost into the
// rolling average.
//
// The average is an EMA (exponentially weighted moving average):
// new = old + (sample−old)/8. One stored value replaces a window buffer,
// and each older sample's influence decays geometrically. The 1/8 is
// TCP's RTT-smoothing constant (RFC 6298): small enough to damp scheduler
// noise, large enough to track a real shift within ~8 samples — and a
// power of two, so the division compiles to a shift, keeping this
// integer-only on the span path.
//
// The read-modify-write is deliberately racy: two simultaneous samples
// can lose one update, which skews a diagnostic average by one sample —
// a CAS loop would cost more than that is worth. Load/Store stay atomic,
// so there is no torn value and no data race, only a lost sample.
func (tracer *Tracer) recordOverhead(nanos int64) {
	old := tracer.overheadNS.Load()
	if old == 0 {
		tracer.overheadNS.Store(nanos)
		return
	}
	tracer.overheadNS.Store(old + (nanos-old)/8)
}
