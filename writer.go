package gospan

import (
	"log/slog"
	"time"
)

// maxBatchSize caps how many events one WriteBatch call carries. Large
// enough to amortize a sink's per-call overhead under load, small enough
// that the reused buffer stays cache-friendly and a slow sink never holds
// a monster batch hostage.
const maxBatchSize = 1024

// run is the writer goroutine: the single owner of the sink. It delivers
// events to WriteBatch as the queue drains (a batch of one is a stream),
// ticks Flush on the flush interval, and — because every Sink method is
// called from this goroutine only, including the final Close — sink
// authors never see concurrency.
func (tracer *Tracer) run() {
	// done is how Close learns the drain finished; closing it also
	// publishes closeErr (channel close is a memory barrier).
	defer close(tracer.done)
	ticker := time.NewTicker(tracer.flushInterval)
	defer ticker.Stop()

	// One buffer, reused for every delivery — this is why the Sink
	// contract forbids retaining the Batch.
	batch := make([]Event, 0, maxBatchSize)

	for {
		select {
		case event := <-tracer.events:
			// Deliver immediately with whatever else is already queued:
			// under light load that's a batch of one reaching the sink
			// near-instantly; under pressure the drain amortizes.
			batch = tracer.drainInto(append(batch[:0], event))
			tracer.recordCompletedSpans(batch)
			tracer.writeBatch(batch)
		case <-ticker.C:
			tracer.flushSink()
		case <-tracer.stop:
			// Close flipped closed before closing stop, so no new events
			// are entering. Drain what's buffered, commit, close the sink,
			// exit. Producers mid-send when closed flipped may still land
			// events here — the drain catches them; anything later is lost
			// by design (post-Close calls are no-ops).
			for {
				select {
				case event := <-tracer.events:
					batch = tracer.drainInto(append(batch[:0], event))
					tracer.recordCompletedSpans(batch)
					tracer.writeBatch(batch)
				default:
					tracer.flushSink()
					tracer.closeSink()
					return
				}
			}
		}
	}
}

// drainInto empties whatever is queued right now into batch, up to
// maxBatchSize, without ever blocking.
func (tracer *Tracer) drainInto(batch []Event) []Event {
	for len(batch) < maxBatchSize {
		select {
		case event := <-tracer.events:
			batch = append(batch, event)
		default:
			return batch
		}
	}
	return batch
}

// writeBatch delivers one batch, containing any sink failure: an error or
// panic is one failed delivery — counted, optionally logged, never a dead
// writer or a crashed process.
func (tracer *Tracer) writeBatch(batch []Event) {
	defer tracer.recoverSinkPanic()
	if err := tracer.sink.WriteBatch(Batch{Events: batch}); err != nil {
		tracer.writeErrors.Add(1)
		tracer.warn("gospan: sink WriteBatch failed", slog.Any("error", err))
		return
	}
	tracer.written.Add(uint64(len(batch)))
}

// flushSink ticks the sink's commit moment, with the same containment as
// writeBatch.
func (tracer *Tracer) flushSink() {
	defer tracer.recoverSinkPanic()
	if err := tracer.sink.Flush(); err != nil {
		tracer.writeErrors.Add(1)
		tracer.warn("gospan: sink Flush failed", slog.Any("error", err))
	}
}

// closeSink closes the sink from the writer goroutine — the sink's entire
// lifecycle, construction aside, happens on one goroutine. Its error is
// published to Tracer.Close via closeErr (sequenced by the done close).
func (tracer *Tracer) closeSink() {
	defer tracer.recoverSinkPanic()
	tracer.closeErr = tracer.sink.Close()
}

// recoverSinkPanic contains a panicking sink: counted as a write error,
// rate-limited complaint, writer keeps running.
func (tracer *Tracer) recoverSinkPanic() {
	if r := recover(); r != nil {
		tracer.writeErrors.Add(1)
		tracer.warn("gospan: recovered sink panic", slog.Any("panic", r))
	}
}
