package gospan

import (
	"context"
	"log/slog"
	"time"
)

// SlogSink returns a Sink that emits one log record per finished span into
// logger — spans join your existing log flow, no file involved, and any
// logging library with a slog.Handler bridge can receive them. The record's
// message is the span name; its level tracks the status (Info, Warn for
// canceled, Error for failed); span_id, trace_id, duration, status, and the
// span's merged attributes ride along as record attrs.
//
// A nil logger falls back to slog.Default. Spans still open when the
// tracer closes emit nothing — the incomplete-span story belongs to the
// file sinks, not the log flow.
func SlogSink(logger *slog.Logger) Sink {
	if logger == nil {
		logger = slog.Default()
	}
	return &slogSink{
		logger: logger,
		open:   make(map[int64]*openSpan),
	}
}

// maxOpenSlogSpans caps the open-span map. In steady state open holds only
// spans currently in flight — each entry is deleted when its end event
// arrives — but an end lost to buffer pressure would otherwise strand its
// entry until Close, an unbounded leak under sustained drops. The cap holds
// it to a bounded set; past it a new span's pre-end attrs go untracked (its
// end still emits a record, just without them). 65536 is far above any
// realistic in-flight count for a per-span log emitter, so healthy runs
// never reach it.
const maxOpenSlogSpans = 1 << 16

// openSpan accumulates what the log record needs until the end event
// arrives. Normally bounded by spans in flight (each is deleted at its end);
// the maxOpenSlogSpans cap bounds it even when end events are lost.
type openSpan struct {
	attrs []slog.Attr
}

type slogSink struct {
	logger *slog.Logger
	// open is touched only by the writer goroutine (the Sink contract), so
	// it needs no lock. Keyed by span ID.
	open map[int64]*openSpan
}

func (sink *slogSink) WriteBatch(batch Batch) error {
	for _, event := range batch.Events {
		switch event.Kind {
		case EventStart, EventAttrs:
			// Both kinds just accumulate attrs; name and start time are
			// not stored because the end event repeats them. The slice is
			// copied — the batch must not be retained.
			span := sink.open[event.SpanID]
			if span == nil {
				// Past the cap, stop tracking new spans: a later end still
				// emits (emit tolerates a missing entry), just without the
				// pre-end attrs. This bounds the open map when end events are
				// dropped under buffer pressure — the same drop-and-count
				// posture the tracer's buffer uses — instead of leaking until
				// Close.
				if len(sink.open) >= maxOpenSlogSpans {
					continue
				}
				span = &openSpan{}
				sink.open[event.SpanID] = span
			}
			span.attrs = append(span.attrs, event.Attrs...)
		case EventEnd:
			sink.emit(event)
			delete(sink.open, event.SpanID)
		default:
			// Unknown future kind: skip, never fail (SPEC §5's spirit —
			// readers tolerate what they don't understand).
		}
	}
	return nil
}

// emit builds and logs the one record a finished span gets. An end without
// a recorded start (dropped under buffer pressure) still emits fully —
// end events carry Name and StartNS precisely for this.
func (sink *slogSink) emit(event Event) {
	recordAttrs := []slog.Attr{
		slog.Int64("span_id", event.SpanID),
		slog.Int64("trace_id", event.TraceID),
		slog.Duration("duration", time.Duration(event.EndNS-event.StartNS)),
		slog.String("status", event.Status.String()),
	}
	if event.Error != "" {
		recordAttrs = append(recordAttrs, slog.String("error", event.Error))
	}
	if span := sink.open[event.SpanID]; span != nil {
		recordAttrs = append(recordAttrs, mergeLastWins(span.attrs)...)
	}

	level := slog.LevelInfo
	switch event.Status {
	case SpanStatusError:
		level = slog.LevelError
	case SpanStatusCanceled:
		level = slog.LevelWarn
	case SpanStatusOK:
	}
	sink.logger.LogAttrs(context.Background(), level, event.Name, recordAttrs...)
}

// mergeLastWins collapses duplicate keys, keeping each key's position of
// first appearance and value of last write — the reader sees a stable
// order and the promised final values.
func mergeLastWins(attrs []slog.Attr) []slog.Attr {
	merged := make([]slog.Attr, 0, len(attrs))
	position := make(map[string]int, len(attrs))
	for _, attr := range attrs {
		if at, seen := position[attr.Key]; seen {
			merged[at] = attr
			continue
		}
		position[attr.Key] = len(merged)
		merged = append(merged, attr)
	}
	return merged
}

// Flush is a no-op: records were already handed to the logger in
// WriteBatch — a log flow has no commit moment of its own.
func (sink *slogSink) Flush() error { return nil }

// Close discards still-open spans: a log line for a span that never ended
// would be guesswork, and diagnosing incompleteness is the file story.
func (sink *slogSink) Close() error {
	sink.open = nil
	return nil
}
