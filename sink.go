package gospan

import "log/slog"

// EventKind discriminates the three span lifecycle events a Sink receives.
type EventKind int

const (
	// EventStart records that a span began. SpanID, TraceID, ParentID,
	// Name, StartNS, and any attributes passed to Start are set.
	EventStart EventKind = iota
	// EventAttrs carries attributes recorded mid-flight via SetAttrs.
	// SpanID and TraceID identify the span; Attrs holds only the new
	// values. Last write per key wins.
	EventAttrs
	// EventEnd records that a span finished. EndNS, Status, and Error are
	// set. Name and StartNS repeat the start event's values so a sink can
	// write a complete span even when the start event was dropped.
	EventEnd
)

// Event is one span lifecycle occurrence, delivered to sinks in the order
// it happened. Which fields are meaningful depends on Kind; see the
// EventKind constants. Fields may be added compatibly in future versions.
type Event struct {
	Kind     EventKind
	SpanID   int64       // tracer-minted, monotonic, unique within the run
	TraceID  int64       // the root span's SpanID
	ParentID int64       // 0 = trace root
	Name     string      // set on EventStart and EventEnd
	StartNS  int64       // unix nanoseconds; set on EventStart and EventEnd
	EndNS    int64       // unix nanoseconds; set on EventEnd
	Status   SpanStatus  // set on EventEnd
	Error    string      // set on EventEnd when Fail recorded an error
	Attrs    []slog.Attr // set on EventStart and EventAttrs
}

// Batch is the unit of delivery to a Sink: one or more events in the order
// they occurred. It is a struct argument so fields can be added without
// breaking implementers.
type Batch struct {
	Events []Event
}

// Sink is the destination seam — the slog.Handler pattern. The tracer's
// writer goroutine is the only caller: methods are never invoked
// concurrently, so implementations need no locking.
//
// WriteBatch delivers events as the tracer's queue drains — under light
// load a batch of one is a stream. Flush ticks on the tracer's flush
// interval and is the commit/fsync moment; a sink that writes immediately
// may no-op it. Close is called exactly once, by Tracer.Close, after a
// final WriteBatch and Flush; nothing is delivered afterward.
//
// Sinks must not retain the Batch or anything reachable from it (including
// Attrs slices) after the call returns: the writer reuses buffers, and
// under MultiSink the same batch visits every sink. Copy what you keep.
//
// Returned errors never propagate to the traced program; the tracer counts
// them (Stats.WriteErrors) and optionally logs them (WithLogger).
type Sink interface {
	WriteBatch(b Batch) error
	Flush() error
	Close() error
}
