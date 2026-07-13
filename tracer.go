package gospan

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"
)

const (
	defaultBufferSize    = 8192
	defaultFlushInterval = time.Second
)

type config struct {
	bufferSize    int
	flushInterval time.Duration
	blocking      bool
	logger        *slog.Logger
}

// Option configures a Tracer at construction.
type Option func(*config)

// WithBufferSize sets the bounded event buffer's capacity (default 8192).
// When the buffer is full, events are dropped and counted unless
// WithBlockingPolicy is set.
func WithBufferSize(n int) Option {
	return func(config *config) { config.bufferSize = n }
}

// WithFlushInterval sets the cadence at which the sink's Flush is ticked
// (default 1s) — the commit/fsync moment, and the most a hard kill loses.
func WithFlushInterval(duration time.Duration) Option {
	return func(config *config) { config.flushInterval = duration }
}

// WithBlockingPolicy makes producers block when the event buffer is full
// instead of dropping. Close unblocks any blocked producer.
func WithBlockingPolicy() Option {
	return func(config *config) { config.blocking = true }
}

// WithLogger gives the tracer a place to complain (rate-limited, Warn)
// about degraded operation: dropped batches, sink errors, recovered
// internal panics. nil means silent — degradation is still visible in
// Stats.
func WithLogger(l *slog.Logger) Option {
	return func(config *config) { config.logger = l }
}

// Tracer collects spans and delivers them to a Sink from a single writer
// goroutine. Construct one with New; a nil *Tracer is a valid, permanently
// inert tracer (every method is a no-op).
type Tracer struct {
	sink          Sink
	flushInterval time.Duration
	blocking      bool
	logger        *slog.Logger

	// anchorTime is the (wall, monotonic) pair captured once at New; every
	// timestamp is anchorTime + monotonic delta, so in-file durations are
	// immune to wall-clock jumps (SPEC §4).
	anchorTime time.Time

	events chan Event
	stop   chan struct{} // closed by Close; unblocks blocking producers

	ids      atomic.Int64 // last minted span ID; a root's trace ID is its own span ID
	dropped  atomic.Uint64
	closed   atomic.Bool
	lastWarn atomic.Int64 // unix seconds of the last logger warning
}

// New constructs a Tracer that delivers spans to s. Construction is the
// only place gospan surfaces errors; after New succeeds, nothing gospan
// does returns an error or panics into the caller.
func New(sink Sink, options ...Option) (*Tracer, error) {
	if sink == nil {
		return nil, errors.New("gospan: New called with nil Sink")
	}
	config := config{
		bufferSize:    defaultBufferSize,
		flushInterval: defaultFlushInterval,
	}
	for _, opt := range options {
		opt(&config)
	}
	if config.bufferSize <= 0 {
		return nil, fmt.Errorf("gospan: buffer size must be positive, got %d", config.bufferSize)
	}
	if config.flushInterval <= 0 {
		return nil, fmt.Errorf("gospan: flush interval must be positive, got %v", config.flushInterval)
	}
	tracer := &Tracer{
		sink:          sink,
		flushInterval: config.flushInterval,
		blocking:      config.blocking,
		logger:        config.logger,
		anchorTime:    time.Now(),
		events:        make(chan Event, config.bufferSize),
		stop:          make(chan struct{}),
	}
	return tracer, nil
}

// now returns the current time as unix nanoseconds, derived monotonically
// from the anchor: W₀ + (M_now − M₀).
func (tracer *Tracer) now() int64 {
	return tracer.anchorTime.Add(time.Since(tracer.anchorTime)).UnixNano()
}

// spanContextKey carries the current span through a context.Context.
type spanContextKey struct{}

// FromContext returns the span carried by ctx, or nil if there is none.
// A nil result is safe to use like any other span.
func FromContext(ctx context.Context) *Span {
	if ctx == nil {
		return nil
	}
	span, _ := ctx.Value(spanContextKey{}).(*Span)
	return span
}

// Start begins a span named name. The parent is read off ctx; with no span
// in ctx the new span roots a fresh trace. The returned context carries the
// new span. On a nil Tracer it returns (ctx, nil), both safe to use.
func (tracer *Tracer) Start(parent context.Context, name string, attrs ...slog.Attr) (ctx context.Context, span *Span) {
	ctx = parent // a recovered panic still returns the caller's context
	if tracer == nil {
		return ctx, nil
	}
	defer tracer.guard()

	id := tracer.ids.Add(1)
	var parentID, traceID int64
	if parentSpan := FromContext(parent); parentSpan != nil {
		parentID = parentSpan.id
		traceID = parentSpan.traceID
	} else {
		traceID = id // a root's trace ID is its own span ID
	}
	span = &Span{
		tracer:  tracer,
		id:      id,
		traceID: traceID,
		parent:  parentID,
		name:    name,
		startNS: tracer.now(),
	}
	tracer.send(Event{
		Kind:     EventStart,
		SpanID:   id,
		TraceID:  traceID,
		ParentID: parentID,
		Name:     name,
		StartNS:  span.startNS,
		Attrs:    attrs,
	})
	if parent == nil {
		parent = context.Background() // keep the nil-safety promise even for a nil ctx
	}
	return context.WithValue(parent, spanContextKey{}, span), span
}

// send enqueues an event for the writer. Full buffer: drop and count
// (default) or block until there is room (WithBlockingPolicy); Close
// unblocks blocked producers. After Close, send is a pure no-op.
//
//nolint:gocritic // hugeParam: by-value is deliberate — a pointer would make every event escape to the heap on the hot path; chunk-8 benchmarks hold this accountable
func (tracer *Tracer) send(event Event) {
	if tracer.closed.Load() {
		return
	}
	if tracer.blocking {
		select {
		case tracer.events <- event:
		case <-tracer.stop:
		}
		return
	}
	select {
	case tracer.events <- event:
	default:
		tracer.dropped.Add(1)
	}
}

// guard recovers a panic escaping a public entry point. gospan's own bugs
// are never the reason the traced program goes down: the failure is logged
// (rate-limited) and swallowed.
func (tracer *Tracer) guard() {
	if r := recover(); r != nil {
		tracer.warn("gospan: recovered internal panic", slog.Any("panic", r))
	}
}

// warn logs to the configured logger at Warn, at most once per second.
func (tracer *Tracer) warn(msg string, attrs ...slog.Attr) {
	if tracer == nil || tracer.logger == nil {
		return
	}
	now := time.Now().Unix()
	last := tracer.lastWarn.Load()
	if now == last || !tracer.lastWarn.CompareAndSwap(last, now) {
		return
	}
	tracer.logger.LogAttrs(context.Background(), slog.LevelWarn, msg, attrs...)
}
