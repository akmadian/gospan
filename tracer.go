package gospan

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// defaultBufferSize is a burst absorber, not a backpressure queue. At
	// 112 bytes per Event, 8192 is under 1 MB of flat, paid-once memory —
	// and at two events per span it holds ~4096 spans' worth of burst,
	// roughly 150ms of full-tilt production over one sink hiccup (a slow
	// commit, a GC pause, a disk stall). It is deliberately not larger:
	// sustained overload should surface as Dropped in Stats, not hide in
	// memory — a 10× buffer wouldn't prevent that problem, it would report
	// it 10× later.
	defaultBufferSize = 8192

	// defaultFlushInterval is the durability heartbeat, not delivery
	// latency (events reach the sink as the queue drains). One second is
	// where its three effects all stop mattering: a hard kill loses ≤1s of
	// trace tail, a live snapshot reads as current to a human, and a
	// buffering sink pays one transaction per second regardless of volume.
	defaultFlushInterval = time.Second

	// defaultOverheadSamplingCadence times every 128th span: measuring every
	// Start/End pair costs two extra clock reads per span — the instrument
	// taxing the pipeline it measures — and a 1-in-128 EMA converges plenty
	// fast at tracing-worthy volumes. WithOverheadSampling tunes it.
	defaultOverheadSamplingCadence = 128
)

type config struct {
	bufferSize              int
	flushInterval           time.Duration
	blockOnQueueFull        bool
	logger                  *slog.Logger
	overheadSamplingCadence int
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
	return func(config *config) { config.blockOnQueueFull = true }
}

// WithLogger gives the tracer a place to complain (rate-limited, Warn)
// about degraded operation: dropped batches, sink errors, recovered
// internal panics. nil means silent — degradation is still visible in
// Stats.
func WithLogger(l *slog.Logger) Option {
	return func(config *config) { config.logger = l }
}

// WithOverheadSampling makes every Nth span time its own tracer cost for
// Stats.OverheadPerSpan (default 128). every = 1 measures every span —
// the ultimate accuracy in span-cost measurement, at the price of two
// extra clock reads per span: the instrument taxing the pipeline it
// measures. Raise it to cheapen tracing further on hot workloads.
func WithOverheadSampling(everyNSpans int) Option {
	return func(config *config) { config.overheadSamplingCadence = everyNSpans }
}

// Tracer collects spans and delivers them to a Sink from a single writer
// goroutine. Construct one with New; a nil *Tracer is a valid, permanently
// inert tracer (every method is a no-op).
type Tracer struct {
	sink                    Sink
	flushInterval           time.Duration
	blockOnQueueFull        bool
	logger                  *slog.Logger
	overheadSamplingCadence uint64 // 1 = every span

	// anchorTime is the (wall, monotonic) pair captured once at New; every
	// timestamp is anchorTime + monotonic delta, so in-file durations are
	// immune to wall-clock jumps (SPEC §4).
	anchorTime time.Time

	events chan Event
	stop   chan struct{} // closed by Close; unblocks blocking producers, tells the writer to drain
	done   chan struct{} // closed by the writer after drain + sink close; publishes closeErr

	// closeErr is the sink's Close error, written by the writer goroutine
	// strictly before it closes done — reading it after <-done is safe.
	closeErr error

	ids         atomic.Int64 // last minted span ID; a root's trace ID is its own span ID
	dropped     atomic.Uint64
	writeErrors atomic.Uint64
	closed      atomic.Bool
	lastWarn    atomic.Int64 // unix seconds of the last logger warning

	// Stats counters and gauges (see Stats for meanings).
	started        atomic.Uint64
	completed      atomic.Uint64
	written        atomic.Uint64
	spansInFlight  atomic.Int64
	tracesInFlight atomic.Int64
	overheadNS     atomic.Int64 // EMA of sampled per-span tracer cost

	// summaries holds the per-name aggregates behind Summary(). Owned by
	// the writer goroutine; the mutex only mediates Summary() readers, so
	// producers never contend on it.
	summaryMutex sync.Mutex
	summaries    map[string]*summaryAccumulator
}

// New constructs a Tracer that delivers spans to sink and starts its
// writer goroutine. Construction is the only place gospan surfaces errors;
// after New succeeds, nothing gospan does returns an error or panics into
// the caller.
func New(sink Sink, options ...Option) (*Tracer, error) {
	if sink == nil {
		return nil, errors.New("gospan: New called with nil Sink")
	}
	config := config{
		bufferSize:              defaultBufferSize,
		flushInterval:           defaultFlushInterval,
		overheadSamplingCadence: defaultOverheadSamplingCadence,
	}
	for _, opt := range options {
		opt(&config)
	}
	// Validation happens after all options apply, not inside each Option:
	// New is the one place a caller handles errors, so bad values must
	// surface here rather than panic inside an option closure.
	if config.bufferSize <= 0 {
		return nil, fmt.Errorf("gospan: buffer size must be positive, got %d", config.bufferSize)
	}
	if config.flushInterval <= 0 {
		return nil, fmt.Errorf("gospan: flush interval must be positive, got %v", config.flushInterval)
	}
	if config.overheadSamplingCadence < 1 {
		return nil, fmt.Errorf("gospan: overhead sampling must be every 1st span or sparser, got %d", config.overheadSamplingCadence)
	}
	tracer := &Tracer{
		sink:                    sink,
		flushInterval:           config.flushInterval,
		blockOnQueueFull:        config.blockOnQueueFull,
		logger:                  config.logger,
		overheadSamplingCadence: uint64(config.overheadSamplingCadence), // validated ≥ 1 above
		anchorTime:              time.Now(),
		events:                  make(chan Event, config.bufferSize),
		stop:                    make(chan struct{}),
		done:                    make(chan struct{}),
		summaries:               make(map[string]*summaryAccumulator),
	}
	go tracer.run()
	return tracer, nil
}

// Close drains the buffer, flushes and closes the sink, and makes the
// Tracer permanently inert — subsequent calls on it are no-ops, and Close
// itself is idempotent. ctx bounds the wait: on expiry Close returns
// ctx.Err() immediately while the writer finishes draining and closes the
// sink in the background (the sink is still closed exactly once).
func (tracer *Tracer) Close(ctx context.Context) error {
	if tracer == nil {
		return nil
	}
	defer tracer.guard()
	// The CAS makes Close idempotent and sequences shutdown: closed flips
	// first so producers go inert, then stop closes — unblocking any
	// producer stuck on a full buffer and telling the writer to drain.
	// A second Close finds closed already true and just waits like the
	// first one does, so both observe the same completion.
	if tracer.closed.CompareAndSwap(false, true) {
		close(tracer.stop)
	}
	select {
	case <-tracer.done:
		return tracer.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
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

	// The started counter doubles as the sampling clock: every Nth span
	// (WithOverheadSampling, default 128) times its own Start+End tracer
	// cost for Stats.OverheadPerSpan.
	sampled := tracer.started.Add(1)%tracer.overheadSamplingCadence == 0
	var sampleBegin time.Time
	if sampled {
		sampleBegin = time.Now()
	}
	tracer.spansInFlight.Add(1)

	// One atomic counter mints every ID. Monotonic int64s keep the SQLite
	// B-tree append-only, and a child can name its parent while the parent
	// is still sitting in the buffer — DB auto-increment could do neither
	// (D2). A root reuses its own span ID as the trace ID, so trace
	// identity costs no second counter.
	id := tracer.ids.Add(1)
	var parentID, traceID int64
	if parentSpan := FromContext(parent); parentSpan != nil {
		parentID = parentSpan.id
		traceID = parentSpan.traceID
	} else {
		traceID = id
		tracer.tracesInFlight.Add(1)
	}
	span = &Span{
		tracer:  tracer,
		id:      id,
		traceID: traceID,
		parent:  parentID,
		name:    name,
		startNS: tracer.now(),
		sampled: sampled,
	}
	// Emitted at occurrence, not held until End: a crashed run keeps every
	// span that was open — usually the one you care most about — and a live
	// snapshot always shows running work (D1).
	tracer.send(Event{
		Kind:     EventStart,
		SpanID:   id,
		TraceID:  traceID,
		ParentID: parentID,
		Name:     name,
		StartNS:  span.startNS,
		Attrs:    attrs,
	})
	if sampled {
		// Half the sample: End adds its own cost and folds the sum into
		// the EMA, so OverheadPerSpan reports the full pair.
		span.startCostNS = time.Since(sampleBegin).Nanoseconds()
	}
	if parent == nil {
		parent = context.Background() // keep the nil-safety promise even for a nil ctx
	}
	return context.WithValue(parent, spanContextKey{}, span), span
}

// Track starts a leaf span and returns its closer:
//
//	defer tracer.Track(ctx, "ffmpeg-extract")()
//
// The span starts when Track is evaluated and ends when the closer runs.
// Track cannot return a context, so nothing can nest under it — leaf-only
// by construction. On a nil Tracer the returned closer is still callable.
func (tracer *Tracer) Track(ctx context.Context, name string, attrs ...slog.Attr) func() {
	// Discarding the child context is the design, not an omission: with no
	// ctx returned, nothing can ever nest under a Track span — leaf-only is
	// enforced by the signature instead of documentation. And span.End as a
	// method value is nil-safe (End on a nil *Span is a no-op), so tracing
	// off still hands back a callable closer.
	_, span := tracer.Start(ctx, name, attrs...)
	return span.End
}

// defaultTracer backs the package-level mirrors. There is no ambient
// default: until SetDefault, package-level calls are no-ops (nil is off).
var defaultTracer atomic.Pointer[Tracer]

// SetDefault makes tracer the one used by the package-level Start and
// Track — the slog.SetDefault pattern.
func SetDefault(tracer *Tracer) {
	defaultTracer.Store(tracer)
}

// Default returns the tracer set by SetDefault, or nil if none was set.
func Default() *Tracer {
	return defaultTracer.Load()
}

// Start begins a span on the default tracer. See Tracer.Start.
func Start(ctx context.Context, name string, attrs ...slog.Attr) (context.Context, *Span) {
	return Default().Start(ctx, name, attrs...)
}

// Track starts a leaf span on the default tracer. See Tracer.Track.
func Track(ctx context.Context, name string, attrs ...slog.Attr) func() {
	return Default().Track(ctx, name, attrs...)
}

// send enqueues an event for the writer. Full buffer: drop and count
// (default) or block until there is room (WithBlockingPolicy); Close
// unblocks blocked producers. After Close, send is a pure no-op.
func (tracer *Tracer) send(event Event) {
	// Closed comes first and is a pure no-op, not a drop: Dropped measures
	// buffer pressure on a live tracer, and counting post-Close sends would
	// pollute that signal with lifecycle noise.
	if tracer.closed.Load() {
		return
	}
	if tracer.blockOnQueueFull {
		// The stop channel is Close's escape hatch: a producer blocked on a
		// full buffer must never outlive the tracer, so Close closes stop
		// and every blocked send falls through (the event is lost, which
		// Close's drain semantics accept for post-Close stragglers).
		select {
		case tracer.events <- event:
		case <-tracer.stop:
		}
		return
	}
	// The default arm is the entire hot-path promise: a full buffer costs
	// one atomic increment and returns — producers never stall on the
	// destination, no matter how sick it is.
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
	// recover only works when called directly by the deferred function, so
	// every public entry writes "defer tracer.guard()" rather than wrapping
	// its body. This catches gospan's own bugs only — a caller's panic
	// passes through untouched (their defers, including span.End, still
	// run; that's how a panicking span gets its end time).
	if r := recover(); r != nil {
		tracer.warn("gospan: recovered internal panic", slog.Any("panic", r))
	}
}

// warn logs to the configured logger at Warn, at most once per second.
func (tracer *Tracer) warn(msg string, attrs ...slog.Attr) {
	if tracer == nil || tracer.logger == nil {
		return
	}
	// At most one warning per wall-clock second, coordinated by CAS: every
	// caller in the same second sees now == last and stays quiet, and of
	// the racers crossing into a new second exactly one wins the swap.
	// Losing a complaint is fine — flooding the caller's log is not, and
	// the counters in Stats never lose anything.
	now := time.Now().Unix()
	last := tracer.lastWarn.Load()
	if now == last || !tracer.lastWarn.CompareAndSwap(last, now) {
		return
	}
	// The logger is user code, and warn runs in the two places a second
	// panic would escape all containment: inside guard's recover handler
	// (its recover is already spent) and inside the writer loop (an
	// unrecovered goroutine panic kills the process). So warn shields
	// itself — silently, because complaining about the complaint channel
	// has nowhere left to go; Stats still counted the original failure.
	defer func() { _ = recover() }()
	tracer.logger.LogAttrs(context.Background(), slog.LevelWarn, msg, attrs...)
}
