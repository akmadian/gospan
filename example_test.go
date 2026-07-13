package gospan_test

import (
	"context"
	"errors"
	"log/slog"

	"github.com/akmadian/gospan"
)

// The quickstart: pick a destination, construct once, set as default,
// instrument with one-liners everywhere else.
func Example() {
	tracer, err := gospan.New(gospan.SlogSink(slog.Default()))
	if err != nil {
		// Construction is the only moment gospan can fail.
		panic(err)
	}
	defer tracer.Close(context.Background())
	gospan.SetDefault(tracer)

	ctx, span := gospan.Start(context.Background(), "process-asset",
		slog.String("asset", "a.raw"))
	defer span.End()

	resizeImage(ctx)
}

func resizeImage(ctx context.Context) {
	defer gospan.Track(ctx, "resize-image")() // one-line leaf span
	decodeAndScalePixels()
}

func decodeAndScalePixels() {}

// Fail records why a span didn't succeed; End still ends it. Cancellation
// is classified automatically from the error value.
func ExampleSpan_Fail() {
	tracer, _ := gospan.New(gospan.SlogSink(slog.Default()))
	defer tracer.Close(context.Background())

	processAsset := func(ctx context.Context) error {
		ctx, span := tracer.Start(ctx, "process-asset")
		defer span.End()

		if err := extractFrames(ctx); err != nil {
			span.Fail(err) // status=error, or canceled if errors.Is says so
			return err
		}
		return nil
	}
	_ = processAsset(context.Background())
}

func extractFrames(context.Context) error { return errors.New("boom") }

// Channel pipelines: ctx follows the call stack, but items cross
// goroutines through channels — so the span context travels on the item,
// and FromContext closes the root at the final stage.
func ExampleFromContext() {
	tracer, _ := gospan.New(gospan.SlogSink(slog.Default()))
	defer tracer.Close(context.Background())

	type pipelineItem struct {
		ctx  context.Context // carries this item's root span
		path string
	}

	// Intake mints the item's root trace.
	item := pipelineItem{path: "/a.raw"}
	item.ctx, _ = tracer.Start(context.Background(), "asset",
		slog.String("path", item.path))

	// Each stage nests under the item's root, wherever it runs.
	_, hashSpan := tracer.Start(item.ctx, "hash")
	hashSpan.End()

	// The final stage closes the root.
	gospan.FromContext(item.ctx).End()
}

// A semaphore wait is just a child span: the duration IS the wait time,
// the numbers are attributes.
func ExampleTracer_Start_semaphoreWait() {
	tracer, _ := gospan.New(gospan.SlogSink(slog.Default()))
	defer tracer.Close(context.Background())
	ctx := context.Background()

	_, waitSpan := tracer.Start(ctx, "acquire-budget",
		slog.Int64("tokens_requested", 4), slog.Int64("budget_total", 16))
	acquireTokens(ctx, 4)
	waitSpan.End()
}

func acquireTokens(context.Context, int64) {}

// Live log lines AND the post-run trace file: fan-out is itself a Sink,
// so composition costs the tracer nothing.
func ExampleMultiSink() {
	fileSink := gospan.SlogSink(slog.Default()) // stand-in for sqlite.New("./traces")
	tracer, _ := gospan.New(gospan.MultiSink(
		gospan.SlogSink(slog.Default()),
		fileSink,
	))
	defer tracer.Close(context.Background())

	gospan.SetDefault(tracer)
}

// Summary answers "how is my code doing" live, without waiting for the
// file; Stats answers "how is the tracer doing".
func ExampleTracer_Summary() {
	tracer, _ := gospan.New(gospan.SlogSink(slog.Default()))
	defer tracer.Close(context.Background())

	_, span := tracer.Start(context.Background(), "extract-frames")
	span.End()

	extract := tracer.Summary()["extract-frames"]
	slog.Info("extract", "p90", extract.P90, "count", extract.Count, "errors", extract.Errors)

	stats := tracer.Stats()
	slog.Info("tracer health", "dropped", stats.Dropped, "overhead", stats.OverheadPerSpan)
}
