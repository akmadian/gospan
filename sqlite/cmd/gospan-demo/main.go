// Command gospan-demo records one small, realistic trace and fans it out to
// two destinations at once: a slog logger (the lines that stream by as spans
// finish) and a SQLite file. It then prints the tracer's in-memory aggregates
// (Summary and Stats) and reopens the file to rebuild the trace as a
// waterfall with plain SQL — the whole point of the SQLite destination: when
// a run ends, your traces are an ordinary database you can query.
//
// Run it from the sqlite module:
//
//	cd sqlite && go run ./cmd/gospan-demo
//
// It writes gospan-demo.sqlite (overwriting any prior run), prints three
// small tables, and leaves the file behind with a hint for exploring it.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/akmadian/gospan"
	"github.com/akmadian/gospan/sqlite"
)

func main() {
	// A library never exits the process, but a demo command may: log.Fatalf
	// reports the failure and stops. Everything gospan itself can fail at
	// surfaces here, at construction, and nowhere later.
	if err := run(); err != nil {
		log.Fatalf("gospan-demo: %v", err)
	}
}

func run() error {
	// Write gospan-demo.sqlite into the current directory, overwriting any
	// prior run, so the file has a short, stable path you can hand to sqlite3
	// afterward — exactly what sqlite.WithName is for. Importing this package
	// also registers the modernc driver, so the database/sql read-back below
	// needs no separate import.
	sink, err := sqlite.New(".", sqlite.WithName("gospan-demo.sqlite", true))
	if err != nil {
		return fmt.Errorf("opening trace sink: %w", err)
	}

	// A slog logger for the live half of the demo: a text handler to stdout
	// (so its lines stay in order with the tables printed below), with the
	// timestamp dropped so the output is stable enough to screenshot.
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return attr
		},
	}))

	// MultiSink fans every span out to both destinations: the logger (spans
	// join your existing log flow, live) and the file (queryable afterward).
	// WithOverheadSampling(1) times every span rather than the default
	// 1-in-128, so a handful-of-spans demo still reports a real per-span cost
	// in Stats instead of zero.
	tracer, err := gospan.New(
		gospan.MultiSink(gospan.SlogSink(logger), sink),
		gospan.WithOverheadSampling(1),
	)
	if err != nil {
		return fmt.Errorf("constructing tracer: %w", err)
	}

	recordTrace(tracer)

	// Close drains the buffer, commits the final flush, and closes the file.
	// After it returns the in-memory aggregates are complete and the trace is
	// ours to read back.
	if err := tracer.Close(context.Background()); err != nil {
		return fmt.Errorf("closing tracer: %w", err)
	}

	reportSummary(tracer) // in memory: how your code did
	reportStats(tracer)   // in memory: what tracing itself cost
	if err := reportTree(sink.Path()); err != nil {
		return err // from SQL: the shape of the run
	}

	fmt.Fprintf(os.Stdout, "\ntrace file: %s   (a plain SQLite database)\n", sink.Path())
	fmt.Fprintf(os.Stdout, "explore it:  sqlite3 %s < scripts/trace-tree.sql\n", sink.Path())
	return nil
}

// recordTrace produces one small trace: a root span carrying a couple of
// attributes, two child spans (one of which fails), and a Track leaf. The
// brief sleeps exist only so the durations differ enough to rank — real work
// would supply its own time.
func recordTrace(tracer *gospan.Tracer) {
	ctx, root := tracer.Start(context.Background(), "process-asset",
		slog.String("asset", "sunset.raw"),
		slog.Int64("bytes", 4_194_304))
	defer root.End()

	// Child one: decode the image. A Track leaf (read-exif) nests beneath it —
	// Track is leaf-only by construction, so nothing nests under the leaf in
	// turn.
	decodeCtx, decode := tracer.Start(ctx, "decode-image")
	func() {
		defer tracer.Track(decodeCtx, "read-exif")()
		time.Sleep(2 * time.Millisecond)
	}()
	time.Sleep(6 * time.Millisecond)
	decode.End()

	// Child two: resize the image — and fail. Fail records why the span did
	// not succeed; End still ends it, and the status lands in the file as 1
	// (error). The duration is measured regardless.
	_, resize := tracer.Start(ctx, "resize-image", slog.Int("target_px", 512))
	time.Sleep(4 * time.Millisecond)
	resize.Fail(errors.New("unsupported color profile"))
	resize.End()
}

// reportSummary prints the tracer's in-memory, per-name aggregates — the "how
// did your code do" view, available without touching the file. Called after
// Close, so every span has been folded in.
func reportSummary(tracer *gospan.Tracer) {
	summary := tracer.Summary()
	names := make([]string, 0, len(summary))
	for name := range summary {
		names = append(names, name)
	}
	sort.Strings(names) // map order is random; a stable table reads better

	// Count and Errors are exact. Summary's percentiles are deliberately
	// approximate (a lock-free histogram, ~12.5% error), so showing them next
	// to the file's exact durations would look inconsistent — exact
	// percentiles are one SQL query away (scripts/by-name.sql), not here.
	fmt.Fprintln(os.Stdout, "span summary (in memory):")
	table := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(table, "  SPAN\tCOUNT\tERRORS")
	for _, name := range names {
		stat := summary[name]
		fmt.Fprintf(table, "  %s\t%d\t%d\n", name, stat.Count, stat.Errors)
	}
	_ = table.Flush()
}

// reportStats prints the tracer's self-health — what tracing cost and what, if
// anything, it dropped. Nothing drops in a demo; OverheadPerSpan is a real
// measurement because the tracer sampled every span.
func reportStats(tracer *gospan.Tracer) {
	stats := tracer.Stats()
	// The intuitive few: spans in and out, whether anything was dropped, and
	// the per-span cost. Stats also exposes event-level counters (Written,
	// WriteErrors) that count start/end events, not spans, so they read oddly
	// next to a four-span trace — left to the API, not shown here.
	fmt.Fprintf(os.Stdout,
		"\ntracer stats: started=%d completed=%d dropped=%d overhead=%s/span\n",
		stats.Started, stats.Completed, stats.Dropped, stats.OverheadPerSpan)
}

// reportTree opens the finished file and rebuilds the trace as a waterfall:
// each span indented under its parent, in start order, with its duration and
// status. The query is the recursive-CTE walk also shipped as
// scripts/trace-tree.sql — the "where did the time go in this run" picture,
// straight from SQL.
func reportTree(path string) error {
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		return fmt.Errorf("opening trace file: %w", err)
	}
	defer func() { _ = db.Close() }()

	const query = `
WITH RECURSIVE tree AS (
    SELECT id, name, duration_ns, status, 0 AS depth,
           printf('%020d', start_ns) AS sort_key
    FROM spans_named
    WHERE parent_id IS NULL
    UNION ALL
    SELECT child.id, child.name, child.duration_ns, child.status,
           parent.depth + 1,
           parent.sort_key || '/' || printf('%020d', child.start_ns)
    FROM spans_named child
    JOIN tree parent ON child.parent_id = parent.id
)
SELECT depth, name, duration_ns, status FROM tree ORDER BY sort_key`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("querying trace file: %w", err)
	}
	defer func() { _ = rows.Close() }()

	fmt.Fprintln(os.Stdout, "\ntrace waterfall (from SQL):")
	table := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	for rows.Next() {
		var depth int
		var name string
		var durationNS sql.NullInt64
		var status int
		if err := rows.Scan(&depth, &name, &durationNS, &status); err != nil {
			return fmt.Errorf("scanning row: %w", err)
		}
		duration := "running"
		if durationNS.Valid {
			duration = time.Duration(durationNS.Int64).String()
		}
		fmt.Fprintf(table, "  %s%s\t%s\t%s\n",
			strings.Repeat("  ", depth), name, duration, statusLabel(status))
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading rows: %w", err)
	}
	return table.Flush()
}

// statusLabel renders a span's stored status for the waterfall: blank for a
// clean finish, gospan's own word ("error"/"canceled") otherwise.
func statusLabel(status int) string {
	if gospan.SpanStatus(status) == gospan.SpanStatusOK {
		return ""
	}
	return gospan.SpanStatus(status).String()
}
