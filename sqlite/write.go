package sqlite

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/akmadian/gospan"
)

// spanRow is one pending spans-table row. A start and its end landing in
// the same flush window fold into a single complete row here, so only
// spans that outlive a flush interval ever pay a second write (D1).
type spanRow struct {
	id       int64
	traceID  int64
	parentID int64 // 0 = trace root, stored as NULL
	name     string
	startNS  int64
	endNS    int64 // 0 = still running, stored as NULL
	status   gospan.SpanStatus
	errText  string
}

// attrRow is one pending attrs-table row, already resolved and flattened.
// INSERT OR REPLACE on the (span_id, key) primary key makes last-wins a
// schema property, so repeats across SetAttrs calls need no bookkeeping.
type attrRow struct {
	spanID int64
	key    string
	kind   slog.Kind
	value  any
}

// WriteBatch folds the batch into the pending buffers: span starts open a
// row, ends complete their pending row when present (the coalesced common
// case) or queue an upsert when the start already flushed — or never
// arrived; end events carry Name and StartNS so an orphan still becomes a
// complete row. Attrs resolve and flatten immediately, which also
// satisfies the no-retention contract: nothing reachable from the Batch
// survives this call.
func (sink *Sink) WriteBatch(batch gospan.Batch) error {
	for _, event := range batch.Events {
		switch event.Kind {
		case gospan.EventStart:
			row := &spanRow{
				id:       event.SpanID,
				traceID:  event.TraceID,
				parentID: event.ParentID,
				name:     event.Name,
				startNS:  event.StartNS,
			}
			sink.pendingSpans = append(sink.pendingSpans, row)
			sink.pendingByID[event.SpanID] = row
			flattenAttrs("", event.Attrs, func(key string, value slog.Value) {
				sink.pendingAttrs = append(sink.pendingAttrs, newAttrRow(event.SpanID, key, value))
			})
		case gospan.EventAttrs:
			flattenAttrs("", event.Attrs, func(key string, value slog.Value) {
				sink.pendingAttrs = append(sink.pendingAttrs, newAttrRow(event.SpanID, key, value))
			})
		case gospan.EventEnd:
			if row := sink.pendingByID[event.SpanID]; row != nil {
				row.endNS = event.EndNS
				row.status = event.Status
				row.errText = event.Error
				continue
			}
			// The start flushed earlier (or was dropped): a full row rides
			// the upsert, completing either way.
			sink.pendingSpans = append(sink.pendingSpans, &spanRow{
				id:       event.SpanID,
				traceID:  event.TraceID,
				parentID: event.ParentID,
				name:     event.Name,
				startNS:  event.StartNS,
				endNS:    event.EndNS,
				status:   event.Status,
				errText:  event.Error,
			})
		default:
			// Unknown future kind: skip, never fail.
		}
	}
	return nil
}

// Flush commits everything pending in one transaction — the durability
// moment the tracer ticks on its flush interval. On failure the pending
// buffers are dropped, not retried: unbounded retry against a sick disk
// would hoard memory forever, and the tracer already counts the loss
// (drop-and-count, DESIGN §4).
func (sink *Sink) Flush() error {
	if len(sink.pendingSpans) == 0 && len(sink.pendingAttrs) == 0 {
		return nil
	}
	defer sink.clearPending()

	tx, err := sink.db.Begin()
	if err != nil {
		return fmt.Errorf("gospan/sqlite: beginning commit: %w", err)
	}
	if err := sink.writePending(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("gospan/sqlite: committing: %w", err)
	}
	// The commit is durable: the names inserted this flush now exist on
	// disk, so promote them into the committed cache for reuse. Only after a
	// successful commit — never before — so a rolled-back flush can never
	// poison nameIDs (see internName).
	for name, id := range sink.internedThisFlush {
		sink.nameIDs[name] = id
	}
	return nil
}

func (sink *Sink) clearPending() {
	sink.pendingSpans = sink.pendingSpans[:0]
	sink.pendingAttrs = sink.pendingAttrs[:0]
	clear(sink.pendingByID)
	clear(sink.internedThisFlush)
}

func (sink *Sink) writePending(tx *sql.Tx) error {
	// The upsert covers all three span lifecycles with one statement:
	// fresh complete rows insert, a bare start inserts with NULL end_ns,
	// and an end whose start committed earlier conflicts on the primary
	// key and completes the existing row.
	upsertSpan, err := tx.Prepare(`
		INSERT INTO spans (id, trace_id, parent_id, name_id, start_ns, end_ns, status, error)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET
			end_ns = excluded.end_ns,
			status = excluded.status,
			error  = excluded.error`)
	if err != nil {
		return fmt.Errorf("gospan/sqlite: preparing span upsert: %w", err)
	}
	defer upsertSpan.Close()

	for _, row := range sink.pendingSpans {
		nameID, err := sink.internName(tx, row.name)
		if err != nil {
			return err
		}
		var parentID, endNS, errText any // NULL unless present
		if row.parentID != 0 {
			parentID = row.parentID
		}
		if row.endNS != 0 {
			endNS = row.endNS
		}
		if row.errText != "" {
			errText = row.errText
		}
		if _, err := upsertSpan.Exec(row.id, row.traceID, parentID, nameID, row.startNS, endNS, int(row.status), errText); err != nil {
			return fmt.Errorf("gospan/sqlite: writing span %d: %w", row.id, err)
		}
	}

	if len(sink.pendingAttrs) == 0 {
		return nil
	}
	replaceAttr, err := tx.Prepare(
		`INSERT OR REPLACE INTO attrs (span_id, key, kind, value) VALUES (?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("gospan/sqlite: preparing attr write: %w", err)
	}
	defer replaceAttr.Close()
	for _, row := range sink.pendingAttrs {
		if _, err := replaceAttr.Exec(row.spanID, row.key, int(row.kind), row.value); err != nil {
			return fmt.Errorf("gospan/sqlite: writing attr %q of span %d: %w", row.key, row.spanID, err)
		}
	}
	return nil
}

// internName returns the names-table id for name, inserting on first
// sight. Span names are a small, stable set (~dozens), so the in-memory
// map makes interning one lookup on the steady path.
func (sink *Sink) internName(tx *sql.Tx, name string) (int64, error) {
	// Committed names first, then names already inserted earlier in THIS
	// transaction — either way a hit avoids a duplicate INSERT.
	if id, seen := sink.nameIDs[name]; seen {
		return id, nil
	}
	if id, seen := sink.internedThisFlush[name]; seen {
		return id, nil
	}
	result, err := tx.Exec("INSERT INTO names (name) VALUES (?)", name)
	if err != nil {
		return 0, fmt.Errorf("gospan/sqlite: interning name %q: %w", name, err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("gospan/sqlite: reading interned id for %q: %w", name, err)
	}
	// Held aside, not committed: promoted into nameIDs only after this
	// flush's transaction commits (see Flush). Writing straight to nameIDs
	// here would survive a rollback and leave a span pointing at a names row
	// that was rolled back — the span then vanishes from, or is mislabeled
	// by, the canonical spans⨝names join, silently, on the degraded path.
	sink.internedThisFlush[name] = id
	return id, nil
}

// flattenAttrs walks attrs depth-first, resolving LogValuers and
// flattening groups into dot-joined keys (SPEC §2): group "db" holding
// "query" emits the single key "db.query". Attrs with empty keys outside
// a group are dropped, following slog's own convention.
func flattenAttrs(prefix string, attrs []slog.Attr, emit func(key string, value slog.Value)) {
	for _, attr := range attrs {
		value := attr.Value.Resolve() // LogValuer indirection; Resolve contains its own panics
		if value.Kind() == slog.KindGroup {
			groupPrefix := prefix
			if attr.Key != "" {
				groupPrefix = prefix + attr.Key + "."
			}
			flattenAttrs(groupPrefix, value.Group(), emit)
			continue
		}
		if attr.Key == "" {
			continue
		}
		emit(prefix+attr.Key, value)
	}
}

func newAttrRow(spanID int64, key string, value slog.Value) attrRow {
	kind, cell := attrCell(value)
	return attrRow{spanID: spanID, key: key, kind: kind, value: cell}
}

// attrCell maps a resolved slog.Value onto a natively-typed SQLite cell
// plus the slog.Kind the viewer renders by: durations stay durations, not
// mystery integers, because the kind column says what the integer means.
func attrCell(value slog.Value) (slog.Kind, any) {
	kind := value.Kind()
	switch kind {
	case slog.KindString:
		return kind, value.String()
	case slog.KindInt64:
		return kind, value.Int64()
	case slog.KindUint64:
		// Bit-preserving reinterpretation: SQLite INTEGER is int64, and
		// the kind column tells readers to view the bits as unsigned —
		// nothing is lost, even above MaxInt64.
		return kind, int64(value.Uint64()) //nolint:gosec // G115: deliberate two's-complement round-trip
	case slog.KindFloat64:
		return kind, value.Float64()
	case slog.KindBool:
		return kind, value.Bool()
	case slog.KindDuration:
		return kind, value.Duration().Nanoseconds()
	case slog.KindTime:
		return kind, value.Time().UnixNano()
	case slog.KindAny, slog.KindGroup, slog.KindLogValuer:
		// Group and LogValuer never reach here (flattened and resolved
		// above); Any formats to text — typed queries need typed
		// constructors (D3).
		return slog.KindAny, fmt.Sprintf("%v", value.Any())
	default:
		return slog.KindAny, fmt.Sprintf("%v", value.Any())
	}
}
