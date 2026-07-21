# Trace analysis scripts

Generic, first-day analysis queries for any gospan trace file. Each is a
plain `sqlite3` script (with `.mode box` headers); run one against a
finished trace file:

```sh
sqlite3 ./traces/gospan-<timestamp>-<pid>.sqlite < by-name.sql
```

| Script | Answers |
|---|---|
| `by-name.sql` | Per-name count, errors, canceled, and exact min/mean/p50/p90/p99/max — the SQL ground truth behind the in-memory `Summary()`. |
| `slowest-spans.sql` | The 20 slowest completed spans, each with its attributes. |
| `triage.sql` | Failed, canceled, and incomplete (never-ended) spans. |
| `parallelism.sql` | Effective parallelism per trace: busy-time ÷ wall-clock. |
| `trace-tree.sql` | Each trace as an indented waterfall — every span nested under its parent, in start order. |

All of them read the built-in `spans_named` view (spans joined to their
names, with a derived `duration_ns`), so they work on any file the sink
writes. Times are nanoseconds. To compare across runs, `ATTACH` a second
file and union the two.
