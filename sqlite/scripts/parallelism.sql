-- Effective parallelism per trace: total span busy-time divided by the
-- trace's wall-clock span. ~1 means the work ran serially; N means N-way
-- overlap on average -- a quick read on whether a pipeline is actually
-- using its workers. Only completed spans count toward busy-time.
-- Run: sqlite3 your-trace.sqlite < parallelism.sql
.mode box
.headers on

SELECT
    trace_id,
    COUNT(*)                     AS spans,
    MAX(end_ns) - MIN(start_ns)  AS wall_ns,
    SUM(duration_ns)             AS busy_ns,
    ROUND(CAST(SUM(duration_ns) AS REAL) /
          NULLIF(MAX(end_ns) - MIN(start_ns), 0), 2) AS parallelism
FROM spans_named
WHERE end_ns IS NOT NULL
GROUP BY trace_id
ORDER BY parallelism DESC;
