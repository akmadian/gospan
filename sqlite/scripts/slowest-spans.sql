-- The 20 slowest completed spans, each with a compact dump of its
-- attributes -- the "what took so long, and under what conditions" query.
-- Run: sqlite3 your-trace.sqlite < slowest-spans.sql
.mode box
.headers on

SELECT
    sn.trace_id,
    sn.name,
    sn.duration_ns,
    sn.status,
    sn.error,
    (SELECT group_concat(key || '=' || value, ', ')
     FROM attrs WHERE span_id = sn.id) AS attrs
FROM spans_named sn
WHERE sn.end_ns IS NOT NULL
ORDER BY sn.duration_ns DESC
LIMIT 20;
