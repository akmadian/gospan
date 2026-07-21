-- Each trace as an indented waterfall: every span nested under its parent,
-- in start order, with its duration and status. The "where did the time go
-- inside this run" view -- a text flamegraph. Roots are the spans with no
-- parent; children sort beneath their parent by start time.
-- Run: sqlite3 your-trace.sqlite < trace-tree.sql
.mode box
.headers on

WITH RECURSIVE tree AS (
    SELECT id, trace_id, name, duration_ns, status, 0 AS depth,
           printf('%020d', start_ns) AS sort_key
    FROM spans_named
    WHERE parent_id IS NULL
    UNION ALL
    SELECT child.id, child.trace_id, child.name, child.duration_ns,
           child.status, parent.depth + 1,
           parent.sort_key || '/' || printf('%020d', child.start_ns)
    FROM spans_named child
    JOIN tree parent ON child.parent_id = parent.id
)
SELECT
    trace_id,
    printf('%*s%s', depth * 2, '', name)                              AS span,
    duration_ns,
    CASE status WHEN 1 THEN 'error' WHEN 2 THEN 'canceled' ELSE '' END AS status
FROM tree
ORDER BY trace_id, sort_key;
