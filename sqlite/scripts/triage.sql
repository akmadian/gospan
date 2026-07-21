-- Failures and incompletes: spans that ended badly (error or canceled) or
-- never ended at all (end_ns IS NULL -- a crash, os.Exit, or a dropped end
-- event). The first place to look after a run went wrong.
-- Run: sqlite3 your-trace.sqlite < triage.sql
.mode box
.headers on

SELECT
    trace_id,
    name,
    CASE
        WHEN end_ns IS NULL THEN 'incomplete'
        WHEN status = 1      THEN 'error'
        WHEN status = 2      THEN 'canceled'
    END          AS outcome,
    duration_ns,
    error
FROM spans_named
WHERE end_ns IS NULL OR status <> 0
ORDER BY trace_id, start_ns;
