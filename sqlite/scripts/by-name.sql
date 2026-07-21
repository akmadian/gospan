-- Per-span-name summary: counts, error/cancel tallies, and exact duration
-- percentiles -- the SQL ground truth behind the tracer's in-memory,
-- approximate Summary(). SQLite has no PERCENTILE function, so durations are
-- ranked within each name and picked by nearest rank (matching Summary's
-- definition). Run: sqlite3 your-trace.sqlite < by-name.sql
.mode box
.headers on

WITH ranked AS (
    SELECT
        name,
        status,
        duration_ns,
        ROW_NUMBER() OVER (PARTITION BY name ORDER BY duration_ns) AS rank_in_name,
        COUNT(*)     OVER (PARTITION BY name)                      AS name_count
    FROM spans_named
    WHERE end_ns IS NOT NULL
)
SELECT
    name,
    name_count                        AS count,
    SUM(status = 1)                   AS errors,
    SUM(status = 2)                   AS canceled,
    MIN(duration_ns)                  AS min_ns,
    CAST(AVG(duration_ns) AS INTEGER) AS mean_ns,
    MAX(CASE WHEN rank_in_name = MAX(1, CAST(ROUND(0.50 * name_count) AS INTEGER)) THEN duration_ns END) AS p50_ns,
    MAX(CASE WHEN rank_in_name = MAX(1, CAST(ROUND(0.90 * name_count) AS INTEGER)) THEN duration_ns END) AS p90_ns,
    MAX(CASE WHEN rank_in_name = MAX(1, CAST(ROUND(0.99 * name_count) AS INTEGER)) THEN duration_ns END) AS p99_ns,
    MAX(duration_ns)                  AS max_ns
FROM ranked
GROUP BY name
ORDER BY count DESC;
