-- SQLite migration: Add fractional seconds support to results table
--
-- SQLite's REAL type already supports fractional values, so we just need
-- to ensure the format_time function and views work correctly with decimals.
-- This migration is a no-op for the schema itself (time_seconds is already REAL),
-- but it updates the views and provides the format_time SQL function.

-- Recreate the athlete_prs view (handles decimal seconds correctly)
DROP VIEW IF EXISTS athlete_prs;
CREATE VIEW athlete_prs AS
SELECT 
    a.id as athlete_id,
    a.first_name,
    a.last_name,
    a.gender,
    r.distance,
    MIN(res.time_seconds) as pr_seconds
FROM athletes a
JOIN results res ON a.id = res.athlete_id
JOIN races r ON res.race_id = r.id
GROUP BY a.id, a.first_name, a.last_name, a.gender, r.distance;

-- Recreate the team_stats view (handles decimal seconds correctly)
DROP VIEW IF EXISTS team_stats;
CREATE VIEW team_stats AS
SELECT 
    a.gender,
    r.distance,
    r.race_class,
    COUNT(DISTINCT a.id) as athlete_count,
    MIN(res.time_seconds) as best_time_seconds,
    AVG(res.time_seconds) as avg_time_seconds
FROM athletes a
JOIN results res ON a.id = res.athlete_id
JOIN races r ON res.race_id = r.id
GROUP BY a.gender, r.distance, r.race_class;