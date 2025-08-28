-- Update database schema to support fractional seconds
-- This script converts time_seconds from INTEGER to NUMERIC(8,3)
-- to support times like 16:45.123 (stored as 1005.123 seconds)

BEGIN;

-- Drop dependent views first
DROP VIEW IF EXISTS athlete_prs;
DROP VIEW IF EXISTS team_stats;

-- First, let's add the new column with fractional seconds support
ALTER TABLE results ADD COLUMN time_seconds_decimal NUMERIC(8,3);

-- Copy existing integer seconds to the new decimal column
UPDATE results SET time_seconds_decimal = time_seconds::NUMERIC(8,3);

-- Drop the old integer column
ALTER TABLE results DROP COLUMN time_seconds CASCADE;

-- Rename the new column to the original name
ALTER TABLE results RENAME COLUMN time_seconds_decimal TO time_seconds;

-- Add NOT NULL constraint
ALTER TABLE results ALTER COLUMN time_seconds SET NOT NULL;

-- Recreate the index
DROP INDEX IF EXISTS idx_results_time;
CREATE INDEX idx_results_time ON results(time_seconds);

-- Update the athlete_prs view to handle decimal seconds
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

-- Update the team_stats view to handle decimal seconds
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

-- Update the format_time function to handle fractional seconds
-- New format: MM:SS.ss (minutes:seconds.centiseconds)
DROP FUNCTION IF EXISTS format_time(INTEGER);
CREATE OR REPLACE FUNCTION format_time(seconds NUMERIC)
RETURNS TEXT AS $$
DECLARE
    total_seconds INTEGER;
    fractional_part NUMERIC;
    minutes INTEGER;
    secs INTEGER;
    centiseconds INTEGER;
BEGIN
    -- Handle NULL input
    IF seconds IS NULL THEN
        RETURN NULL;
    END IF;
    
    -- Split into whole seconds and fractional part
    total_seconds := FLOOR(seconds)::INTEGER;
    fractional_part := seconds - total_seconds;
    
    -- Calculate minutes and remaining seconds
    minutes := total_seconds / 60;
    secs := total_seconds % 60;
    
    -- Get centiseconds (hundredths)
    centiseconds := ROUND(fractional_part * 100)::INTEGER;
    
    -- Format as MM:SS.ss
    RETURN LPAD(minutes::TEXT, 2, '0') || ':' || 
           LPAD(secs::TEXT, 2, '0') || '.' ||
           LPAD(centiseconds::TEXT, 2, '0');
END;
$$ LANGUAGE plpgsql;

COMMIT;
