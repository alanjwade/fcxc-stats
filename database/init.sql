-- Database initialization script for Cross Country Statistics Tracker

-- Create database extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Athletes table
CREATE TABLE athletes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    gender VARCHAR(10) NOT NULL CHECK (gender IN ('male', 'female')),
    graduation_year INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Venues table
CREATE TABLE venues (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(200) NOT NULL,
    location VARCHAR(200),
    state VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Meets table
CREATE TABLE meets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(200) NOT NULL,
    meet_date DATE NOT NULL,
    venue_id UUID REFERENCES venues(id),
    season VARCHAR(10) NOT NULL,
    milesplit_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Races table (individual races within a meet)
CREATE TABLE races (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    meet_id UUID REFERENCES meets(id) ON DELETE CASCADE,
    distance VARCHAR(20) NOT NULL, -- e.g., '5K', '3K', '1600m'
    race_class VARCHAR(20) NOT NULL, -- e.g., 'varsity', 'jv', 'freshman'
    gender VARCHAR(10) NOT NULL CHECK (gender IN ('male', 'female', 'mixed')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Results table
CREATE TABLE results (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    race_id UUID REFERENCES races(id) ON DELETE CASCADE,
    athlete_id UUID REFERENCES athletes(id) ON DELETE CASCADE,
    time_seconds INTEGER NOT NULL, -- Time in seconds for easier calculations
    place INTEGER,
    varsity_points INTEGER DEFAULT 0, -- Points scored for varsity team
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for better performance
CREATE INDEX idx_athletes_name ON athletes(last_name, first_name);
CREATE INDEX idx_athletes_gender ON athletes(gender);
CREATE INDEX idx_meets_date ON meets(meet_date);
CREATE INDEX idx_meets_season ON meets(season);
CREATE INDEX idx_races_meet ON races(meet_id);
CREATE INDEX idx_races_class_gender ON races(race_class, gender);
CREATE INDEX idx_results_race ON results(race_id);
CREATE INDEX idx_results_athlete ON results(athlete_id);
CREATE INDEX idx_results_time ON results(time_seconds);

-- Create a view for easy PR queries
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

-- Create a view for team statistics
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

-- Function to format time from seconds to MM:SS format
CREATE OR REPLACE FUNCTION format_time(seconds INTEGER)
RETURNS TEXT AS $$
BEGIN
    RETURN LPAD((seconds / 60)::TEXT, 2, '0') || ':' || 
           LPAD((seconds % 60)::TEXT, 2, '0');
END;
$$ LANGUAGE plpgsql;
