-- SQLite database schema for Cross Country Statistics Tracker

-- Analytics table for tracking page views
CREATE TABLE IF NOT EXISTS page_views (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    page_path VARCHAR(500) NOT NULL,
    user_agent TEXT,
    ip_address TEXT,
    referer TEXT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    session_id VARCHAR(100)
);

-- Index for analytics queries
CREATE INDEX IF NOT EXISTS idx_page_views_timestamp ON page_views(timestamp);
CREATE INDEX IF NOT EXISTS idx_page_views_page_path ON page_views(page_path);

-- Athletes table
CREATE TABLE IF NOT EXISTS athletes (
    id TEXT PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    gender VARCHAR(10) NOT NULL CHECK (gender IN ('male', 'female')),
    school VARCHAR(200),
    graduation_year INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Venues table
CREATE TABLE IF NOT EXISTS venues (
    id TEXT PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    location VARCHAR(200),
    state VARCHAR(50),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Meets table
CREATE TABLE IF NOT EXISTS meets (
    id TEXT PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    meet_date DATE NOT NULL,
    venue_id TEXT REFERENCES venues(id),
    season VARCHAR(10) NOT NULL,
    milesplit_url TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Races table (individual races within a meet)
CREATE TABLE IF NOT EXISTS races (
    id TEXT PRIMARY KEY,
    meet_id TEXT REFERENCES meets(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    distance VARCHAR(20) NOT NULL,
    race_class VARCHAR(20) NOT NULL,
    gender VARCHAR(10) NOT NULL CHECK (gender IN ('male', 'female', 'mixed')),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Results table
CREATE TABLE IF NOT EXISTS results (
    id TEXT PRIMARY KEY,
    race_id TEXT REFERENCES races(id) ON DELETE CASCADE,
    athlete_id TEXT REFERENCES athletes(id) ON DELETE CASCADE,
    time_seconds REAL NOT NULL,
    place INTEGER,
    varsity_points INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for better performance
CREATE INDEX IF NOT EXISTS idx_athletes_name ON athletes(last_name, first_name);
CREATE INDEX IF NOT EXISTS idx_athletes_gender ON athletes(gender);
CREATE INDEX IF NOT EXISTS idx_meets_date ON meets(meet_date);
CREATE INDEX IF NOT EXISTS idx_meets_season ON meets(season);
CREATE INDEX IF NOT EXISTS idx_races_meet ON races(meet_id);
CREATE INDEX IF NOT EXISTS idx_races_class_gender ON races(race_class, gender);
CREATE INDEX IF NOT EXISTS idx_results_race ON results(race_id);
CREATE INDEX IF NOT EXISTS idx_results_athlete ON results(athlete_id);
CREATE INDEX IF NOT EXISTS idx_results_time ON results(time_seconds);

-- View for easy PR queries
CREATE VIEW IF NOT EXISTS athlete_prs AS
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

-- View for team statistics
CREATE VIEW IF NOT EXISTS team_stats AS
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


