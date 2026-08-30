#!/usr/bin/env python3
"""
Cross Country Statistics We    # Exclude localhost/development IPs
    if client_ip in EXCLUDED_IPS:
        return Falsepplication

Flask web application that provides:
1. CSV export of athlete performance data
2. Team statistics dashboard
3. Individual athlete statistics
4. Main navigation dashboard
"""

import os
import io
import csv
import uuid
import random
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
import decimal
from flask import Flask, render_template, jsonify, send_file, request, redirect, url_for
from sqlalchemy import create_engine, text
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'your-secret-key-here')

# Release tag embedded in the image at build time (Dockerfile ARG APP_VERSION);
# falls back to 'dev' for local runs. Exposed to every template.
APP_VERSION = os.environ.get('APP_VERSION', 'dev')


@app.context_processor
def inject_app_version():
    return {'app_version': APP_VERSION}


# Database connection
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is required")

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False, "timeout": 30})

TAGLINES = [
    "Who wears short shorts? We wear short shorts.",
    "Our sport is your sport's punishment.",
    "5K? We do that before breakfast.",
    "Your sport's workout is our warmup.",
    "Pain is temporary. PRs are forever.",
    "Mud is just a free exfoliant.",
    "Hills don't scare us. Hills motivate us. Hills are lying.",
    "Some heroes wear capes. We wear race bibs.",
    "We run cross country because the country isn't going to cross itself.",
    "Other sports get courts and turf. We get hills, mud, and glory.",
    "Not all who wander are lost. Some are doing fartlek intervals.",
    "Our sport requires no equipment. Just suffering.",
    "Three miles of beautiful, voluntary suffering.",
    "The trail doesn't care about your excuses.",
    "We're the ones who actually enjoy the 'fun run' at school.",
    "Cross country: the original trail blazing.",
]


def init_db():
    """Create database tables if they don't exist."""
    statements = [
        """CREATE TABLE IF NOT EXISTS page_views (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            page_path VARCHAR(500) NOT NULL,
            user_agent TEXT,
            ip_address TEXT,
            referer TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            session_id VARCHAR(100)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_page_views_timestamp ON page_views(timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_page_views_page_path ON page_views(page_path)",
        """CREATE TABLE IF NOT EXISTS athletes (
            id TEXT PRIMARY KEY,
            first_name VARCHAR(100) NOT NULL,
            last_name VARCHAR(100) NOT NULL,
            gender VARCHAR(10) NOT NULL CHECK (gender IN ('male', 'female')),
            school VARCHAR(200),
            graduation_year INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS venues (
            id TEXT PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            location VARCHAR(200),
            state VARCHAR(50),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS meets (
            id TEXT PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            meet_date DATE NOT NULL,
            venue_id TEXT REFERENCES venues(id),
            season VARCHAR(10) NOT NULL,
            milesplit_url TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS races (
            id TEXT PRIMARY KEY,
            meet_id TEXT REFERENCES meets(id) ON DELETE CASCADE,
            name VARCHAR(100) NOT NULL,
            distance VARCHAR(20) NOT NULL,
            race_class VARCHAR(20) NOT NULL,
            gender VARCHAR(10) NOT NULL CHECK (gender IN ('male', 'female', 'mixed')),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS results (
            id TEXT PRIMARY KEY,
            race_id TEXT REFERENCES races(id) ON DELETE CASCADE,
            athlete_id TEXT REFERENCES athletes(id) ON DELETE CASCADE,
            time_seconds REAL NOT NULL,
            place INTEGER,
            varsity_points INTEGER DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )""",
        "CREATE INDEX IF NOT EXISTS idx_athletes_name ON athletes(last_name, first_name)",
        "CREATE INDEX IF NOT EXISTS idx_athletes_gender ON athletes(gender)",
        "CREATE INDEX IF NOT EXISTS idx_meets_date ON meets(meet_date)",
        "CREATE INDEX IF NOT EXISTS idx_meets_season ON meets(season)",
        "CREATE INDEX IF NOT EXISTS idx_races_meet ON races(meet_id)",
        "CREATE INDEX IF NOT EXISTS idx_races_class_gender ON races(race_class, gender)",
        "CREATE INDEX IF NOT EXISTS idx_results_race ON results(race_id)",
        "CREATE INDEX IF NOT EXISTS idx_results_athlete ON results(athlete_id)",
        "CREATE INDEX IF NOT EXISTS idx_results_time ON results(time_seconds)",
    ]
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))


init_db()


@app.context_processor
def inject_season_context():
    """Make season data available in every template."""
    try:
        with get_db_connection() as conn:
            rows = conn.execute(text(
                "SELECT DISTINCT season FROM meets ORDER BY season DESC"
            )).fetchall()
            available_seasons = [row.season for row in rows]
    except Exception:
        available_seasons = []
    selected_season = request.args.get('season', CURRENT_SEASON)
    return {
        'current_season': CURRENT_SEASON,
        'available_seasons': available_seasons,
        'selected_season': selected_season,
    }


# Filter for Fort Collins High School only
SCHOOL_FILTER = "Fort Collins High School"

# The active season; data for prior seasons is accessible via the "Past Seasons" dropdown
CURRENT_SEASON = '2026'

# Analytics configuration - simplified
EXCLUDED_IPS = {
    '127.0.0.1',  # localhost
    '::1',        # localhost IPv6
    '192.168.1.1', # your development machine
    # Add your local machine IP here if accessing from different machine
}

def should_track_simple(path: str) -> bool:
    """Determine if this page view should be tracked - simplified version."""
    # Get client IP (handle proxy headers)
    client_ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    if client_ip:
        client_ip = client_ip.split(',')[0].strip()
    
    # Exclude localhost/development IPs
    if client_ip in EXCLUDED_IPS:
        return False
    
    # Only track team stats and athlete pages
    if path == '/team/stats':
        return True
    elif path.startswith('/athlete/'):
        return True
    else:
        return False

def track_analytics(page_type: str):
    """Track a page view for analytics."""
    try:
        # Get client IP (handle proxy headers)
        client_ip = request.headers.get('X-Forwarded-For', request.remote_addr)
        if client_ip:
            client_ip = client_ip.split(',')[0].strip()
        
        # Exclude localhost/development IPs
        if client_ip in EXCLUDED_IPS:
            return
        
        user_agent = request.headers.get('User-Agent', '')
        print(f"Tracking analytics: page_type={page_type}, ip={client_ip}")
        
        # Generate session ID based on IP and user agent for unique visitor counting
        session_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{client_ip}:{user_agent}"))
        
        with engine.connect() as conn:
            print(f"About to execute SQL with page_type: {page_type}")
            conn.execute(
                text("""
                    INSERT INTO page_views (page_path, user_agent, ip_address, session_id)
                    VALUES (:page_type, :user_agent, :ip_address, :session_id)
                """),
                {
                    "page_type": page_type,
                    "user_agent": user_agent[:100],  # Truncate user agent
                    "ip_address": client_ip,
                    "session_id": session_id
                }
            )
            conn.commit()
            print(f"Successfully inserted analytics record for: {page_type}")
    except Exception as e:
        logger.error(f"Error tracking analytics: {e}")

def format_time(seconds: Union[float, decimal.Decimal]) -> str:
    """Format time from seconds to MM:SS.ss format with fractional seconds."""
    if seconds is None:
        return "N/A"
    
    # Convert to float if it's a Decimal
    if isinstance(seconds, decimal.Decimal):
        seconds = float(seconds)
    
    # Split into whole seconds and fractional part
    total_seconds = int(seconds)
    fractional_part = seconds - total_seconds
    
    # Calculate minutes and remaining seconds
    minutes = total_seconds // 60
    secs = total_seconds % 60
    
    # Get centiseconds (hundredths)
    centiseconds = int(round(fractional_part * 100))
    
    # Format as MM:SS.ss
    return f"{minutes:02d}:{secs:02d}.{centiseconds:02d}"

def distance_to_miles(distance: str) -> float:
    """Convert race distance to miles for pace calculation."""
    distance_map = {
        # Kilometer distances
        '5K': 3.10686,    # 5 kilometers = 3.10686 miles
        '3K': 1.86411,    # 3 kilometers = 1.86411 miles
        '8K': 4.97097,    # 8 kilometers = 4.97097 miles
        '10K': 6.21371,   # 10 kilometers = 6.21371 miles
        
        # Mile distances
        '1M': 1.0,        # 1 mile
        '2M': 2.0,        # 2 miles
        '3M': 3.0,        # 3 miles
        '1 Mile': 1.0,    # 1 mile (alternate format)
        '2 Mile': 2.0,    # 2 miles (alternate format)
        '3 Mile': 3.0,    # 3 miles (alternate format)
        
        # Meter distances
        '1600M': 0.99419, # 1600 meters = 0.99419 miles
        '3200M': 1.98838, # 3200 meters = 1.98838 miles
        '5000M': 3.10686, # 5000 meters = 3.10686 miles
        '8000M': 4.97097, # 8000 meters = 4.97097 miles
        '10000M': 6.21371, # 10000 meters = 6.21371 miles
    }
    if distance not in distance_map:
        logger.warning(f"Unknown distance '{distance}' - defaulting to 1.0 miles for pace calculation")
    return distance_map.get(distance, 1.0)  # Default to 1.0 if unknown distance

def calculate_pace(time_seconds: Union[float, decimal.Decimal], distance: str) -> str:
    """Calculate pace per mile in MM:SS.ss format."""
    if time_seconds is None:
        return "N/A"
    
    # Convert to float if it's a Decimal
    if isinstance(time_seconds, decimal.Decimal):
        time_seconds = float(time_seconds)
    
    miles = distance_to_miles(distance)
    if miles == 0:
        return "N/A"
    
    pace_seconds = time_seconds / miles
    pace_minutes = int(pace_seconds // 60)
    pace_secs = int(pace_seconds % 60)
    pace_centiseconds = int(round((pace_seconds % 1) * 100))
    
    return f"{pace_minutes:02d}:{pace_secs:02d}.{pace_centiseconds:02d}"

def get_supported_distances():
    """Return list of all supported race distances."""
    return ['5K', '3K', '8K', '10K', '1M', '2M', '3M', '1 Mile', '2 Mile', '3 Mile', 
            '1600M', '3200M', '5000M', '8000M', '10000M']

def get_db_connection():
    """Get database connection."""
    return engine.connect()

# Make functions available in templates
app.jinja_env.globals.update(
    format_time=format_time,
    calculate_pace=calculate_pace
)

@app.route('/')
def index():
    """Main dashboard page."""
    season = request.args.get('season', CURRENT_SEASON)
    try:
        with get_db_connection() as conn:
            stats_query = text("""
                SELECT 
                    COUNT(DISTINCT a.id) as total_athletes,
                    COUNT(DISTINCT m.id) as total_meets,
                    COUNT(DISTINCT r.id) as total_races,
                    COUNT(res.id) as total_results
                FROM athletes a
                JOIN results res ON a.id = res.athlete_id
                JOIN races r ON res.race_id = r.id
                JOIN meets m ON r.meet_id = m.id
                WHERE a.school = :school
                AND (:season = 'all' OR m.season = :season)
            """)
            
            stats = conn.execute(stats_query, {"school": SCHOOL_FILTER, "season": season}).fetchone()
            
            recent_meets_query = text("""
                SELECT DISTINCT m.name, m.meet_date, v.name as venue_name
                FROM meets m
                LEFT JOIN venues v ON m.venue_id = v.id
                JOIN races r ON r.meet_id = m.id
                JOIN results res ON res.race_id = r.id
                JOIN athletes a ON a.id = res.athlete_id
                WHERE a.school = :school
                AND (:season = 'all' OR m.season = :season)
                ORDER BY m.meet_date DESC
                LIMIT 5
            """)
            
            recent_meets = conn.execute(recent_meets_query, {"school": SCHOOL_FILTER, "season": season}).fetchall()
            
            return render_template('index.html',
                                 stats=stats,
                                 recent_meets=recent_meets,
                                 tagline=random.choice(TAGLINES))
    
    except Exception as e:
        logger.error(f"Error loading dashboard: {e}")
        return render_template('error.html', error=str(e)), 500

@app.route('/export/csv')
def export_csv():
    """Export athlete data as CSV with one column per meet."""
    season = request.args.get('season', CURRENT_SEASON)
    try:
        with get_db_connection() as conn:
            # Get all meets (where Fort Collins athletes participated)
            meets_query = text("""
                SELECT DISTINCT 
                    m.id,
                    m.name,
                    m.meet_date
                FROM meets m
                JOIN races r ON r.meet_id = m.id
                JOIN results res ON res.race_id = r.id
                JOIN athletes a ON a.id = res.athlete_id
                WHERE a.school = :school
                AND (:season = 'all' OR m.season = :season)
                ORDER BY m.meet_date, m.name
            """)
            
            meets = conn.execute(meets_query, {"school": SCHOOL_FILTER, "season": season}).fetchall()
            
            # Get athletes who competed in the selected season
            athletes_query = text("""
                SELECT DISTINCT 
                    a.id,
                    a.first_name,
                    a.last_name,
                    a.gender
                FROM athletes a
                JOIN results res ON a.id = res.athlete_id
                JOIN races r ON res.race_id = r.id
                JOIN meets m ON r.meet_id = m.id
                WHERE a.school = :school
                AND (:season = 'all' OR m.season = :season)
                ORDER BY a.last_name, a.first_name
            """)
            
            athletes = conn.execute(athletes_query, {"school": SCHOOL_FILTER, "season": season}).fetchall()
            
            # Prepare CSV data
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Header row: First Name, Last Name, Gender, then time/pace columns per meet
            header = ['First Name', 'Last Name', 'Gender']
            for meet in meets:
                # Format meet name and date for column headers
                meet_header = f"{meet.name} ({meet.meet_date})"
                header.append(f"{meet_header} Time")
                header.append(f"{meet_header} Pace")
            
            writer.writerow(header)
            
            # Data rows
            for athlete in athletes:
                row = [athlete.first_name, athlete.last_name, athlete.gender]
                
                # For each meet, find this athlete's time (if they participated)
                for meet in meets:
                    # Get athlete's time and distance in this meet
                    time_query = text("""
                        SELECT res.time_seconds, r.distance
                        FROM results res
                        JOIN races r ON res.race_id = r.id
                        JOIN meets m ON r.meet_id = m.id
                        WHERE res.athlete_id = :athlete_id
                        AND m.id = :meet_id
                        ORDER BY res.time_seconds ASC
                        LIMIT 1
                    """)
                    
                    result = conn.execute(time_query, {
                        'athlete_id': athlete.id,
                        'meet_id': meet.id
                    }).fetchone()
                    
                    if result:
                        row.append(format_time(result.time_seconds))
                        row.append(calculate_pace(result.time_seconds, result.distance))
                    else:
                        row.append('')  # Empty time cell if athlete didn't participate
                        row.append('')  # Empty pace cell if athlete didn't participate
                
                writer.writerow(row)
            
            output.seek(0)
            
            # Create response
            return send_file(
                io.BytesIO(output.getvalue().encode()),
                mimetype='text/csv',
                as_attachment=True,
                download_name=f'fcxc_results_by_meet_{datetime.now().strftime("%Y%m%d")}.csv'
            )
    
    except Exception as e:
        logger.error(f"Error generating CSV export: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/team/stats')
def team_stats():
    """Team statistics page with best times by gender."""
    track_analytics('team_stats')
    season = request.args.get('season', CURRENT_SEASON)
    
    try:
        with get_db_connection() as conn:
            boys_stats_query = text("""
                WITH best_times AS (
                    SELECT 
                        a.first_name,
                        a.last_name,
                        r.distance,
                        MIN(res.time_seconds) as best_time
                    FROM athletes a
                    JOIN results res ON a.id = res.athlete_id
                    JOIN races r ON res.race_id = r.id
                    JOIN meets m ON r.meet_id = m.id
                    WHERE a.gender = 'male' AND a.school = :school
                    AND (:season = 'all' OR m.season = :season)
                    GROUP BY a.first_name, a.last_name, r.distance
                ),
                best_times_with_details AS (
                    SELECT 
                        bt.first_name,
                        bt.last_name,
                        bt.distance,
                        bt.best_time,
                        a.id as athlete_id,
                        m.name as meet_name,
                        r.name as race_name,
                        m.meet_date,
                        ROW_NUMBER() OVER (PARTITION BY bt.first_name, bt.last_name, bt.distance ORDER BY m.meet_date DESC, res.id) as rn
                    FROM best_times bt
                    JOIN results res ON bt.best_time = res.time_seconds
                    JOIN athletes a ON res.athlete_id = a.id AND a.first_name = bt.first_name AND a.last_name = bt.last_name
                    JOIN races r ON res.race_id = r.id AND r.distance = bt.distance
                    JOIN meets m ON r.meet_id = m.id
                    WHERE (:season = 'all' OR m.season = :season)
                )
                SELECT 
                    first_name,
                    last_name,
                    distance,
                    best_time,
                    athlete_id,
                    meet_name,
                    race_name,
                    meet_date
                FROM best_times_with_details
                WHERE rn = 1
                ORDER BY distance, best_time ASC
            """)
            
            girls_stats_query = text("""
                WITH best_times AS (
                    SELECT 
                        a.first_name,
                        a.last_name,
                        r.distance,
                        MIN(res.time_seconds) as best_time
                    FROM athletes a
                    JOIN results res ON a.id = res.athlete_id
                    JOIN races r ON res.race_id = r.id
                    JOIN meets m ON r.meet_id = m.id
                    WHERE a.gender = 'female' AND a.school = :school
                    AND (:season = 'all' OR m.season = :season)
                    GROUP BY a.first_name, a.last_name, r.distance
                ),
                best_times_with_details AS (
                    SELECT 
                        bt.first_name,
                        bt.last_name,
                        bt.distance,
                        bt.best_time,
                        a.id as athlete_id,
                        m.name as meet_name,
                        r.name as race_name,
                        m.meet_date,
                        ROW_NUMBER() OVER (PARTITION BY bt.first_name, bt.last_name, bt.distance ORDER BY m.meet_date DESC, res.id) as rn
                    FROM best_times bt
                    JOIN results res ON bt.best_time = res.time_seconds
                    JOIN athletes a ON res.athlete_id = a.id AND a.first_name = bt.first_name AND a.last_name = bt.last_name
                    JOIN races r ON res.race_id = r.id AND r.distance = bt.distance
                    JOIN meets m ON r.meet_id = m.id
                    WHERE (:season = 'all' OR m.season = :season)
                )
                SELECT 
                    first_name,
                    last_name,
                    distance,
                    best_time,
                    athlete_id,
                    meet_name,
                    race_name,
                    meet_date
                FROM best_times_with_details
                WHERE rn = 1
                ORDER BY distance, best_time ASC
            """)
            
            boys_stats = conn.execute(boys_stats_query, {"school": SCHOOL_FILTER, "season": season}).fetchall()
            girls_stats = conn.execute(girls_stats_query, {"school": SCHOOL_FILTER, "season": season}).fetchall()
            
            # Process data by distance
            boys_by_distance = {}
            girls_by_distance = {}
            for stat in boys_stats:
                if stat.distance not in boys_by_distance:
                    boys_by_distance[stat.distance] = []
                boys_by_distance[stat.distance].append(stat)
            for stat in girls_stats:
                if stat.distance not in girls_by_distance:
                    girls_by_distance[stat.distance] = []
                girls_by_distance[stat.distance].append(stat)

            # Normalize keys for ordering
            def normalize_distance(d):
                d_lower = d.lower().replace(' ', '')
                if d_lower in ['5k', '5km', '5000m']:
                    return '5k'
                if d_lower in ['2m', '2mi', '2mile', '2 miles', '2mi.', '2mile.']:
                    return '2M'
                return d

            def ordered_distances(dist_dict):
                # Map normalized keys to original keys
                norm_map = {normalize_distance(d): d for d in dist_dict.keys()}
                order = ['5k', '2M']
                ordered = [norm_map[o] for o in order if o in norm_map]
                ordered += sorted([d for d in dist_dict.keys() if normalize_distance(d) not in order])
                return ordered

            boys_distances = ordered_distances(boys_by_distance)
            girls_distances = ordered_distances(girls_by_distance)

            return render_template('team_stats.html',
                                 boys_by_distance=boys_by_distance,
                                 girls_by_distance=girls_by_distance,
                                 boys_distances=boys_distances,
                                 girls_distances=girls_distances,
                                 format_time=format_time)
    
    except Exception as e:
        logger.error(f"Error loading team stats: {e}")
        return render_template('error.html', error=str(e)), 500

@app.route('/athlete/<athlete_id>')
def athlete_stats(athlete_id):
    """Individual athlete statistics page."""
    # Track this page view
    track_analytics('athlete_page')
    season = request.args.get('season', CURRENT_SEASON)
    
    try:
        with get_db_connection() as conn:
            # Get athlete info (only Fort Collins High School athletes)
            athlete_query = text("""
                SELECT first_name, last_name, gender, graduation_year, school
                FROM athletes
                WHERE id = :athlete_id AND school = :school
            """)
            
            athlete = conn.execute(athlete_query, {
                'athlete_id': athlete_id, 
                'school': SCHOOL_FILTER
            }).fetchone()
            if not athlete:
                return render_template('error.html', error='Athlete not found'), 404
            
            # Get athlete's results over time
            results_query = text("""
                SELECT 
                    res.time_seconds,
                    res.place,
                    res.varsity_points,
                    r.distance,
                    r.race_class,
                    r.name as race_name,
                    m.name as meet_name,
                    m.meet_date,
                    v.name as venue_name
                FROM results res
                JOIN races r ON res.race_id = r.id
                JOIN meets m ON r.meet_id = m.id
                LEFT JOIN venues v ON m.venue_id = v.id
                WHERE res.athlete_id = :athlete_id
                ORDER BY m.meet_date DESC
            """)
            
            results = conn.execute(results_query, {'athlete_id': athlete_id}).fetchall()
            
            # Get PRs by distance
            prs_query = text("""
                SELECT 
                    r.distance,
                    MIN(res.time_seconds) as pr_seconds,
                    COUNT(res.id) as race_count
                FROM results res
                JOIN races r ON res.race_id = r.id
                WHERE res.athlete_id = :athlete_id
                GROUP BY r.distance
                ORDER BY 
                    CASE 
                        WHEN r.distance = '5K' THEN 1 
                        ELSE 2 
                    END,
                    r.distance
            """)
            
            prs = conn.execute(prs_query, {'athlete_id': athlete_id}).fetchall()
            
            # Create a dictionary of PR times by distance for easy lookup
            pr_times = {pr.distance: pr.pr_seconds for pr in prs}
            
            # Calculate number of varsity races run
            varsity_races = sum(1 for result in results if result.race_class == 'varsity')
            
            return render_template('athlete_stats.html',
                                 athlete=athlete,
                                 athlete_id=athlete_id,
                                 results=results,
                                 prs=prs,
                                 pr_times=pr_times,
                                 season=season,
                                 varsity_races=varsity_races,
                                 format_time=format_time)
    
    except Exception as e:
        logger.error(f"Error loading athlete stats: {e}")
        return render_template('error.html', error=str(e)), 500

@app.route('/athletes')
def athletes_list():
    """List all Fort Collins High School athletes."""
    season = request.args.get('season', CURRENT_SEASON)
    try:
        with get_db_connection() as conn:
            athletes_query = text("""
                SELECT 
                    a.id,
                    a.first_name,
                    a.last_name,
                    a.gender,
                    a.graduation_year,
                    COUNT(res.id) as race_count,
                    MIN(CASE WHEN r.distance = '5K' THEN res.time_seconds END) as best_time
                FROM athletes a
                JOIN results res ON a.id = res.athlete_id
                JOIN races r ON res.race_id = r.id
                JOIN meets m ON r.meet_id = m.id
                WHERE a.school = :school
                AND (:season = 'all' OR m.season = :season)
                GROUP BY a.id, a.first_name, a.last_name, a.gender, a.graduation_year
                ORDER BY a.last_name, a.first_name
            """)
            
            athletes = conn.execute(athletes_query, {"school": SCHOOL_FILTER, "season": season}).fetchall()
            
            return render_template('athletes_list.html',
                                 athletes=athletes,
                                 format_time=format_time)
    
    except Exception as e:
        logger.error(f"Error loading athletes list: {e}")
        return render_template('error.html', error=str(e)), 500


@app.route('/api/athlete/search')
def athlete_search_api():
    """Search athletes by name and return top matches."""
    query = request.args.get('q', '').strip()
    season = request.args.get('season', CURRENT_SEASON)
    if len(query) < 2:
        return jsonify([])

    try:
        with get_db_connection() as conn:
            search_query = text("""
                SELECT
                    a.id,
                    a.first_name,
                    a.last_name,
                    a.gender,
                    MAX(m.meet_date) as last_meet_date
                FROM athletes a
                LEFT JOIN results res ON a.id = res.athlete_id
                LEFT JOIN races r ON res.race_id = r.id
                LEFT JOIN meets m ON r.meet_id = m.id
                WHERE a.school = :school
                AND LOWER(a.first_name || ' ' || a.last_name) LIKE :pattern
                GROUP BY a.id, a.first_name, a.last_name, a.gender
                ORDER BY
                    CASE
                        WHEN LOWER(a.first_name || ' ' || a.last_name) = :exact THEN 0
                        WHEN LOWER(a.last_name) = :exact OR LOWER(a.first_name) = :exact THEN 1
                        ELSE 2
                    END,
                    a.last_name,
                    a.first_name
                LIMIT 8
            """)

            pattern = f"%{query.lower()}%"
            rows = conn.execute(search_query, {
                'school': SCHOOL_FILTER,
                'pattern': pattern,
                'exact': query.lower(),
            }).fetchall()

            matches = []
            for row in rows:
                matches.append({
                    'athlete_id': row.id,
                    'name': f"{row.first_name} {row.last_name}",
                    'gender': row.gender,
                    'last_meet_date': str(row.last_meet_date) if row.last_meet_date else None,
                    'url': url_for('athlete_stats', athlete_id=row.id, season=season),
                })

            return jsonify(matches)

    except Exception as e:
        logger.error(f"Error searching athletes: {e}")
        return jsonify([])


@app.route('/athlete/search')
def athlete_search_redirect():
    """Redirect search to the best athlete match."""
    query = request.args.get('q', '').strip()
    season = request.args.get('season', CURRENT_SEASON)

    if not query:
        return redirect(url_for('athletes_list', season=season))

    try:
        with get_db_connection() as conn:
            best_match_query = text("""
                SELECT
                    a.id
                FROM athletes a
                WHERE a.school = :school
                AND LOWER(a.first_name || ' ' || a.last_name) LIKE :pattern
                ORDER BY
                    CASE
                        WHEN LOWER(a.first_name || ' ' || a.last_name) = :exact THEN 0
                        WHEN LOWER(a.last_name) = :exact OR LOWER(a.first_name) = :exact THEN 1
                        ELSE 2
                    END,
                    a.last_name,
                    a.first_name
                LIMIT 1
            """)

            result = conn.execute(best_match_query, {
                'school': SCHOOL_FILTER,
                'pattern': f"%{query.lower()}%",
                'exact': query.lower(),
            }).fetchone()

            if result:
                return redirect(url_for('athlete_stats', athlete_id=result.id, season=season))
            return redirect(url_for('athletes_list', season=season))

    except Exception as e:
        logger.error(f"Error redirecting athlete search: {e}")
        return redirect(url_for('athletes_list', season=season))

@app.route('/api/athlete/<athlete_id>/progress/<distance>')
def athlete_progress_api(athlete_id, distance):
    """API endpoint for athlete progress data."""
    scope = request.args.get('scope', 'all')
    try:
        with get_db_connection() as conn:
            if distance == 'all':
                # Return data for all races
                progress_query = text("""
                    SELECT 
                        res.time_seconds,
                        r.distance,
                        m.meet_date,
                        m.name as meet_name
                    FROM results res
                    JOIN races r ON res.race_id = r.id
                    JOIN meets m ON r.meet_id = m.id
                    WHERE res.athlete_id = :athlete_id
                    AND (:scope = 'all' OR m.season = :current_season)
                    ORDER BY m.meet_date ASC
                """)
                
                progress = conn.execute(progress_query, {
                    'athlete_id': athlete_id,
                    'scope': scope,
                    'current_season': CURRENT_SEASON,
                }).fetchall()
                
                data = []
                for p in progress:
                    time_seconds = float(p.time_seconds)
                    race_distance = p.distance
                    pace_seconds = time_seconds / distance_to_miles(race_distance)
                    # 5k in miles
                    five_k_miles = 3.10686
                    if abs(distance_to_miles(race_distance) - five_k_miles) < 0.01:
                        five_k_time = time_seconds
                    else:
                        five_k_time = pace_seconds * five_k_miles
                    data.append({
                        'date': str(p.meet_date),
                        'time': time_seconds,
                        'pace': calculate_pace(time_seconds, race_distance),
                        'pace_seconds': pace_seconds,
                        'meet': p.meet_name,
                        'distance': race_distance,
                        'formatted_time': format_time(time_seconds),
                        'five_k_time': five_k_time,
                        'formatted_five_k_time': format_time(five_k_time)
                    })
                
                return jsonify(data)
            else:
                # Original single distance logic
                progress_query = text("""
                    SELECT 
                        res.time_seconds,
                        m.meet_date,
                        m.name as meet_name
                    FROM results res
                    JOIN races r ON res.race_id = r.id
                    JOIN meets m ON r.meet_id = m.id
                    WHERE res.athlete_id = :athlete_id
                    AND r.distance = :distance
                    AND (:scope = 'all' OR m.season = :current_season)
                    ORDER BY m.meet_date ASC
                """)
                
                progress = conn.execute(progress_query, {
                    'athlete_id': athlete_id,
                    'distance': distance,
                    'scope': scope,
                    'current_season': CURRENT_SEASON,
                }).fetchall()
                
                data = [{
                    'date': str(p.meet_date),
                    'time': float(p.time_seconds),
                    'pace': calculate_pace(float(p.time_seconds), distance),
                    'pace_seconds': float(p.time_seconds) / distance_to_miles(distance),
                    'meet': p.meet_name,
                    'formatted_time': format_time(float(p.time_seconds)),
                    'five_k_time': float(p.time_seconds),
                    'formatted_five_k_time': format_time(float(p.time_seconds))
                } for p in progress]
                
                return jsonify(data)
    
    except Exception as e:
        logger.error(f"Error getting athlete progress: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/analytics-dashboard-x7j9k2')
def analytics_dashboard():
    """Hidden analytics dashboard - URL obfuscated to keep it private."""
    try:
        with get_db_connection() as conn:
            # Overall stats
            overall_stats = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_views,
                    COUNT(DISTINCT session_id) as unique_visitors,
                    SUM(CASE WHEN page_path = 'team_stats' THEN 1 ELSE 0 END) as team_stats_views,
                    SUM(CASE WHEN page_path = 'athlete_page' THEN 1 ELSE 0 END) as athlete_page_views,
                    COUNT(DISTINCT date(timestamp)) as active_days
                FROM page_views
                WHERE timestamp >= datetime('now', '-30 days')
            """)).fetchone()
            
            # Daily activity (last 30 days)
            daily_stats = conn.execute(text("""
                SELECT 
                    date(timestamp) as date,
                    SUM(CASE WHEN page_path = 'team_stats' THEN 1 ELSE 0 END) as team_stats_views,
                    SUM(CASE WHEN page_path = 'athlete_page' THEN 1 ELSE 0 END) as athlete_page_views,
                    COUNT(DISTINCT session_id) as unique_visitors
                FROM page_views
                WHERE timestamp >= datetime('now', '-30 days')
                GROUP BY date(timestamp)
                ORDER BY date DESC
            """)).fetchall()
            
            # Hourly pattern (last 7 days)
            hourly_stats = conn.execute(text("""
                SELECT 
                    CAST(strftime('%H', timestamp) AS INTEGER) as hour,
                    COUNT(*) as views
                FROM page_views
                WHERE timestamp >= datetime('now', '-7 days')
                GROUP BY strftime('%H', timestamp)
                ORDER BY hour
            """)).fetchall()
            
            # Recent activity (last 50 views)
            recent_activity = conn.execute(text("""
                SELECT 
                    page_path,
                    timestamp,
                    ip_address
                FROM page_views
                ORDER BY timestamp DESC
                LIMIT 50
            """)).fetchall()
            
            return render_template('analytics_dashboard.html',
                                overall_stats=overall_stats,
                                daily_stats=daily_stats,
                                hourly_stats=hourly_stats,
                                recent_activity=recent_activity)
    
    except Exception as e:
        logger.error(f"Error loading analytics dashboard: {e}")
        return render_template('error.html', error=str(e)), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
