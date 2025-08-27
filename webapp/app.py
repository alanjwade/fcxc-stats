#!/usr/bin/env python3
"""
Cross Country Statistics Web Application

Flask web application that provides:
1. CSV export of athlete performance data
2. Team statistics dashboard
3. Individual athlete statistics
4. Main navigation dashboard
"""

import os
import io
import csv
from datetime import datetime
from typing import List, Dict, Any, Optional
from flask import Flask, render_template, jsonify, send_file, request, redirect, url_for
from sqlalchemy import create_engine, text
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'your-secret-key-here')

# Database connection
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is required")

engine = create_engine(DATABASE_URL)

# Filter for Fort Collins High School only
SCHOOL_FILTER = "Fort Collins High School"

def format_time(seconds: int) -> str:
    """Format time from seconds to MM:SS format."""
    minutes = seconds // 60
    secs = seconds % 60
    return f"{minutes:02d}:{secs:02d}"

def get_db_connection():
    """Get database connection."""
    return engine.connect()

@app.route('/')
def index():
    """Main dashboard page."""
    try:
        with get_db_connection() as conn:
            # Get basic statistics for Fort Collins High School only
            stats_query = text("""
                SELECT 
                    COUNT(DISTINCT a.id) as total_athletes,
                    COUNT(DISTINCT m.id) as total_meets,
                    COUNT(DISTINCT r.id) as total_races,
                    COUNT(res.id) as total_results
                FROM athletes a
                LEFT JOIN results res ON a.id = res.athlete_id
                LEFT JOIN races r ON res.race_id = r.id
                LEFT JOIN meets m ON r.meet_id = m.id
                WHERE a.school = :school
            """)
            
            stats = conn.execute(stats_query, {"school": SCHOOL_FILTER}).fetchone()
            
            # Get recent meets (where Fort Collins athletes participated)
            recent_meets_query = text("""
                SELECT DISTINCT m.name, m.meet_date, v.name as venue_name
                FROM meets m
                LEFT JOIN venues v ON m.venue_id = v.id
                JOIN races r ON r.meet_id = m.id
                JOIN results res ON res.race_id = r.id
                JOIN athletes a ON a.id = res.athlete_id
                WHERE a.school = :school
                ORDER BY m.meet_date DESC
                LIMIT 5
            """)
            
            recent_meets = conn.execute(recent_meets_query, {"school": SCHOOL_FILTER}).fetchall()
            
            return render_template('index.html', 
                                 stats=stats, 
                                 recent_meets=recent_meets)
    
    except Exception as e:
        logger.error(f"Error loading dashboard: {e}")
        return render_template('error.html', error=str(e)), 500

@app.route('/export/csv')
def export_csv():
    """Export athlete data as CSV with one column per meet."""
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
                ORDER BY m.meet_date, m.name
            """)
            
            meets = conn.execute(meets_query, {"school": SCHOOL_FILTER}).fetchall()
            
            # Get all Fort Collins High School athletes
            athletes_query = text("""
                SELECT DISTINCT 
                    a.id,
                    a.first_name,
                    a.last_name,
                    a.gender
                FROM athletes a
                WHERE a.school = :school
                ORDER BY a.last_name, a.first_name
            """)
            
            athletes = conn.execute(athletes_query, {"school": SCHOOL_FILTER}).fetchall()
            
            # Prepare CSV data
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Header row: First Name, Last Name, Gender, then one column per meet
            header = ['First Name', 'Last Name', 'Gender']
            for meet in meets:
                # Format meet name and date for column header
                meet_header = f"{meet.name} ({meet.meet_date})"
                header.append(meet_header)
            
            writer.writerow(header)
            
            # Data rows
            for athlete in athletes:
                row = [athlete.first_name, athlete.last_name, athlete.gender]
                
                # For each meet, find this athlete's time (if they participated)
                for meet in meets:
                    # Get athlete's time in this meet
                    time_query = text("""
                        SELECT res.time_seconds
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
                    else:
                        row.append('')  # Empty cell if athlete didn't participate
                
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
    try:
        with get_db_connection() as conn:
            # Get best times by gender and distance for Fort Collins High School
            boys_stats_query = text("""
                SELECT 
                    a.first_name,
                    a.last_name,
                    r.distance,
                    MIN(res.time_seconds) as best_time,
                    m.name as meet_name,
                    m.meet_date
                FROM athletes a
                JOIN results res ON a.id = res.athlete_id
                JOIN races r ON res.race_id = r.id
                JOIN meets m ON r.meet_id = m.id
                WHERE a.gender = 'male' AND a.school = :school
                GROUP BY a.id, a.first_name, a.last_name, r.distance, m.name, m.meet_date
                ORDER BY r.distance, best_time ASC
            """)
            
            girls_stats_query = text("""
                SELECT 
                    a.first_name,
                    a.last_name,
                    r.distance,
                    MIN(res.time_seconds) as best_time,
                    m.name as meet_name,
                    m.meet_date
                FROM athletes a
                JOIN results res ON a.id = res.athlete_id
                JOIN races r ON res.race_id = r.id
                JOIN meets m ON r.meet_id = m.id
                WHERE a.gender = 'female' AND a.school = :school
                GROUP BY a.id, a.first_name, a.last_name, r.distance, m.name, m.meet_date
                ORDER BY r.distance, best_time ASC
            """)
            
            boys_stats = conn.execute(boys_stats_query, {"school": SCHOOL_FILTER}).fetchall()
            girls_stats = conn.execute(girls_stats_query, {"school": SCHOOL_FILTER}).fetchall()
            
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
            
            return render_template('team_stats.html',
                                 boys_by_distance=boys_by_distance,
                                 girls_by_distance=girls_by_distance,
                                 format_time=format_time)
    
    except Exception as e:
        logger.error(f"Error loading team stats: {e}")
        return render_template('error.html', error=str(e)), 500

@app.route('/athlete/<athlete_id>')
def athlete_stats(athlete_id):
    """Individual athlete statistics page."""
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
                ORDER BY r.distance
            """)
            
            prs = conn.execute(prs_query, {'athlete_id': athlete_id}).fetchall()
            
            # Calculate total varsity points
            total_points = sum(result.varsity_points for result in results)
            
            return render_template('athlete_stats.html',
                                 athlete=athlete,
                                 results=results,
                                 prs=prs,
                                 total_points=total_points,
                                 format_time=format_time)
    
    except Exception as e:
        logger.error(f"Error loading athlete stats: {e}")
        return render_template('error.html', error=str(e)), 500

@app.route('/athletes')
def athletes_list():
    """List all Fort Collins High School athletes."""
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
                    MIN(res.time_seconds) as best_time
                FROM athletes a
                LEFT JOIN results res ON a.id = res.athlete_id
                WHERE a.school = :school
                GROUP BY a.id, a.first_name, a.last_name, a.gender, a.graduation_year
                ORDER BY a.last_name, a.first_name
            """)
            
            athletes = conn.execute(athletes_query, {"school": SCHOOL_FILTER}).fetchall()
            
            return render_template('athletes_list.html',
                                 athletes=athletes,
                                 format_time=format_time)
    
    except Exception as e:
        logger.error(f"Error loading athletes list: {e}")
        return render_template('error.html', error=str(e)), 500

@app.route('/api/athlete/<athlete_id>/progress/<distance>')
def athlete_progress_api(athlete_id, distance):
    """API endpoint for athlete progress data."""
    try:
        with get_db_connection() as conn:
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
                ORDER BY m.meet_date ASC
            """)
            
            progress = conn.execute(progress_query, {
                'athlete_id': athlete_id,
                'distance': distance
            }).fetchall()
            
            data = [{
                'date': str(p.meet_date),
                'time': p.time_seconds,
                'meet': p.meet_name,
                'formatted_time': format_time(p.time_seconds)
            } for p in progress]
            
            return jsonify(data)
    
    except Exception as e:
        logger.error(f"Error getting athlete progress: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
