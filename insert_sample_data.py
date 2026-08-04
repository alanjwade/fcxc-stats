#!/usr/bin/env python3
"""
Sample data insertion script for Cross Country Statistics Tracker
This script adds some sample data to test the system functionality.
"""

import os
import sys
import uuid
from sqlalchemy import create_engine, text
from datetime import datetime

def insert_sample_data():
    """Insert sample cross country data for testing."""
    
    database_url = os.getenv('DATABASE_URL', 'sqlite:///fcxc_stats.db')
    engine = create_engine(database_url, connect_args={"check_same_thread": False, "timeout": 30})
    
    try:
        with engine.connect() as conn:
            print("Inserting sample data...")
            
            # Insert sample venue
            venue_id = str(uuid.uuid4())
            conn.execute(
                text("INSERT INTO venues (id, name, location, state) VALUES (:id, :name, :location, :state)"),
                {"id": venue_id, "name": "Spring Canyon Park", "location": "Fort Collins", "state": "CO"}
            )
            print(f"Created venue: {venue_id}")
            
            # Insert sample meet
            meet_id = str(uuid.uuid4())
            conn.execute(
                text("""
                    INSERT INTO meets (id, name, meet_date, venue_id, season, milesplit_url)
                    VALUES (:id, :name, :meet_date, :venue_id, :season, :url)
                """),
                {
                    "id": meet_id,
                    "name": "Sample Cross Country Meet 2024",
                    "meet_date": "2024-09-15",
                    "venue_id": venue_id,
                    "season": "2024",
                    "url": "https://co.milesplit.com/meets/sample"
                }
            )
            print(f"Created meet: {meet_id}")
            
            # Insert sample race (Boys Varsity 5K)
            race_id = str(uuid.uuid4())
            conn.execute(
                text("""
                    INSERT INTO races (id, meet_id, name, distance, race_class, gender)
                    VALUES (:id, :meet_id, :name, :distance, :race_class, :gender)
                """),
                {
                    "id": race_id,
                    "meet_id": meet_id,
                    "name": "Varsity Boys",
                    "distance": "5K",
                    "race_class": "varsity",
                    "gender": "male"
                }
            )
            print(f"Created race: {race_id}")
            
            # Sample athletes and results
            sample_results = [
                ("John", "Smith", 16, 45, 1, 7),  # 16:45, 1st place, 7 points
                ("Mike", "Johnson", 16, 52, 2, 6),  # 16:52, 2nd place, 6 points
                ("David", "Wilson", 17, 8, 3, 5),   # 17:08, 3rd place, 5 points
                ("Chris", "Brown", 17, 15, 4, 4),   # 17:15, 4th place, 4 points
                ("Ryan", "Davis", 17, 22, 5, 3),    # 17:22, 5th place, 3 points
                ("Alex", "Miller", 17, 30, 6, 2),   # 17:30, 6th place, 2 points
                ("Tyler", "Garcia", 17, 35, 7, 1),  # 17:35, 7th place, 1 point
                ("Jake", "Martinez", 17, 42, 8, 0), # 17:42, 8th place, 0 points
                ("Sam", "Anderson", 17, 48, 9, 0),  # 17:48, 9th place, 0 points
                ("Ben", "Taylor", 17, 55, 10, 0),   # 17:55, 10th place, 0 points
            ]
            
            for first_name, last_name, minutes, seconds, place, points in sample_results:
                # Create athlete
                athlete_id = str(uuid.uuid4())
                conn.execute(
                    text("""
                        INSERT INTO athletes (id, first_name, last_name, gender, graduation_year)
                        VALUES (:id, :first_name, :last_name, :gender, :grad_year)
                    """),
                    {
                        "id": athlete_id,
                        "first_name": first_name,
                        "last_name": last_name,
                        "gender": "male",
                        "grad_year": 2025
                    }
                )
                
                # Calculate total seconds
                time_seconds = minutes * 60 + seconds
                
                # Create result
                conn.execute(
                    text("""
                        INSERT INTO results (id, race_id, athlete_id, time_seconds, place, varsity_points)
                        VALUES (:id, :race_id, :athlete_id, :time_seconds, :place, :points)
                    """),
                    {
                        "id": str(uuid.uuid4()),
                        "race_id": race_id,
                        "athlete_id": athlete_id,
                        "time_seconds": time_seconds,
                        "place": place,
                        "points": points
                    }
                )
                
                print(f"Added athlete: {first_name} {last_name} - {minutes}:{seconds:02d} (Place: {place})")
            
            # Add a sample girls race too
            girls_race_id = str(uuid.uuid4())
            conn.execute(
                text("""
                    INSERT INTO races (id, meet_id, name, distance, race_class, gender)
                    VALUES (:id, :meet_id, :name, :distance, :race_class, :gender)
                """),
                {
                    "id": girls_race_id,
                    "meet_id": meet_id,
                    "name": "Varsity Girls",
                    "distance": "5K",
                    "race_class": "varsity",
                    "gender": "female"
                }
            )
            
            # Sample girls results
            girls_results = [
                ("Sarah", "Johnson", 19, 15, 1, 7),  # 19:15, 1st place
                ("Emily", "Davis", 19, 28, 2, 6),    # 19:28, 2nd place
                ("Ashley", "Wilson", 19, 35, 3, 5),  # 19:35, 3rd place
                ("Jessica", "Brown", 19, 42, 4, 4),  # 19:42, 4th place
                ("Amanda", "Miller", 19, 50, 5, 3),  # 19:50, 5th place
                ("Rachel", "Garcia", 19, 58, 6, 2),  # 19:58, 6th place
                ("Lauren", "Martinez", 20, 5, 7, 1), # 20:05, 7th place
            ]
            
            for first_name, last_name, minutes, seconds, place, points in girls_results:
                athlete_id = str(uuid.uuid4())
                conn.execute(
                    text("""
                        INSERT INTO athletes (id, first_name, last_name, gender, graduation_year)
                        VALUES (:id, :first_name, :last_name, :gender, :grad_year)
                    """),
                    {
                        "id": athlete_id,
                        "first_name": first_name,
                        "last_name": last_name,
                        "gender": "female",
                        "grad_year": 2025
                    }
                )
                
                time_seconds = minutes * 60 + seconds
                
                conn.execute(
                    text("""
                        INSERT INTO results (id, race_id, athlete_id, time_seconds, place, varsity_points)
                        VALUES (:id, :race_id, :athlete_id, :time_seconds, :place, :points)
                    """),
                    {
                        "id": str(uuid.uuid4()),
                        "race_id": girls_race_id,
                        "athlete_id": athlete_id,
                        "time_seconds": time_seconds,
                        "place": place,
                        "points": points
                    }
                )
                
                print(f"Added athlete: {first_name} {last_name} - {minutes}:{seconds:02d} (Place: {place})")
            
            conn.commit()
            print("\n✅ Sample data inserted successfully!")
            print("🌐 You can now view the dashboard at http://localhost")
            print("📊 Try the following features:")
            print("   - Team Statistics: View best times by gender")
            print("   - Athletes List: Browse individual athlete profiles")  
            print("   - CSV Export: Download performance data")
            
    except Exception as e:
        print(f"Error inserting sample data: {e}")
        return False
        
    return True

if __name__ == "__main__":
    insert_sample_data()
