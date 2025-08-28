#!/usr/bin/env python3
"""
Cross Country Statistics Scraper

This module scrapes race results from co.milesplit.com and stores them in the database.
It reads configuration from a YAML file to determine which races to scrape.
"""

import os
import sys
import re
import time
import yaml
import logging
import argparse
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import DictCursor
from sqlalchemy import create_engine, text
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class RaceConfig:
    meet_name: str
    race_name: str
    distance: str
    race_class: str
    gender: str
    venue: str
    date: str
    season: str
    url: Optional[str] = None
    file: Optional[str] = None

@dataclass
class Athlete:
    first_name: str
    last_name: str
    gender: str
    school: str
    graduation_year: Optional[int] = None

@dataclass
class Result:
    athlete: Athlete
    time_seconds: float  # Changed to float to support fractional seconds
    place: int
    varsity_points: int = 0

class MileSplitScraper:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.engine = create_engine(database_url)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def clear_database(self):
        """Clear all existing race data from the database before scraping."""
        logger.info("Clearing existing race data from database...")
        
        try:
            with self.engine.begin() as conn:
                # Delete in proper order due to foreign key constraints
                conn.execute(text("DELETE FROM results"))
                conn.execute(text("DELETE FROM races"))  
                conn.execute(text("DELETE FROM meets"))
                conn.execute(text("DELETE FROM venues"))
                conn.execute(text("DELETE FROM athletes"))
                logger.info("Database cleared successfully")
        except Exception as e:
            logger.error(f"Error clearing database: {e}")
            raise

    def load_race_config(self, config_path: str) -> List[RaceConfig]:
        """Load race configuration from YAML file."""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                races = []
                for race_data in config.get('races', []):
                    races.append(RaceConfig(**race_data))
                return races
        except Exception as e:
            logger.error(f"Error loading config file {config_path}: {e}")
            return []

    def map_gender_for_db(self, config_gender: str) -> str:
        """Map configuration gender values to database gender values."""
        gender_map = {
            'boys': 'male',
            'girls': 'female',
            'mixed': 'mixed',
            'male': 'male',  # Already correct
            'female': 'female'  # Already correct
        }
        return gender_map.get(config_gender.lower(), config_gender.lower())

    def parse_time_to_seconds(self, time_str: str) -> Optional[float]:
        """Parse time string (MM:SS.ss, MM:SS, or extended formats) to total seconds with fractional support."""
        time_str = time_str.strip()
        
        # Handle different time formats seen in MileSplit (order matters!)
        patterns = [
            r'(\d{1,2}):(\d{2}):(\d{2})\.(\d{2})', # H:MM:SS.ss (for very long times) - CHECK FIRST
            r'(\d{1,2}):(\d{2}):(\d{2})',        # H:MM:SS - CHECK SECOND  
            r'(\d{1,2}):(\d{2})\.(\d{2})',      # MM:SS.ss
            r'(\d{1,2}):(\d{2})',              # MM:SS
            r'(\d{3,4})\.(\d{2})'              # SSS.ss or SSSS.ss (seconds only)
        ]
        
        for i, pattern in enumerate(patterns):
            match = re.match(pattern, time_str)
            if match:
                groups = match.groups()
                
                if i == 0:  # H:MM:SS.ss
                    hours, minutes, seconds, hundredths = groups
                    return float(int(hours) * 3600 + int(minutes) * 60 + int(seconds)) + float(int(hundredths)) / 100.0
                elif i == 1:  # H:MM:SS
                    hours, minutes, seconds = groups
                    return float(int(hours) * 3600 + int(minutes) * 60 + int(seconds))
                elif i == 2:  # MM:SS.ss
                    minutes, seconds, hundredths = groups
                    return float(int(minutes) * 60 + int(seconds)) + float(int(hundredths)) / 100.0
                elif i == 3:  # MM:SS
                    minutes, seconds = groups
                    return float(int(minutes) * 60 + int(seconds))
                elif i == 4:  # SSS.ss or SSSS.ss
                    seconds, hundredths = groups
                    return float(int(seconds)) + float(int(hundredths)) / 100.0
        
        logger.warning(f"Could not parse time format: {time_str}")
        return None

    def scrape_race_results(self, source: str, is_file: bool = False) -> List[Result]:
        """Scrape race results from MileSplit URL or local HTML file."""
        try:
            if is_file:
                # Read from local file
                if not os.path.exists(source):
                    logger.error(f"Local file not found: {source}")
                    return []
                
                with open(source, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                soup = BeautifulSoup(html_content, 'html.parser')
            else:
                # Fetch from URL
                response = self.session.get(source, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
            
            results = []
            
            # First try to find results in a <pre> tag (MileSplit raw format)
            pre_tag = soup.find('pre')
            if pre_tag:
                results = self.parse_pre_formatted_results(pre_tag.get_text())
                if results:
                    logger.info(f"Parsed {len(results)} results from pre-formatted text in {source}")
                    return results
            
            # Fall back to table parsing
            # Find the results table - MileSplit uses different table structures
            # Look for tables with class 'table' or common MileSplit table classes
            possible_tables = soup.find_all('table')
            
            results_table = None
            for table in possible_tables:
                # Check if this table contains race results by looking for time patterns
                table_text = table.get_text()
                if re.search(r'\d{1,2}:\d{2}', table_text):  # Look for time patterns like MM:SS
                    results_table = table
                    break
            
            if not results_table:
                # If no table found, try to parse from the raw text content
                # MileSplit sometimes displays results in markdown-like format
                return self.parse_results_from_text(soup.get_text())
            
            # Parse table rows
            rows = results_table.find_all('tr')
            header_processed = False
            
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 4:
                    continue
                
                # Skip header rows
                if not header_processed and any('place' in cell.get_text().lower() or 
                                              'name' in cell.get_text().lower() or
                                              'time' in cell.get_text().lower() 
                                              for cell in cells):
                    header_processed = True
                    continue
                
                try:
                    # Parse race result from table row
                    result = self.parse_table_row(cells)
                    if result:
                        results.append(result)
                        
                except Exception as e:
                    logger.warning(f"Error parsing table row: {e}")
                    continue
            
            logger.info(f"Scraped {len(results)} results from {source}")
            return results
            
        except requests.RequestException as e:
            logger.error(f"Error fetching {source}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error scraping {source}: {e}")
            return []

    def parse_pre_formatted_results(self, text: str) -> List[Result]:
        """Parse race results from pre-formatted text (MileSplit raw format)."""
        results = []
        lines = text.split('\n')
        
        logger.info("Parsing pre-formatted results...")
        
        # Skip header lines and find data lines
        in_results = False
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Skip header lines until we find the separator line
            if '====' in line:
                in_results = True
                continue
                
            if not in_results:
                continue
            
            # Parse data lines with fixed-width format
            # Example: "    1   1/124   3392 Ryan Ruffer                                  M   Fossil Ridge High School                   15:57  5:08"
            if len(line) < 50:  # Skip short lines
                continue
                
            # Try to extract place, bib, name, gender, school, and time using regex
            # The format appears to be: Place Div/Tot Bib# Name Sex School Time Pace
            match = re.match(r'^\s*(\d+)\s+\d+/\d+\s+\d+\s+(.+?)\s+(M|F)\s+(.+?)\s+(\d{1,2}:\d{2}(?:\.\d{2})?)\s+\d+:\d+\s*$', line)
            
            if match:
                place = int(match.group(1))
                name = match.group(2).strip()
                gender = match.group(3).strip()
                school = match.group(4).strip()
                time_str = match.group(5).strip()
                
                # Parse time to seconds
                time_seconds = self.parse_time_to_seconds(time_str)
                if time_seconds is None:
                    logger.warning(f"Could not parse time: {time_str}")
                    continue
                
                # Parse name into first and last
                name_parts = name.split()
                if len(name_parts) >= 2:
                    first_name = name_parts[0]
                    last_name = ' '.join(name_parts[1:])
                else:
                    first_name = name
                    last_name = ''
                
                # Determine gender string
                gender_str = 'male' if gender == 'M' else 'female'
                
                athlete = Athlete(
                    first_name=first_name,
                    last_name=last_name,
                    gender=gender_str,
                    school=school
                )
                
                result = Result(
                    athlete=athlete,
                    time_seconds=time_seconds,
                    place=place
                )
                
                results.append(result)
                logger.debug(f"Parsed: {place}. {first_name} {last_name} ({gender_str}) - {time_str}")
        
        logger.info(f"Parsed {len(results)} results from pre-formatted text")
        return results

    def parse_results_from_text(self, text: str) -> List[Result]:
        """Parse race results from raw text when HTML tables aren't available."""
        results = []
        lines = text.split('\n')
        
        # Debug: Log some sample lines to see what we're working with
        logger.info("Sample lines from webpage:")
        sample_lines = [line.strip() for line in lines if line.strip() and '|' in line][:10]
        for line in sample_lines:
            logger.info(f"  {line}")
        
        # Look for lines that match the MileSplit result pattern
        # Example: "| 1 |   | Fossil Ridge High School Joey Benson | 9 | Fossil Ridge High School | 18:21.00 | 1 |"
        result_pattern = r'\|\s*(\d+)\s*\|[^|]*\|\s*([^|]+?)\s*\|\s*(\d+)\s*\|[^|]*\|\s*(\d{1,2}:\d{2}(?:\.\d{2})?|\d{2}:\d{2}:\d{2}(?:\.\d{2})?)\s*\|'
        
        current_gender = 'male'  # Default assumption
        current_race_class = 'varsity'  # Default assumption
        
        lines_checked = 0
        matches_found = 0
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            lines_checked += 1
                
            # Check for gender and race class indicators in headers
            line_lower = line.lower()
            if any(word in line_lower for word in ['girls', 'female', 'women']):
                current_gender = 'female'
            elif any(word in line_lower for word in ['boys', 'male', 'men']):
                current_gender = 'male'
                
            if 'jv ' in line_lower or 'junior varsity' in line_lower:
                current_race_class = 'jv'
            elif 'varsity' in line_lower:
                current_race_class = 'varsity'
            elif 'freshman' in line_lower:
                current_race_class = 'freshman'
            
            # Look for result patterns
            match = re.search(result_pattern, line)
            if match:
                matches_found += 1
                logger.info(f"Found match in line: {line}")
                try:
                    place = int(match.group(1))
                    name_and_school = match.group(2).strip()
                    grade = int(match.group(3))
                    time_str = match.group(4).strip()
                    
                    logger.info(f"  Parsed: place={place}, name_school='{name_and_school}', grade={grade}, time='{time_str}'")
                    
                    # Parse the athlete name from the combined string
                    # Pattern is usually: "School Name First Last"
                    # We need to extract just the "First Last" part
                    
                    # Split by spaces and find where athlete name likely starts
                    words = name_and_school.split()
                    
                    # Common school indicators that help us identify where athlete name starts
                    school_indicators = ['high', 'school', 'middle', 'academy', 'charter', 'classical']
                    
                    # Find the last occurrence of school indicators
                    last_school_word_idx = -1
                    for i, word in enumerate(words):
                        if word.lower() in school_indicators:
                            last_school_word_idx = i
                    
                    # Extract athlete name (words after the school name)
                    if last_school_word_idx >= 0 and last_school_word_idx < len(words) - 2:
                        school_name = ' '.join(words[:last_school_word_idx + 1])
                        first_name = words[last_school_word_idx + 1]
                        last_name = words[last_school_word_idx + 2]
                    elif len(words) >= 2:
                        # Fallback: take last two words as name, school unknown
                        school_name = "Unknown School"
                        first_name = words[-2]
                        last_name = words[-1]
                    else:
                        logger.warning(f"Could not parse athlete name from: {name_and_school}")
                        continue
                    
                    logger.info(f"  Athlete: {first_name} {last_name} from {school_name}")
                    
                    # Parse time
                    time_seconds = self.parse_time_to_seconds(time_str)
                    if not time_seconds:
                        logger.warning(f"Could not parse time: {time_str}")
                        continue
                    
                    # Calculate varsity points (top 7 finishers typically score for varsity)
                    varsity_points = 0
                    if current_race_class == 'varsity' and place <= 7:
                        varsity_points = max(0, 8 - place)
                    
                    athlete = Athlete(
                        first_name=first_name,
                        last_name=last_name,
                        gender=current_gender,
                        school=school_name
                    )
                    
                    result = Result(
                        athlete=athlete,
                        time_seconds=time_seconds,
                        place=place,
                        varsity_points=varsity_points
                    )
                    
                    results.append(result)
                    logger.debug(f"Parsed result: {first_name} {last_name}, {time_str} -> {time_seconds}s, place {place}")
                    
                except (ValueError, IndexError) as e:
                    logger.warning(f"Error parsing line: {line[:100]}... - {e}")
                    continue
        
        logger.info(f"Checked {lines_checked} lines, found {matches_found} potential matches, parsed {len(results)} results from text content")
        return results

    def parse_table_row(self, cells) -> Optional[Result]:
        """Parse a table row into a Result object."""
        try:
            # Extract data (adjust indices based on MileSplit table structure)
            place_text = cells[0].get_text(strip=True)
            name_text = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            time_text = cells[-2].get_text(strip=True)  # Time is usually second to last
            
            # Parse place
            place_match = re.search(r'(\d+)', place_text)
            place = int(place_match.group(1)) if place_match else 0
            
            # Parse time
            time_seconds = self.parse_time_to_seconds(time_text)
            if not time_seconds:
                return None
            
            # Parse name - this is tricky as format varies
            name_parts = name_text.split()
            if len(name_parts) < 2:
                return None
            
            # Simple heuristic: take last two words as first and last name
            first_name = name_parts[-2] if len(name_parts) > 1 else name_parts[0]
            last_name = name_parts[-1]
            
            # Determine gender (will be set by the calling function based on page context)
            gender = 'male'  # Default, will be updated by caller
            
            # Calculate varsity points
            varsity_points = max(0, 8 - place) if place <= 7 else 0
            
            athlete = Athlete(
                first_name=first_name,
                last_name=last_name,
                gender=gender,
                school="Unknown School"  # Table format doesn't include school info
            )
            
            result = Result(
                athlete=athlete,
                time_seconds=time_seconds,
                place=place,
                varsity_points=varsity_points
            )
            
            return result
            
        except Exception as e:
            logger.warning(f"Error parsing table row: {e}")
            return None

    def determine_gender(self, soup: BeautifulSoup, name_text: str) -> str:
        """Determine gender from page content or default to 'male'."""
        # Look for gender indicators in the page title or headers
        title = soup.find('title')
        headers = soup.find_all(['h1', 'h2', 'h3'])
        
        text_content = ' '.join([title.get_text() if title else ''] + 
                               [h.get_text() for h in headers]).lower()
        
        if any(word in text_content for word in ['girls', 'female', 'women']):
            return 'female'
        elif any(word in text_content for word in ['boys', 'male', 'men']):
            return 'male'
        
        # Default fallback
        return 'male'

    def get_or_create_venue(self, venue_name: str) -> str:
        """Get existing venue or create new one, return venue_id."""
        with self.engine.connect() as conn:
            # Check if venue exists
            result = conn.execute(
                text("SELECT id FROM venues WHERE name = :name"),
                {"name": venue_name}
            ).fetchone()
            
            if result:
                return str(result[0])
            
            # Create new venue
            result = conn.execute(
                text("INSERT INTO venues (name) VALUES (:name) RETURNING id"),
                {"name": venue_name}
            )
            conn.commit()
            return str(result.fetchone()[0])

    def get_or_create_athlete(self, athlete: Athlete) -> str:
        """Get existing athlete or create new one, return athlete_id."""
        with self.engine.connect() as conn:
            # Check if athlete exists
            result = conn.execute(
                text("""
                    SELECT id FROM athletes 
                    WHERE first_name = :first_name 
                    AND last_name = :last_name 
                    AND gender = :gender
                    AND school = :school
                """),
                {
                    "first_name": athlete.first_name,
                    "last_name": athlete.last_name,
                    "gender": athlete.gender,
                    "school": athlete.school
                }
            ).fetchone()
            
            if result:
                return str(result[0])
            
            # Create new athlete
            result = conn.execute(
                text("""
                    INSERT INTO athletes (first_name, last_name, gender, school, graduation_year) 
                    VALUES (:first_name, :last_name, :gender, :school, :graduation_year) 
                    RETURNING id
                """),
                {
                    "first_name": athlete.first_name,
                    "last_name": athlete.last_name,
                    "gender": athlete.gender,
                    "school": athlete.school,
                    "graduation_year": athlete.graduation_year
                }
            )
            conn.commit()
            return str(result.fetchone()[0])

    def store_race_results(self, race_config: RaceConfig, results: List[Result]):
        """Store race results in the database, avoiding duplicates."""
        if not results:
            logger.warning(f"No results to store for race: {race_config.race_name}")
            return
        
        try:
            with self.engine.connect() as conn:
                # Get or create venue
                venue_id = self.get_or_create_venue(race_config.venue)
                
                # Check if meet already exists, if not create it
                meet_result = conn.execute(
                    text("""
                        SELECT id FROM meets 
                        WHERE name = :name AND meet_date = :meet_date AND venue_id = :venue_id
                    """),
                    {
                        "name": race_config.meet_name,
                        "meet_date": race_config.date,
                        "venue_id": venue_id
                    }
                ).fetchone()
                
                if meet_result:
                    meet_id = str(meet_result[0])
                else:
                    # Create new meet
                    meet_result = conn.execute(
                        text("""
                            INSERT INTO meets (name, meet_date, venue_id, season, milesplit_url)
                            VALUES (:name, :meet_date, :venue_id, :season, :url)
                            RETURNING id
                        """),
                        {
                            "name": race_config.meet_name,
                            "meet_date": race_config.date,
                            "venue_id": venue_id,
                            "season": race_config.season,
                            "url": race_config.url
                        }
                    )
                    meet_id = str(meet_result.fetchone()[0])
                
                # Check if race already exists
                race_result = conn.execute(
                    text("""
                        SELECT id FROM races 
                        WHERE meet_id = :meet_id AND name = :name AND distance = :distance 
                        AND race_class = :race_class AND gender = :gender
                    """),
                    {
                        "meet_id": meet_id,
                        "name": race_config.race_name,
                        "distance": race_config.distance,
                        "race_class": race_config.race_class,
                        "gender": self.map_gender_for_db(race_config.gender)
                    }
                ).fetchone()
                
                if race_result:
                    race_id = str(race_result[0])
                    logger.info(f"Race already exists: {race_config.race_name} - checking for new results")
                    
                    # Get existing results for this race to avoid duplicates
                    existing_results = conn.execute(
                        text("""
                            SELECT a.first_name, a.last_name, a.school, res.time_seconds, res.place
                            FROM results res
                            JOIN athletes a ON res.athlete_id = a.id
                            WHERE res.race_id = :race_id
                        """),
                        {"race_id": race_id}
                    ).fetchall()
                    
                    # Create a set of existing results for quick lookup
                    existing_set = set()
                    for existing in existing_results:
                        # Use normalized names and times for comparison
                        first_name = existing.first_name.strip().lower()
                        last_name = existing.last_name.strip().lower()
                        school = existing.school.strip().lower()
                        # Round time to 2 decimal places to handle minor variations
                        time_seconds = round(float(existing.time_seconds), 2)
                        place = existing.place
                        key = (first_name, last_name, school, time_seconds, place)
                        existing_set.add(key)
                    
                    new_results_count = 0
                    skipped_results_count = 0
                    for result in results:
                        # Normalize result data for comparison
                        first_name = result.athlete.first_name.strip().lower()
                        last_name = result.athlete.last_name.strip().lower()
                        school = result.athlete.school.strip().lower()
                        time_seconds = round(result.time_seconds, 2)
                        place = result.place
                        result_key = (first_name, last_name, school, time_seconds, place)
                        
                        if result_key not in existing_set:
                            athlete_id = self.get_or_create_athlete(result.athlete)
                            conn.execute(
                                text("""
                                    INSERT INTO results (race_id, athlete_id, time_seconds, place, varsity_points)
                                    VALUES (:race_id, :athlete_id, :time_seconds, :place, :varsity_points)
                                """),
                                {
                                    "race_id": race_id,
                                    "athlete_id": athlete_id,
                                    "time_seconds": result.time_seconds,
                                    "place": result.place,
                                    "varsity_points": result.varsity_points
                                }
                            )
                            new_results_count += 1
                        else:
                            skipped_results_count += 1
                    
                    if new_results_count > 0:
                        logger.info(f"Added {new_results_count} new results for race: {race_config.race_name}")
                    if skipped_results_count > 0:
                        logger.info(f"Skipped {skipped_results_count} duplicate results for race: {race_config.race_name}")
                    if new_results_count == 0 and skipped_results_count == 0:
                        logger.info(f"No new results to add for race: {race_config.race_name}")
                        
                else:
                    # Create new race
                    race_result = conn.execute(
                        text("""
                            INSERT INTO races (meet_id, name, distance, race_class, gender)
                            VALUES (:meet_id, :name, :distance, :race_class, :gender)
                            RETURNING id
                        """),
                        {
                            "meet_id": meet_id,
                            "name": race_config.race_name,
                            "distance": race_config.distance,
                            "race_class": race_config.race_class,
                            "gender": self.map_gender_for_db(race_config.gender)
                        }
                    )
                    race_id = str(race_result.fetchone()[0])
                    
                    # Store all results for new race
                    for result in results:
                        athlete_id = self.get_or_create_athlete(result.athlete)
                        
                        conn.execute(
                            text("""
                                INSERT INTO results (race_id, athlete_id, time_seconds, place, varsity_points)
                                VALUES (:race_id, :athlete_id, :time_seconds, :place, :varsity_points)
                            """),
                            {
                                "race_id": race_id,
                                "athlete_id": athlete_id,
                                "time_seconds": result.time_seconds,
                                "place": result.place,
                                "varsity_points": result.varsity_points
                            }
                        )
                    
                    logger.info(f"Created new race and stored {len(results)} results: {race_config.race_name}")
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error storing race results: {e}")
            raise

def main():
    """Main function to run the scraper."""
    parser = argparse.ArgumentParser(description='Cross Country Statistics Scraper')
    parser.add_argument('--clear-db', action='store_true', 
                       help='Clear all existing data before scraping')
    parser.add_argument('--config', type=str, default='/app/config/races.yaml',
                       help='Path to races configuration file')
    
    args = parser.parse_args()
    
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        logger.error("DATABASE_URL environment variable not set")
        sys.exit(1)
    
    config_path = args.config
    if not os.path.exists(config_path):
        # Fallback to environment variable if file not found
        config_path = os.getenv('CONFIG_PATH', '/app/config/races.yaml')
        if not os.path.exists(config_path):
            logger.error(f"Config file not found: {config_path}")
            sys.exit(1)
    
    scraper = MileSplitScraper(database_url)
    race_configs = scraper.load_race_config(config_path)
    
    if not race_configs:
        logger.error("No race configurations found")
        sys.exit(1)
    
    logger.info(f"Found {len(race_configs)} races to scrape")
    
    # Clear database if requested
    if args.clear_db:
        logger.info("Clearing existing data before scraping...")
        scraper.clear_database()
    
    # Process each race, checking for duplicates
    for race_config in race_configs:
        logger.info(f"Processing race: {race_config.meet_name} - {race_config.race_name}")
        try:
            # Determine source and type
            if race_config.file:
                # Use local file - look in the scraper directory
                if race_config.file.startswith('pages/'):
                    # File path is relative to scraper directory
                    file_path = os.path.join(os.path.dirname(__file__), race_config.file)
                else:
                    # File path is relative to config file
                    file_path = os.path.join(os.path.dirname(config_path), race_config.file)
                results = scraper.scrape_race_results(file_path, is_file=True)
            elif race_config.url:
                # Use URL
                results = scraper.scrape_race_results(race_config.url, is_file=False)
            else:
                logger.error(f"No source (URL or file) specified for race: {race_config.race_name}")
                continue
                
            if results:
                scraper.store_race_results(race_config, results)
            else:
                logger.warning(f"No results found for race: {race_config.race_name}")
                
        except Exception as e:
            logger.error(f"Error processing race {race_config.race_name}: {e}")
            continue
        
        # Be respectful to the server
        time.sleep(2)
    
    logger.info("Scraping completed")

if __name__ == "__main__":
    main()
