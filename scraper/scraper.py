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
    algorithm: Optional[str] = 'default'
    results_title: Optional[str] = None
    race_number: Optional[int] = None

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
        patterns = [
            r'(\d{1,2}):(\d{2}):(\d{2})\.(\d{1,2})', # H:MM:SS.s or H:MM:SS.ss (for very long times)
            r'(\d{1,2}):(\d{2}):(\d{2})',        # MM:SS:ss (hundredths)
            r'(\d{1,2}):(\d{2})\.(\d{1,2})',      # MM:SS.s or MM:SS.ss
            r'(\d{1,2}):(\d{2})',              # MM:SS
            r'(\d{3,4})\.(\d{1,2})'              # SSS.s or SSS.ss or SSSS.s or SSSS.ss (seconds only)
        ]
        for i, pattern in enumerate(patterns):
            match = re.match(pattern, time_str)
            if match:
                groups = match.groups()
                if i == 0:  # H:MM:SS.s or H:MM:SS.ss
                    hours, minutes, seconds, fractional = groups
                    # Handle variable decimal places (1 or 2 digits)
                    if len(fractional) == 1:
                        fractional_seconds = float(int(fractional)) / 10.0
                    else:
                        fractional_seconds = float(int(fractional)) / 100.0
                    total = float(int(hours) * 3600 + int(minutes) * 60 + int(seconds)) + fractional_seconds
                elif i == 1:  # MM:SS:ss (hundredths)
                    minutes, seconds, hundredths = groups
                    total = float(int(minutes) * 60 + int(seconds)) + float(int(hundredths)) / 100.0
                elif i == 2:  # MM:SS.s or MM:SS.ss
                    minutes, seconds, fractional = groups
                    # Handle variable decimal places (1 or 2 digits)
                    if len(fractional) == 1:
                        fractional_seconds = float(int(fractional)) / 10.0
                    else:
                        fractional_seconds = float(int(fractional)) / 100.0
                    total = float(int(minutes) * 60 + int(seconds)) + fractional_seconds
                elif i == 3:  # MM:SS
                    minutes, seconds = groups
                    total = float(int(minutes) * 60 + int(seconds))
                elif i == 4:  # SSS.s or SSS.ss or SSSS.s or SSSS.ss
                    seconds, fractional = groups
                    # Handle variable decimal places (1 or 2 digits)
                    if len(fractional) == 1:
                        fractional_seconds = float(int(fractional)) / 10.0
                    else:
                        fractional_seconds = float(int(fractional)) / 100.0
                    total = float(int(seconds)) + fractional_seconds
                # Sanity check: reject times over 1 hour (3600 seconds)
                if total > 3600:
                    print(f"ERROR: Parsed time exceeds sanity threshold: {total} seconds from '{time_str}'")
                    logger.error(f"Sanity check failed: time {total} from '{time_str}'")
                    sys.exit(1)
                return total
        logger.warning(f"Could not parse time format: {time_str}")
        return None

    def scrape_john_martin_format(self, source: str, is_file: bool = False, gender: str = 'unknown') -> List[Result]:
        """Scrape race results using the John Martin format (custom algorithm)."""
        import re
        try:
            if is_file:
                if not os.path.exists(source):
                    logger.error(f"File not found: {source}")
                    return []
                with open(source, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                soup = BeautifulSoup(html_content, 'html.parser')
            else:
                response = self.session.get(source, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table')
            if not table:
                logger.warning("No table found in John Martin format file.")
                return []
            results = []
            time_pattern = re.compile(r'^(\d{1,2}:\d{2}(?:\.\d{2})?|\d{1,2}:\d{2}:\d{2}(?:\.\d{2})?|\d{2,4}\.\d{2})$')
            rows = table.find_all('tr')
            # Skip header row if present
            if rows and len(rows) > 1:
                rows = rows[1:]
            for row in rows:
                cells = row.find_all('td')
                if len(cells) != 4:
                    continue
                place_val = cells[0].get_text(strip=True)
                name_val = cells[1].get_text(strip=True)
                school_val = cells[2].get_text(strip=True)
                time_val = cells[3].get_text(strip=True)
                if not time_pattern.match(time_val):
                    logger.error(f"Invalid time format found in row: place='{place_val}', name='{name_val}', school='{school_val}', time='{time_val}'")
                    print(f"ERROR: Invalid time format in John Martin file: place='{place_val}', name='{name_val}', school='{school_val}', time='{time_val}'")
                    sys.exit(1)
                try:
                    place = int(place_val)
                    name = name_val
                    school = school_val
                    time_seconds = self.parse_time_to_seconds(time_val)
                    if time_seconds is None:
                        print(f"ERROR: Could not parse time format: place='{place_val}', name='{name_val}', school='{school_val}', time='{time_val}'")
                        logger.error(f"Could not parse time format: place='{place_val}', name='{name_val}', school='{school_val}', time='{time_val}'")
                        sys.exit(1)
                    name_parts = name.split()
                    first_name = name_parts[0]
                    last_name = ' '.join(name_parts[1:]) if len(name_parts) > 1 else ''
                    first_name = self.normalize_name(first_name)
                    last_name = self.normalize_name(last_name)
                    athlete = Athlete(
                        first_name=first_name,
                        last_name=last_name,
                        gender=self.map_gender_for_db(gender),
                        school=self.normalize_school_name(school)
                    )
                    result = Result(
                        athlete=athlete,
                        time_seconds=time_seconds,
                        place=place
                    )
                    results.append(result)
                except Exception as e:
                    print(f"ERROR: Exception parsing row: place='{place_val}', name='{name_val}', school='{school_val}', time='{time_val}' - {e}")
                    logger.error(f"Error parsing John Martin row: place='{place_val}', name='{name_val}', school='{school_val}', time='{time_val}' - {e}")
                    sys.exit(1)
            logger.info(f"Parsed {len(results)} results from John Martin format.")
            return results
        except Exception as e:
            logger.error(f"Error scraping John Martin format: {e}")
            raise

    def scrape_thornton_combined_format(self, source: str, is_file: bool = False, gender: str = 'unknown', race_config: Optional[RaceConfig] = None) -> List[Result]:
        """Scrape race results using the Thornton combined format (custom algorithm)."""
        import re
        try:
            if is_file:
                if not os.path.exists(source):
                    logger.error(f"File not found: {source}")
                    return []
                with open(source, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                soup = BeautifulSoup(html_content, 'html.parser')
            else:
                response = self.session.get(source, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')

            # Find the pre-formatted text section
            pre_tag = soup.find('pre')
            if not pre_tag:
                logger.warning("No <pre> tag found in Thornton combined format file.")
                return []

            text = pre_tag.get_text()
            if not text:
                logger.warning("No text content found in <pre> tag.")
                return []

            # Determine which specific race section to parse based on race_config
            if not race_config:
                logger.error("No race configuration provided for Thornton combined format")
                return []
            
            # Map race_config to the specific race header in the file
            race_class = race_config.race_class.lower()
            gender_str = race_config.gender.lower()
            
            # Determine the target race header and gender for parsing
            target_race_header = None
            target_gender = None
            
            if race_class == "jv" and gender_str == "boys":
                target_race_header = "JV Boys 5000 Meter Run"
                target_gender = "M"
            elif race_class == "jv" and gender_str == "girls":
                target_race_header = "JV Girls 5000 Meter Run"
                target_gender = "F"
            elif race_class == "varsity" and gender_str == "boys":
                target_race_header = "Varsity Boys 5000 Meter Run"
                target_gender = "M"
            elif race_class == "varsity" and gender_str == "girls":
                target_race_header = "Varsity Girls 5000 Meter Run"
                target_gender = "F"
            else:
                logger.error(f"Unknown race combination: {race_class} {gender_str}")
                return []

            logger.info(f"Looking for race section: {target_race_header}")
            
            # Find the start of this race section
            start_pattern = re.escape(target_race_header)
            start_match = re.search(start_pattern, text, re.IGNORECASE)
            
            if not start_match:
                logger.warning(f"Could not find race section: {target_race_header}")
                return []
            
            start_pos = start_match.end()
            
            # Find the end of this race section (next race header or team scores)
            end_patterns = [
                r"Team Scores",
                r"JV (?:Boys|Girls) 5000 Meter Run",
                r"Varsity (?:Boys|Girls) 5000 Meter Run"
            ]
            
            end_pos = len(text)
            for pattern in end_patterns:
                end_match = re.search(pattern, text[start_pos:], re.IGNORECASE)
                if end_match:
                    potential_end = start_pos + end_match.start()
                    if potential_end > start_pos:
                        end_pos = potential_end
                        break
            
            race_text = text[start_pos:end_pos]
            logger.info(f"Extracted {len(race_text)} characters for {target_race_header}")
            
            # Parse this specific race section
            results = self.parse_thornton_race_text(race_text, race_config.gender)
            
            logger.info(f"Parsed {len(results)} total results from Thornton combined format for {target_race_header}.")
            return results
            
        except Exception as e:
            logger.error(f"Error scraping Thornton combined format: {e}")
            raise

    def parse_thornton_race_text(self, text: str, gender: str) -> List[Result]:
        """Parse a single race section from Thornton format text."""
        results = []
        lines = text.split('\n')
        
        logger.info(f"Parsing Thornton race text with {len(lines)} lines")
        
        # Find the data lines - they start after the header separator
        in_results = False
        header_separator_found = False
        
        for line_num, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            
            # Look for the separator line with dashes
            if '-' * 10 in line:  # Line with many dashes indicates start of data
                in_results = True
                header_separator_found = True
                continue
            
            if not in_results:
                continue
                
            # Stop if we hit team results
            if "Team Scores" in line or "Total Time:" in line:
                logger.info(f"Hit team results section at line {line_num}, stopping parse")
                break
                
            # Parse data lines
            # Format: Place Name Year School Time Points
            # Example: "  1 Flint Hartsky         11 Laramie High School  16:42.45      1"
            
            # Use regex to parse the fixed-width format
            # The format appears to be fixed-width: place(3) name(variable) grade(2) school(variable) time(8) points(variable)
            # Since split by multiple spaces doesn't work due to truncated school names, use a different approach
            
            # Try to match the specific pattern where we know the time format
            time_match = re.search(r'(\d{1,2}:\d{2}\.\d{2})', line)
            if time_match:
                time_str = time_match.group(1)
                time_start = time_match.start()
                time_end = time_match.end()
                
                # Everything before the time should contain: place, name, grade, school
                before_time = line[:time_start].strip()
                # Everything after the time should contain: points
                after_time = line[time_end:].strip()
                
                # Parse the part before time - split by multiple spaces to separate name section from grade+school section
                parts_before = re.split(r'\s{2,}', before_time)
                if len(parts_before) >= 2:
                    # First part: place and name
                    place_name_part = parts_before[0].strip()
                    place_name_match = re.match(r'^(\d+)\s+(.+)$', place_name_part)
                    if not place_name_match:
                        continue
                    
                    place = int(place_name_match.group(1))
                    name = place_name_match.group(2).strip()
                    
                    # Second part: grade and school (joined if there were more parts)
                    grade_school_part = ' '.join(parts_before[1:]).strip()
                    grade_school_match = re.match(r'^(\d{1,2})\s+(.+)$', grade_school_part)
                    if not grade_school_match:
                        continue
                        
                    year = grade_school_match.group(1).strip()
                    school = grade_school_match.group(2).strip()
                    
                    # Points from after time
                    points = after_time
                    
                else:
                    # Fall back to original regex
                    match = re.match(r'^\s*(\d+)\s+(.+?)\s+(\d{2})\s+(.+?)\s+(\d{1,2}:\d{2}\.\d{2})\s*(\d*)\s*$', line)
                    if match:
                        place = int(match.group(1))
                        name = match.group(2).strip()
                        year = match.group(3).strip()
                        school = match.group(4).strip()
                        time_str = match.group(5).strip()
                        points = match.group(6).strip() if match.group(6) else ""
                    else:
                        continue
            else:
                # No time found, skip this line
                continue
            
            if place is not None:
                try:
                    # Variables already set by parsing logic above
                    
                    # Parse time to seconds
                    time_seconds = self.parse_time_to_seconds(time_str)
                    if time_seconds is None:
                        logger.warning(f"Could not parse time: {time_str} in line: {line}")
                        continue
                    
                    # Parse name into first and last
                    name_parts = name.split()
                    if len(name_parts) >= 2:
                        first_name = name_parts[0]
                        last_name = ' '.join(name_parts[1:])
                    else:
                        first_name = name
                        last_name = ''
                    
                    first_name = self.normalize_name(first_name)
                    last_name = self.normalize_name(last_name)
                    
                    # Fix truncated school names for Thornton format
                    school = self.fix_thornton_school_name(school)
                    
                    athlete = Athlete(
                        first_name=first_name,
                        last_name=last_name,
                        gender=self.map_gender_for_db(gender),
                        school=self.normalize_school_name(school)
                    )
                    
                    result = Result(
                        athlete=athlete,
                        time_seconds=time_seconds,
                        place=place
                    )
                    
                    results.append(result)
                    logger.debug(f"Parsed: {place}. {first_name} {last_name} - {school} - {time_str}")
                    
                except Exception as e:
                    logger.warning(f"Error parsing Thornton line '{line}': {e}")
                    continue
            else:
                # Log lines that don't match for debugging
                if line and not line.startswith('=') and len(line) > 10:
                    logger.debug(f"Line didn't match pattern: '{line}'")
        
        if not header_separator_found:
            logger.warning("No header separator found in race text")
        
        # Validate that the highest place number matches the total number of results
        if results:
            places = [result.place for result in results if result.place is not None]
            if places:
                max_place = max(places)
                total_results = len(results)
                if max_place != total_results:
                    logger.warning(f"Place number validation failed: highest place is {max_place} but total results is {total_results}. Some results may be missing.")
                else:
                    logger.info(f"Place number validation passed: {max_place} places match {total_results} total results")
            
        logger.info(f"Parsed {len(results)} results from Thornton race text")
        return results

    def scrape_raw_combined_format(self, source: str, is_file: bool = False, race_config: Optional[RaceConfig] = None) -> List[Result]:
        """Scrape race results using the raw combined format (custom algorithm with results_title)."""
        import re
        try:
            if is_file:
                if not os.path.exists(source):
                    logger.error(f"File not found: {source}")
                    return []
                with open(source, 'r', encoding='utf-8') as f:
                    content = f.read()
            else:
                response = self.session.get(source, timeout=30)
                response.raise_for_status()
                content = response.text

            # Check if race configuration is provided
            if not race_config:
                logger.error("No race configuration provided for raw combined format")
                return []
            
            # Get the results_title from the race configuration
            results_title = getattr(race_config, 'results_title', None)
            if not results_title:
                logger.error("No results_title specified in race configuration for raw combined format")
                return []

            logger.info(f"Looking for results section: {results_title}")
            
            # Find the start of the results section using results_title
            start_idx = content.find(results_title)
            if start_idx == -1:
                logger.warning(f"Results title '{results_title}' not found in content")
                return []

            # Get content starting from the results_title
            section_content = content[start_idx:]
            lines = section_content.splitlines()

            results = []
            results_started = False
            header_lines_skipped = 0
            max_header_lines = 20  # Allow up to 20 lines of headers after results_title
            
            # Skip the first line (which is the results_title itself)
            start_line_idx = 1
            
            logger.info(f"Processing {len(lines)} lines from results section")
            
            for line_idx, line in enumerate(lines[start_line_idx:], start_line_idx):
                line = line.strip()
                if not line:
                    continue
                
                # Skip header lines (equals signs, column headers, etc.)
                if re.match(r'^=+$', line) or re.match(r'^\s*Pl\s+Athlete\s+Yr\s+Team\s+Time', line):
                    header_lines_skipped += 1
                    logger.debug(f"Skipping header line {line_idx}: '{line[:30]}...'")
                    continue
                line = line.strip()
                if not line:
                    continue
                
                # Stop if we reach another section (Team Results, another race, etc.)
                stop_patterns = [
                    r'^Team\s+Results',
                    r'^Team\s+Scores',
                    r'^Scoring\s+Summary',
                    r'^\s*(JV|Varsity)\s+(Boys|Girls)',  # Another race section
                    r'^\s*\d+\.\s*[A-Za-z]+\s+[A-Za-z]+.*Team\s+Results',  # Team scoring line
                    r'^={5,}',  # Section dividers with multiple equals signs
                    r'^\s*Pl\s+Team\s+Points',  # Team scoring header
                    # Stop when we see the next race type (different from our current one)
                    r'^Womens\s+\d+,?\d*\s+Meters' if 'Mens' in results_title else r'^Mens\s+\d+,?\d*\s+Meters',
                    # Stop when we see a different race level (JV vs Varsity)
                    r'JV' if 'Varsity' in results_title else r'Varsity',
                ]
                
                # Skip the first line (which is the results_title itself)
                start_idx = 1
                
                for pattern in stop_patterns:
                    if re.search(pattern, line, re.IGNORECASE):
                        logger.info(f"Stopping parse at line {line_idx}: found section delimiter '{line[:50]}...'")
                        return results
                
                # Try to match a result line: place, name, year, school, time, [points]
                # Common formats:
                # "  1 John Doe           12 School Name       16:42.45    1"
                # "  1 John Doe           12 School Name       16:42.45"
                result_pattern = r'^\s*(\d+)\s+([A-Za-z\'\-\.\s]+?)\s+(\d{1,2})\s+(.+?)\s+(\d{1,2}:\d{2}\.\d{2})(?:\s+(\d+))?\s*$'
                match = re.match(result_pattern, line)
                
                if match:
                    results_started = True
                    try:
                        place = int(match.group(1))
                        name = match.group(2).strip()
                        year = match.group(3).strip()
                        school = match.group(4).strip()
                        time_str = match.group(5).strip()
                        points = match.group(6).strip() if match.group(6) else ""

                        # Parse time to seconds
                        time_seconds = self.parse_time_to_seconds(time_str)
                        if time_seconds is None:
                            logger.warning(f"Could not parse time: {time_str} in line: {line}")
                            continue

                        # Parse name into first and last
                        name_parts = name.split()
                        if len(name_parts) >= 2:
                            first_name = name_parts[0]
                            last_name = ' '.join(name_parts[1:])
                        else:
                            first_name = name
                            last_name = ''

                        first_name = self.normalize_name(first_name)
                        last_name = self.normalize_name(last_name)

                        # Clean up school name
                        school = school.replace('High Sc', 'High School')
                        school = school.replace(' HS', ' High School')
                        
                        athlete = Athlete(
                            first_name=first_name,
                            last_name=last_name,
                            gender=self.map_gender_for_db(race_config.gender),
                            school=self.normalize_school_name(school)
                        )

                        result = Result(
                            athlete=athlete,
                            time_seconds=time_seconds,
                            place=place
                        )

                        results.append(result)
                        logger.debug(f"Parsed: {place}. {first_name} {last_name} - {school} - {time_str}")
                        
                    except Exception as e:
                        logger.warning(f"Error parsing raw_combined line '{line}': {e}")
                        continue
                        
                elif results_started and line.strip() == '':
                    # Empty line after results started might indicate end of results
                    continue
                    
                elif not results_started:
                    # Allow for headers before results start
                    header_lines_skipped += 1
                    if header_lines_skipped > max_header_lines:
                        logger.warning(f"Skipped {header_lines_skipped} header lines without finding results. Stopping.")
                        break
                    continue
                else:
                    # Log lines that don't match for debugging
                    if len(line) > 10 and not line.startswith('=') and not line.startswith('-'):
                        logger.debug(f"Line didn't match result pattern: '{line[:50]}...'")

            logger.info(f"Parsed {len(results)} results from raw_combined format for {results_title}")
            return results
            
        except Exception as e:
            logger.error(f"Error scraping raw_combined format: {e}")
            raise

    def scrape_raw_windsor_combined_format(self, source: str, is_file: bool = False, race_config: Optional[RaceConfig] = None) -> List[Result]:
        """Scrape race results using the Windsor combined format (custom algorithm with results_title)."""
        import re
        try:
            if is_file:
                if not os.path.exists(source):
                    logger.error(f"File not found: {source}")
                    return []
                with open(source, 'r', encoding='utf-8') as f:
                    content = f.read()
            else:
                response = self.session.get(source, timeout=30)
                response.raise_for_status()
                content = response.text

            # Check if race configuration is provided
            if not race_config:
                logger.error("No race configuration provided for raw Windsor combined format")
                return []
            
            # Get the results_title from the race configuration
            results_title = getattr(race_config, 'results_title', None)
            if not results_title:
                logger.error("No results_title specified in race configuration for raw Windsor combined format")
                return []

            logger.info(f"Looking for Windsor race section: {results_title}")
            
            # Find the start of the results section using results_title
            start_idx = content.find(results_title)
            if start_idx == -1:
                logger.warning(f"Results title '{results_title}' not found in content")
                return []

            # Get content starting from the results_title
            section_content = content[start_idx:]
            lines = section_content.splitlines()

            results = []
            
            # Skip the first line (which is the results_title itself)
            # Skip team results section - look for the athlete results table
            current_line_idx = 1
            in_team_section = True
            
            logger.info(f"Processing {len(lines)} lines from Windsor results section")
            
            # Skip team results - look for the athlete results header
            while current_line_idx < len(lines) and in_team_section:
                line = lines[current_line_idx].strip()
                
                # Look for the athlete results header (starts with Pl, Athlete, etc.)
                if re.match(r'^\s*Pl\s+Athlete\s+Yr.*Team.*Time', line):
                    # Found the athlete results header, skip it and the separator line
                    current_line_idx += 1
                    if current_line_idx < len(lines) and re.match(r'^=+$', lines[current_line_idx].strip()):
                        current_line_idx += 1
                    in_team_section = False
                    break
                    
                current_line_idx += 1
            
            if in_team_section:
                logger.warning(f"Could not find athlete results section for {results_title}")
                return []
            
            # Now parse the athlete results
            for line_idx in range(current_line_idx, len(lines)):
                line = lines[line_idx].strip()
                if not line:
                    continue
                
                # Stop if we reach another section (next race, end of results)
                stop_patterns = [
                    r'^Windsor HS.*Boys|^Windsor HS.*Girls',  # Next Windsor race section
                    r'^[A-Z][a-z]+ HS.*Boys|^[A-Z][a-z]+ HS.*Girls',  # Any other school's race section
                    r'^={3,}$',  # Section dividers
                    r'^\s*Middle School',  # Middle school sections to ignore
                ]
                
                for pattern in stop_patterns:
                    if re.search(pattern, line, re.IGNORECASE):
                        logger.info(f"Stopping Windsor parse at line {line_idx}: found section delimiter '{line[:50]}...'")
                        logger.info(f"Parsed {len(results)} results from Windsor format for {results_title}")
                        return results
                
                # Parse Windsor format result line:
                # "   1 STEVESON, Lucas         SR 8088 Cheyenne East High S          1    15:52.3        --- "
                # Place, LASTNAME, Firstname, Year, Bib, School, Score, Time, Gap
                result_pattern = r'^\s*(\d+)\s+([A-Z\s\-\']+),\s*([A-Za-z\s\-\']+?)\s+(SR|JR|SO|FR|\d{1,2})\s+(\d+)\s+(.+?)\s+(\d+|)\s+(\d{1,2}:\d{2}\.\d+)\s+.*$'
                match = re.match(result_pattern, line)
                
                if match:
                    try:
                        place = int(match.group(1))
                        last_name_raw = match.group(2).strip()
                        first_name = match.group(3).strip()
                        year_str = match.group(4).strip()
                        bib = match.group(5).strip()
                        school_raw = match.group(6).strip()
                        score = match.group(7).strip()
                        time_str = match.group(8).strip()
                        
                        # Normalize last name (convert from ALL CAPS)
                        last_name = last_name_raw.title()
                        
                        # Convert year abbreviation to graduation year
                        current_year = 2025
                        if year_str in ['SR', '12']:
                            graduation_year = current_year
                        elif year_str in ['JR', '11']:
                            graduation_year = current_year + 1
                        elif year_str in ['SO', '10']:
                            graduation_year = current_year + 2
                        elif year_str in ['FR', '9']:
                            graduation_year = current_year + 3
                        else:
                            graduation_year = None

                        # Clean up school name
                        school = self.fix_thornton_school_name(school_raw.strip())
                        
                        # Parse time
                        time_seconds = self.parse_time_to_seconds(time_str)
                        if time_seconds is None:
                            logger.warning(f"Could not parse time '{time_str}' for {first_name} {last_name}")
                            continue

                        # Create athlete object
                        # Map gender from race config to database format
                        db_gender = 'male' if race_config.gender == 'boys' else 'female'
                        
                        athlete = Athlete(
                            first_name=first_name,
                            last_name=last_name,
                            gender=db_gender,
                            school=school,
                            graduation_year=graduation_year
                        )

                        result = Result(
                            athlete=athlete,
                            time_seconds=time_seconds,
                            place=place
                        )
                        results.append(result)
                        
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Error parsing Windsor result line: '{line}' - {e}")
                        continue
                else:
                    # Log lines that don't match for debugging
                    if len(line) > 10 and not line.startswith('=') and not line.startswith('-'):
                        logger.debug(f"Windsor line didn't match result pattern: '{line[:50]}...'")

            logger.info(f"Parsed {len(results)} results from Windsor format for {results_title}")
            return results
            
        except Exception as e:
            logger.error(f"Error scraping Windsor combined format: {e}")
            raise

    def fix_thornton_school_name(self, school_name: str) -> str:
        """Fix truncated school names specifically from Thornton format parsing."""
        # Mapping of truncated names to full names found in Thornton results
        school_mappings = {
            'Fort Collins': 'Fort Collins High School',  # Desert Twilight format
            'Fort Collins High Sc': 'Fort Collins High School',
            'Fossil Ridge High Sc': 'Fossil Ridge High School', 
            'Rocky Mountain High': 'Rocky Mountain High School',
            'Denver East High Sch': 'Denver East High School',
            'Clear Creek High Sch': 'Clear Creek High School',
            'Fort Lupton High Sch': 'Fort Lupton High School',
            'Westminster High Sch': 'Westminster High School',
            'Wheat Ridge High Sch': 'Wheat Ridge High School',
            'Cheyenne Central Hig': 'Cheyenne Central High School',
            'Cheyenne East High S': 'Cheyenne East High School',
            'Prospect Ridge Acade': 'Prospect Ridge Academy',
            'Frederick High Schoo': 'Frederick High School',
            'Ascent Classical Aca': 'Ascent Classical Academy of Northern Colorado',
            'Ascent Classical Academy of Nort': 'Ascent Classical Academy of Northern Colorado'
        }
        
        # Return the corrected name if found in mapping, otherwise return original
        return school_mappings.get(school_name, school_name)

    def scrape_race_results(self, source: str, is_file: bool = False, algorithm: str = 'default', gender: str = 'unknown', race_config: Optional[RaceConfig] = None) -> List[Result]:
        """Scrape race results using the selected algorithm."""
        logger.info(f"Algorithm selected: '{algorithm}' for source: {source}")
        if algorithm == 'john_martin':
            return self.scrape_john_martin_format(source, is_file, gender=gender)
        elif algorithm == 'thornton_combined':
            return self.scrape_thornton_combined_format(source, is_file, gender=gender, race_config=race_config)
        elif algorithm == 'raw_combined':
            return self.scrape_raw_combined_format(source, is_file, race_config=race_config)
        elif algorithm == 'raw_windsor_combined':
            return self.scrape_raw_windsor_combined_format(source, is_file, race_config=race_config)
        elif algorithm == 'desert_twilight':
            return self.scrape_desert_twilight_format(source, is_file, race_config=race_config)
        elif algorithm == 'loveland_sweetheart':
            return self.scrape_loveland_sweetheart_format(source, is_file, race_config=race_config)
        elif algorithm == 'longs_peak':
            return self.scrape_longs_peak_format(source, is_file, race_config=race_config)
        elif algorithm == 'northern_conference':
            return self.scrape_northern_conference_format(source, is_file, race_config=race_config)
        # Default algorithm
        try:
            if is_file:
                if not os.path.exists(source):
                    logger.error(f"File not found: {source}")
                    return []
                with open(source, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                soup = BeautifulSoup(html_content, 'html.parser')
            else:
                response = self.session.get(source, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
            results = []
            pre_tag = soup.find('pre')
            if pre_tag:
                results = self.parse_pre_formatted_results(pre_tag.get_text())
                if results:
                    return results
            possible_tables = soup.find_all('table')
            results_table = None
            for table in possible_tables:
                table_text = table.get_text()
                if re.search(r'\d{1,2}:\d{2}', table_text):
                    results_table = table
                    break
            if not results_table:
                return self.parse_results_from_text(soup.get_text())
            rows = results_table.find_all('tr')
            header_processed = False
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 4:
                    continue
                if not header_processed and any('place' in cell.get_text().lower() or 
                                              'name' in cell.get_text().lower() or
                                              'time' in cell.get_text().lower() 
                                              for cell in cells):
                    header_processed = True
                    continue
                try:
                    result = self.parse_table_row(cells)
                    # Normalize names for default algorithm
                    if result:
                        result.athlete.first_name = self.normalize_name(result.athlete.first_name)
                        result.athlete.last_name = self.normalize_name(result.athlete.last_name)
                        results.append(result)
                except Exception as e:
                    logger.warning(f"Error parsing row: {e}")
            logger.info(f"Scraped {len(results)} results from {source}")
            return results
        except requests.RequestException as e:
            logger.error(f"Error fetching {source}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error scraping {source}: {e}")
            return []

    def scrape_desert_twilight_format(self, source: str, is_file: bool = False, race_config: Optional[RaceConfig] = None) -> List[Result]:
        """Scrape race results using the Desert Twilight format."""
        import re
        try:
            if is_file:
                if not os.path.exists(source):
                    logger.error(f"File not found: {source}")
                    return []
                with open(source, 'r', encoding='utf-8') as f:
                    content = f.read()
            else:
                response = self.session.get(source, timeout=30)
                response.raise_for_status()
                content = response.text

            # Check if race configuration is provided
            if not race_config:
                logger.error("No race configuration provided for Desert Twilight format")
                return []
            
            # Get the results_title from the race configuration
            results_title = getattr(race_config, 'results_title', None)
            if not results_title:
                logger.error("No results_title specified in race configuration for Desert Twilight format")
                return []

            logger.info(f"Looking for Desert Twilight race section: {results_title}")
            
            # Find the start of the results section using results_title
            start_idx = content.find(results_title)
            if start_idx == -1:
                logger.warning(f"Results title '{results_title}' not found in content")
                return []

            # Get content starting from the results_title
            section_content = content[start_idx:]
            lines = section_content.splitlines()

            results = []
            
            logger.info(f"Processing {len(lines)} lines from Desert Twilight results section")
            
            # Start from line 1 (skip the results_title line)
            current_line_idx = 1
            
            # Skip header lines until we find the first result (place "1")
            while current_line_idx < len(lines):
                line = lines[current_line_idx].strip()
                
                # Look specifically for place "1" to start actual results
                if line == "1":
                    break
                
                current_line_idx += 1
            
            if current_line_idx >= len(lines):
                logger.warning(f"Could not find results for {results_title}")
                return []
            
            # Parse the results - format is:
            # place
            # optional abbreviation (2 letters)  
            # name
            # team
            # time
            # year info (with optional PR indicator)
            i = current_line_idx
            while i < len(lines):
                line = lines[i].strip()
                if not line:
                    i += 1
                    continue
                
                # Check if we've reached the end (next race section)
                if line in ['Team Scores', 'Charts'] or line.startswith('View All Records'):
                    break
                
                # Look for place number
                if re.match(r'^\d+$', line):
                    try:
                        place = int(line)
                        i += 1
                        
                        # Check for optional abbreviation on next line (2 letters or malformed like "A(" or "M(")
                        if i < len(lines):
                            next_line = lines[i].strip()
                            # Match complete abbreviations (2 letters) or malformed ones (letter + symbol)
                            if re.match(r'^[A-Z]{2}$', next_line) or re.match(r'^[A-Z][^a-zA-Z\s]', next_line):
                                # Skip the abbreviation (valid or malformed)
                                i += 1
                        
                        # Get name (should be next line)
                        if i >= len(lines):
                            break
                        name_line = lines[i].strip()
                        if not name_line:
                            i += 1
                            continue
                        i += 1
                        
                        # Get team (should be next line)
                        if i >= len(lines):
                            break
                        team_line = lines[i].strip()
                        if not team_line:
                            i += 1
                            continue
                        i += 1
                        
                        # Get time (should be next line)
                        if i >= len(lines):
                            break
                        time_line = lines[i].strip()
                        if not time_line:
                            i += 1
                            continue
                        i += 1
                        
                        # Get year info (should be next line)
                        if i >= len(lines):
                            break
                        year_line = lines[i].strip()
                        if not year_line:
                            i += 1
                            continue
                        i += 1
                        
                        # Parse the extracted data
                        first_name, last_name = self.parse_desert_twilight_name(name_line)
                        school = self.fix_thornton_school_name(team_line)
                        time_seconds = self.parse_time_to_seconds(time_line)
                        graduation_year = self.parse_desert_twilight_year(year_line)
                        
                        if time_seconds is None:
                            logger.warning(f"Could not parse time '{time_line}' for {first_name} {last_name}")
                            continue
                        
                        # Create athlete object
                        # Map gender from race config to database format
                        db_gender = 'male' if race_config.gender == 'boys' else 'female'
                        
                        athlete = Athlete(
                            first_name=first_name,
                            last_name=last_name,
                            gender=db_gender,
                            school=school,
                            graduation_year=graduation_year
                        )

                        result = Result(
                            athlete=athlete,
                            time_seconds=time_seconds,
                            place=place
                        )
                        results.append(result)
                        
                        logger.debug(f"Parsed: {place}. {first_name} {last_name} ({school}) - {time_line}")
                        
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Error parsing Desert Twilight result at line {i}: {e}")
                        i += 1
                        continue
                else:
                    i += 1

            logger.info(f"Parsed {len(results)} results from Desert Twilight format for {results_title}")
            return results
            
        except Exception as e:
            logger.error(f"Error scraping Desert Twilight format: {e}")
            raise

    def parse_desert_twilight_name(self, name_line: str) -> Tuple[str, str]:
        """Parse a name from Desert Twilight format."""
        # Names can have parentheses like "Addison (Addy) Ritzenhein" 
        # or just be normal "Oliver Horton"
        name = name_line.strip()
        
        # Handle names with parentheses - extract the main name
        if '(' in name and ')' in name:
            # Remove the parenthetical part
            name = re.sub(r'\([^)]*\)', '', name).strip()
        
        # Split by space to get first and last name
        name_parts = name.split()
        if len(name_parts) >= 2:
            first_name = name_parts[0]
            last_name = ' '.join(name_parts[1:])
        else:
            # Fallback if only one name part
            first_name = name_parts[0] if name_parts else "Unknown"
            last_name = "Unknown"
        
        return first_name, last_name

    def parse_desert_twilight_year(self, year_line: str) -> Optional[int]:
        """Parse graduation year from Desert Twilight year format."""
        # Format examples: "PR • Yr: 11", "Yr: 12", "SR • Yr: 12", "Yr: SR"
        year_match = re.search(r'Yr:\s*(\d{1,2}|SR|JR|SO|FR)', year_line)
        
        if year_match:
            year_str = year_match.group(1).strip()
            current_year = 2025
            
            # Convert year to graduation year
            if year_str in ['12', 'SR']:
                return current_year
            elif year_str in ['11', 'JR']:
                return current_year + 1
            elif year_str in ['10', 'SO']:
                return current_year + 2
            elif year_str in ['9', 'FR']:
                return current_year + 3
            elif year_str.isdigit():
                grade = int(year_str)
                if grade >= 9 and grade <= 12:
                    return current_year + (12 - grade)
        
        return None

    def scrape_northern_conference_format(self, source: str, is_file: bool = False, race_config: Optional[RaceConfig] = None) -> List[Result]:
        """Scrape race results using the Northern Conference Championships format."""
        try:
            if is_file:
                if not os.path.exists(source):
                    logger.error(f"File not found: {source}")
                    return []
                with open(source, 'r', encoding='utf-8') as f:
                    content = f.read()
            else:
                response = self.session.get(source, timeout=30)
                response.raise_for_status()
                content = response.text

            if not race_config:
                logger.error("No race configuration provided for Northern Conference format")
                return []
            
            # Determine race number based on race class and gender
            race_number = getattr(race_config, 'race_number', None)
            if not race_number:
                logger.error("No race_number specified in race configuration for Northern Conference format")
                return []

            logger.info(f"Looking for Northern Conference Race #{race_number}")
            logger.info(f"Content length: {len(content)} characters")
            
            lines = content.splitlines()
            results = []
            
            # Find the race section by looking for "Race #X"
            race_marker = f"Race #{race_number}"
            section_start = -1
            
            for i, line in enumerate(lines):
                line_stripped = line.strip()
                if line_stripped.startswith(race_marker):
                    section_start = i
                    logger.info(f"Found race section '{race_marker}' at line {i}")
                    break
            
            if section_start == -1:
                logger.warning(f"Race section '{race_marker}' not found in content")
                return []
            
            # Find "Individual Results" section after the race marker
            individual_results_start = -1
            for i in range(section_start, len(lines)):
                if "Individual Results" in lines[i]:
                    individual_results_start = i
                    logger.info(f"Found 'Individual Results' at line {i}")
                    break
            
            if individual_results_start == -1:
                logger.warning("'Individual Results' section not found")
                return []
            
            # Parse from individual results section
            i = individual_results_start + 1
            
            # Skip the header line (Athlete, Yr., #, Team, Score, Time, Gap, Avg. Mile)
            while i < len(lines):
                line = lines[i].strip()
                if line and not line.startswith('Athlete'):
                    break
                i += 1
            
            while i < len(lines):
                line = lines[i].strip()
                
                # Stop at next race section or end of file
                if line.startswith('Race #') or not line:
                    if line.startswith('Race #'):
                        break
                    i += 1
                    continue
                
                # Try to parse result line
                # Format: "1	GABRIELSON, Trent	SR	9382	Thompson Valley High School	1	15:28.2	---	4:58.5"
                # Fields: Place, LASTNAME, firstname, Year, Bib#, Team, Score, Time, Gap, Pace
                # We care about: Place, name, year (optional), score (for varsity points), time
                
                # Split by tabs
                parts = line.split('\t')
                
                if len(parts) >= 8:
                    try:
                        place_str = parts[0].strip()
                        name_str = parts[1].strip()
                        year_str = parts[2].strip()
                        # bib is parts[3] - we don't need it
                        team_str = parts[4].strip()
                        score_str = parts[5].strip()
                        time_str = parts[6].strip()
                        # gap is parts[7], pace is parts[8] - we don't need them
                        
                        # Parse place
                        place = int(place_str)
                        
                        # Parse name (format: "LASTNAME, Firstname")
                        if ',' in name_str:
                            name_parts = name_str.split(',', 1)
                            last_name = name_parts[0].strip()
                            first_name = name_parts[1].strip() if len(name_parts) > 1 else ''
                        else:
                            logger.warning(f"Name format unexpected: {name_str}")
                            i += 1
                            continue
                        
                        # Parse time to seconds
                        time_seconds = self.parse_time_to_seconds(time_str)
                        if time_seconds is None:
                            logger.warning(f"Could not parse time: {time_str} in line: {line}")
                            i += 1
                            continue

                        # Normalize names
                        first_name = self.normalize_name(first_name)
                        last_name = self.normalize_name(last_name)

                        # Determine varsity points based on race class
                        varsity_points = 0
                        if race_config.race_class.lower() == 'varsity':
                            varsity_points = 1
                        
                        athlete = Athlete(
                            first_name=first_name,
                            last_name=last_name,
                            gender=self.map_gender_for_db(race_config.gender),
                            school=self.normalize_school_name(team_str)
                        )

                        result = Result(
                            athlete=athlete,
                            time_seconds=time_seconds,
                            place=place,
                            varsity_points=varsity_points
                        )

                        results.append(result)
                        logger.debug(f"Parsed: {place}. {first_name} {last_name} - {team_str} - {time_str}")
                        
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Error parsing Northern Conference line '{line}': {e}")
                else:
                    logger.debug(f"Skipping line with insufficient fields: {line}")
                
                i += 1

            logger.info(f"Parsed {len(results)} results from Northern Conference format for Race #{race_number}")
            return results
            
        except Exception as e:
            logger.error(f"Error scraping Northern Conference format: {e}")
            raise

    def scrape_longs_peak_format(self, source: str, is_file: bool = False, race_config: Optional[RaceConfig] = None) -> List[Result]:
        """Scrape race results using the Longs Peak Invitational format."""
        try:
            if is_file:
                if not os.path.exists(source):
                    logger.error(f"File not found: {source}")
                    return []
                with open(source, 'r', encoding='utf-8') as f:
                    content = f.read()
            else:
                response = self.session.get(source, timeout=30)
                response.raise_for_status()
                content = response.text

            if not race_config:
                logger.error("No race configuration provided for Longs Peak format")
                return []

            logger.info(f"Looking for Longs Peak results")
            logger.info(f"Content length: {len(content)} characters")
            
            lines = content.splitlines()
            results = []
            
            # Find the start of results section (after "High School Boys" or "High School Girls")
            section_start = -1
            gender_marker = "High School Boys" if race_config.gender.lower() in ['male', 'boys'] else "High School Girls"
            
            for i, line in enumerate(lines):
                line_stripped = line.strip()
                if line_stripped == gender_marker:
                    section_start = i
                    logger.info(f"Found results section '{gender_marker}' at line {i}")
                    break
            
            if section_start == -1:
                logger.warning(f"Results section '{gender_marker}' not found in content")
                return []
            
            # Parse from the section start
            i = section_start + 1
            
            while i < len(lines):
                line = lines[i].strip()
                
                # Skip empty lines
                if not line:
                    i += 1
                    continue
                
                # Stop at footer or end indicators
                if 'Number of records:' in line or 'MileSplit PRO' in line:
                    break
                
                # Skip DNS entries (lines starting with "-- --")
                if line.startswith('-- --'):
                    logger.debug(f"Skipping DNS entry: {line}")
                    i += 1
                    continue
                
                # Try to parse result line
                # Format: "1 1 296 Antheney HERRE Loveland Classical High School 5:19 16:31"
                # Or:     "10 -- 10 Caleb ESTANOL Estes Park High School 5:44 17:48" (non-scoring)
                # Or:     "42 (36) 284 Jovian KNOELL Golden View Classical Academy 6:14 19:22" (non-scoring team member beyond top 5)
                # IMPORTANT: Format is "Firstname LASTNAME" where LASTNAME is in ALL CAPS
                
                # Pattern to match result lines
                result_pattern = r'^(\d+|--)\s+(\d+|--|\(\d+\))\s+(\d+)\s+([A-Za-z][A-Za-z\'\-\.]*(?:\s+[A-Za-z][A-Za-z\'\-\.]*)*)\s+([A-Z][A-Z\'\-\.]+)\s+(.+?)\s+(\d{1,2}:\d{2})\s+(\d{1,2}:\d{2}(?:\.\d+)?)\s*$'
                match = re.match(result_pattern, line)
                
                if match:
                    place_str = match.group(1).strip()
                    points_str = match.group(2).strip()
                    bib = match.group(3).strip()
                    first_name = match.group(4).strip()  # Firstname (mixed case)
                    last_name = match.group(5).strip()   # LASTNAME (all caps)
                    team = match.group(6).strip()
                    pace = match.group(7).strip()
                    time_str = match.group(8).strip()
                    
                    # Skip entries with no place (DNS)
                    if place_str == '--':
                        logger.debug(f"Skipping DNS entry: {line}")
                        i += 1
                        continue
                    
                    try:
                        place = int(place_str)
                        
                        # Parse time to seconds
                        time_seconds = self.parse_time_to_seconds(time_str)
                        if time_seconds is None:
                            logger.warning(f"Could not parse time: {time_str} in line: {line}")
                            i += 1
                            continue

                        # Normalize names
                        first_name = self.normalize_name(first_name)
                        last_name = self.normalize_name(last_name)

                        # Determine varsity points (all Longs Peak races are varsity)
                        varsity_points = 1
                        
                        athlete = Athlete(
                            first_name=first_name,
                            last_name=last_name,
                            gender=self.map_gender_for_db(race_config.gender),
                            school=self.normalize_school_name(team)
                        )

                        result = Result(
                            athlete=athlete,
                            time_seconds=time_seconds,
                            place=place,
                            varsity_points=varsity_points
                        )

                        results.append(result)
                        logger.debug(f"Parsed: {place}. {first_name} {last_name} - {team} - {time_str}")
                        
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Error parsing Longs Peak line '{line}': {e}")
                else:
                    # Log lines that don't match for debugging
                    if len(line) > 5 and not line.startswith('Place') and 'Pace' not in line and 'Time' not in line:
                        logger.debug(f"Line didn't match result pattern: '{line}'")
                
                i += 1

            logger.info(f"Parsed {len(results)} results from Longs Peak format")
            return results
            
        except Exception as e:
            logger.error(f"Error scraping Longs Peak format: {e}")
            raise

    def scrape_loveland_sweetheart_format(self, source: str, is_file: bool = False, race_config: Optional[RaceConfig] = None) -> List[Result]:
        """Scrape race results using the Loveland Sweetheart format."""
        try:
            if is_file:
                if not os.path.exists(source):
                    logger.error(f"File not found: {source}")
                    return []
                with open(source, 'r', encoding='utf-8') as f:
                    content = f.read()
            else:
                response = self.session.get(source, timeout=30)
                response.raise_for_status()
                content = response.text

            if not race_config:
                logger.error("No race configuration provided for Loveland Sweetheart format")
                return []
            
            results_title = getattr(race_config, 'results_title', None)
            if not results_title:
                logger.error("No results_title specified in race configuration for Loveland Sweetheart format")
                return []

            logger.info(f"Looking for Loveland Sweetheart results section: {results_title}")
            logger.info(f"Content length: {len(content)} characters")
            
            lines = content.splitlines()
            results = []
            
            # Find the start of our specific race section
            section_start = -1
            logger.info(f"File has {len(lines)} lines, searching for '{results_title}'")
            
            for i, line in enumerate(lines):
                line_stripped = line.strip()
                if line_stripped == results_title:
                    section_start = i
                    logger.info(f"Found section '{results_title}' at line {i}")
                    break
            
            if section_start == -1:
                logger.warning(f"Results title '{results_title}' not found in content")
                return []
            
            # Parse from the section start
            i = section_start + 1
            in_team_results = False
            in_individual_results = False
            
            while i < len(lines):
                line = lines[i].strip()
                
                # Skip empty lines
                if not line:
                    i += 1
                    continue
                
                # Check if we hit the start of team results header 
                if re.match(r'^Rank\s+Team\s+Score\s+Avg', line):
                    in_team_results = True
                    in_individual_results = False
                    logger.debug(f"Entering team results section at line {i}")
                    i += 1
                    continue
                
                # Check if we hit individual results header
                if re.match(r'^\s*Pl\s+Athlete\s+Yr\s+Team', line):
                    in_team_results = False
                    in_individual_results = True
                    logger.debug(f"Entering individual results section at line {i}")
                    i += 1
                    continue
                
                # Skip separator lines (but don't change section state)
                if re.match(r'^={5,}', line) or re.match(r'^-{5,}', line):
                    i += 1
                    continue
                
                # Check if we've reached the next race section (stop parsing)
                next_race_patterns = [
                    r'^HS Varsity (Boys|Girls) 5K$',
                    r'^(Boys|Girls) HS Open 5K$'
                ]
                
                for pattern in next_race_patterns:
                    if re.match(pattern, line):
                        # Only stop if this is a different race than what we're looking for
                        if line != results_title:
                            logger.info(f"Reached next race section at line {i}: {line}")
                            return results
                
                # If we're in team results, skip this line
                if in_team_results:
                    logger.debug(f"Skipping team results line: '{line}'")
                    i += 1
                    continue
                
                # If we're in individual results, try to parse result lines
                if in_individual_results:
                    # Skip separator lines
                    if re.match(r'^={5,}', line) or re.match(r'^-{5,}', line):
                        logger.debug(f"Skipping separator line: '{line}'")
                        i += 1
                        continue
                    
                    # Try to parse a result line
                    # Format: " 57 SPIERS, Sylvia 9 Fort Collins High Sc 49 24:41.0 6:10.1"
                    # Or:     " 70 BRADFORD, Charleigh 9 Windsor Charter Acad 25:24.0 6:53.1" (no score)
                    # Handle cases where place might be "--" (DNF)
                    
                    # Try pattern with score first  
                    result_pattern_with_score = r'^\s*(\d+|--)\s+([^,]+),\s*([^\s]+)\s+(\S+)\s+(.+?)\s+(\d+)\s+(\d{1,2}:\d{2}\.\d+)\s+([\d:\.]+|---)\s*$'
                    match = re.match(result_pattern_with_score, line)
                    
                    if not match:
                        # Try pattern without score (no score number between school and time)
                        result_pattern_no_score = r'^\s*(\d+|--)\s+([^,]+),\s*([^\s]+)\s+(\S+)\s+(.+?)\s+(\d{1,2}:\d{2}\.\d+)\s+([\d:\.]+|---)\s*$'
                        match = re.match(result_pattern_no_score, line)
                    
                    if match:
                        place_str = match.group(1).strip()
                        
                        # Skip DNF entries (place is "--")
                        if place_str == '--':
                            logger.debug(f"Skipping DNF entry: {line}")
                            i += 1
                            continue
                        
                        try:
                            place = int(place_str)
                            last_name = match.group(2).strip()
                            first_name = match.group(3).strip()
                            year = match.group(4).strip()
                            school = match.group(5).strip()
                            
                            # Check if we matched the pattern with score or without score
                            if len(match.groups()) == 8:  # Pattern with score (8 groups)
                                score = match.group(6).strip()
                                time_str = match.group(7).strip()
                            else:  # Pattern without score (7 groups)
                                score = ""
                                time_str = match.group(6).strip()
                            
                            # Skip if no time (DNF)
                            if not time_str or time_str == '--':
                                logger.debug(f"Skipping entry with no time: {line}")
                                i += 1
                                continue
                            
                            # Parse time to seconds
                            time_seconds = self.parse_time_to_seconds(time_str)
                            if time_seconds is None:
                                logger.warning(f"Could not parse time: {time_str} in line: {line}")
                                i += 1
                                continue

                            # Normalize names
                            first_name = self.normalize_name(first_name)
                            last_name = self.normalize_name(last_name)

                            # Clean up school name
                            school = school.replace('High Sc', 'High School')
                            school = school.replace(' HS', ' High School')
                            
                            # Determine if this is varsity or JV based on the race title
                            varsity_points = 0
                            if 'Varsity' in results_title:
                                varsity_points = 1
                            
                            athlete = Athlete(
                                first_name=first_name,
                                last_name=last_name,
                                gender=self.map_gender_for_db(race_config.gender),
                                school=self.normalize_school_name(school)
                            )

                            result = Result(
                                athlete=athlete,
                                time_seconds=time_seconds,
                                place=place,
                                varsity_points=varsity_points
                            )

                            results.append(result)
                            logger.debug(f"Parsed: {place}. {first_name} {last_name} - {school} - {time_str}")
                            
                        except (ValueError, IndexError) as e:
                            logger.warning(f"Error parsing Loveland Sweetheart line '{line}': {e}")
                    else:
                        if len(line.strip()) > 5 and not line.startswith('=') and not line.startswith('-') and 'Pl' not in line and 'Rank' not in line:
                            logger.info(f"Line didn't match result pattern: '{line.strip()}'")
                        elif len(line.strip()) > 0:
                            logger.debug(f"Skipping non-result line: '{line.strip()}'")
                
                i += 1

            logger.info(f"Parsed {len(results)} results from Loveland Sweetheart format for {results_title}")
            return results
            
        except Exception as e:
            logger.error(f"Error scraping Loveland Sweetheart format: {e}")
            raise

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
                    school=self.normalize_school_name(school)
                )
                
                result = Result(
                    athlete=athlete,
                    time_seconds=time_seconds,
                    place=place
                )
                
                results.append(result)
                logger.debug(f"Parsed: {place}. {first_name} {last_name} ({gender_str}) - {time_str}")
        
        # Validate that the highest place number matches the total number of results
        if results:
            places = [result.place for result in results if result.place is not None]
            if places:
                max_place = max(places)
                total_results = len(results)
                if max_place != total_results:
                    logger.warning(f"Place number validation failed: highest place is {max_place} but total results is {total_results}. Some results may be missing.")
                else:
                    logger.info(f"Place number validation passed: {max_place} places match {total_results} total results")
        
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
                        school=self.normalize_school_name(school_name)
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
        
        # Validate that the highest place number matches the total number of results
        if results:
            places = [result.place for result in results if result.place is not None]
            if places:
                max_place = max(places)
                total_results = len(results)
                if max_place != total_results:
                    logger.warning(f"Place number validation failed: highest place is {max_place} but total results is {total_results}. Some results may be missing.")
                else:
                    logger.info(f"Place number validation passed: {max_place} places match {total_results} total results")
        
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
                    
                    # Bulk process new results for better performance
                    new_results = []
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
                            new_results.append({
                                "race_id": race_id,
                                "athlete_id": athlete_id,
                                "time_seconds": result.time_seconds,
                                "place": result.place,
                                "varsity_points": result.varsity_points
                            })
                        else:
                            skipped_results_count += 1
                    
                    # Bulk insert new results
                    if new_results:
                        conn.execute(
                            text("""
                                INSERT INTO results (race_id, athlete_id, time_seconds, place, varsity_points)
                                VALUES (:race_id, :athlete_id, :time_seconds, :place, :varsity_points)
                            """),
                            new_results
                        )
                    
                    new_results_count = len(new_results)
                    
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

    def normalize_name(self, name: str) -> str:
        """Capitalize first letter, lower case the rest for each word in a name."""
        return ' '.join([w.capitalize() for w in name.split()])

    def normalize_school_name(self, school: str) -> str:
        """Normalize school names to standard formats."""
        school = school.strip()
        
        # Map common school name variations to standardized names
        school_mappings = {
            'Fort Collins': 'Fort Collins High School',
            'Fort Collins HS': 'Fort Collins High School',
            'Fort Collins High Sc': 'Fort Collins High School',
            'FCHS': 'Fort Collins High School',
            'Fossil Ridge HS': 'Fossil Ridge High School',
            'Fossil Ridge': 'Fossil Ridge High School',
            'Rocky Mountain HS': 'Rocky Mountain High School',
            'Rocky Mountain': 'Rocky Mountain High School',
        }
        
        # Check exact matches first
        if school in school_mappings:
            return school_mappings[school]
        
        # Check for partial matches and common patterns
        if school.endswith(' HS') and not school.endswith(' High School'):
            # Convert "School Name HS" to "School Name High School"
            base_name = school[:-3].strip()
            return f"{base_name} High School"
        
        return school

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
                results = scraper.scrape_race_results(file_path, is_file=True, algorithm=getattr(race_config, 'algorithm', 'default'), gender=getattr(race_config, 'gender', 'unknown'), race_config=race_config)
            elif race_config.url:
                # Use URL
                results = scraper.scrape_race_results(race_config.url, is_file=False, algorithm=getattr(race_config, 'algorithm', 'default'), gender=getattr(race_config, 'gender', 'unknown'), race_config=race_config)
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
        
        # Only sleep for URL-based requests to be respectful to servers
        if race_config.url and not race_config.file:
            time.sleep(2)
    
    logger.info("Scraping completed")

if __name__ == "__main__":
    main()
