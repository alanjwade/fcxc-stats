"""
Parser for Loveland Sweetheart and Hawk JV Championships format.

Handles plain-text results with specific format including grade, bib, school,
score, time, and gap columns. Uses section_title matching.
"""

import re
from typing import List, Dict, Optional

from .base import BaseParser, ParsedResult


class LovelandSweetheartParser(BaseParser):
    """Parses Loveland Sweetheart and Hawk JV results text format."""
    parser_name = "loveland_sweetheart"

    def can_parse(self, content: str) -> bool:
        # Check for the specific Loveland/Hawk format: place, lastname, firstname, grade, bib, school, score, time, gap
        return bool(re.search(r'^\s*\d+\s+[A-Z\s\-\']+,\s+[A-Za-z\s\-\']+?\s+(SR|JR|SO|FR)', content, re.MULTILINE))

    def extract_races(self, content: str) -> Dict[str, List[ParsedResult]]:
        sections = {}
        current_section = None
        current_lines = []

        for line in content.split('\n'):
            # Look for section headers (e.g., "HS Varsity Boys 5K", "Boys 5000 Meters")
            header_match = re.match(r'^(.+? (?:Boys|Girls) .+?)\s*$', line.strip())
            if header_match and not re.match(r'^\s*\d+\s+[A-Z]', line):
                if current_section and current_lines:
                    results = self._parse_section('\n'.join(current_lines))
                    if results:
                        sections[current_section] = results
                current_section = header_match.group(1).strip()
                current_lines = []
                continue
            current_lines.append(line)

        # Last section
        if current_section and current_lines:
            results = self._parse_section('\n'.join(current_lines))
            if results:
                sections[current_section] = results

        return sections

    def _parse_section(self, text: str) -> List[ParsedResult]:
        results = []
        for line in text.split('\n'):
            line = line.strip()
            if not line or line.startswith('=') or line.startswith('-'):
                continue

            # Pattern: Place LASTNAME, Firstname Year Bib School Score Time Gap
            pat = r'^\s*(\d+)\s+([A-Z\s\-\']+),\s+([A-Za-z\s\-\']+?)\s+(SR|JR|SO|FR|\d{1,2})\s+(\d+)\s+(.+?)\s+(?:\d+)\s+(\d{1,2}:\d{2}\.\d{2})\s+.*$'
            m = re.match(pat, line)
            if m:
                try:
                    place = int(m.group(1))
                    last_name = m.group(2).strip().title()
                    first_name = m.group(3).strip().title()
                    year_str = m.group(4).strip()
                    school = m.group(6).strip()
                    time_str = m.group(7).strip()

                    time_seconds = self.parse_time_to_seconds(time_str)
                    if time_seconds is None:
                        continue

                    grad_year = self._guess_graduation_year(year_str)
                    results.append(ParsedResult(
                        first_name=first_name,
                        last_name=last_name,
                        school=school,
                        time_seconds=time_seconds,
                        place=place,
                        graduation_year=grad_year,
                    ))
                except (ValueError, IndexError):
                    continue
        return results