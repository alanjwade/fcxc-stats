"""
Parser for Liberty Bell combined format.

Handles large combined HTML files containing multiple race sections.
Each section is identified by a results_title (section_title in YAML config).
"""

import re
from typing import List, Dict, Optional

from .base import BaseParser, ParsedResult


class RawCombinedParser(BaseParser):
    """Parses large combined HTML files with multiple race sections."""
    parser_name = "raw_combined"

    def can_parse(self, content: str) -> bool:
        # Liberty Bell style: has "Mens 5,000 Meters" or "Womens 5,000 Meters"
        return bool(re.search(r'(?:Mens|Womens)\s+\d{1,2},?\d{3}\s+Meters', content))

    def extract_races(self, content: str) -> Dict[str, List[ParsedResult]]:
        """
        Extract all race sections found in the content.
        Returns a dict mapping discovered section_title -> list of results.
        """
        sections = {}
        # Find all section headers
        section_pattern = re.compile(r'(Mens|Womens)\s+\d{1,2},?\d{3}\s+Meters\s+.*?(?=Team Results|Mens|Womens|\Z)', re.DOTALL)
        for match in section_pattern.finditer(content):
            section_text = match.group(0)
            # Extract the title from first line
            title_line = section_text.split('\n')[0].strip()
            race_key = title_line.strip()
            results = self._parse_section(section_text)
            if results:
                sections[race_key] = results
        return sections

    def _parse_section(self, section_text: str) -> List[ParsedResult]:
        results = []
        lines = section_text.split('\n')
        # Skip title line
        for line in lines[1:]:
            line = line.strip()
            if not line or re.match(r'^=+$', line) or re.match(r'^\s*Pl\s+Athlete', line):
                continue

            # Stop at Team Results or next section
            if re.match(r'^Team\s+(Results|Scores)', line, re.IGNORECASE):
                break

            # Pattern: Place Name Year School Time [Points]
            pat = r'^\s*(\d+)\s+([A-Za-z\'\-\.\s]+?)\s+(\d{1,2})\s+(.+?)\s+(\d{1,2}:\d{2}(?:\.\d{2})?)(?:\s+(\d+))?\s*$'
            m = re.match(pat, line)
            if m:
                try:
                    place = int(m.group(1))
                    name = m.group(2).strip()
                    year = m.group(3).strip()
                    school = m.group(4).strip()
                    time_str = m.group(5).strip()

                    time_seconds = self.parse_time_to_seconds(time_str)
                    if time_seconds is None:
                        continue

                    # Parse name into first and last
                    name_parts = name.split()
                    if len(name_parts) >= 2:
                        first_name = name_parts[0]
                        last_name = ' '.join(name_parts[1:])
                    else:
                        first_name = name
                        last_name = ''

                    school = school.replace('High Sc', 'High School').replace(' HS', ' High School')

                    results.append(ParsedResult(
                        first_name=first_name.title(),
                        last_name=last_name.title(),
                        school=school,
                        time_seconds=time_seconds,
                        place=place,
                    ))
                except (ValueError, IndexError):
                    continue
        return results