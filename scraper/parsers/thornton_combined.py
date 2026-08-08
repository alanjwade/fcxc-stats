"""
Parser for Thornton Cross Country Invitational combined format.

Handles combined HTML files where race data is in <pre> tags
with a specific tabular format. Uses section title matching.
"""

import re
from typing import List, Dict, Optional
from bs4 import BeautifulSoup

from .base import BaseParser, ParsedResult


class ThorntonCombinedParser(BaseParser):
    """Parses Thornton combined format with <pre> tag text sections."""
    parser_name = "thornton_combined"

    def can_parse(self, content: str) -> bool:
        return "Thornton" in content and "Cross Country" in content

    def extract_races(self, content: str) -> Dict[str, List[ParsedResult]]:
        """Extract all races from the combined Thornton page."""
        soup = BeautifulSoup(content, 'html.parser')
        pre_tag = soup.find('pre')
        if not pre_tag:
            return {}

        text = pre_tag.get_text()
        sections = {}

        # Split by race headers: "Thornton High School Invitational - JV Boys" etc.
        race_header_pattern = re.compile(
            r'^(Thornton High School Invitational - (?:JV|Varsity) (?:Boys|Girls))',
            re.MULTILINE
        )
        last_end = 0
        for match in race_header_pattern.finditer(text):
            section_title = match.group(1)
            start = match.start()
            if last_end > 0:
                # Process previous section
                prev_text = text[last_end:start]
                results = self._parse_race_text(prev_text)
                if results:
                    sections[section_title] = results
            last_end = start

        # Process last section
        if last_end > 0:
            prev_text = text[last_end:]
            results = self._parse_race_text(prev_text)
            if results:
                sections[section_title] = results

        return sections

    def _parse_race_text(self, text: str) -> List[ParsedResult]:
        """Parse a single race's text from Thornton format."""
        results = []
        for line in text.split('\n'):
            line = line.strip()
            if not line:
                continue

            # Pattern: Place Last, First Year School Time
            pat = r'^\s*(\d+)\s+([A-Z\'\-]+)\s*,\s*([A-Za-z\'\-]+)\s+(SR|JR|SO|FR|\d{2})\s+\d+\s+(.+?)\s+(\d{1,2}:\d{2}(?:\.\d{2})?)'
            m = re.match(pat, line)
            if m:
                try:
                    place = int(m.group(1))
                    last_name = m.group(2).strip().title()
                    first_name = m.group(3).strip().title()
                    year_str = m.group(4).strip()
                    school = m.group(5).strip()
                    time_str = m.group(6).strip()

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