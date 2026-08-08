"""
Parser for Windsor Wizards combined format.

Handles combined HTML files with pre-formatted text race sections.
Each section starts with a header like "Windsor HS Varsity Boys" etc.
"""

import re
from typing import List, Dict, Optional

from .base import BaseParser, ParsedResult


class RawWindsorCombinedParser(BaseParser):
    """Parses Windsor Wizards combined format with pre-formatted text sections."""
    parser_name = "raw_windsor_combined"

    def can_parse(self, content: str) -> bool:
        return "Windsor" in content and bool(re.search(r'Windsor HS (Varsity|JV) (Boys|Girls)', content))

    def extract_races(self, content: str) -> Dict[str, List[ParsedResult]]:
        sections = {}
        # Find all Windsor section headers
        header_pattern = re.compile(r'(Windsor HS (?:Varsity|JV) (?:Boys|Girls))')
        for match in header_pattern.finditer(content):
            section_title = match.group(1)
            # Extract content from this header to the next header or end
            start = match.end()
            next_header = re.search(r'Windsor HS (?:Varsity|JV) (?:Boys|Girls)', content[start:])
            end = start + next_header.start() if next_header else len(content)
            section_text = content[start:end]
            results = self._parse_section(section_text)
            if results:
                sections[section_title] = results
        return sections

    def _parse_section(self, text: str) -> List[ParsedResult]:
        results = []
        for line in text.split('\n'):
            line = line.strip()
            if not line:
                continue

            # Pattern: Place LastName, FirstName Year School Score Time
            pat = r'^\s*(\d+)\s+([A-Z\s\'\-]+),\s+([A-Za-z\s\'\-]+?)\s+(SR|JR|SO|FR|\d{2})\s+\d+\s+(.+?)\s+(\d+)\s+(\d{1,2}:\d{2}\.\d+)'
            m = re.match(pat, line)
            if m:
                try:
                    place = int(m.group(1))
                    last_name = m.group(2).strip().title()
                    first_name = m.group(3).strip().title()
                    year_str = m.group(4).strip()
                    school = m.group(5).strip()
                    time_str = m.group(7).strip()

                    time_seconds = self.parse_time_to_seconds(time_str)
                    if time_seconds is None:
                        continue

                    school = self._normalize_school_name(school)
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