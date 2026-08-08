"""
Parser for Northern Conference Championships format.

Handles combined plain-text results with multiple races identified
by a race_number (1-6). Each race is a specific section in the file.
"""

import re
from typing import List, Dict, Optional

from .base import BaseParser, ParsedResult


class NorthernConferenceParser(BaseParser):
    """Parses Northern Conference combined text with race_number sections."""
    parser_name = "northern_conference"

    def can_parse(self, content: str) -> bool:
        return "Northern Conference" in content

    def extract_races(self, content: str) -> Dict[str, List[ParsedResult]]:
        """
        Extract races. The YAML config passes race_number (1-6) per race,
        which corresponds to the order of race sections in the file.
        Returns a dict keyed by race_number (as string) -> results.
        """
        sections = {}

        # Split content into race sections by looking for section header lines
        # like "Race 1", "RACE 1", or similar numeric markers
        race_num_pattern = re.compile(r'^\s*(?:Race|RACE|EVENT)?\s*#?\s*(\d+)\s*$', re.IGNORECASE | re.MULTILINE)
        matches = list(race_num_pattern.finditer(content))

        if not matches:
            # Fallback: single race
            return {"1": self._parse_section(content)}

        for idx, match in enumerate(matches):
            start = match.end()
            end = matches[idx + 1].start() if idx + 1 < len(matches) else len(content)
            race_num = match.group(1)
            results = self._parse_section(content[start:end])
            if results:
                sections[str(race_num)] = results

        return sections

    def _parse_section(self, text: str) -> List[ParsedResult]:
        results = []
        for line in text.split('\n'):
            line = line.strip()
            if not line or line.startswith('=') or line.startswith('-'):
                continue

            # Pattern: Place Last, First Grade School Time
            pat = r'^\s*(\d+)\s+([A-Z\'\-]+),\s+([A-Za-z\'\-]+)\s+(\d{1,2})\s+(.+?)\s+(\d{1,2}:\d{2}(?:\.\d{2})?)'
            m = re.match(pat, line)
            if m:
                try:
                    place = int(m.group(1))
                    last_name = m.group(2).strip().title()
                    first_name = m.group(3).strip().title()
                    school = m.group(5).strip()
                    time_str = m.group(6).strip()

                    time_seconds = self.parse_time_to_seconds(time_str)
                    if time_seconds is None:
                        continue

                    results.append(ParsedResult(
                        first_name=first_name,
                        last_name=last_name,
                        school=school,
                        time_seconds=time_seconds,
                        place=place,
                    ))
                except (ValueError, IndexError):
                    continue
        return results