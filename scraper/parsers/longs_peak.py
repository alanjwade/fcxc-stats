"""
Parser for Longs Peak Invitational format.

Handles plain-text results with a specific format.
Two separate files (boys.txt, girls.txt), one race per file.
"""

import re
from typing import List, Dict, Optional

from .base import BaseParser, ParsedResult


class LongsPeakParser(BaseParser):
    """Parses Longs Peak Invitational plain-text results."""
    parser_name = "longs_peak"

    def can_parse(self, content: str) -> bool:
        return "Longs Peak" in content

    def extract_races(self, content: str) -> Dict[str, List[ParsedResult]]:
        results = []
        for line in content.split('\n'):
            line = line.strip()
            if not line:
                continue

            # Pattern: Place Last, First Grade School Time
            pat = r'^\s*(\d+)\s+([A-Z\'\-]+),\s+([A-Za-z\'\-]+)\s+(\d{1,2})\s+(.+?)\s+(\d{1,2}:\d{2}(?:\.\d{2})?)'
            m = re.match(pat, line)
            if m:
                try:
                    place = int(m.group(1))
                    last_name = m.group(2).strip().title()
                    first_name = m.group(3).strip().title()
                    grade = m.group(4).strip()
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
        return {"default": results} if results else {}