"""
Parser for Desert Twilight format.

Handles results from Desert Twilight XC Invite (plain text files).
"""

import re
from typing import List, Dict, Optional, Tuple

from .base import BaseParser, ParsedResult


class DesertTwilightParser(BaseParser):
    """Parses Desert Twilight plain-text results."""
    parser_name = "desert_twilight"

    def can_parse(self, content: str) -> bool:
        return "Desert Twilight" in content

    def extract_races(self, content: str) -> Dict[str, List[ParsedResult]]:
        results = []
        lines = content.split('\n')
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if not line:
                i += 1
                continue

            # Look for athlete name in format "Smith, John" or "John Smith"
            name_match = re.match(r'^([A-Za-z\'\-]+(?:,\s+[A-Za-z\'\-]+)?)\s*$', line)
            if name_match and i + 2 < len(lines):
                name_line = name_match.group(1)
                # Check the next non-empty line for a time
                j = i + 1
                while j < len(lines) and not lines[j].strip():
                    j += 1
                if j < len(lines):
                    time_match = re.match(r'^\s*(\d{1,2}:\d{2}(?:\.\d{2})?)\s*$', lines[j].strip())
                    if time_match:
                        time_str = time_match.group(1)
                        time_seconds = self.parse_time_to_seconds(time_str)
                        if time_seconds:
                            # Parse name
                            if ',' in name_line:
                                last, first = name_line.split(',', 1)
                                first_name = first.strip().title()
                                last_name = last.strip().title()
                            else:
                                parts = name_line.split()
                                first_name = parts[0].title()
                                last_name = parts[-1].title() if len(parts) > 1 else ""

                            results.append(ParsedResult(
                                first_name=first_name,
                                last_name=last_name,
                                school="",
                                time_seconds=time_seconds,
                                place=len(results) + 1,
                            ))
                            i = j + 1
                            continue
            i += 1
        return {"default": results} if results else {}