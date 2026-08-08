"""
Parser for John Martin Invitational format.

4-column table: Place, Name, School, Time (no grade column)
Used by John Martin XC Invitational 2025 pages.
"""

import re
from typing import List, Dict
from bs4 import BeautifulSoup

from .base import BaseParser, ParsedResult


class JohnMartinParser(BaseParser):
    """Parses John Martin format: 4-column table (Place, Name, School, Time)."""
    parser_name = "john_martin"

    def can_parse(self, content: str) -> bool:
        # John Martin pages have "John Martin" in the title and use a simple table
        return "John Martin" in content and "<table" in content

    def extract_races(self, content: str) -> Dict[str, List[ParsedResult]]:
        soup = BeautifulSoup(content, 'html.parser')
        results = []
        for table in soup.find_all('table'):
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 4:
                    continue
                try:
                    place_text = cells[0].get_text().strip()
                    if not place_text.isdigit():
                        continue
                    place = int(place_text)

                    # Name column: may be "Last, First" or "First Last"
                    name_text = cells[1].get_text().strip()
                    school = cells[2].get_text().strip()
                    time_str = cells[3].get_text().strip()

                    if ',' in name_text:
                        parts = name_text.split(',', 1)
                        last_name = parts[0].strip()
                        first_name = parts[1].strip()
                    else:
                        name_parts = name_text.rsplit(None, 1)
                        if len(name_parts) >= 2:
                            first_name = name_parts[0].strip()
                            last_name = name_parts[1].strip()
                        else:
                            first_name = name_text.strip()
                            last_name = ""

                    time_seconds = self.parse_time_to_seconds(time_str)
                    if time_seconds is None:
                        continue

                    results.append(ParsedResult(
                        first_name=first_name.title(),
                        last_name=last_name.title(),
                        school=school,
                        time_seconds=time_seconds,
                        place=place,
                    ))
                except (ValueError, IndexError):
                    continue
        return {"default": results} if results else {}