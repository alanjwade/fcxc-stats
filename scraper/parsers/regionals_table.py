"""
Parser for Region 4 / State Championship HTML table format.

Handles HTML pages with results in <table> elements.
Used by Colorado 5A Region 4 and State Championships.
"""

import re
from typing import List, Dict, Optional
from bs4 import BeautifulSoup

from .base import BaseParser, ParsedResult


class RegionalsTableParser(BaseParser):
    """Parses HTML table results for Region 4 / State Championships."""
    parser_name = "regionals_table"

    def can_parse(self, content: str) -> bool:
        # MileSplit table-based results pages (Region 4, State Championships,
        # etc.) use results <table> elements with a distinctive id prefix like
        # m5000mfinalsFinals / "5000 Meter Run Finals".
        return ("<table" in content
                and ("m5000mfinals" in content
                     or "5000 Meter Run Finals" in content))

    def extract_races(self, content: str) -> Dict[str, List[ParsedResult]]:
        soup = BeautifulSoup(content, 'html.parser')
        results = []
        results_table = None

        for table in soup.find_all('table'):
            if re.search(r'\d{1,2}:\d{2}', table.get_text()):
                results_table = table
                break

        if results_table:
            rows = results_table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 4:
                    continue
                result = self._parse_table_row(cells)
                if result:
                    results.append(result)

        return {"default": results} if results else {}

    def _parse_table_row(self, cells) -> Optional[ParsedResult]:
        try:
            cell_texts = [c.get_text().strip() for c in cells]
            place = None
            name = ""
            school = ""
            time_str = ""

            for i, text in enumerate(cell_texts):
                if not text:
                    continue
                if place is None and text.isdigit():
                    place = int(text)
                elif re.match(r'\d{1,2}:\d{2}', text):
                    time_str = text
                elif text and not text.isdigit() and not re.match(r'\d{1,2}:\d{2}', text):
                    if not name:
                        name = text
                    elif not school:
                        school = text

            if place is None or not time_str:
                return None

            time_seconds = self.parse_time_to_seconds(time_str)
            if time_seconds is None:
                return None

            # Parse name into first/last
            if ',' in name:
                last, first = name.split(',', 1)
                first_name = first.strip().title()
                last_name = last.strip().title()
            else:
                parts = name.split()
                if len(parts) >= 2:
                    first_name = parts[0].strip().title()
                    last_name = parts[-1].strip().title()
                else:
                    first_name = name.strip()
                    last_name = ""

            return ParsedResult(
                first_name=first_name,
                last_name=last_name,
                school=school,
                time_seconds=time_seconds,
                place=place,
            )
        except Exception:
            return None