"""
Parser for MileSplit "formatted" results that embed the raw /api/v1/meets/{id}/
performances payload.

These pages render their results purely from an AJAX API call; the HTML shell
contains no result rows. The downloader (download_page.py) appends the captured
payload as a JSON <script id="milesplit-api-data"> block, and this parser reads
that back to produce results without any network access.
"""

import re
import json
from typing import List, Dict, Optional

from bs4 import BeautifulSoup

from .base import BaseParser, ParsedResult


class MilesplitApiParser(BaseParser):
    """Parses results embedded from the MileSplit performances API."""
    parser_name = "milesplit_api"

    MARKER = "id=\"milesplit-api-data\""

    def can_parse(self, content: str) -> bool:
        return self.MARKER in content

    def extract_races(self, content: str) -> Dict[str, List[ParsedResult]]:
        soup = BeautifulSoup(content, "html.parser")
        results = []
        for tag in soup.find_all("script", {"id": "milesplit-api-data"}):
            payload = tag.string or tag.get_text()
            data = json.loads(payload)
            rows = data.get("data", []) if isinstance(data, dict) else None
            if not isinstance(rows, list):
                continue
            for entry in rows:
                result = self._row_to_result(entry)
                if result:
                    results.append(result)
        # The embedded payload is race-filtered at download time, so a single
        # "default" section maps cleanly onto the single-race YAML entry.
        return {"default": results} if results else {}

    def _row_to_result(self, entry: dict) -> Optional[ParsedResult]:
        first = (entry.get("firstName") or "").strip()
        last = (entry.get("lastName") or "").strip()
        if not first and not last:
            return None
        mark = (entry.get("mark") or "").strip()
        time_seconds = self.parse_time_to_seconds(mark)
        if time_seconds is None or time_seconds <= 0:
            return None

        place_str = entry.get("place")
        try:
            place = int(place_str) if place_str not in (None, "") else 0
        except (ValueError, TypeError):
            place = 0

        school = (entry.get("teamName") or "").strip()
        grad = entry.get("gradYear")
        grad_year = int(grad) if isinstance(grad, (int, float)) and grad > 1000 else None

        return ParsedResult(
            first_name=first,
            last_name=last,
            school=self.normalize_school_name(school),
            time_seconds=time_seconds,
            place=place,
            graduation_year=grad_year,
        )