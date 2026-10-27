"""
Default parser for standard MileSplit race result pages.

Handles single-race HTML pages where results are in a <pre> tag
or an HTML table. Used by Rocky Mountain Lobo, Vista Nation, etc.

Two <pre> layouts occur in the downloaded pages:

  * formatted results, e.g. ``1 Smith, John SR 12 ... 16:03``
  * raw fixed-width results, e.g.
    ``1  1/124  3392 Ryan Ruffer  M  Fossil Ridge High School  15:57  5:08``

Both are supported; the raw layout carries the school name, which callers
(``check_parser.py`` and ``find_valid_parser``) use to confirm the home team
is actually present.
"""

import re
from typing import List, Dict, Optional
from bs4 import BeautifulSoup

from .base import BaseParser, ParsedResult

# Raw MileSplit fixed-width layout:
#   Place Div/Tot Bib# Name Sex School Time Pace
_RAW_RESULT_RE = re.compile(
    r'^\s*(\d+)\s+\d+/\d+\s+\d+\s+(.+?)\s+(M|F)\s+(.+?)\s+'
    r'(\d{1,2}:\d{2}(?:\.\d{2})?)\s+\d+:\d+\s*$'
)


class DefaultParser(BaseParser):
    """Handles standard MileSplit result pages with a single race."""
    parser_name = "default"

    def can_parse(self, content: str) -> bool:
        return bool(re.search(r'\d{1,2}:\d{2}', content))

    def extract_races(self, content: str) -> Dict[str, List[ParsedResult]]:
        soup = BeautifulSoup(content, 'html.parser')
        results = []

        # Try <pre> tag first
        pre_tag = soup.find('pre')
        if pre_tag:
            pre_text = pre_tag.get_text()
            results = self._parse_pre_text(pre_text)
            if not results:
                results = self._parse_raw_pre_text(pre_text)
            if results:
                return {"default": results}

        # Try HTML tables
        for table in soup.find_all('table'):
            if re.search(r'\d{1,2}:\d{2}', table.get_text()):
                results = self._parse_table(table)
                if results:
                    return {"default": results}

        # Fallback: raw text
        text_results = self._parse_text_results(soup.get_text())
        if text_results:
            return {"default": text_results}
        return {}

    def _parse_pre_text(self, text: str) -> List[ParsedResult]:
        results = []
        pattern = r'^\s*(\d+)\s+([A-Z\'\-\s]+),\s+([A-Za-z\-\s\']+?)\s+(?:SR|JR|SO|FR|\d{1,2})\s+\d+\s+.*?(\d{1,2}:\d{2}(?:\.\d{2})?)'
        for line in text.split('\n'):
            line = line.strip()
            if not line or line.startswith('=') or line.startswith('-'):
                continue
            m = re.match(pattern, line)
            if m:
                try:
                    time_s = self.parse_time_to_seconds(m.group(4))
                    if time_s is None:
                        continue
                    results.append(ParsedResult(
                        first_name=m.group(3).strip().title(),
                        last_name=m.group(2).strip().title(),
                        school="", time_seconds=time_s, place=int(m.group(1))))
                except (ValueError, IndexError):
                    continue
        return results

    def _parse_raw_pre_text(self, text: str) -> List[ParsedResult]:
        """Parse the raw fixed-width MileSplit <pre> layout.

        Example line:
            1   1/124   3392 Ryan Ruffer   M   Fossil Ridge High School   15:57  5:08

        The name is split the same way the production scraper splits it (first
        token = first name, remainder = last name) so both code paths agree.
        """
        results = []
        for line in text.split('\n'):
            m = _RAW_RESULT_RE.match(line)
            if not m:
                continue
            time_s = self.parse_time_to_seconds(m.group(5))
            if time_s is None:
                continue
            name_parts = m.group(2).split()
            if len(name_parts) >= 2:
                first_name = name_parts[0]
                last_name = ' '.join(name_parts[1:])
            else:
                first_name = m.group(2).strip()
                last_name = ''
            results.append(ParsedResult(
                first_name=first_name,
                last_name=last_name,
                school=m.group(4).strip(),
                time_seconds=time_s,
                place=int(m.group(1))))
        return results

    def _parse_table(self, table) -> List[ParsedResult]:
        results = []
        header_done = False
        for row in table.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if len(cells) < 4:
                continue
            if not header_done and any(k in c.get_text().lower() for c in cells for k in ['place', 'name', 'time']):
                header_done = True
                continue
            ct = [c.get_text().strip() for c in cells]
            place = None
            time_str = ""
            for t in ct:
                if not t:
                    continue
                if place is None and t.isdigit():
                    place = int(t)
                elif re.match(r'\d{1,2}:\d{2}', t):
                    time_str = t
            if place is None or not time_str:
                continue
            ts = self.parse_time_to_seconds(time_str)
            if ts is None:
                continue
            results.append(ParsedResult(
                first_name="", last_name="", school="",
                time_seconds=ts, place=place))
        return results

    def _parse_text_results(self, text: str) -> List[ParsedResult]:
        results = []
        for line in text.split('\n'):
            m = re.match(r'^\s*(\d+)\s+([A-Za-z\-\']+)\s+([A-Za-z\-\']+)\s+.*?(\d{1,2}:\d{2}(?:\.\d{2})?)', line)
            if m:
                try:
                    ts = self.parse_time_to_seconds(m.group(4))
                    if ts:
                        results.append(ParsedResult(
                            first_name=m.group(2).strip().title(),
                            last_name=m.group(3).strip().title(),
                            school="", time_seconds=ts, place=int(m.group(1))))
                except (ValueError, IndexError):
                    continue
        return results