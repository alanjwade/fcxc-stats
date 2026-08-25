"""
Base parser class and data model for all race result parsers.

All parsers should subclass BaseParser and implement can_parse() and
extract_races(). They are auto-registered in ParserRegistry via the
__init_subclass__ hook.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import re


@dataclass
class ParsedResult:
    """A single parsed athlete result."""
    first_name: str
    last_name: str
    school: str
    time_seconds: float
    place: int
    graduation_year: Optional[int] = None


class ParserRegistry:
    """Auto-registry for parser classes."""
    _registry: List[type] = []

    @classmethod
    def register(cls, parser_cls: type) -> None:
        cls._registry.append(parser_cls)

    @classmethod
    def get_all(cls) -> List[type]:
        return list(cls._registry)

    @classmethod
    def find_parser(cls, content: str) -> Optional['BaseParser']:
        """Find the first parser that can handle the given content."""
        for parser_cls in cls._registry:
            parser = parser_cls()
            if parser.can_parse(content):
                return parser
        return None


class BaseParser(ABC):
    """Abstract base class for all parsers."""

    # Override in subclasses for human-readable identification
    parser_name: str = "unknown"

    def __init_subclass__(cls, **kwargs):
        """Auto-register any subclass of BaseParser."""
        super().__init_subclass__(**kwargs)
        if cls.parser_name != "unknown":
            ParserRegistry.register(cls)

    @abstractmethod
    def can_parse(self, content: str) -> bool:
        """
        Return True if this parser can handle the provided content.
        Content is the raw text of the page (HTML or plain text).
        """
        ...

    @abstractmethod
    def extract_races(self, content: str) -> Dict[str, List[ParsedResult]]:
        """
        Extract all races from the content.

        Returns a dict mapping section_title (or a synthetic key like
        "default") to a list of ParsedResult objects.

        For single-race files, use a key like "default".
        For combined files, use the section titles so the caller can
        match them to the YAML config.
        """
        ...

    def parse_time_to_seconds(self, time_str: str) -> Optional[float]:
        """
        Parse a time string to total seconds as a float.

        Supports H:MM:SS.s/.ss, MM:SS:ss (hundredths, e.g. John Martin's
        "24:00:00" = 24 minutes), MM:SS.s/.ss, MM:SS, and SSS.s/.ss formats.
        Pattern ORDER matters: the MM:SS:ss (minutes:hundredths) form must be
        tried before treating a third group as hours, because MileSplit's
        raw John Martin pages encode hundredths in the third position.
        """
        import re
        time_str = time_str.strip()

        patterns = [
            r'(\d{1,2}):(\d{2}):(\d{2})\.(\d{1,2})',  # H:MM:SS.s or H:MM:SS.ss
            r'(\d{1,2}):(\d{2}):(\d{2})',             # MM:SS:ss (hundredths)
            r'(\d{1,2}):(\d{2})\.(\d{1,2})',          # MM:SS.s or MM:SS.ss
            r'(\d{1,2}):(\d{2})',                      # MM:SS
            r'(\d{3,4})\.(\d{1,2})',                   # SSS.s / SSS.ss / SSSS.s / SSSS.ss
        ]

        for i, pattern in enumerate(patterns):
            match = re.match(pattern, time_str)
            if match:
                groups = match.groups()

                def frac(digits: str) -> float:
                    """Convert 1- or 2-digit decimal digits to fractional seconds."""
                    return (float(int(digits)) / 10.0) if len(digits) == 1 else (float(int(digits)) / 100.0)

                if i == 0:  # H:MM:SS.s or H:MM:SS.ss
                    hours, minutes, seconds, fractional = groups
                    total = float(int(hours) * 3600 + int(minutes) * 60 + int(seconds)) + frac(fractional)
                elif i == 1:  # MM:SS:ss (hundredths)
                    minutes, seconds, hundredths = groups
                    total = float(int(minutes) * 60 + int(seconds)) + float(int(hundredths)) / 100.0
                elif i == 2:  # MM:SS.s or MM:SS.ss
                    minutes, seconds, fractional = groups
                    total = float(int(minutes) * 60 + int(seconds)) + frac(fractional)
                elif i == 3:  # MM:SS
                    minutes, seconds = groups
                    total = float(int(minutes) * 60 + int(seconds))
                else:  # SSS.s / SSS.ss / SSSS.s / SSSS.ss
                    seconds, fractional = groups
                    total = float(int(seconds)) + frac(fractional)

                # Sanity check: cross country times should never exceed an hour.
                if total > 3600:
                    logger = __import__('logging').getLogger(__name__)
                    logger.warning(f"Sanity check failed: time {total} from '{time_str}'")
                    return None
                return total

        return None

    # School name normalization mappings (from the proven scraper).
    _SCHOOL_MAPPINGS = {
        'Fort Collins': 'Fort Collins High School',
        'Fort Collins HS': 'Fort Collins High School',
        'Fort Collins High Sc': 'Fort Collins High School',
        'FCHS': 'Fort Collins High School',
        'Fossil Ridge': 'Fossil Ridge High School',
        'Fossil Ridge HS': 'Fossil Ridge High School',
        'Fossil Ridge High Sc': 'Fossil Ridge High School',
        'Rocky Mountain': 'Rocky Mountain High School',
        'Rocky Mountain HS': 'Rocky Mountain High School',
        'Rocky Mountain High': 'Rocky Mountain High School',
        'Denver East High Sch': 'Denver East High School',
        'Clear Creek High Sch': 'Clear Creek High School',
        'Fort Lupton High Sch': 'Fort Lupton High School',
        'Westminster High Sch': 'Westminster High School',
        'Wheat Ridge High Sch': 'Wheat Ridge High School',
        'Cheyenne Central Hig': 'Cheyenne Central High School',
        'Cheyenne East High S': 'Cheyenne East High School',
        'Prospect Ridge Acade': 'Prospect Ridge Academy',
        'Frederick High Schoo': 'Frederick High School',
        'Ascent Classical Aca': 'Ascent Classical Academy of Northern Colorado',
        'Ascent Classical Academy of Nort': 'Ascent Classical Academy of Northern Colorado',
    }

    def normalize_school_name(self, school: str) -> str:
        """Normalize school names to standard formats."""
        school = (school or '').strip()

        # Exact match on known mappings.
        if school in self._SCHOOL_MAPPINGS:
            return self._SCHOOL_MAPPINGS[school]

        # Convert "School Name HS" to "School Name High School".
        if school.endswith(' HS') and not school.endswith(' High School'):
            base_name = school[:-3].strip()
            return f"{base_name} High School"

        # Convert truncated "High Sc"/"High Sch" endings.
        if school.endswith(' High Sc'):
            return school.replace(' High Sc', ' High School')
        if school.endswith(' High Sch'):
            return school.replace(' High Sch', ' High School')
        if school.endswith(' High Schoo'):
            return school.replace(' High Schoo', ' High School')

        return school

    def _normalize_school_name(self, name: str) -> str:
        """Alias for normalize_school_name (kept for backward compatibility)."""
        return self.normalize_school_name(name)

    def _guess_graduation_year(self, year_str: str, current_year: int = 2025) -> Optional[int]:
        """Convert year abbreviation (SR, JR, SO, FR) to graduation year."""
        mapping = {
            'SR': current_year,
            'JR': current_year + 1,
            'SO': current_year + 2,
            'FR': current_year + 3,
            '12': current_year,
            '11': current_year + 1,
            '10': current_year + 2,
            '9': current_year + 3,
        }
        return mapping.get(year_str.upper().strip())

    def _strip_html(self, html: str) -> str:
        """Remove HTML tags from a string."""
        import re
        return re.sub(r'<[^>]+>', '', html).strip()


def distance_meters(distance: str) -> Optional[int]:
    """Convert a distance token from YAML ('5K', '1600m', '2M', ...) to meters."""
    if not distance:
        return None
    d = str(distance).strip().lower().replace(' ', '')
    m = re.match(r'(\d+(?:\.\d+)?)\s*(m|k|km|mile)', d)
    if not m:
        return None
    val, unit = float(m.group(1)), m.group(2)
    if unit == 'k':
        return int(val * 1000)
    if unit == 'km':
        return int(val * 1000)
    if unit == 'mile':
        return int(val * 1609.34)
    if unit == 'm':
        return int(val)
    return None


def time_bounds_for_distance(distance: str) -> tuple:
    """Reasonable time window (min_seconds, max_seconds) for a race distance.

    Base window is the cross-country 5K band deemed plausible (10–50 minutes,
    i.e. 600–3000 s), scaled linearly by distance.
    """
    d_m = distance_meters(distance)
    if not d_m or d_m <= 0:
        return (600, 3000)
    factor = d_m / 5000.0
    return (max(120, int(600 * factor)), int(3000 * factor))


def validate_parsed_results(results, team_names=None, distance=None) -> dict:
    """Sanity-check a list of ParsedResult for an obviously-broken parse.

    Returns a report dict:
      {
        'total': n,
        'ok': bool,                 # True if every "required" criterion passed
        'criteria': {name: bool|None},
        'failures': [names of failed criteria],
        'details': {...},
      }

    Required criteria (when applicable):
      - parse_results   : at least one result survived extraction.
      - has_home_team   : some result is from the home school (Fort Collins).
                          Enforced only if team_names is provided.
      - times_reasonable: every time is within the distance's time window.
      - places_valid    : places are positive and ascending (not garbage).
    """
    results = list(results or [])
    report = {
        'total': len(results),
        'criteria': {},
        'failures': [],
        'details': {},
    }

    # 1) Parsed results present.
    ok = len(results) > 0
    report['criteria']['parsed_results'] = ok
    if not ok:
        report['failures'].append('parsed_results')

    # 2) Home team presence (only when we know which team to look for).
    home_names = [n for n in (team_names or []) if n]
    if home_names:
        normalized = [n.lower().strip() for n in home_names]
        home_count = 0
        for r in results:
            school = (r.school or '').lower().strip()
            if any(n == school or n in school or school in n for n in normalized):
                home_count += 1
        report['details']['home_team_count'] = home_count
        ok = home_count > 0
        report['criteria']['has_home_team'] = ok
        if not ok:
            report['failures'].append('has_home_team')
    else:
        report['criteria']['has_home_team'] = None

    # 3) Reasonable times.
    min_s, max_s = time_bounds_for_distance(distance or '')
    out_of_range = []
    for r in results:
        if r.time_seconds is None:
            continue
        if r.time_seconds < min_s or r.time_seconds > max_s:
            out_of_range.append((r.first_name, r.last_name, r.time_seconds))
    ok_out = len(out_of_range) == 0
    report['criteria']['times_reasonable'] = ok_out
    report['details']['out_of_range'] = out_of_range
    if not ok_out:
        report['failures'].append('times_reasonable')

    # 4) Places valid (ascending, positive).
    bad_places = []
    last = 0
    for r in results:
        placeless = r.place is None or r.place < 1
        if placeless:
            bad_places.append((r.first_name, r.last_name, r.place))
        else:
            if r.place < last:
                bad_places.append((r.first_name, r.last_name, r.place))
        last = r.place
    ok_places = len(bad_places) == 0
    report['criteria']['places_valid'] = ok_places
    report['details']['bad_places'] = bad_places
    if not ok_places:
        report['failures'].append('places_valid')

    report['ok'] = not report['failures']
    return report