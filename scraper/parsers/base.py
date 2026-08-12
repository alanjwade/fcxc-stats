"""
Base parser class and data model for all race result parsers.

All parsers should subclass BaseParser and implement can_parse() and
extract_races(). They are auto-registered in ParserRegistry via the
__init_subclass__ hook.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Dict, Any


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