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
        Parse a time string (MM:SS.ss, MM:SS, H:MM:SS.ss, etc.)
        to total seconds as a float.
        """
        import re
        time_str = time_str.strip()

        patterns = [
            r'(\d{1,2}):(\d{2}):(\d{2})\.(\d{2})',  # H:MM:SS.ss
            r'(\d{1,2}):(\d{2}):(\d{2})',             # H:MM:SS
            r'(\d{1,2}):(\d{2})\.(\d{2})',            # MM:SS.ss
            r'(\d{1,2}):(\d{2})',                      # MM:SS
            r'(\d{3,4})\.(\d{2})',                     # SSS.ss or SSSS.ss
        ]

        for i, pattern in enumerate(patterns):
            match = re.match(pattern, time_str)
            if match:
                groups = match.groups()
                if i == 0:  # H:MM:SS.ss
                    h, m, s, c = groups
                    return float(int(h) * 3600 + int(m) * 60 + int(s)) + float(int(c)) / 100.0
                elif i == 1:  # H:MM:SS
                    h, m, s = groups
                    return float(int(h) * 3600 + int(m) * 60 + int(s))
                elif i == 2:  # MM:SS.ss
                    m, s, c = groups
                    return float(int(m) * 60 + int(s)) + float(int(c)) / 100.0
                elif i == 3:  # MM:SS
                    m, s = groups
                    return float(int(m) * 60 + int(s))
                elif i == 4:  # SSS.ss or SSSS.ss
                    s, c = groups
                    return float(int(s)) + float(int(c)) / 100.0
        return None

    def _normalize_school_name(self, name: str) -> str:
        """Normalize inconsistencies in school names."""
        replacements = {
            # Add school name normalizations here as needed
        }
        name = name.strip()
        for old, new in replacements.items():
            name = name.replace(old, new)
        return name

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