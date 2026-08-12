"""
Parser registry and auto-discovery for race result formats.

Each parser module in this directory should subclass BaseParser and implement
can_parse() and extract_races(). Parsers are auto-discovered via the
__init_subclass__ hook in BaseParser.
"""

from .base import BaseParser, ParsedResult, ParserRegistry

# Import all parsers so they register themselves.
# Order matters: more-specific parsers must be imported (and therefore tried)
# before generic ones. DefaultParser is intentionally last since it is the
# broadest fallback.
from . import john_martin
from . import thornton_combined
from . import raw_windsor_combined
from . import desert_twilight
from . import northern_conference
from . import regionals_table
from . import longs_peak
from . import loveland_sweetheart
from . import raw_combined
from . import default_parser


def find_parser(content: str) -> BaseParser:
    """Find the first parser that can handle the given content."""
    for parser_cls in ParserRegistry.get_all():
        parser = parser_cls()
        if parser.can_parse(content):
            return parser
    return None


def get_parser_names() -> list[str]:
    """Return a list of registered parser names."""
    return [cls.__name__ for cls in ParserRegistry.get_all()]