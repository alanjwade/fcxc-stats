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
from . import milesplit_api
from . import default_parser


def find_parser(content: str) -> BaseParser:
    """Find the first parser that can handle the given content."""
    for parser_cls in ParserRegistry.get_all():
        parser = parser_cls()
        if parser.can_parse(content):
            return parser
    return None


def find_valid_parser(content: str, team_names=None, distance=None):
    """Return the first parser whose extracted results pass validation.

    Unlike find_parser (which only checks can_parse), this runs each capable
    parser's extract_races() and validates the output via
    parsers.base.validate_parsed_results, so the chosen parser is one that
    actually yields sensible results (home-team present, reasonable times, etc.).

    Returns (parser, sections, report). If no candidate validates, returns
    (None, {}, report) with a report reflecting the best attempt.
    """
    from .base import validate_parsed_results

    best = None
    best_report = None
    for parser_cls in ParserRegistry.get_all():
        parser = parser_cls()
        try:
            if not parser.can_parse(content):
                continue
            sections = parser.extract_races(content)
        except Exception:
            continue
        if not sections:
            continue
        flat = [r for lst in sections.values() for r in lst]
        report = validate_parsed_results(flat, team_names=team_names, distance=None)
        if best_report is None or report['total'] > best_report.get('total', 0):
            best = parser
            selected = sections
            best_report = report
        if report['ok']:
            return parser, sections, report
    return best, (selected if best else {}), best_report


def get_parser_names() -> list[str]:
    """Return a list of registered parser names."""
    return [cls.__name__ for cls in ParserRegistry.get_all()]