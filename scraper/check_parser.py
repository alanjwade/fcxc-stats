#!/usr/bin/env python3
"""
Check which registered parser (if any) can properly parse a MileSplit source
file. "Properly" means the extracted results pass sanity validation:
results present, home team (Fort Collins HS) represented, times within the
distance's plausible window (10–50 min for a 5K), and places ascending.

Usage:
    python check_parser.py <source_file> [--distance 5K] [--team "Fort Collins High School"]

Run from the scraper/ directory, or use the wrapper `./check_parser`.

Exit code: 0 if a parser parsed it properly, 1 otherwise.
"""

import argparse
import sys
import os
import json

from parsers import get_parser_names
from parsers.base import validate_parsed_results, time_bounds_for_distance, ParserRegistry

HOME_TEAM_NAMES = ['Fort Collins High School', 'Fort Collins HS', 'FCHS', 'Fort Collins']


def get_parsers():
    """Return (class_name, parser_instance) for every registered parser."""
    out = []
    for cls in ParserRegistry.get_all():
        try:
            out.append((cls.__name__, cls()))
        except Exception:
            continue
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("source", help="Path to the downloaded results file (HTML or text)")
    ap.add_argument("--distance", default="5K",
                    help="Race distance for the time window (default 5K)")
    ap.add_argument("--team", action="append", default=[],
                    help="Home team name variant to require (repeatable); "
                         "defaults to Fort Collins variants")
    ap.add_argument("--json", action="store_true", help="Print result as JSON")
    args = ap.parse_args(argv)

    if not os.path.exists(args.source):
        print(f"Error: file not found: {args.source}", file=sys.stderr)
        return 1

    with open(args.source, "r", encoding="utf-8", errors="replace") as f:
        content = f.read()

    team_names = args.team or HOME_TEAM_NAMES
    min_s, max_s = time_bounds_for_distance(args.distance)

    winners = []
    details = []
    for parser_cls, parser in get_parsers():
        try:
            if not parser.can_parse(content):
                continue
            sections = parser.extract_races(content)
        except Exception as e:
            continue
        if not sections:
            continue
        flat = [r for lst in sections.values() for r in lst]
        report = validate_parsed_results(flat, team_names=team_names, distance=args.distance)
        entry = {
            "parser": parser_cls,
            "results": len(flat),
            "ok": report["ok"],
            "failures": report["failures"],
            "criteria": report["criteria"],
            "home_team_count": report["details"].get("home_team_count"),
        }
        details.append(entry)
        if report["ok"]:
            winners.append(entry)
        else:
            print(f"[FAIL] {entry['parser']}: {len(flat)} results, "
                  f"failures={entry['failures']}")

    if winners:
        w = winners[0]
        print(f"[OK]   {w['parser']} parses it properly: {w['results']} results.")
    else:
        print("[none] No registered parser parsed this file correctly.")

    if args.json:
        print(json.dumps({"winners": winners, "all": details}, indent=2))

    return 0 if winners else 1


if __name__ == "__main__":
    raise SystemExit(main())