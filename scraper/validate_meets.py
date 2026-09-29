#!/usr/bin/env python3
"""Validate the canonical sources file (sources/meets.yaml) before scraping.

Catches the failure modes that have silently corrupted data:

  * a duplicated mapping key (`gender: "girls"` then `gender: "boys"` -- plain
    YAML keeps the last one, which published a girls race as boys);
  * a missing or invalid `gender`/`class` (`race.get('gender', 'boys')` used to
    default a missing gender to boys);
  * `gender: "mixed"`, which cannot be stored at all (the athletes table only
    accepts male/female, so the race is dropped at insert time);
  * a race whose name says "Girls" while `gender:` says boys (or vice versa);
  * a duplicate race entry (same meet/date/race/distance/class/gender) -- the
    downloader used to append another copy on every re-run;
  * a missing source, or a source.path that points at a file that isn't there.

Usage:
    python validate_meets.py [path/to/meets.yaml]

Defaults to <repo>/sources/meets.yaml, so it works from anywhere; the wrapper
`./validate_meets` runs it with the scraper venv. Exit code 0 when the file is
clean, 1 otherwise.
"""

import argparse
import os
import sys

from meets_config import format_problems, validate_file

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_SOURCES = os.path.join(PROJECT_ROOT, "sources", "meets.yaml")


def resolve_path(path):
    """Resolve a user-supplied path against cwd, then the repo root."""
    if os.path.isabs(path):
        return path
    for base in (os.getcwd(), PROJECT_ROOT):
        candidate = os.path.abspath(os.path.join(base, path))
        if os.path.exists(candidate):
            return candidate
    return os.path.abspath(os.path.join(PROJECT_ROOT, path))


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("sources", nargs="?", default=DEFAULT_SOURCES,
                        help="Path to meets.yaml (default: %s)" % DEFAULT_SOURCES)
    parser.add_argument("--no-file-check", action="store_true",
                        help="Do not check that source.type=file paths exist")
    args = parser.parse_args(argv)

    path = resolve_path(args.sources)
    config, problems = validate_file(path, check_files=not args.no_file_check)
    if problems:
        print(format_problems(problems, path), file=sys.stderr)
        return 1

    meets = config.get("meets") or []
    races = sum(len(meet.get("races") or []) for meet in meets)
    print("[OK]  %s: %d meet entries, %d race entries - no problems."
          % (path, len(meets), races))
    return 0


if __name__ == "__main__":
    sys.exit(main())
