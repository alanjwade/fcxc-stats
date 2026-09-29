"""Validation for the canonical sources file (sources/meets.yaml).

This module is the single source of truth for what a meet/race entry may
contain. It is shared by:

  * ``scraper.py::load_sources_config`` -- refuses to load a bad config
  * ``validate_meets.py``               -- reports every problem at once

Two 2025 girls races were published as boys races because meets.yaml declared
them that way and nothing checked:

  * one carried a duplicate ``gender:`` key (plain YAML quietly keeps the
    last one), and
  * one had no ``gender:`` at all, where ``race.get('gender', 'boys')``
    silently defaulted it to boys.

Nothing here is inferred or defaulted: an entry has to say what it means, and a
config that doesn't is rejected rather than guessed at.

An entry is reported as a duplicate only when it repeats the *whole* entry --
same meet, date, race, distance, class, gender, source and section. One race
fed by several different source files is legitimate (a division split across
result pages is one race row), so that is left alone.
"""

import os
import re
from typing import Any, Dict, List, Optional, Tuple

import yaml

# Values a race entry may declare. These match what the site can render:
# webapp/templates/athlete_stats.html badges exactly varsity/jv/freshman.
ALLOWED_GENDERS = ("boys", "girls")
ALLOWED_CLASSES = ("varsity", "jv", "freshman")

# A `mixed` race can never be stored: map_gender_for_db('mixed') returns
# 'mixed', but the `athletes` table only accepts ('male', 'female'), so the
# INSERT fails its CHECK constraint and store_race_results() drops the entire
# race (main() logs "Error processing race" and carries on). Refuse it up front
# instead of losing the results at insert time.
UNSUPPORTED_GENDERS = ("mixed",)

SOURCE_TYPES = ("file", "url")

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SEASON_RE = re.compile(r"^\d{4}$")

# Words that name a gender in a race's display name. Used to catch a name and a
# `gender:` that disagree -- the exact shape of the 2025 girls-races bug (a race
# named "High School Girls JV" declared as boys).
_GENDER_WORDS = {
    "girls": ("girls", "girl", "womens", "women", "female"),
    "boys": ("boys", "boy", "mens", "men", "male"),
}


class DuplicateKeyError(yaml.constructor.ConstructorError):
    """Raised when meets.yaml declares the same mapping key twice."""


class StrictLoader(yaml.SafeLoader):
    """A SafeLoader for which a duplicated mapping key is an error.

    Plain YAML keeps the last value for a duplicate key, which is how
    ``gender: "girls"`` followed by ``gender: "boys"`` became a boys race.
    """


def _construct_mapping(loader, node, deep=False):
    loader.flatten_mapping(node)
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise DuplicateKeyError(
                "while constructing a mapping",
                node.start_mark,
                "found duplicate key %r" % (key,),
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


StrictLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_mapping
)


def load_sources(path: str) -> Dict[str, Any]:
    """Load a meets.yaml, treating a duplicate key as an error.

    Raises FileNotFoundError, or a yaml.YAMLError subclass (including
    DuplicateKeyError) for malformed YAML.
    """
    with open(path, encoding="utf-8") as handle:
        return yaml.load(handle, Loader=StrictLoader)


def _gender_conflicts_with_name(race_name: str, gender: str) -> Optional[str]:
    """Return the word in `race_name` that contradicts `gender`, if any."""
    words = re.findall(r"[a-z]+", str(race_name).lower())
    for other, needles in _GENDER_WORDS.items():
        if other == gender:
            continue
        for word in words:
            if word in needles:
                return word
    return None


def validate_meets(config, sources_dir: Optional[str] = None,
                   check_files: bool = True) -> List[str]:
    """Return a list of human-readable problems; an empty list means clean.

    `sources_dir` is the directory that source.path values are relative to; it
    is only needed for the file-existence check (skip that with
    check_files=False).
    """
    problems: List[str] = []

    if not isinstance(config, dict):
        return ["the top level of the file is not a mapping "
                "(expected a 'meets:' list)"]
    meets = config.get("meets")
    if not isinstance(meets, list):
        return ["no 'meets:' list found"]

    seen: Dict[Tuple[str, ...], str] = {}

    for index, meet in enumerate(meets, 1):
        where = "meets[%d]" % index
        if not isinstance(meet, dict):
            problems.append("%s: not a mapping" % where)
            continue

        name = meet.get("name")
        label = str(name).strip() if name else where
        if not name or not str(name).strip():
            problems.append("%s: missing 'name'" % where)

        date = meet.get("date")
        if not date:
            problems.append("%s (%s): missing 'date'" % (where, label))
        elif not _DATE_RE.match(str(date)):
            problems.append("%s (%s): 'date' must be YYYY-MM-DD, got %r"
                            % (where, label, str(date)))

        season = meet.get("season")
        if not season:
            problems.append("%s (%s): missing 'season'" % (where, label))
        elif not _SEASON_RE.match(str(season)):
            problems.append("%s (%s): 'season' must be a 4-digit year, got %r"
                            % (where, label, str(season)))

        problems.extend(_validate_source(meet, where, label, sources_dir,
                                         check_files))

        races = meet.get("races")
        if not isinstance(races, list) or not races:
            problems.append("%s (%s): 'races' must be a non-empty list"
                            % (where, label))
            continue

        for race_index, race in enumerate(races, 1):
            problems.extend(_validate_race(race, where, race_index, label, seen,
                                           str(name) if name else None,
                                           str(date) if date else None,
                                           _source_identity(meet.get("source"))))

    return problems


def _source_identity(source) -> Optional[str]:
    """What a race's results are read from: source.path or source.url."""
    if not isinstance(source, dict):
        return None
    return str(source.get("path") or source.get("url") or "") or None


def _validate_source(meet: dict, where: str, label: str,
                     sources_dir: Optional[str],
                     check_files: bool) -> List[str]:
    """Check a meet entry's `source:` block."""
    source = meet.get("source")
    if not isinstance(source, dict):
        return ["%s (%s): missing 'source' mapping" % (where, label)]

    problems = []
    source_type = source.get("type", "file")
    if source_type not in SOURCE_TYPES:
        problems.append("%s (%s): source.type must be one of %s, got %r"
                        % (where, label, ", ".join(SOURCE_TYPES), source_type))

    path = source.get("path")
    if source_type == "file":
        if not path:
            problems.append("%s (%s): source.path is required when source.type "
                            "is 'file'" % (where, label))
        elif check_files and sources_dir:
            full = os.path.join(sources_dir, str(path))
            if not os.path.exists(full):
                problems.append("%s (%s): source file not found: %s"
                                % (where, label, full))
    elif source_type == "url" and not source.get("url"):
        problems.append("%s (%s): source.url is required when source.type is "
                        "'url'" % (where, label))
    return problems


def _validate_race(race, where: str, race_index: int, label: str,
                   seen: Dict[Tuple[str, ...], str],
                   meet_name: Optional[str],
                   date: Optional[str],
                   source_key: Optional[str]) -> List[str]:
    """Check one entry in a meet's `races:` list."""
    rwhere = "%s races[%d]" % (where, race_index)
    if not isinstance(race, dict):
        return ["%s: not a mapping" % rwhere]

    problems = []
    race_name = race.get("name")
    rlabel = "%s / %s" % (label, race_name) if race_name else rwhere
    if not race_name:
        problems.append("%s: missing 'name'" % rwhere)

    distance = race.get("distance")
    if not distance:
        problems.append("%s (%s): missing 'distance' (e.g. \"5K\")"
                        % (rwhere, rlabel))

    race_class = race.get("class")
    if race_class is None:
        problems.append(
            "%s (%s): missing 'class' - declare one of %s (this used to be "
            "defaulted to 'varsity')"
            % (rwhere, rlabel, ", ".join(ALLOWED_CLASSES)))
    elif race_class not in ALLOWED_CLASSES:
        problems.append("%s (%s): unknown class %r - expected one of %s"
                        % (rwhere, rlabel, race_class, ", ".join(ALLOWED_CLASSES)))

    gender = race.get("gender")
    if gender is None:
        problems.append(
            "%s (%s): missing 'gender' - declare \"boys\" or \"girls\" (this "
            "used to be defaulted to boys)" % (rwhere, rlabel))
    elif gender in UNSUPPORTED_GENDERS:
        problems.append(
            "%s (%s): gender %r cannot be stored - the athletes table only "
            "accepts male/female, so this race would be dropped at insert time"
            % (rwhere, rlabel, gender))
    elif gender not in ALLOWED_GENDERS:
        problems.append("%s (%s): unknown gender %r - expected one of %s"
                        % (rwhere, rlabel, gender, ", ".join(ALLOWED_GENDERS)))
    elif race_name:
        word = _gender_conflicts_with_name(race_name, gender)
        if word:
            problems.append(
                "%s (%s): the name says %r but gender is %r - one of the two is "
                "wrong" % (rwhere, rlabel, word, gender))

    # A *complete* duplicate -- same race identity AND the same source section --
    # is always an accident: it re-reads a file that is already read, which is
    # how re-running the downloader appended five copies of one John Martin 2026
    # entry. The same race identity fed by *different* source files is
    # legitimate: a division split across several result pages is one race row
    # fed by several files (Liberty Bell 2026 "Division 1 Boys" is five page
    # files whose results sum to the single race row).
    section = race.get("section_title")
    values = (meet_name, date, race_name, distance, race_class, gender,
              source_key, "" if section is None else str(section))
    if all(value is not None for value in values):
        key = tuple(str(value) for value in values)
        if key in seen:
            problems.append(
                "%s (%s): duplicate entry - this race, from the same source, is "
                "already declared at %s. Re-running the downloader used to "
                "append another copy of the entry each time."
                % (rwhere, rlabel, seen[key]))
        else:
            seen[key] = rwhere

    return problems


def format_problems(problems: List[str], source_label: Optional[str] = None) -> str:
    """Render a problem list as a multi-line report."""
    header = "sources config%s has %d problem(s):" % (
        " (%s)" % source_label if source_label else "", len(problems))
    return "\n".join([header] + ["  - " + problem for problem in problems])


def validate_file(path: str, check_files: bool = True):
    """Load + validate a meets.yaml.

    Returns (config, problems). A missing file, malformed YAML or a duplicate
    key is reported as a single problem rather than raised, so callers can
    print one consistent report. `config` is None in those cases.
    """
    sources_dir = os.path.dirname(os.path.abspath(path))
    try:
        config = load_sources(path)
    except FileNotFoundError:
        return None, ["file not found: %s" % path]
    except yaml.YAMLError as exc:
        return None, ["invalid YAML: %s" % " ".join(str(exc).split())]
    return config, validate_meets(config, sources_dir=sources_dir,
                                  check_files=check_files)


