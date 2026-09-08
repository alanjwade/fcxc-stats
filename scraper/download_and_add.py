#!/usr/bin/env python3
"""
Download a MileSplit results page, infer its metadata, let the user verify
it, save the raw page into sources/ and append a meets.yaml entry.

Inferred fields: meet name, season, date, venue, and race
(distance/class/gender) — taken from the page's JSON-LD / <meta> tags
and the URL query filters.

Usage:
    python download_and_add.py <URL>
    python download_and_add.py <URL> --season 2026 --date 2026-08-21 \
        --venue "Spring Canyon Park, Fort Collins CO" --distance 5K \
        --class varsity --gender girls --name girls_varsity
    python download_and_add.py <MEET_FORMATTED_URL> --dry-run \
        --filter-distance 5K --filter-gender boys --filter-gender girls

Overrides (use to correct anything not inferred correctly):
    Meet name: --meet     Date: --date      Season: --season   Venue: --venue
    Distance:  --distance Class: --class    Gender: --gender   Race: --race-name
    File stem: --name     Source folder: --dir

You'll be asked to confirm the resolved metadata. Say 'n' to get a reminder of
the flag for each field (then re-run with corrections). Use --yes to skip.
"""

import argparse
import json
import os
import re
import sys
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from bs4 import BeautifulSoup

from download_page import download_html, fetch_milesplit_api, embed_api_results

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCES_DIR = os.path.join(PROJECT_ROOT, "sources")
MEETS_YAML = os.path.join(SOURCES_DIR, "meets.yaml")

DISTANCE_BY_EVENT = {
    "5000m": "5K", "5k": "5K", "5km": "5K",
    "3000m": "3K", "3k": "3K",
    "1600m": "1600m", "800m": "800m", "400m": "400m",
    "2mile": "2M", "2m": "2M", "mile": "1M",
}
CLASS_DISPLAY = {"varsity": "Varsity", "jv": "JV", "freshman": "Freshman", "open": "Open"}
GENDER_DISPLAY = {"boys": "Boys", "girls": "Girls", "mixed": "Mixed"}
_YEAR = re.compile(r"\b(20\d{2})\b")


def slugify(text):
    """To lowercase, strip non-alphanumerics into single underscores."""
    s = re.sub(r"[^a-z0-9]+", "_", (text or "").lower()).strip("_")
    return s or "section"


def parse_query(url):
    """Extract meeting id and event/gender/division query params from a URL."""
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    m = re.search(r"/meets/(\d+)(?:-[^/]*)?", url)
    slug = None
    s = re.search(r"/meets/\d+-(.+)/", url)
    if s:
        slug = s.group(1)
    return {
        "meet_id": m.group(1) if m else None,
        "url_slug": slug,
        "event": qs.get("event", [None])[0],
        "gender": qs.get("gender", [None])[0],
        "division": qs.get("division", [None])[0],
    }


_EVENT_DISTANCE = {
    "5000m": 5000, "5k": 5000, "5km": 5000,
    "3000m": 3000, "3k": 3000,
    "1600m": 1600, "800m": 800, "400m": 400,
    "2mile": 3218, "2m": 3218, "mile": 1609,
}

EVENT_PARAM_BY_DISTANCE = {
    5000: "5000m",
    3000: "3000m",
    1600: "1600m",
    800: "800m",
    400: "400m",
    3218: "2mile",
    1609: "mile",
}


def distance_from_meters(distance_m):
    if distance_m is None:
        return None
    return DISTANCE_BY_EVENT.get(str(distance_m).lower()) or {
        5000: "5K",
        3000: "3K",
        1600: "1600m",
        800: "800m",
        400: "400m",
        3218: "2M",
        1609: "1M",
    }.get(distance_m)


def normalize_distance_filter(value):
    """Normalize distance filter values to canonical form (e.g. 5k -> 5K)."""
    if value is None:
        return None
    v = str(value).strip().lower()
    if not v:
        return None
    return DISTANCE_BY_EVENT.get(v, str(value).strip())


def normalize_division_bucket(division_name):
    """Map MileSplit division labels into local race-class buckets.

    Rule requested for Liberty Bell style meets:
      - JV and Open are both JV races
      - everything else is Division 1
    """
    d = (division_name or "").strip().lower()
    if any(k in d for k in ("jv", "junior varsity", "open")):
        return "jv"
    return "division1"


def division_display_name(division_name):
    d = (division_name or "").strip().lower()
    if "open" in d:
        return "Open"
    if "jv" in d or "junior varsity" in d:
        return "JV"
    return "Division 1"


def class_from_bucket(bucket):
    return "jv" if bucket == "jv" else "varsity"


def gender_from_api_name(name):
    g = (name or "").strip().lower()
    if g in ("boys", "men", "male"):
        return "boys"
    if g in ("girls", "women", "female"):
        return "girls"
    return "mixed"


def build_race_url(base_url, event_param=None, gender_param=None, division_param=None):
    """Build a formatted-results URL with race query filters."""
    parsed = urlparse(base_url)
    qs = parse_qs(parsed.query)
    qs["type"] = ["formatted"]
    if event_param:
        qs["event"] = [event_param]
    if gender_param:
        qs["gender"] = [gender_param]
    if division_param:
        qs["division"] = [division_param]

    query = urlencode([(k, v) for k, vals in qs.items() for v in vals], doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, query, parsed.fragment))


def discover_race_groups(api_data):
    """Return unique race groups from MileSplit API payload for meet-level URLs."""
    data = api_data.get("data") if isinstance(api_data, dict) else None
    if not isinstance(data, list):
        return []

    seen = set()
    groups = []
    for row in data:
        event_distance = row.get("eventDistance")
        gender_name = row.get("genderName")
        division_name = row.get("divisionName")
        key = (event_distance, (gender_name or "").strip(), (division_name or "").strip())
        if key in seen:
            continue
        seen.add(key)
        groups.append(
            {
                "event_distance": event_distance,
                "gender_name": gender_name,
                "division_name": division_name,
                "bucket": normalize_division_bucket(division_name),
                "division_display": division_display_name(division_name),
            }
        )

    def sort_key(g):
        # Stable ordering: distance asc, girls before boys, Division 1 then JV/Open.
        gender_rank = {"Girls": 0, "Boys": 1}.get(g.get("gender_name"), 2)
        div = (g.get("division_display") or "").lower()
        div_rank = 0 if "division 1" in div else (1 if "jv" in div else 2)
        dist = g.get("event_distance")
        dist_rank = int(dist) if isinstance(dist, int) else 999999
        return (dist_rank, gender_rank, div_rank, div)

    return sorted(groups, key=sort_key)


def filter_api_for_url(api_data, url):
    """Return a copy of the API payload restricted to the race the URL filters point to.

    The /api/v1/meets/{id}/performances response contains every race at the meet.
    Each YAML entry points to a single downloaded file for ONE race, so we narrow
    the embedded data down to the matching event/gender/division before saving.
    """
    if not isinstance(api_data, dict):
        return api_data
    data = api_data.get("data")
    if not isinstance(data, list):
        return api_data
    q = parse_query(url)

    target_distance = _EVENT_DISTANCE.get((q["event"] or "").lower()) if q["event"] else None

    gender_map = {"girls": "Girls", "boys": "Boys", "women": "Girls", "men": "Boys",
                   "female": "Girls", "male": "Boys"}
    target_gender = gender_map.get((q["gender"] or "").lower(), q["gender"]) if q["gender"] else None
    target_division = (q["division"] or "").strip() or None
    # MileSplit labels a race where "everyone runs in one race" (no Varsity/JV
    # split) as "Complete". Rows in such a pooled race carry divisionName=None
    # because there is no real division, so filtering on divisionName ==
    # "Complete" would match nothing. Treat Complete/all/combined as "filter by
    # no division" — gender + distance already isolate the race.
    if target_division and target_division.lower() in ("complete", "all", "combined"):
        target_division = None

    filtered = data
    if target_distance is not None:
        filtered = [d for d in filtered if d.get("eventDistance") == target_distance]
    if target_gender:
        filtered = [d for d in filtered if d.get("genderName") == target_gender]
    if target_division:
        filtered = [d for d in filtered if d.get("divisionName") == target_division]

    def place_key(entry):
        try:
            return int(entry.get("place") or 0)
        except (ValueError, TypeError):
            return 9999

    out = dict(api_data)
    out["data"] = sorted(filtered, key=place_key)
    return out


def extract_json_ld(html):
    """Parse the first JSON-LD SportsEvent-style block into a dict."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string)
        except Exception:
            continue
        if isinstance(data, dict) and data.get("@type"):
            return data
    return {}


def extract_og(html):
    """Pull meta description / og:* tags into a dict."""
    soup = BeautifulSoup(html, "html.parser")
    out = {}
    for tag in soup.find_all("meta"):
        key = tag.get("property") or tag.get("name")
        content = tag.get("content")
        if key and content:
            out[key] = content
    return out

def infer_metadata(url, html, args):
    """Infer meet/race metadata from the URL + page, honoring CLI overrides."""
    q = parse_query(url)
    ld = extract_json_ld(html)
    og = extract_og(html)

    # Meet name ------------------------------------------------------
    raw = ld.get("name") or ""
    if not raw:
        og_title = (og.get("og:title") or "").split(" - ")[0].strip()
        raw = og_title
    if not raw and q["url_slug"]:
        raw = q["url_slug"].replace("-", " ").title()
    meet_name = args.meet or raw or ""

    # Season / date ---------------------------------------------------
    date = (args.date or ld.get("startDate") or "")[:10]
    season = args.season or (date[:4] if date else None)
    if not season:
        probe = f"{meet_name} {q.get('url_slug') or ''}"
        ym = _YEAR.search(probe)
        season = ym.group(1) if ym else None

    # Venue ------------------------------------------------------------
    loc = ld.get("location") or {}
    addr = loc.get("address") or {}
    parts = []
    if loc.get("name"):
        parts.append(loc["name"])
    if addr.get("addressLocality") and addr.get("addressRegion"):
        parts.append(f"{addr['addressLocality']} {addr['addressRegion']}")
    elif addr.get("addressRegion"):
        parts.append(addr["addressRegion"])
    venue = args.venue or (", ".join(parts) if parts else None)

    # Race -------------------------------------------------------------
    distance = args.distance or distance_from_event(q["event"]) or distance_from_text(ld, og)
    cls = args.race_class or class_from_division(q["division"]) or class_from_text(ld, og)
    gender = args.gender or gender_from_param(q["gender"]) or gender_from_text(ld, og)
    cls = cls or "varsity"
    gender = gender or "mixed"
    distance = distance or "5K"

    if args.race_name:
        race_name = args.race_name
    elif args.race_class and args.gender:
        race_name = f"{CLASS_DISPLAY.get(cls, cls.title())} {GENDER_DISPLAY.get(gender, gender.title())}".strip()
    else:
        class_word = CLASS_DISPLAY.get(cls) or cls.title()
        gender_word = GENDER_DISPLAY.get(gender) or gender.title()
        race_name = f"{class_word} {gender_word}".strip()

    return {
        "meet_name": meet_name,
        "date": date,
        "season": season,
        "venue": venue,
        "distance": distance,
        "class": cls,
        "gender": gender,
        "race_name": race_name,
    }

def distance_from_event(event):
    if not event:
        return None
    return DISTANCE_BY_EVENT.get(event.lower())


def distance_from_text(ld, og):
    """Find a race distance mentioned in the page description text."""
    text = " ".join(v for v in [ld.get("description", ""), og.get("og:description", "")] if v)
    low = text.lower()
    for needle, dist in [
        ("5000 meters", "5K"), ("5,000 meters", "5K"), ("5k", "5K"),
        ("3000 meter", "3K"), ("1600 meter", "1600m"), ("800 meter", "800m"),
    ]:
        if needle in low:
            return dist
    return None


def class_from_division(division):
    if not division:
        return None
    d = division.lower().replace("-", " ").replace("_", " ")
    if "junior varsity" in d or "jv" in d:
        return "jv"
    if "fresh" in d:
        return "freshman"
    if "open" in d:
        # Open is treated as JV for Liberty Bell style pages.
        return "jv"
    if "var" in d:
        return "varsity"
    return None


def class_from_text(ld, og):
    text = " ".join(v for v in [ld.get("description", ""), og.get("og:description", "")] if v).lower()
    if re.search(r"\bjunior varsity\b|\bjv\b", text):
        return "jv"
    if re.search(r"\bfreshman\b|\bfreshmen\b|\bfrosh\b", text):
        return "freshman"
    if re.search(r"\bvarsity\b", text):
        return "varsity"
    if re.search(r"\bopen\b", text):
        return "open"
    return None


def gender_from_param(gender):
    if not gender:
        return None
    g = gender.lower()
    if g in ("girls", "girl", "women", "female", "f"):
        return "girls"
    if g in ("boys", "boy", "men", "male", "m"):
        return "boys"
    if g in ("mixed", "coed"):
        return "mixed"
    return None


def gender_from_text(ld, og):
    text = " ".join(v for v in [ld.get("description", ""), og.get("og:description", "")] if v).lower()
    if re.search(r"\bgirls?\b", text):
        return "girls"
    if re.search(r"\bboys?\b", text):
        return "boys"
    return None
def display_metadata(meta):
    """Return a human-readable summary of the inferred metadata."""
    lines = []
    for label, key in [
        ("Meet name", "meet_name"),
        ("Date", "date"),
        ("Season", "season"),
        ("Venue", "venue"),
        ("Race", "race_name"),
        ("Distance", "distance"),
        ("Class", "class"),
        ("Gender", "gender"),
    ]:
        lines.append(f"  {label:10}: {meta[key] or '(unknown)'}")
    return "\n".join(lines)


def confirm(meta):
    """Ask the user to verify. Returns True to proceed, False to exit."""
    print("\nResolved metadata:\n" + display_metadata(meta))
    print("\nThe values above were inferred. Answer:")
    ans = input("  Is this correct? [y/N] ").strip().lower()
    return ans in ("y", "yes")


def print_override_help():
    print(
        "\nRe-run with these flags to correct any inferred value:\n"
        "  --meet      meet name\n"
        "  --date      race date (YYYY-MM-DD)\n"
        "  --season    season/year\n"
        "  --venue     venue\n"
        "  --distance  distance (e.g. 5K, 1600m)\n"
        "  --class     class (varsity, jv, freshman, open)\n"
        "  --gender    gender (boys, girls, mixed)\n"
        "  --race-name race display name\n"
        "  --dir       source folder slug\n"
        "  --name     file stem (no extension)\n"
    )


def write_page(html, season, dir_slug, filename):
    """Save the raw HTML into sources/pages/{season}/{dir_slug}/filename."""
    folder = os.path.join(SOURCES_DIR, "pages", season if season else "unknown", dir_slug)
    folder = os.path.join(folder)
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    return path


def build_yaml_entry(meta, rel_path):
    """Build the formatted meets.yaml entry text for the new meet."""
    name = meta["meet_name"]
    date = meta["date"]
    season = meta["season"]
    venue = meta.get("venue") or ""
    display = f"{name} {season}" if name and not re.search(r"\b20\d{2}\b$", name) else name

    entry = f"""  # ===========================================================================
  # {display}
  # Source: 1 HTML file ({rel_path.rsplit('/', 1)[-1]})
  # Parser: auto (auto-detected)
  # ===========================================================================
  - name: "{ensure_quoted(display)}"
    date: "{ensure_quoted(date)}"
    venue: "{ensure_quoted(venue)}"
    season: "{ensure_quoted(season)}"
    source:
      type: file
      path: "{rel_path}"
    races:
      - name: "{ensure_quoted(meta['race_name'])}"
        distance: "{ensure_quoted(meta['distance'])}"
        class: "{meta['class']}"
        gender: "{meta['gender']}"
"""
    return entry


def ensure_quoted(value):
    s = str(value or "").replace('"', "'")
    return s


def insert_into_meets_yaml(meets_path, entry_text):
    """Insert an entry at the end of the meets list, before the team block.

    Walks up from the 'team:' line through the team-section comment header and
    any blank lines, and inserts the new entry just after the last meet.
    """
    with open(meets_path, encoding="utf-8") as f:
        lines = f.read().split("\n")

    team_line = next((i for i, l in enumerate(lines) if l.strip() == "team:"), None)
    if team_line is None:
        raise RuntimeError("Could not find the 'team:' block in meets.yaml")

    # Walk up from team: while lines are part of the team header (blank or #).
    insert_at = team_line
    while insert_at > 0:
        prev = lines[insert_at - 1].strip()
        if prev == "" or prev.startswith("#"):
            insert_at -= 1
        else:
            break

    prefix = "\n".join(lines[:insert_at]).rstrip("\n")
    suffix = "\n".join(lines[insert_at:])
    block = "\n" + entry_text.rstrip("\n") + "\n\n"
    updated = prefix + block + suffix + "\n"

    with open(meets_path, "w", encoding="utf-8") as f:
        f.write(updated)
    return meets_path


def default_filename(meta):
    cls = meta["class"]
    gender = meta["gender"]
    if gender in ("boys", "girls") and cls:
        return f"{gender}_{cls}.html"
    if gender in ("boys", "girls"):
        return f"{gender}.html"
    return "results.html"


def validate(meta):
    missing = [k for k in ("meet_name", "date", "season") if not meta.get(k)]
    for k in ("class", "gender", "distance"):
        if not meta.get(k):
            missing.append(k)
    return missing


def is_meet_level_formatted_url(url):
    """True when URL points at formatted results without race-level filters."""
    q = parse_query(url)
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    is_formatted = (qs.get("type", [""])[0].lower() == "formatted")
    has_any_filter = bool(q.get("event") or q.get("gender") or q.get("division"))
    return is_formatted and not has_any_filter


def build_meta_for_group(base_meta, group):
    """Derive per-race metadata for one discovered race group."""
    gender = gender_from_api_name(group.get("gender_name"))
    bucket = group.get("bucket")
    race_class = class_from_bucket(bucket)
    distance = distance_from_meters(group.get("event_distance")) or "5K"
    race_prefix = group.get("division_display") or ("JV" if bucket == "jv" else "Division 1")
    race_name = f"{race_prefix} {GENDER_DISPLAY.get(gender, gender.title())}".strip()

    meta = dict(base_meta)
    meta.update(
        {
            "distance": distance,
            "class": race_class,
            "gender": gender,
            "race_name": race_name,
        }
    )
    return meta


def apply_group_filters(groups, base_meta, args):
    """Filter discovered meet race-groups by distance and/or gender flags."""
    distance_filters = args.filter_distance or []
    gender_filters = [g.lower() for g in (args.filter_gender or [])]

    distance_set = set()
    for d in distance_filters:
        nd = normalize_distance_filter(d)
        if nd:
            distance_set.add(nd)
    gender_set = {g for g in gender_filters if g in ("boys", "girls", "mixed")}

    if not distance_set and not gender_set:
        return groups

    out = []
    for group in groups:
        meta = build_meta_for_group(base_meta, group)
        if distance_set and meta.get("distance") not in distance_set:
            continue
        if gender_set and meta.get("gender") not in gender_set:
            continue
        out.append(group)
    return out


def default_filename_for_group(meta, group):
    """Race-specific filename for meet-level expansion."""
    gender = meta["gender"]
    div = slugify((group.get("division_display") or "division1").replace(" ", "_"))
    dist = slugify(meta.get("distance") or "results")
    return f"{gender}_{div}_{dist}.html"


def process_single_url(url, args):
    """Existing single-race workflow for URLs that already include filters."""
    print(f"Downloading: {url}")
    html = download_html(url, wait_seconds=args.wait)

    api_data = fetch_milesplit_api(url)
    if api_data and isinstance(api_data.get("data"), list) and api_data["data"]:
        filtered = filter_api_for_url(api_data, url)
        html = embed_api_results(html, {"data": filtered.get("data", [])})
    else:
        print("Warning: could not fetch results API for this URL; "
              "saving the raw page without embedded results.", file=sys.stderr)

    meta = infer_metadata(url, html, args)

    print("\nResolved metadata:")
    print(display_metadata(meta))

    if not args.yes:
        if not confirm(meta):
            print_override_help()
            return 1

    missing = validate(meta)
    if missing:
        print("\nThese required fields are missing or could not be inferred:")
        for f in missing:
            print(f"   - {f}")
        print_override_help()
        return 1

    name_arg = args.name or None
    filename = (name_arg + ".html") if name_arg else default_filename(meta)
    dir_slug = args.dir or slugify(meta["meet_name"])
    season = meta["season"] or "unknown"

    page_path = write_page(html, season, dir_slug, filename)
    rel_path = os.path.relpath(page_path, SOURCES_DIR)

    entry = build_yaml_entry(meta, rel_path)

    if args.dry_run:
        print()
        print("DRY RUN: no files written and meets.yaml unchanged.")
        print(f"Would save page: {page_path}")
        print("Would add meets.yaml entry:")
        print(entry)
        return 0

    insert_into_meets_yaml(args.meets_yaml, entry)

    print()
    print(f"✓ Page saved: {page_path}")
    print("✓ meets.yaml entry added:")
    print(entry)
    print("Next: run `python scraper/scraper.py --sources sources/meets.yaml`")
    return 0


def process_meet_level_url(url, args):
    """Expand one formatted meet URL into per-race downloads + YAML entries."""
    print(f"Downloading meet page: {url}")
    meet_html = download_html(url, wait_seconds=args.wait)

    api_data = fetch_milesplit_api(url)
    if not api_data or not isinstance(api_data.get("data"), list) or not api_data["data"]:
        print("Error: could not fetch meet API results for a meet-level formatted URL.", file=sys.stderr)
        return 1

    base_meta = infer_metadata(url, meet_html, args)
    groups = discover_race_groups(api_data)
    groups = apply_group_filters(groups, base_meta, args)
    if not groups:
        print("No race groups match the requested filters.")
        return 0

    print(f"Discovered {len(groups)} race groups from meet API.")
    preview = []
    for g in groups:
        m = build_meta_for_group(base_meta, g)
        event_param = EVENT_PARAM_BY_DISTANCE.get(g.get("event_distance"))
        race_url = build_race_url(
            url,
            event_param=event_param,
            gender_param=g.get("gender_name"),
            division_param=g.get("division_name"),
        )
        preview.append("  - " + m["race_name"] + f" ({m['distance']}) -> {race_url}")
    print("\nRace entries to add:\n" + "\n".join(preview))

    if args.dry_run:
        season = base_meta["season"] or "unknown"
        dir_slug = args.dir or slugify(base_meta["meet_name"])
        print()
        print("DRY RUN: no files written and meets.yaml unchanged.")
        print(f"Would write under: pages/{season}/{dir_slug}/")
        return 0

    if not args.yes:
        ans = input("\nProceed with all discovered races? [y/N] ").strip().lower()
        if ans not in ("y", "yes"):
            print("Cancelled.")
            return 1

    season = base_meta["season"] or "unknown"
    dir_slug = args.dir or slugify(base_meta["meet_name"])
    written = []
    used_filenames = set()

    for group in groups:
        event_param = EVENT_PARAM_BY_DISTANCE.get(group.get("event_distance"))
        gender_name = group.get("gender_name")
        division_name = group.get("division_name")
        race_url = build_race_url(url, event_param=event_param, gender_param=gender_name, division_param=division_name)

        race_html = download_html(race_url, wait_seconds=args.wait)
        filtered = filter_api_for_url(api_data, race_url)
        race_html = embed_api_results(race_html, {"data": filtered.get("data", [])})

        meta = build_meta_for_group(base_meta, group)
        filename = default_filename_for_group(meta, group)
        if filename in used_filenames:
            stem = filename[:-5] if filename.endswith(".html") else filename
            suffix = 2
            candidate = f"{stem}_{suffix}.html"
            while candidate in used_filenames:
                suffix += 1
                candidate = f"{stem}_{suffix}.html"
            filename = candidate
        used_filenames.add(filename)
        page_path = write_page(race_html, season, dir_slug, filename)
        rel_path = os.path.relpath(page_path, SOURCES_DIR)

        entry = build_yaml_entry(meta, rel_path)
        insert_into_meets_yaml(args.meets_yaml, entry)
        written.append((page_path, entry))

    print()
    print(f"✓ Saved {len(written)} race page files under pages/{season}/{dir_slug}/")
    print(f"✓ Added {len(written)} entries to {args.meets_yaml}")
    print("Next: run `python scraper/scraper.py --sources sources/meets.yaml`")
    return 0


def run(args):
    """Orchestrate download, infer, confirm, write files + meets.yaml entry."""
    url = args.url
    if is_meet_level_formatted_url(url):
        return process_meet_level_url(url, args)
    return process_single_url(url, args)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Download a MileSplit page, infer+verify metadata, save the "
        "page, and append a meets.yaml entry.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("url", help="MileSplit results URL")
    parser.add_argument("--meet", help="Meet name (overrides inferred)")
    parser.add_argument("--date", help="Race date YYYY-MM-DD (overrides inferred)")
    parser.add_argument("--season", help="Season/year (overrides inferred)")
    parser.add_argument("--venue", help="Venue (overrides inferred)")
    parser.add_argument("--distance", help="Distance e.g. 5K, 1600m")
    parser.add_argument("--class", dest="race_class", help="class: varsity, jv, freshman, open")
    parser.add_argument("--gender", help="gender: boys, girls, mixed")
    parser.add_argument("--race-name", help="Race display name e.g. 'Varsity Girls'")
    parser.add_argument("--name", help="Output file stem (no extension)")
    parser.add_argument("--dir", help="Source folder slug (meet slug)")
    parser.add_argument("--wait", type=int, default=3, help="Seconds to wait for JS render")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview what would be downloaded/added without writing files")
    parser.add_argument("--filter-distance", action="append", default=[],
                        help="Meet-level only: include only this distance (e.g. 5K). Can repeat.")
    parser.add_argument("--filter-gender", action="append", default=[], choices=["boys", "girls", "mixed"],
                        help="Meet-level only: include only this gender. Can repeat.")
    parser.add_argument("--yes", action="store_true", help="Skip the confirmation prompt")
    parser.add_argument("--meets-yaml", default=MEETS_YAML,
                        help=f"Target meets.yaml path (default: {MEETS_YAML})")
    args = parser.parse_args(argv)

    # argparse '--class' is dest='race_class'; references use args.race_class
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())