# Code Conventions

## Python / environment

- Python **3.11** (the image is `python:3.11-slim`).
- **Pin all dependencies with `==`** in `webapp/requirements.txt` and
  `scraper/requirements.txt`. Do not use unpinned ranges.
- Webapp runtime deps: `flask==3.1.0`, `sqlalchemy==2.0.36`. Scraper adds
  `requests`, `beautifulsoup4`, `lxml`, `pyyaml`, `playwright`.
- Type hints are used throughout (`from typing import ...`); keep that style.

## Webapp (Flask)

- `webapp/app.py` uses **raw SQL via SQLAlchemy `text()`** (`from sqlalchemy
  import create_engine, text`) — there are **no ORM models**. `engine =
  create_engine(DATABASE_URL, connect_args={"check_same_thread": False,
  "timeout": 30})`.
- `DATABASE_URL` is required; the app raises `ValueError` if unset.
- Secrets/config come from environment variables (`SECRET_KEY`,
  `DATABASE_URL`); never hardcode the production secret.
- Templates live in `webapp/templates/` (Jinja2, `base.html` + page templates).
- Prefer adding routes/stats logic in `app.py` unless it's a self-contained
  feature — the app is intentionally a single-file Flask app.

## Scraper parsers (`scraper/parsers/`)

- **Every new parser subclasses `BaseParser`** and sets a unique `parser_name`.
  Subclasses are **auto-registered** via `__init_subclass__` on `BaseParser` —
  there is no manual registry list to update.
- Implement two methods:
  - `can_parse(self, content: str) -> bool` — return True for content this
    parser handles.
  - `extract_races(self, content: str) -> Dict[str, List[ParsedResult]]` —
    map section title (or `"default"`) to a list of `ParsedResult` objects.
- **Reuse `BaseParser.parse_time_to_seconds`** for all time parsing (it handles
  the full range of MileSplit formats, including the John Martin hundredths
  quirk where `MM:SS:ss` is minutes:hundredths). Don't reimplement time parsing.
- **Reuse `BaseParser.normalize_school_name`** (and the `_SCHOOL_MAPPINGS`
  table) so school names stay consistent across parsers. Add new abbreviations
  to the mapping in `base.py`, not in individual parsers.
- Extend `BaseParser` rather than duplicating shared logic.

## Schema / migrations

- Schema lives in `database/init.sql`. Changes that alter an existing database
  belong in a **new numbered file** in `database/migrations/`
  (e.g. `002_*.sql`) rather than only editing `init.sql`, so live databases can
  be upgraded without a full rebuild.