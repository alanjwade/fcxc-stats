# Tests & Local Development

## Running the webapp locally

**Fastest (no Docker)** — uses a local venv created on first run, auto-points
at `data/fcxc_stats.db`:

```bash
./run_dev.sh           # normal
./run_dev.sh --debug   # Flask auto-reload
```

Open http://localhost:5000.

**As a full container build** — same as production image, binds port 5000
directly (no external proxy network needed):

```bash
docker compose -f docker-compose.local.yml up --build
docker compose -f docker-compose.local.yml down
```

## Tests

- **There is no pytest/unittest suite.** Test scripts are standalone `test_*.py`
  files at the repo root or in `scraper/`, using plain `assert` statements and
  `print` output, run manually with:
  ```bash
  python test_fractional_seconds.py
  python test_pace_calculations.py
  python test_validation.py
  python test_analytics.py
  ```
- **`test_*.py` files are gitignored** (`.gitignore: test_*.py`) — they are
  local scratch verification scripts and are **not committed**. If you add a new
  one, expect it to stay untracked.
- A recurring task is verifying **time parsing edge cases** (fractional seconds,
  the John Martin `MM:SS:ss` hundredths quirk) and **pace calculations** — there
  are existing scripts for both; extend or reuse them.
- For parser behavior, `test_scraper_vs_db.py` compares scraped output against
  the database. Run it on real ingested data after parser changes.

## Verification before finishing changes

- Webapp: run `./run_dev.sh --debug` and exercise the affected route.
- Scraper/parser changes: run the relevant `test_*.py` scripts and an
  incremental scrape against a **copy** of the database before any full refresh.
- Confirm new/changed deps are pinned (`==`) and the lock/requirements are in
  sync with what ships in the Dockerfile.