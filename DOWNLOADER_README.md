# Page Downloader

A standalone script using Playwright to download MileSplit pages for offline processing.

## Files

- `scraper/download_page.py` - Python script that uses Playwright to download web pages

## Setup

Requires the scraper virtual environment with Playwright installed:

```bash
cd scraper
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

## Usage

```bash
cd scraper
source .venv/bin/activate
python download_page.py "URL" "pages/output_file.html"
```

## Examples

Download the Regionals 2025 page:
```bash
python download_page.py \
  "https://co.milesplit.com/meets/694555-colorado-5a-region-4-cross-country-2025/results" \
  "pages/regionals_2025_5a_region_4.html"
```

Download with auto-generated filename:
```bash
python download_page.py "https://co.milesplit.com/meets/694555-colorado-5a-region-4-cross-country-2025/results"
```

## How It Works

1. Launches a headless Chromium browser using Playwright
2. Navigates to the specified URL
3. Waits for dynamic content to load (3 seconds)
4. Extracts the complete HTML content
5. Saves to the specified file

## Features

- Headless browser execution
- 60-second timeout for slow pages
- Automatic directory creation
- Dynamic content rendering (JavaScript-heavy sites)

## Requirements

- Python 3 with the scraper virtual environment
- Playwright and Chromium (`playwright install chromium`)
- See `scraper/requirements.txt` for Python dependencies
## Automated download + meets.yaml registration

`scraper/download_and_add.py` (wrapper: `scraper/download_and_add`) downloads a
MileSplit results page, infers its metadata from the page's JSON-LD / `<meta>`
tags and the URL filters, asks you to confirm, then saves the raw page into
`sources/pages/` **and** appends a corresponding entry to `sources/meets.yaml`.

```bash
cd scraper
./download_and_add "https://co.milesplit.com/meets/<ID>-<slug>/results?type=formatted&event=5000m&gender=Girls&division=Varsity"
```

Inferred fields:
- **Meet name / season / date / venue** — from the page `<script type="application/ld+json">` (SportsEvent) and `og:`/description tags.
- **Race distance / class / gender** — from the URL query filters (`event`, `gender`, `division`), falling back to the page description.

You'll be asked `Is this correct? [y/N]`. If you answer `n`, the script prints
the flag that overrides each field and exits; re-run with corrections:

| Override flag | Field                  | Example                                    |
| ------------- | ----------------------- | ------------------------------------------ |
| `--meet`      | meet display name       | `--meet "Liberty Bell Cross Country Inv"` |
| `--date`      | race date               | `--date 2026-09-13`                        |
| `--season`    | season/year             | `--season 2026`                            |
| `--venue`     | venue                   | `--venue "Heritage HS"`                    |
| `--distance`  | race distance           | `--distance 5K`                            |
| `--class`     | race class              | `--class varsity`                          |
| `--gender`    | race gender             | `--gender girls`                           |
| `--race-name` | race display name       | `--race-name "Varsity Girls"`              |
| `--dir`       | source folder slug      | `--dir liberty_bell`                       |
| `--name`      | output file stem        | `--name girls_varsity`                     |

Use `--yes` to skip the confirmation prompt (for scripting). The script saves
the page to `sources/pages/{season}/{slug}/{file}.html` and inserts a formatted
entry into `meets.yaml` before the `team:` block, preserving the file's layout.

### Results are embedded for "formatted" pages

MileSplit "formatted" results pages render their rows purely from the
`/api/v1/meets/{id}/performances` API — the HTML shell contains **no results**.
`download_and_add` therefore also fetches that API (reliably, via `requests`)
and appends the race-filtered payload as a
`<script type="application/json" id="milesplit-api-data">` block. That makes the
saved file self-contained and is what the `milesplit_api` parser reads.

To upgrade an existing file that was downloaded before this change (it will have
no results), re-embed the data in place without re-downloading:

```bash
cd scraper
./.venv/bin/python embed_results.py "<path>.html" "<results_url>"
```

## Parser validation (does a parser already handle this?)

Not every page format has a parser yet. `scraper/check_parser.py` tries every
registered parser against a source file and reports which one (if any) parses
it **properly**, where "properly" means the extracted results pass sanity
validation from `parsers/base.py::validate_parsed_results`:

- **parsed_results** — at least one result was extracted.
- **has_home_team** — at least one result is from the home school
  (Fort Collins High School).
- **times_reasonable** — every time falls within the distance's plausible
  window (10–50 minutes scaled for a 5K);
- **places_valid** — places are positive and ascending.

```bash
cd scraper
./check_parser ../sources/pages/2026/foo/girls_varsity.html --distance 5K
# [OK]  MilesplitApiParser parses it properly: 71 results.
```

Exit code `0` if any parser parses it properly, `1` otherwise. The same
validation runs automatically inside `run_scraper` — when a parser's output
fails a criterion you'll see a `WARNING: ... fail validation` in the log instead
of silent garbage results.

## Running the scraper (after adding to meets.yaml)

`scraper/run_scraper.sh` wraps `scraper/scraper.py` and handles the Python
environment, working directory, and database URL so you don't have to worry
about them:

```bash
cd scraper
./run_scraper            # scrape sources/meets.yaml into the local SQLite DB
./run_scraper --clear-db # wipe the DB first, then scrape
```

It uses the scraper venv, runs from the `scraper/` directory (so the `parsers`
package is importable), and defaults `DATABASE_URL` to the repo-local
`data/fcxc_stats.db`. Override the DB with `DATABASE_URL=... ./run_scraper` or
point at a different sources file with `--sources <path>` (or
`FCXC_SOURCES=<path>`). Passing `--sources`/`--config` yourself disables the
default.
