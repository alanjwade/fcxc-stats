# Cross Country Statistics Tracker

An application for tracking Fort Collins High School cross country team statistics with web scraping capabilities from MileSplit.com.

## Features

- **Data Collection**: Semi-automated scraping of race results from co.milesplit.com
- **Database Storage**: SQLite database for storing athlete and meet information
- **CSV Export**: Generate comprehensive athlete performance reports
- **Team Statistics**: View best times by gender and overall team performance
- **Athlete Profiles**: Individual athlete statistics including PRs and varsity points

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Scraper  │───▶│     SQLite      │◀──▶│  Web Dashboard  │
│  (Standalone)   │    │    Database     │    │ (Docker/Flask)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

The scraper runs standalone (not in Docker) and populates a SQLite database.
The webapp runs in Docker and reads the same database. A reverse proxy is
handled externally (e.g. via a homelab Docker orchestration layer).

## Project Structure

```
fcxc_stats/
├── sources/              # ★ Canonical data sources
│   ├── meets.yaml        #   Single source of truth for race metadata
│   ├── pages/            #   Raw downloaded pages (HTML/TXT)
│   │   └── 2025/         #   Organized by season/year
│   │       ├── rocky_mountain_lobo/
│   │       ├── vista_nation/
│   │       ├── john_martin/
│   │       ├── thornton/
│   │       ├── liberty_bell/
│   │       ├── windsor_wizards/
│   │       ├── desert_twilight/
│   │       ├── loveland_sweetheart/
│   │       ├── longs_peak/
│   │       ├── northern_conference/
│   │       ├── hawk_jv_champs/
│   │       ├── region_4/
│   │       └── state_championships/
│   └── archive/          #   Past seasons archive
├── scraper/              # Data scraping module (standalone)
│   ├── parsers/          # ★ Modular per-format parsers
│   │   ├── base.py       #   Base class + auto-registry
│   │   ├── default_parser.py
│   │   ├── john_martin.py
│   │   ├── thornton_combined.py
│   │   ├── raw_combined.py
│   │   ├── raw_windsor_combined.py
│   │   ├── desert_twilight.py
│   │   ├── loveland_sweetheart.py
│   │   ├── longs_peak.py
│   │   ├── northern_conference.py
│   │   └── regionals_table.py
│   ├── .venv/            # Python virtual environment
│   ├── requirements.txt
│   ├── scraper.py        # Main scraping logic
│   └── download_page.py  # Playwright page downloader
├── webapp/               # Flask web application (Docker)
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── app.py            # Main Flask application
│   └── templates/        # HTML templates
├── database/             # Database schema
│   ├── init.sql          # SQLite schema
│   └── migrations/       # Versioned schema migrations
│       └── 001_fractional_seconds.sql
├── config/               # Legacy config (migrated to sources/meets.yaml)
│   └── races.yaml
├── data/                 # SQLite database (shared between scraper & webapp)
├── docker-compose.yml    # Webapp Docker deployment
└── deploy.sh             # Deployment script
```

## Running the Scraper

The scraper runs outside of Docker using a local virtual environment.

### Setup

```bash
cd scraper
python3 -m venv --system-site-packages .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Usage

```bash
cd scraper
source .venv/bin/activate

# NEW: Run using the canonical sources config (auto-detects parsers)
DATABASE_URL=sqlite:///$(pwd)/../data/fcxc_stats.db python scraper.py --sources ../sources/meets.yaml

# Standard run — skips existing data, adds only new results
DATABASE_URL=sqlite:///$(pwd)/../data/fcxc_stats.db python scraper.py --config ../config/races.yaml

# Full database refresh — clear and reload all data
DATABASE_URL=sqlite:///$(pwd)/../data/fcxc_stats.db python scraper.py --clear-db --sources ../sources/meets.yaml
```

### Downloading Pages with Playwright

Some MileSplit pages require JavaScript rendering. Use the standalone downloader:

```bash
cd scraper
source .venv/bin/activate

# Download with structured output path
python download_page.py --url "https://co.milesplit.com/meets/..." \
                        --season 2025 \
                        --meet "Meet Name" \
                        --name "descriptive_name"

# Old-style positional arguments still work
python download_page.py "https://co.milesplit.com/meets/..." "output.html"
```

The downloader saves pages to `sources/pages/{season}/{meet_slug}/{name}.html`.

### Scraper Features

- **Smart Duplicate Prevention**: Automatically skips existing results
- **Incremental Updates**: Add new races without affecting existing data
- **Flexible Configuration**: YAML-based race definitions in `config/races.yaml`
- **Robust Error Handling**: Continues processing even if individual races fail

## Data Source Organization

Race metadata is now centralized in `sources/meets.yaml` — the single source of truth.
Raw pages are stored in `sources/pages/{season}/{meet_name}/` with descriptive filenames.

### Adding a New Race (2026+ Season)

1. **Download the page**:
   ```bash
   python scraper/download_page.py --url <URL> --season 2026 --meet "Meet Name" --name "boys_varsity"
   ```
   This saves to `sources/pages/2026/meet_name/boys_varsity.html`.

2. **Add metadata** in `sources/meets.yaml`:
   ```yaml
   - name: "Meet Name 2026"
     date: "2026-09-01"
     venue: "Venue Name"
     season: "2026"
     source:
       type: file
       path: "pages/2026/meet_name/boys_varsity.html"
     races:
       - name: "Varsity Boys"
         distance: "5K"
         class: "varsity"
         gender: "boys"
   ```

3. **If the page format is new**, create a parser in `scraper/parsers/`:
   ```python
   from .base import BaseParser, ParsedResult

   class MyNewParser(BaseParser):
       parser_name = "my_new_format"
       def can_parse(self, content): ...
       def extract_races(self, content): ...
   ```

4. **Run the scraper** to ingest the data:
   ```bash
   cd scraper
   python scraper.py --sources ../sources/meets.yaml
   ```

## Deploying the Webapp

The webapp runs as a Docker container, deployed to `/home/alan/homelab/fcxc-stats/`.

### Deploy

```bash
./deploy.sh deploy   # Copy files to homelab directory
./deploy.sh start    # Build and start the container
```

### Other Commands

```bash
./deploy.sh stop     # Stop the webapp container
./deploy.sh restart  # Restart the webapp container
./deploy.sh logs     # View webapp logs
./deploy.sh backup   # Backup the SQLite database
```

The webapp container joins the `proxy-network` Docker network and is
accessible via a reverse proxy configured elsewhere.

### Manual Docker Usage

```bash
cd /home/alan/homelab/fcxc-stats
docker compose up -d --build    # Start
docker compose down             # Stop
docker compose logs -f webapp   # Logs
```
