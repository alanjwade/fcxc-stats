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
├── scraper/              # Data scraping module (standalone)
│   ├── .venv/            # Python virtual environment
│   ├── requirements.txt
│   ├── scraper.py        # Main scraping logic
│   ├── download_page.py  # Playwright page downloader
│   └── pages/            # Downloaded HTML pages
├── webapp/               # Flask web application (Docker)
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── app.py            # Main Flask application
│   └── templates/        # HTML templates
├── database/             # Database schema
│   └── init.sql          # SQLite schema
├── config/               # Configuration files
│   └── races.yaml        # Race definitions
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

# Standard run — skips existing data, adds only new results
DATABASE_URL=sqlite:///$(pwd)/../data/fcxc_stats.db python scraper.py --config ../config/races.yaml

# Full database refresh — clear and reload all data
DATABASE_URL=sqlite:///$(pwd)/../data/fcxc_stats.db python scraper.py --clear-db --config ../config/races.yaml
```

### Downloading Pages with Playwright

Some MileSplit pages require JavaScript rendering. Use the standalone downloader:

```bash
cd scraper
source .venv/bin/activate
python download_page.py "https://co.milesplit.com/meets/..." "pages/output.html"
```

### Scraper Features

- **Smart Duplicate Prevention**: Automatically skips existing results
- **Incremental Updates**: Add new races without affecting existing data
- **Flexible Configuration**: YAML-based race definitions in `config/races.yaml`
- **Robust Error Handling**: Continues processing even if individual races fail

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
