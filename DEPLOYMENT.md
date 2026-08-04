# Cross Country Statistics Tracker — Deployment Guide

## Overview

- **Scraper**: Runs standalone on your dev machine using a Python virtual environment
- **Webapp**: Runs as a Docker container deployed to `/home/alan/homelab/fcxc-stats/`
- **Reverse Proxy / TLS**: Handled externally by the homelab Docker orchestration

## Scraper Setup

### One-time setup

```bash
cd scraper
python3 -m venv --system-site-packages .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Running the scraper

```bash
cd scraper
source .venv/bin/activate

# Standard run (incremental — skips existing data)
DATABASE_URL=sqlite:///$(pwd)/../data/fcxc_stats.db python scraper.py --config ../config/races.yaml

# Full refresh (clear + re-scrape)
DATABASE_URL=sqlite:///$(pwd)/../data/fcxc_stats.db python scraper.py --clear-db --config ../config/races.yaml
```

The scraper writes to `data/fcxc_stats.db` in the project root. This same
database is mounted into the webapp container.

### Downloading MileSplit pages

For pages that need JavaScript rendering:

```bash
cd scraper
source .venv/bin/activate
python download_page.py "URL" "pages/output.html"
```

Then reference the file in `config/races.yaml` with the `file:` key.

## Webapp Deployment

### Deploy to homelab

```bash
./deploy.sh deploy    # Copies webapp/, config/, and docker-compose.yml to homelab
./deploy.sh start     # Builds and starts the container
```

### Other commands

```bash
./deploy.sh stop      # Stop the webapp
./deploy.sh restart   # Restart the webapp
./deploy.sh logs      # Tail webapp logs
./deploy.sh backup    # Copy the SQLite database to a timestamped backup file
```

### Manual deployment

```bash
cd /home/alan/homelab/fcxc-stats
docker compose up -d --build
```

### Configuration

The webapp reads `DATABASE_URL` from its environment (set in `docker-compose.yml`).
The SQLite database at `data/fcxc_stats.db` is bind-mounted into the container
at `/data/fcxc_stats.db`.

The `VIRTUAL_HOST` environment variable is used by the external reverse proxy
to route traffic to this container.

## Database

The database is a single SQLite file at `data/fcxc_stats.db`. Tables are
created automatically on startup by both the scraper and webapp if they
don't already exist. The schema is defined in `database/init.sql` for reference.

### Backup

```bash
./deploy.sh backup
# or manually:
cp data/fcxc_stats.db "backup_$(date +%Y%m%d_%H%M%S).db"
```

## Troubleshooting

**Webapp not starting:**
```bash
cd /home/alan/homelab/fcxc-stats
docker compose logs webapp
```

**Database empty after deploy:**
Run the scraper to populate the database, then restart the webapp:
```bash
cd scraper && source .venv/bin/activate
DATABASE_URL=sqlite:///$(pwd)/../data/fcxc_stats.db python scraper.py --config ../config/races.yaml
```

**Scraper import errors:**
Ensure you activated the venv: `source scraper/.venv/bin/activate`
