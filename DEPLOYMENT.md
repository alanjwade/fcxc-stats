# Cross Country Statistics Tracker — Deployment Guide

## Overview

- **Scraper**: Runs standalone on your dev machine using a Python virtual environment
- **Webapp**: Runs as a Docker container (two deployment strategies — see below)
- **Reverse Proxy / TLS**: Handled externally by the homelab Docker orchestration

## Webapp Deployment Strategies

Two strategies exist. The old one is still in place; the GHCR strategy is the
recommended path going forward.

| | Old (rsync + build on server) | New (GHCR image) |
|---|---|---|
| Requires source on server | Yes | No |
| Build happens | On the server | In GitHub Actions |
| Update flow | rsync → docker build | push to main → Actions → pull |
| Script | `deploy_homelab00.sh` | `deploy_homelab00_ghcr.sh` |
| Compose file | `docker-compose.yml` | `docker-compose.ghcr.yml` |

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

---

## Local Webapp Testing

### Option 1 — Without Docker (fastest for development)

```bash
# From the project root
./run_dev.sh           # Normal mode
./run_dev.sh --debug   # Auto-reload on code changes
```

Open http://localhost:5000. Requires `flask` and `sqlalchemy` installed in your
Python environment. The script automatically points at `data/fcxc_stats.db`.

### Option 2 — Containerized local build

Tests the full Docker image exactly as it would run in production:

```bash
docker compose -f docker-compose.local.yml up --build
```

Open http://localhost:5000. No external networks or reverse proxy needed.
Use `Ctrl-C` / `docker compose -f docker-compose.local.yml down` to stop.

---

## GHCR Deployment (new, recommended)

This strategy builds the Docker image in GitHub Actions and publishes it to
GitHub Container Registry. The server only needs Docker — no source code, no
build step on the server.

### Step 1 — Set up GitHub (one-time)

1. **Push this repo to GitHub** if not already done (it's `alanjwade/fcxc-stats`).
2. The workflow file at `.github/workflows/build-push.yml` is already committed.
   It will trigger automatically on the next push to `main` that touches
   `webapp/` or `config/`.
3. **Make the package public** (optional but simplifies server setup):
   - After the first successful workflow run, go to your GitHub profile →
     **Packages** → `fcxc-stats` → **Package settings** → change visibility to
     **Public**.
   - If you leave it private, the server must authenticate before pulling
     (see Step 2b below).
4. No additional GitHub secrets are needed — the workflow uses the built-in
   `GITHUB_TOKEN`.

### Step 2 — Set up the server (one-time)

SSH into the server and:

```bash
# Create the data directory
mkdir -p ~/homelab00-config/websites/volumes/sites/fcxc_web/data

# If the GHCR package is PRIVATE, log Docker in to GHCR:
# (create a PAT at https://github.com/settings/tokens with 'read:packages' scope)
echo YOUR_PAT | docker login ghcr.io -u alanjwade --password-stdin

# If the GHCR package is PUBLIC, no login is needed.
```

Also confirm the `proxy-network` Docker network exists on the server (it's
created by whatever runs your reverse proxy — nginx-proxy, Traefik, etc.).

### Step 3 — Deploy from your dev machine

```bash
./deploy_homelab00_ghcr.sh
```

This copies `docker-compose.ghcr.yml` and the database to the server, then
pulls the latest image and restarts the container in one step.

### Updating after a code change

1. Commit and push to `main` — GitHub Actions builds and pushes `latest`
   automatically.
2. Run `./deploy_homelab00_ghcr.sh` to pull and restart on the server.
   (Or SSH in and run `docker compose -f docker-compose.ghcr.yml pull && docker
   compose -f docker-compose.ghcr.yml up -d` manually.)

### Image reference

```
ghcr.io/alanjwade/fcxc-stats:latest     # always the most recent main build
ghcr.io/alanjwade/fcxc-stats:sha-XXXXX  # pinned to a specific commit
```

---

## Old Deployment (rsync + build on server)

### Deploy to homelab00

```bash
./deploy_homelab00.sh
# Then on the server:
ssh homelab@homelab00 'cd /home/homelab/homelab00-config/websites/volumes/sites/fcxc_web && docker compose up -d --build'
```

### Deploy to local homelab path (deploy.sh)

```bash
./deploy.sh deploy    # Copies webapp/, config/, and docker-compose.yml to homelab
./deploy.sh start     # Builds and starts the container
```

### Other deploy.sh commands

```bash
./deploy.sh stop      # Stop the webapp
./deploy.sh restart   # Restart the webapp
./deploy.sh logs      # Tail webapp logs
./deploy.sh backup    # Copy the SQLite database to a timestamped backup file
```

### Configuration

The webapp reads `DATABASE_URL` from its environment (set in the compose file).
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
