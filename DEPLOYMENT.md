# Cross Country Statistics Tracker — Deployment Guide

## Overview

- **Scraper**: Runs standalone on your dev machine using a Python virtual environment
- **Webapp**: Docker container deployed to homelab01 via homelab-infra
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

## Production Deployment (homelab01)

The image is built by GitHub Actions on push to `main` and published to GHCR.
The `homelab-deployment/` folder contains the three files that belong in
`~/homelab-infra/hosts/homelab01/fcxc-stats/`.

### Step 1 — Build and push the Docker image

**Option A: Push a Git tag (recommended for production releases)**

```bash
git tag v1.0.0
git push origin v1.0.0
```

GitHub Actions will build and push the image as `ghcr.io/alanjwade/fcxc-stats:v1.0.0`.

**Option B: Push to main (automatic, uses SHA tags)**

```bash
git commit -m "Your changes"
git push origin main
```

GitHub Actions will build and push with tags:
- `latest` (for the latest main commit)
- `sha-abc123d` (for the specific commit)

**Then update `homelab-deployment/docker-compose.yml`:**

```yaml
image: ghcr.io/alanjwade/fcxc-stats:v1.0.0  # use the tag from Step 1
```

Check [Packages](https://github.com/alanjwade/fcxc-stats/pkgs/container/fcxc-stats) to see available tags.

### Step 2 — Copy files to homelab-infra (one-time)

```bash
scp -r homelab-deployment/ homelab@homelab01:~/homelab-infra/hosts/homelab01/fcxc-stats
```

For subsequent updates, re-copy only the compose file after updating the tag:

```bash
scp homelab-deployment/docker-compose.yml homelab@homelab01:~/homelab-infra/hosts/homelab01/fcxc-stats/
```

### Step 3 — First-time server setup (one-time)

On homelab01:

```bash
mkdir -p /opt/homelab/fcxc-stats/data

cd ~/homelab-infra/hosts/homelab01/fcxc-stats
cp .env.example .env

# If the GHCR package is private, authenticate with a PAT (read:packages scope):
echo YOUR_PAT | docker login ghcr.io -u alanjwade --password-stdin
```

### Step 4 — Deploy

```bash
cd ~/homelab-infra/hosts/homelab01/fcxc-stats
docker compose up -d
docker compose logs -f
```

### Step 5 — Seed the database (first time only)

Run the scraper locally, then copy the database to the server:

```bash
scp data/fcxc_stats.db homelab@homelab01:/opt/homelab/fcxc-stats/data/
```

### Updating

To push a new version to production:

1. Commit your changes and push to `main` (or work in a branch)
2. Tag the release:
   ```bash
   git tag v1.0.1
   git push origin v1.0.1
   ```
3. Wait for [Actions](https://github.com/alanjwade/fcxc-stats/actions) to build
4. Update `homelab-deployment/docker-compose.yml` with the new tag (e.g., `v1.0.1`)
5. Deploy:
   ```bash
   scp homelab-deployment/docker-compose.yml homelab@homelab01:~/homelab-infra/hosts/homelab01/fcxc-stats/
   ssh homelab@homelab01 'cd ~/homelab-infra/hosts/homelab01/fcxc-stats && docker compose pull && docker compose up -d'
   ```

### Backup

The database at `/opt/homelab/fcxc-stats/data` is backed up per
`homelab-deployment/backup.yml`. No manual steps needed if homelab-infra's
backup system is configured.

---

## Troubleshooting

**Webapp not starting:**
```bash
ssh homelab@homelab01 'cd ~/homelab-infra/hosts/homelab01/fcxc-stats && docker compose logs fcxc-stats'
```

**Database empty after deploy:**
Run the scraper locally, then copy the database to the server (see Step 5 above).

**Scraper import errors:**
Ensure the venv is active: `source scraper/.venv/bin/activate`
