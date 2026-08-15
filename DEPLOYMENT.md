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

**Important:** the workflow now also sends a repository-dispatch event to the `homelab-infra` repository so the compose file can be updated automatically.

**Option B: Push to main (automatic, uses SHA tags)**

```bash
git commit -m "Your changes"
git push origin main
```

GitHub Actions will build and push with tags:
- `latest` (for the latest main commit)
- `sha-abc123d` (for the specific commit)

This is useful for testing, but the production release flow is still the tagged version.

### Step 2 — Set the secret for the cross-repo trigger

Create a GitHub Actions secret in this repository named `HOMELAB_INFRA_REPO_TOKEN`.

It should be a PAT with access to `alanjwade/homelab-infra` and at least `contents:write` permission. This allows the app repo workflow to trigger the infra repo automation.

### Step 3 — Infra repo automation updates the compose file

In `homelab-infra`, add a workflow that listens for the `fcxc-stats-release` repository dispatch event and updates the image tag in the service compose file.

Example workflow:

```yaml
name: Update fcxc-stats image

on:
  repository_dispatch:
    types: [fcxc-stats-release]

jobs:
  update-compose:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Update image tag
        run: |
          sed -i "s|ghcr.io/alanjwade/fcxc-stats:.*|ghcr.io/alanjwade/fcxc-stats:${{ github.event.client_payload.tag }}|" \
            hosts/homelab01/fcxc-stats/docker-compose.yml

      - name: Commit and push
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add hosts/homelab01/fcxc-stats/docker-compose.yml
          git commit -m "Update fcxc-stats to ${{ github.event.client_payload.tag }}" || exit 0
          git push
```

This keeps the tag update in the infra repo fully automated.

### Step 4 — Server deployment

On homelab01, pull the latest infra repo and redeploy:

```bash
cd ~/homelab-infra
git pull --ff-only
cd hosts/homelab01/fcxc-stats
docker compose pull
docker compose up -d
```

### Step 5 — First-time server setup (one-time)

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

### Step 6 — Seed the database (first time only)

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
3. Wait for the GitHub Action in this repo to build and push the image
4. The workflow triggers the `homelab-infra` update automatically
5. The infra repo updates the compose tag and pushes the change
6. On the server, pull and redeploy:
   ```bash
   cd ~/homelab-infra
   git pull --ff-only
   cd hosts/homelab01/fcxc-stats
   docker compose pull
   docker compose up -d
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
