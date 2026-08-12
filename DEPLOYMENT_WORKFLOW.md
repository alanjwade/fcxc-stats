# Homelab01 Deployment Workflow

This document describes the two-script deployment workflow for deploying fcxc-stats to homelab01.

## Overview

The deployment is split into two scripts to separate concerns:

1. **`bump_version_homelab01.sh`** — Version management (local)
   - Checks that the working directory is clean
   - Bumps the version tag in `homelab-deployment/docker-compose.yml`
   - Commits and pushes the change to git
   - Triggers GitHub Actions to build and push the new image to GHCR

2. **`deploy_homelab01.sh`** — Deployment (remote)
   - Verifies the image exists in GHCR
   - SSHes to homelab01
   - Pulls the latest repo state (including the updated docker-compose.yml with new tag)
   - Restarts the container with the new image

## Prerequisites

### Local Setup
- Working git repository with no uncommitted changes
- SSH key configured for passwordless SSH to homelab01
- Docker installed (optional, for image verification)
- curl available (fallback for image verification)

### On homelab01 (`homelab` user)
- homelab-infra repo cloned at `/home/homelab/homelab-infra`
- `/opt/homelab/fcxc-stats/data/` directory exists with database
- `.env` file at `/home/homelab/homelab-infra/hosts/homelab01/fcxc-stats/.env` with required variables
- Docker and Docker Compose installed
- proxy-network Docker network created
- SSH key-based auth working

**Note:** homelab01 uses the homelab-infra layout (not a direct fcxc-stats clone). Service configs are in `homelab-infra/hosts/homelab01/fcxc-stats/`, data in `/opt/homelab/fcxc-stats/data/`.

## Deployment Workflow

### Step 1: Bump Version & Create Tag (Local)

```bash
./bump_version_homelab01.sh
```

**Behavior:**
- Checks for uncommitted changes (fails if any exist)
- Reads current version from `homelab-deployment/docker-compose.yml`
- Bumps patch version by default (v1.0.0 → v1.0.1)
  - Optionally: `./bump_version_homelab01.sh minor` (v1.0.0 → v1.1.0)
  - Optionally: `./bump_version_homelab01.sh major` (v1.0.0 → v2.0.0)
- Updates `homelab-deployment/docker-compose.yml` with new tag
- Commits: `Bump fcxc-stats version to v1.0.1 for homelab01 deployment`
- Pushes commit to origin/main
- **Creates git tag** `v1.0.1` and pushes it to origin (⚡ **triggers GitHub Actions build**)

**Output:**
```
✓ Repository is clean
Current version: v1.0.0
New version will be: v1.0.1
✓ File updated
✓ Changes committed
✓ Commit pushed to remote
✓ Tag created locally
✓ Tag pushed to remote

GitHub Actions will now build and push: ghcr.io/alanjwade/fcxc-stats:v1.0.1
```

**Why tags?** The GitHub Actions workflow is configured to build Docker images when:
1. A git tag matching `v*` is pushed, OR
2. Files in `webapp/**` or `config/**` change

Using tags ensures we build exactly when intended, and the tag name becomes the image tag (e.g., `v1.0.1`).

**Next:** Wait for GitHub Actions to build and push the image to GHCR (~5-15 minutes)

### Step 2: Monitor GitHub Actions (Optional)

Check that the GitHub Actions workflow has completed:
- Go to https://github.com/alanjwade/fcxc-stats/actions
- Wait for the build to complete
- Verify image is available at `ghcr.io/alanjwade/fcxc-stats:v1.0.1`

### Step 3: Deploy to homelab01 (Remote)

```bash
./deploy_homelab01.sh
```

**Behavior:**
- Extracts version tag from local `homelab-deployment/docker-compose.yml`
- Connects to homelab01 via SSH
- Verifies homelab-infra repo exists at `/home/homelab/homelab-infra`
- Ensures required directories exist: `/home/homelab/homelab-infra/hosts/homelab01/fcxc-stats/` and `/opt/homelab/fcxc-stats/data/`
- Copies updated `docker-compose.yml` to homelab-infra service directory
- Copies database (if it exists locally) to `/opt/homelab/fcxc-stats/data/`
- Verifies `.env` file exists on remote
- Runs: `docker compose pull && docker compose up -d` to pull new image and restart container
- Shows container status

**Output:**
```
Image to deploy: ghcr.io/alanjwade/fcxc-stats:v1.0.1
✓ SSH connection established
✓ homelab-infra repository found
✓ Directories created/verified
✓ docker-compose.yml copied
✓ Database copied
✓ .env file found
✓ Container restarted successfully

CONTAINER ID   IMAGE                                    COMMAND             CREATED         STATUS         PORTS        NAMES
abc123def456   ghcr.io/alanjwade/fcxc-stats:v1.0.1    "python app.py"    2 seconds ago   Up 1 second    5000/tcp     fcxc-stats

Deployment complete!
The fcxc-stats container is now running with image ghcr.io/alanjwade/fcxc-stats:v1.0.1
```

## Typical Complete Workflow

```bash
# 1. Bump version locally and create git tag
./bump_version_homelab01.sh
# Output: Version bumped from v1.0.0 to v1.0.1
#         Commit pushed to main
#         Git tag v1.0.1 pushed (⚡ triggers GitHub Actions)

# 2. Wait for GitHub Actions (~5-15 min)
# Watch: https://github.com/alanjwade/fcxc-stats/actions
# Wait for the workflow to complete and image to be available at:
#   ghcr.io/alanjwade/fcxc-stats:v1.0.1

# 3. Deploy to homelab01
./deploy_homelab01.sh
# Output: Image verified in GHCR
#         Repo updated on homelab01
#         Container restarted with v1.0.1
```

## Troubleshooting

### "Error: Uncommitted changes detected"
The working directory has uncommitted changes. Commit or stash them:
```bash
git status                    # See what changed
git add .                     # Stage changes
git commit -m "..."          # Commit
./bump_version_homelab01.sh   # Try again
```

### "Error: Image not found in GHCR"
The GitHub Actions workflow hasn't finished building the image yet. Check:
- GitHub Actions status: https://github.com/alanjwade/fcxc-stats/actions
- Verify the git tag was pushed: `git tag -l | tail`
- Check the workflow logs to see if there's an error
- Wait for the workflow to complete and try again: `./deploy_homelab01.sh`

If the tag exists but the workflow didn't trigger, try pushing manually:
```bash
git push origin v1.0.1  # Re-push the tag (replace v1.0.1 with your version)
```

### "Error: homelab-infra Git repository not found"
The homelab-infra repo isn't cloned on homelab01. First-time setup:
```bash
ssh homelab@homelab01 "git clone https://github.com/alanjwade/homelab-infra.git /home/homelab/homelab-infra"
mkdir -p /opt/homelab/fcxc-stats/data
# Then copy .env.example to .env and fill in values
scp homelab-deployment/.env.example homelab@homelab01:/home/homelab/homelab-infra/hosts/homelab01/fcxc-stats/.env
```

### "Error: Cannot connect to homelab01 via SSH"
SSH is not working or the connection timed out:
```bash
ssh homelab@homelab01 "echo OK"   # Test SSH manually
# If this fails, check:
# - SSH key is configured
# - homelab01 is reachable on the network
# - SSH port is open
```

### "Error: Git repository not found at /home/homelab/fcxc-stats"
The fcxc-stats repo isn't cloned on homelab01. See ADD_SERVICE.md for setup instructions.

### Container fails to start after deploy
Check logs on homelab01:
```bash
ssh homelab@homelab01 "cd /home/homelab/fcxc-stats/homelab-deployment && docker compose logs -f"
```

## Rollback

To rollback to a previous version:

```bash
# View available versions in homelab-deployment/docker-compose.yml
git log --oneline homelab-deployment/docker-compose.yml | head -10

# Find the commit with the desired version
git show <commit-hash>:homelab-deployment/docker-compose.yml | grep image:

# Either:
# Option 1: Manually edit homelab-deployment/docker-compose.yml, commit, and redeploy
git checkout <commit-hash> -- homelab-deployment/docker-compose.yml
git commit -m "Rollback to version v1.0.0"
git push origin main
./deploy_homelab01.sh

# Option 2: On homelab01, directly restart with old image
ssh homelab@homelab01 "cd /home/homelab/homelab-infra/hosts/homelab01/fcxc-stats && \
  docker compose pull && docker compose up -d"
```

## Notes

- The version tag in `homelab-deployment/docker-compose.yml` is the single source of truth for deployments
- **Git tags trigger GitHub Actions:** When `bump_version_homelab01.sh` creates and pushes a git tag (e.g., `v1.0.1`), it triggers the Docker build workflow
- The GitHub Actions workflow builds the image with tag matching the git tag name (e.g., `ghcr.io/alanjwade/fcxc-stats:v1.0.1`)
- homelab01 uses **homelab-infra layout**: service configs in homelab-infra repo, persistent data in `/opt/homelab/`
- Both scripts check prerequisites before making changes
- `bump_version_homelab01.sh` exits with status 1 on any error
- `deploy_homelab01.sh` copies files over SCP (no git pull) for simplicity
- Use SSH key-based auth; password prompts will block deployment
