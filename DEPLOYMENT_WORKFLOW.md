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
- Docker installed (for image verification)
- curl available (fallback for image verification)

### On homelab01 (`homelab` user)
- fcxc-stats repo cloned at `/home/homelab/fcxc-stats`
- Git configured to pull from GitHub (main branch)
- `.env` file at `/home/homelab/fcxc-stats/homelab-deployment/.env` with required variables
- Docker and Docker Compose installed
- SSH server listening and key-based auth working

## Deployment Workflow

### Step 1: Bump Version (Local)

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
- Pushes to origin/main

**Output:**
```
✓ Repository is clean
Current version: v1.0.0
New version will be: v1.0.1
✓ File updated
✓ Changes committed
✓ Changes pushed to remote
```

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
- Verifies image exists in GHCR (fails if not found)
- Connects to homelab01 via SSH
- Verifies fcxc-stats repo exists at `/home/homelab/fcxc-stats`
- Runs: `git fetch origin && git reset --hard origin/main` (pulls latest changes)
- Verifies docker-compose.yml exists locally after git pull
- Runs: `docker compose pull && docker compose up -d` in `homelab-deployment/` directory
- Shows container status

**Output:**
```
Image to deploy: ghcr.io/alanjwade/fcxc-stats:v1.0.1
✓ Image found in GHCR (via docker manifest)
✓ SSH connection established
✓ Remote repository found
✓ Git repository updated
✓ docker-compose.yml found
✓ Container restarted successfully

CONTAINER ID   IMAGE                                    COMMAND             CREATED         STATUS         PORTS        NAMES
abc123def456   ghcr.io/alanjwade/fcxc-stats:v1.0.1    "python app.py"    2 seconds ago   Up 1 second    5000/tcp     fcxc-stats
```

## Typical Complete Workflow

```bash
# 1. Bump version locally
./bump_version_homelab01.sh
# Output: Version bumped from v1.0.0 to v1.0.1, committed and pushed

# 2. Wait for GitHub Actions (~5-15 min)
# Watch: https://github.com/alanjwade/fcxc-stats/actions

# 3. Deploy to homelab01
./deploy_homelab01.sh
# Output: Image verified, repo updated, container restarted with v1.0.1
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
- Verify the commit with the tag bump was pushed: `git log --oneline | head`
- Wait for the workflow to complete and try again: `./deploy_homelab01.sh`

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
# On homelab01
ssh homelab@homelab01 "cd /home/homelab/fcxc-stats && git log --oneline | head -20"
# Find the commit with the old version tag

# Option 1: Reset to previous commit
ssh homelab@homelab01 "cd /home/homelab/fcxc-stats && git reset --hard <commit-hash>"
ssh homelab@homelab01 "cd /home/homelab/fcxc-stats/homelab-deployment && docker compose pull && docker compose up -d"

# Option 2: Manually update docker-compose.yml
# Edit homelab-deployment/docker-compose.yml to use the old image tag, commit, push, then run ./deploy_homelab01.sh
```

## Notes

- The version tag in `homelab-deployment/docker-compose.yml` is the single source of truth for deployments
- Both scripts check prerequisites before making changes
- `bump_version_homelab01.sh` exits with status 1 on any error
- `deploy_homelab01.sh` requires GitHub Actions to have completed the build before deployment
- Use SSH key-based auth; password prompts will block deployment
