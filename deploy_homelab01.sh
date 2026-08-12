#!/bin/bash

# Cross Country Statistics Tracker - Deploy to homelab01
#
# Verifies that the image exists in GHCR, then deploys to homelab01 by:
#   1. SSHing to homelab01
#   2. Pulling the latest fcxc-stats repo (to get updated docker-compose.yml)
#   3. Restarting the container with the new image and config
#
# Prerequisites on homelab01:
#   - SSH key setup to homelab01 (no password required)
#   - fcxc-stats repo is cloned at /home/homelab/fcxc-stats
#   - .env file exists at /home/homelab/fcxc-stats/homelab-deployment/.env
#   - Docker is running and docker compose is available
#
# Usage:
#   ./deploy_homelab01.sh
#
# Workflow:
#   1. Run bump_version_homelab01.sh to increment the version tag and commit
#   2. Wait for GitHub Actions to build and push the new image to GHCR
#   3. Run this script to deploy the new version to homelab01

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

COMPOSE_FILE="homelab-deployment/docker-compose.yml"
REMOTE_USER="homelab"
REMOTE_HOST="homelab01"
REMOTE_REPO_DIR="/home/homelab/fcxc-stats"
REMOTE_COMPOSE_DIR="${REMOTE_REPO_DIR}/homelab-deployment"
SSH_TARGET="${REMOTE_USER}@${REMOTE_HOST}"

echo -e "${GREEN}Cross Country Statistics Tracker - Deploy to homelab01${NC}"
echo "========================================================"
echo ""

# Extract image tag from docker-compose.yml
echo -e "${YELLOW}Reading image tag from ${COMPOSE_FILE}...${NC}"
if [ ! -f "$COMPOSE_FILE" ]; then
    echo -e "${RED}Error: ${COMPOSE_FILE} not found.${NC}"
    exit 1
fi

IMAGE_TAG=$(grep -m1 'image: ghcr.io/alanjwade/fcxc-stats:' "$COMPOSE_FILE" | sed 's/.*image: //' | awk '{print $1}')
if [ -z "$IMAGE_TAG" ]; then
    echo -e "${RED}Error: Could not extract image tag from ${COMPOSE_FILE}.${NC}"
    exit 1
fi

echo "Image to deploy: ${BLUE}${IMAGE_TAG}${NC}"
echo ""

# Check if image exists in GHCR
echo -e "${YELLOW}Checking if image exists in GHCR...${NC}"

IMAGE_EXISTS=false

# Try docker manifest inspect first (modern Docker)
if command -v docker &> /dev/null; then
    if docker manifest inspect "$IMAGE_TAG" &> /dev/null; then
        IMAGE_EXISTS=true
        echo -e "${GREEN}✓ Image found in GHCR (via docker manifest)${NC}"
    fi
fi

# Fallback to curl if docker manifest didn't work
if [ "$IMAGE_EXISTS" = false ]; then
    # Extract tag name for API call
    TAG_NAME=$(echo "$IMAGE_TAG" | sed 's/.*://')
    if curl -s -f "https://ghcr.io/v2/alanjwade/fcxc-stats/manifests/${TAG_NAME}" -H "Accept: application/vnd.oci.image.manifest.v1+json" &> /dev/null; then
        IMAGE_EXISTS=true
        echo -e "${GREEN}✓ Image found in GHCR (via API)${NC}"
    fi
fi

if [ "$IMAGE_EXISTS" = false ]; then
    echo -e "${RED}Error: Image ${IMAGE_TAG} not found in GHCR.${NC}"
    echo "Make sure you've run './bump_version_homelab01.sh' and the GitHub Actions workflow has completed."
    exit 1
fi

echo ""

# Verify SSH connection
echo -e "${YELLOW}Verifying SSH connection to ${SSH_TARGET}...${NC}"
if ! ssh -o ConnectTimeout=5 "${SSH_TARGET}" "echo 'SSH connection OK'" &> /dev/null; then
    echo -e "${RED}Error: Cannot connect to ${SSH_TARGET} via SSH.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ SSH connection established${NC}"
echo ""

# Check if repo exists on remote
echo -e "${YELLOW}Checking remote repository at ${REMOTE_REPO_DIR}...${NC}"
if ! ssh "${SSH_TARGET}" "test -d '${REMOTE_REPO_DIR}/.git'"; then
    echo -e "${RED}Error: Git repository not found at ${REMOTE_REPO_DIR} on ${SSH_TARGET}.${NC}"
    echo "Please initialize the repository first. See ADD_SERVICE.md for setup instructions."
    exit 1
fi
echo -e "${GREEN}✓ Remote repository found${NC}"
echo ""

# Pull latest changes from git
echo -e "${YELLOW}Pulling latest changes from git...${NC}"
if ssh "${SSH_TARGET}" "cd '${REMOTE_REPO_DIR}' && git fetch origin && git reset --hard origin/main"; then
    echo -e "${GREEN}✓ Git repository updated${NC}"
else
    echo -e "${RED}Error: Failed to update git repository on remote.${NC}"
    exit 1
fi
echo ""

# Verify compose file exists on remote after git pull
echo -e "${YELLOW}Verifying docker-compose.yml on remote...${NC}"
if ! ssh "${SSH_TARGET}" "test -f '${REMOTE_COMPOSE_DIR}/docker-compose.yml'"; then
    echo -e "${RED}Error: docker-compose.yml not found at ${REMOTE_COMPOSE_DIR} after git pull.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ docker-compose.yml found${NC}"
echo ""

# Pull and restart the container
echo -e "${YELLOW}Pulling new image and restarting container...${NC}"
echo "Running: cd ${REMOTE_COMPOSE_DIR} && docker compose pull && docker compose up -d"
echo ""

if ssh "${SSH_TARGET}" "cd '${REMOTE_COMPOSE_DIR}' && docker compose pull && docker compose up -d"; then
    echo ""
    echo -e "${GREEN}✓ Container restarted successfully${NC}"
else
    echo -e "${RED}Error: Failed to restart container on remote.${NC}"
    exit 1
fi

echo ""

# Show container status
echo -e "${YELLOW}Container status:${NC}"
ssh "${SSH_TARGET}" "cd '${REMOTE_COMPOSE_DIR}' && docker compose ps"

echo ""
echo -e "${GREEN}Deployment complete!${NC}"
echo "The fcxc-stats container is now running with image ${BLUE}${IMAGE_TAG}${NC}"
