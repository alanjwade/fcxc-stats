#!/bin/bash

# Cross Country Statistics Tracker - Deploy to homelab01
#
# Verifies that the image exists in GHCR, then deploys to homelab01 using the homelab-infra layout:
#   - Copies docker-compose.yml to hosts/homelab01/fcxc-stats/ in homelab-infra repo
#   - Ensures /opt/homelab/fcxc-stats/data/ exists on the host
#   - Restarts the container with the new image
#
# Prerequisites on homelab01:
#   - SSH key setup to homelab01 (no password required)
#   - homelab-infra repo is cloned at /home/homelab/homelab-infra
#   - /opt/homelab/fcxc-stats/data/ directory exists and is accessible
#   - .env file exists at /home/homelab/homelab-infra/hosts/homelab01/fcxc-stats/.env
#   - proxy-network Docker network exists
#   - Docker and docker compose are installed
#
# Usage:
#   ./deploy_homelab01.sh
#
# Workflow:
#   1. Run bump_version_homelab01.sh to increment the version tag and commit
#   2. Wait for GitHub Actions to build and push the new image to GHCR
#   3. Run this script to deploy the new version to homelab01
#
# See ADD_SERVICE.md for homelab01 setup instructions.

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

COMPOSE_FILE="homelab-deployment/docker-compose.yml"
REMOTE_USER="homelab"
REMOTE_HOST="homelab01"
REMOTE_INFRA_DIR="/home/homelab/homelab-infra/hosts/homelab01/fcxc-stats"
REMOTE_DATA_DIR="/opt/homelab/fcxc-stats/data"
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
echo ""

# Verify SSH connection
echo -e "${YELLOW}Verifying SSH connection to ${SSH_TARGET}...${NC}"
if ! ssh -o ConnectTimeout=5 "${SSH_TARGET}" "echo 'SSH connection OK'" &> /dev/null; then
    echo -e "${RED}Error: Cannot connect to ${SSH_TARGET} via SSH.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ SSH connection established${NC}"
echo ""

# Check if homelab-infra repo exists on remote
echo -e "${YELLOW}Checking homelab-infra repository at /home/homelab/homelab-infra...${NC}"
if ! ssh "${SSH_TARGET}" "test -d '/home/homelab/homelab-infra/.git'"; then
    echo -e "${RED}Error: homelab-infra Git repository not found on ${SSH_TARGET}.${NC}"
    echo "Please clone homelab-infra first. See ADD_SERVICE.md for setup instructions."
    exit 1
fi
echo -e "${GREEN}✓ homelab-infra repository found${NC}"
echo ""

# Ensure remote directories exist
echo -e "${YELLOW}Ensuring remote directories exist...${NC}"
ssh "${SSH_TARGET}" "mkdir -p '${REMOTE_INFRA_DIR}' '${REMOTE_DATA_DIR}'" || true
echo -e "${GREEN}✓ Directories created/verified${NC}"
echo ""

# Copy compose file from homelab-deployment/
echo -e "${YELLOW}Copying docker-compose.yml to homelab-infra...${NC}"
scp "${COMPOSE_FILE}" "${SSH_TARGET}:${REMOTE_INFRA_DIR}/docker-compose.yml"
echo -e "${GREEN}✓ docker-compose.yml copied${NC}"
echo ""

# Copy database if it exists
echo -e "${YELLOW}Checking for database...${NC}"
if [ -f "data/fcxc_stats.db" ]; then
    echo -e "${YELLOW}Copying database to remote...${NC}"
    scp data/fcxc_stats.db "${SSH_TARGET}:${REMOTE_DATA_DIR}/fcxc_stats.db"
    echo -e "${GREEN}✓ Database copied${NC}"
else
    echo -e "${YELLOW}⚠ No database found locally (app will start with empty DB)${NC}"
fi
echo ""

# Verify .env exists on the server
echo -e "${YELLOW}Checking for .env on server...${NC}"
if ! ssh "${SSH_TARGET}" "test -f '${REMOTE_INFRA_DIR}/.env'"; then
    echo -e "${RED}Error: ${REMOTE_INFRA_DIR}/.env not found on server.${NC}"
    echo "Copy homelab-deployment/.env.example to the server, fill in real values, and re-run:"
    echo "  scp homelab-deployment/.env.example ${SSH_TARGET}:${REMOTE_INFRA_DIR}/.env"
    exit 1
fi
echo -e "${GREEN}✓ .env file found${NC}"
echo ""

# Pull latest image and restart
echo -e "${YELLOW}Pulling latest image and restarting container...${NC}"
echo "Running: cd ${REMOTE_INFRA_DIR} && docker compose pull && docker compose up -d"
echo ""

if ssh "${SSH_TARGET}" "cd '${REMOTE_INFRA_DIR}' && docker compose pull && docker compose up -d"; then
    echo ""
    echo -e "${GREEN}✓ Container restarted successfully${NC}"
else
    echo -e "${RED}Error: Failed to restart container on remote.${NC}"
    exit 1
fi

echo ""

# Show container status
echo -e "${YELLOW}Container status:${NC}"
ssh "${SSH_TARGET}" "cd '${REMOTE_INFRA_DIR}' && docker compose ps"

echo ""
echo -e "${GREEN}Deployment complete!${NC}"
echo "The fcxc-stats container is now running with image ${BLUE}${IMAGE_TAG}${NC}"
echo ""
echo -e "${YELLOW}To view logs:${NC}"
echo "  ssh ${SSH_TARGET} 'cd ${REMOTE_INFRA_DIR} && docker compose logs -f'"
