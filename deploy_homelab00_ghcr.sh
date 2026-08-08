#!/bin/bash

# Cross Country Statistics Tracker - Deploy to homelab00 using GHCR image
#
# This script deploys using the pre-built container image from GitHub Container
# Registry instead of syncing source code and building on the server.
#
# The image must already exist in GHCR (built by GitHub Actions on push to main).
# See DEPLOYMENT.md for setup instructions.

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

REMOTE_USER="homelab"
REMOTE_HOST="homelab00"
REMOTE_DIR="/home/homelab/homelab00-config/websites/volumes/sites/fcxc_web"
SSH_TARGET="${REMOTE_USER}@${REMOTE_HOST}"
COMPOSE_FILE="docker-compose.ghcr.yml"

echo -e "${GREEN}Cross Country Statistics Tracker${NC}"
echo "Deploy (GHCR image) to homelab00"
echo "======================================"
echo "Target: ${SSH_TARGET}:${REMOTE_DIR}"
echo ""

# Ensure remote directory structure exists
echo -e "${YELLOW}Ensuring remote directories exist...${NC}"
ssh "${SSH_TARGET}" "mkdir -p '${REMOTE_DIR}/data'"

# Copy docker-compose.ghcr.yml to the server
echo -e "${YELLOW}Copying ${COMPOSE_FILE}...${NC}"
scp "${COMPOSE_FILE}" "${SSH_TARGET}:${REMOTE_DIR}/docker-compose.ghcr.yml"

# Copy database if it exists
if [ -f "data/fcxc_stats.db" ]; then
    echo -e "${YELLOW}Copying database...${NC}"
    scp data/fcxc_stats.db "${SSH_TARGET}:${REMOTE_DIR}/data/fcxc_stats.db"
    echo -e "${GREEN}Database copied.${NC}"
else
    echo -e "${YELLOW}Warning: No database found at data/fcxc_stats.db — webapp will start with an empty database.${NC}"
fi

# Pull latest image and restart on the server
echo -e "${YELLOW}Pulling latest image and restarting on server...${NC}"
ssh "${SSH_TARGET}" "cd '${REMOTE_DIR}' && docker compose -f docker-compose.ghcr.yml pull && docker compose -f docker-compose.ghcr.yml up -d"

echo ""
echo -e "${GREEN}Deploy complete!${NC}"
echo "Image: ghcr.io/alanjwade/fcxc-stats:latest"
echo ""
echo -e "${YELLOW}To check status:${NC}"
echo "  ssh ${SSH_TARGET} 'cd ${REMOTE_DIR} && docker compose -f docker-compose.ghcr.yml ps'"
echo -e "${YELLOW}To view logs:${NC}"
echo "  ssh ${SSH_TARGET} 'cd ${REMOTE_DIR} && docker compose -f docker-compose.ghcr.yml logs -f webapp'"
