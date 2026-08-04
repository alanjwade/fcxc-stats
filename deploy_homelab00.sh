#!/bin/bash

# Cross Country Statistics Tracker - Deploy to homelab00
# Copies project files to homelab@homelab00 via rsync/SSH

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

echo -e "${GREEN}Cross Country Statistics Tracker${NC}"
echo "Deploy to homelab00"
echo "======================================"
echo "Target: ${SSH_TARGET}:${REMOTE_DIR}"
echo ""

# Ensure remote directory structure exists
echo -e "${YELLOW}Ensuring remote directories exist...${NC}"
ssh "${SSH_TARGET}" "mkdir -p '${REMOTE_DIR}/data' '${REMOTE_DIR}/webapp' '${REMOTE_DIR}/config'"

# Copy webapp, config, and docker-compose files
echo -e "${YELLOW}Syncing webapp/...${NC}"
rsync -av --delete -e ssh \
    webapp/ "${SSH_TARGET}:${REMOTE_DIR}/webapp/"

echo -e "${YELLOW}Syncing config/...${NC}"
rsync -av --delete -e ssh \
    config/ "${SSH_TARGET}:${REMOTE_DIR}/config/"

echo -e "${YELLOW}Copying docker-compose.yml...${NC}"
scp docker-compose.yml "${SSH_TARGET}:${REMOTE_DIR}/docker-compose.yml"

# Copy database if it exists
if [ -f "data/fcxc_stats.db" ]; then
    echo -e "${YELLOW}Copying database...${NC}"
    scp data/fcxc_stats.db "${SSH_TARGET}:${REMOTE_DIR}/data/fcxc_stats.db"
    echo -e "${GREEN}Database copied.${NC}"
else
    echo -e "${YELLOW}Warning: No database found at data/fcxc_stats.db — webapp will start with an empty database.${NC}"
fi

echo ""
echo -e "${GREEN}Deploy complete!${NC}"
echo -e "${YELLOW}To start the webapp on homelab00, run:${NC}"
echo "  ssh ${SSH_TARGET} 'cd ${REMOTE_DIR} && docker compose up -d --build'"
