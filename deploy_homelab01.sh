#!/bin/bash

# Cross Country Statistics Tracker - Deploy to homelab01
#
# Deploys using the homelab-infra layout:
#   compose file → hosts/homelab01/fcxc-stats/docker-compose.yml in the infra repo
#   data         → /opt/homelab/fcxc-stats/data/ on the host
#
# Prerequisites on homelab01:
#   - /opt/homelab/fcxc-stats/data/ exists and is owned by the deploy user
#   - .env is present in the service directory (see homelab-deployment/.env.example)
#   - proxy-network Docker network exists
#
# See ADD_SERVICE.md for first-time setup instructions.

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

REMOTE_USER="homelab"
REMOTE_HOST="homelab01"
REMOTE_INFRA_DIR="/home/homelab/homelab-infra/hosts/homelab01/fcxc-stats"
REMOTE_DATA_DIR="/opt/homelab/fcxc-stats/data"
SSH_TARGET="${REMOTE_USER}@${REMOTE_HOST}"

echo -e "${GREEN}Cross Country Statistics Tracker${NC}"
echo "Deploy to homelab01 (homelab-infra layout)"
echo "============================================"
echo "Target: ${SSH_TARGET}"
echo "Service dir: ${REMOTE_INFRA_DIR}"
echo "Data dir:    ${REMOTE_DATA_DIR}"
echo ""

# Ensure remote directories exist
echo -e "${YELLOW}Ensuring remote directories exist...${NC}"
ssh "${SSH_TARGET}" "mkdir -p '${REMOTE_INFRA_DIR}' '${REMOTE_DATA_DIR}'"

# Copy compose file from homelab-deployment/
echo -e "${YELLOW}Copying docker-compose.yml...${NC}"
scp homelab-deployment/docker-compose.yml "${SSH_TARGET}:${REMOTE_INFRA_DIR}/docker-compose.yml"

# Copy database if it exists
if [ -f "data/fcxc_stats.db" ]; then
    echo -e "${YELLOW}Copying database...${NC}"
    scp data/fcxc_stats.db "${SSH_TARGET}:${REMOTE_DATA_DIR}/fcxc_stats.db"
    echo -e "${GREEN}Database copied.${NC}"
else
    echo -e "${YELLOW}Warning: No database found at data/fcxc_stats.db — webapp will start with an empty database.${NC}"
fi

# Verify .env exists on the server
echo -e "${YELLOW}Checking for .env on server...${NC}"
if ! ssh "${SSH_TARGET}" "test -f '${REMOTE_INFRA_DIR}/.env'"; then
    echo -e "${RED}Error: ${REMOTE_INFRA_DIR}/.env not found on server.${NC}"
    echo "Copy homelab-deployment/.env.example to the server, fill in real values, and re-run."
    echo "  scp homelab-deployment/.env.example ${SSH_TARGET}:${REMOTE_INFRA_DIR}/.env"
    exit 1
fi

# Pull latest image and restart
echo -e "${YELLOW}Pulling latest image and restarting...${NC}"
ssh "${SSH_TARGET}" "cd '${REMOTE_INFRA_DIR}' && docker compose pull && docker compose up -d"

echo ""
echo -e "${GREEN}Deploy complete!${NC}"
echo ""
echo -e "${YELLOW}To check status:${NC}"
echo "  ssh ${SSH_TARGET} 'cd ${REMOTE_INFRA_DIR} && docker compose ps'"
echo -e "${YELLOW}To view logs:${NC}"
echo "  ssh ${SSH_TARGET} 'cd ${REMOTE_INFRA_DIR} && docker compose logs -f'"
