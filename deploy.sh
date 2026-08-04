#!/bin/bash

# Cross Country Statistics Tracker - Deployment Script
# Deploys the webapp Docker container to the homelab

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

DEPLOY_DIR="/home/alan/homelab/fcxc-stats"

echo -e "${GREEN}Cross Country Statistics Tracker${NC}"
echo "======================================"

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi

# Function to deploy to homelab
deploy() {
    echo -e "${YELLOW}Deploying to ${DEPLOY_DIR}...${NC}"

    # Create deploy directory if it doesn't exist
    mkdir -p "${DEPLOY_DIR}/data"

    # Copy webapp, config, and docker-compose files
    rsync -av --delete \
        webapp/ "${DEPLOY_DIR}/webapp/"
    rsync -av --delete \
        config/ "${DEPLOY_DIR}/config/"
    cp docker-compose.yml "${DEPLOY_DIR}/docker-compose.yml"

    # Copy database if it exists
    if [ -f "data/fcxc_stats.db" ]; then
        cp data/fcxc_stats.db "${DEPLOY_DIR}/data/fcxc_stats.db"
        echo -e "${GREEN}Database copied to ${DEPLOY_DIR}/data/${NC}"
    else
        echo -e "${YELLOW}Warning: No database found at data/fcxc_stats.db — webapp will start with an empty database.${NC}"
    fi

    echo -e "${GREEN}Files deployed to ${DEPLOY_DIR}${NC}"
    echo -e "${YELLOW}To start the webapp, run:${NC}"
    echo -e "  cd ${DEPLOY_DIR} && docker compose up -d --build"
}

# Function to start the application (from deploy dir)
start_app() {
    echo -e "${GREEN}Starting Cross Country Stats application...${NC}"
    cd "${DEPLOY_DIR}"
    docker compose up -d --build
    echo -e "${GREEN}Application started!${NC}"
}

# Function to stop the application
stop_app() {
    echo -e "${YELLOW}Stopping application...${NC}"
    cd "${DEPLOY_DIR}"
    docker compose down
    echo -e "${GREEN}Application stopped.${NC}"
}

# Function to view logs
view_logs() {
    echo -e "${YELLOW}Viewing application logs...${NC}"
    cd "${DEPLOY_DIR}"
    docker compose logs -f webapp
}

# Function to backup database
backup_db() {
    echo -e "${YELLOW}Backing up database...${NC}"
    timestamp=$(date +"%Y%m%d_%H%M%S")
    cp "${DEPLOY_DIR}/data/fcxc_stats.db" "backup_${timestamp}.db"
    echo -e "${GREEN}Database backed up to backup_${timestamp}.db${NC}"
}

# Main menu
case ${1:-""} in
    "deploy")
        deploy
        ;;
    "start")
        start_app
        ;;
    "stop")
        stop_app
        ;;
    "logs")
        view_logs
        ;;
    "backup")
        backup_db
        ;;
    "restart")
        stop_app
        start_app
        ;;
    *)
        echo "Usage: $0 {deploy|start|stop|restart|logs|backup}"
        echo ""
        echo "Commands:"
        echo "  deploy  - Deploy webapp files to ${DEPLOY_DIR}"
        echo "  start   - Build and start the webapp container"
        echo "  stop    - Stop the webapp container"
        echo "  restart - Restart the webapp container"
        echo "  logs    - View webapp logs"
        echo "  backup  - Backup the SQLite database"
        echo ""
        echo "Example workflow:"
        echo "  $0 deploy   # Copy files to homelab"
        echo "  $0 start    # Start the webapp"
        echo "  $0 logs     # Monitor the application"
        ;;
esac
