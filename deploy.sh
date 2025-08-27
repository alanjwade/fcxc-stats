#!/bin/bash

# Cross Country Statistics Tracker - Deployment Script
# This script helps with common deployment tasks

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Cross Country Statistics Tracker${NC}"
echo "======================================"

# Check if Docker and Docker Compose are installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Docker Compose is not installed. Please install Docker Compose first.${NC}"
    exit 1
fi

# Function to start the application
start_app() {
    echo -e "${GREEN}Starting Cross Country Stats application...${NC}"
    docker-compose up -d db webapp nginx
    echo -e "${GREEN}Application started! Access it at http://localhost${NC}"
}

# Function to run the scraper
run_scraper() {
    echo -e "${YELLOW}Running the data scraper...${NC}"
    echo "Make sure your config/races.yaml file is properly configured."
    docker-compose run --rm scraper
    echo -e "${GREEN}Scraper completed!${NC}"
}

# Function to stop the application
stop_app() {
    echo -e "${YELLOW}Stopping application...${NC}"
    docker-compose down
    echo -e "${GREEN}Application stopped.${NC}"
}

# Function to view logs
view_logs() {
    echo -e "${YELLOW}Viewing application logs...${NC}"
    docker-compose logs -f webapp
}

# Function to backup database
backup_db() {
    echo -e "${YELLOW}Backing up database...${NC}"
    timestamp=$(date +"%Y%m%d_%H%M%S")
    docker-compose exec db pg_dump -U fcxc_user fcxc_stats > "backup_${timestamp}.sql"
    echo -e "${GREEN}Database backed up to backup_${timestamp}.sql${NC}"
}

# Function to initialize environment
init_env() {
    echo -e "${YELLOW}Initializing environment...${NC}"
    if [ ! -f ".env" ]; then
        cp .env.example .env
        echo -e "${YELLOW}Created .env file. Please edit it with your configuration.${NC}"
    else
        echo -e "${YELLOW}.env file already exists.${NC}"
    fi
    
    if [ ! -f "config/races.yaml" ]; then
        echo -e "${YELLOW}config/races.yaml already exists with example data.${NC}"
        echo -e "${YELLOW}Please edit it with your actual race URLs and information.${NC}"
    fi
}

# Main menu
case ${1:-""} in
    "start")
        start_app
        ;;
    "stop")
        stop_app
        ;;
    "scrape")
        run_scraper
        ;;
    "logs")
        view_logs
        ;;
    "backup")
        backup_db
        ;;
    "init")
        init_env
        ;;
    "restart")
        stop_app
        start_app
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|scrape|logs|backup|init}"
        echo ""
        echo "Commands:"
        echo "  start   - Start the web application"
        echo "  stop    - Stop the web application"
        echo "  restart - Restart the web application"
        echo "  scrape  - Run the data scraper"
        echo "  logs    - View application logs"
        echo "  backup  - Backup the database"
        echo "  init    - Initialize environment files"
        echo ""
        echo "Example workflow:"
        echo "  $0 init     # Set up configuration files"
        echo "  $0 start    # Start the application"
        echo "  $0 scrape   # Import race data"
        echo "  $0 logs     # Monitor the application"
        ;;
esac
