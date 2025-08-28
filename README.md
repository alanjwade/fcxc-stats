# Cross Country Statistics Tracker

A containerized application for tracking cross country team statistics across multiple seasons with web scraping capabilities from MileSplit.com.

## Features

- **Data Collection**: Semi-automated scraping of race results from co.milesplit.com
- **Database Storage**: PostgreSQL database for storing athlete and meet information
- **CSV Export**: Generate comprehensive athlete performance reports
- **Team Statistics**: View best times by gender and overall team performance
- **Athlete Profiles**: Individual athlete statistics including PRs and varsity points
- **Containerized Deployment**: Docker-based setup for easy home server deployment

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Scraper  │───▶│   PostgreSQL    │◀──▶│  Web Dashboard  │
│   (Offline)     │    │   Database      │    │   (Flask App)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                       ▲
                                │                       │
                                └───────────────────────┤
                                                        │
                         ┌─────────────────────────────────┐
                         │        Nginx Reverse Proxy      │
                         │        (Port 80/443)            │
                         └─────────────────────────────────┘
```

## Quick Start

1. Initialize the environment: `./deploy.sh init`
2. Configure your race data in `config/races.yaml`
3. Edit `.env` with your settings
4. Start the application: `./deploy.sh start`
5. Import race data: `./deploy.sh scrape`
6. Access the dashboard at `http://localhost`

## Project Structure

```
fcxc_stats/
├── scraper/          # Data scraping module
│   ├── Dockerfile
│   ├── requirements.txt
│   └── scraper.py    # Main scraping logic
├── webapp/           # Flask web application
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── app.py        # Main Flask application
│   └── templates/    # HTML templates
├── database/         # Database schema
│   └── init.sql      # PostgreSQL schema
├── nginx/            # Reverse proxy configuration
│   ├── nginx.conf
│   └── default.conf
├── config/           # Configuration files
│   └── races.yaml    # Race definitions
├── docker-compose.yml
├── deploy.sh         # Deployment script
└── DEPLOYMENT.md     # Detailed deployment guide
```

## ✅ **Scraper Usage**

The MileSplit scraper now includes intelligent duplicate prevention and flexible operation modes:

### Basic Scraping (Prevents Duplicates)
```bash
# Standard run - skips existing data, adds only new results
docker-compose --profile scraper run --rm scraper
```

### Full Database Refresh
```bash
# Clear database and reload all data
docker-compose --profile scraper run --rm scraper python scraper.py --clear-db
```

### Custom Configuration
```bash
# Use custom configuration file
docker-compose --profile scraper run --rm scraper python scraper.py --config /path/to/races.yaml
```

### Scraper Features
✅ **Smart Duplicate Prevention**: Automatically skips existing results  
✅ **Meet/Race Separation**: Clean organization with meet names and race names  
✅ **Incremental Updates**: Add new races without affecting existing data  
✅ **Flexible Configuration**: YAML-based race definitions  
✅ **Robust Error Handling**: Continues processing even if individual races fail  

**Everything works perfectly:**
✅ Database schema with fractional seconds support  
✅ Web dashboard with pace calculations  
✅ CSV export functionality  
✅ Team and athlete analytics  
✅ Docker containerization  
✅ Production-ready deployment
