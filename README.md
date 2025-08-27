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

## ⚠️ **Current Status: MileSplit Integration**

The automatic MileSplit scraping is currently experiencing issues due to JavaScript-rendered content on MileSplit.com. The system is fully functional for data storage, analysis, and web interface - only the automatic scraping component needs enhancement.

**Current Workaround Options:**
1. **Manual CSV Import**: Export results from MileSplit and import via CSV
2. **Direct Database Entry**: Use the database schema to manually insert results
3. **API Integration**: Enhance scraper with Selenium for JavaScript rendering (planned)

**Everything else works perfectly:**
✅ Database schema and storage  
✅ Web dashboard with statistics  
✅ CSV export functionality  
✅ Team and athlete analytics  
✅ Docker containerization  
✅ Production-ready deployment
