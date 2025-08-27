# Cross Country Statistics Tracker

## Deployment Instructions

### Prerequisites
- Docker and Docker Compose installed
- Git (for version control)

### Quick Start

1. **Initialize the project:**
   ```bash
   ./deploy.sh init
   ```

2. **Edit your configuration:**
   - Update `.env` with your database credentials
   - Edit `config/races.yaml` with your actual race data

3. **Start the application:**
   ```bash
   ./deploy.sh start
   ```

4. **Import race data:**
   ```bash
   ./deploy.sh scrape
   ```

5. **Access the dashboard:**
   Open http://localhost in your browser

### Configuration Files

#### `.env`
Contains environment variables for the application:
- Database credentials
- Flask configuration
- Secret keys

#### `config/races.yaml`
Defines the races to scrape:
```yaml
races:
  - name: "Meet Name"
    url: "https://co.milesplit.com/meets/.../formatted/"
    distance: "5K"
    class: "varsity"
    gender: "mixed"
    venue: "Venue Name"
    date: "YYYY-MM-DD"
    season: "YYYY"
```

### Available Commands

```bash
./deploy.sh start     # Start the web application
./deploy.sh stop      # Stop the web application
./deploy.sh restart   # Restart the web application
./deploy.sh scrape    # Run the data scraper
./deploy.sh logs      # View application logs
./deploy.sh backup    # Backup the database
./deploy.sh init      # Initialize environment files
```

### Architecture

The application consists of:

1. **PostgreSQL Database** - Stores all race and athlete data
2. **Python Scraper** - Extracts data from MileSplit.com
3. **Flask Web App** - Provides the dashboard interface
4. **Nginx Reverse Proxy** - Handles web traffic

### Features

- **CSV Export**: Download athlete performance data
- **Team Statistics**: View best times by gender
- **Athlete Profiles**: Individual statistics and progress tracking
- **Responsive Design**: Works on desktop and mobile devices

### Troubleshooting

**Database Connection Issues:**
```bash
docker-compose logs db
```

**Web Application Issues:**
```bash
docker-compose logs webapp
```

**Scraper Issues:**
```bash
docker-compose logs scraper
```

**Reset Everything:**
```bash
docker-compose down -v
docker-compose up -d
```

### Production Considerations

- Change default passwords in `.env`
- Set up SSL certificates for HTTPS
- Configure backups using `./deploy.sh backup`
- Monitor logs regularly
- Update the application periodically

### Data Structure

The application tracks:
- **Athletes**: Name, gender, graduation year
- **Venues**: Meet locations
- **Meets**: Competition events
- **Races**: Individual races within meets
- **Results**: Individual athlete performances
