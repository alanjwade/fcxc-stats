#!/bin/bash

# Certbot renewal script for Fort Collins High School Cross Country Stats

echo "$(date): Starting certificate renewal check..."

# Try to renew certificates
docker-compose run --rm certbot renew --quiet

# If renewal was successful, reload nginx
if [ $? -eq 0 ]; then
    echo "$(date): Certificate renewal check completed successfully"
    docker-compose restart nginx
    echo "$(date): Nginx restarted"
else
    echo "$(date): Certificate renewal check failed or no renewal needed"
fi

echo "$(date): Renewal check finished"
