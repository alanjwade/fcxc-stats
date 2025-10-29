#!/bin/bash
# Helper script to download a page using the scraper container

# Default URL
URL="${1:-https://co.milesplit.com/meets/694555-colorado-5a-region-4-cross-country-2025/results}"
OUTPUT_FILE="${2:-}"

echo "Building scraper container with Playwright support..."
docker compose build scraper

echo ""
echo "Downloading page..."
if [ -z "$OUTPUT_FILE" ]; then
    docker compose run --rm -v "$(pwd)/scraper/pages:/app/pages" scraper python download_page.py "$URL"
else
    docker compose run --rm -v "$(pwd)/scraper/pages:/app/pages" scraper python download_page.py "$URL" "$OUTPUT_FILE"
fi

echo ""
echo "Done! Check the scraper/pages/ directory for the downloaded file."
