# Page Downloader

A standalone script using Playwright to download MileSplit pages for offline processing.

## Files

- `download_page.py` - Python script that uses Playwright to download web pages
- `download_page.sh` - Bash helper script to build and run the downloader

## Setup

The scraper Docker container includes Playwright and Chromium. No additional setup needed beyond building the container.

## Usage

### Using the helper script:

```bash
./download_page.sh "URL" "output_file.html"
```

### Using docker compose directly:

```bash
docker compose run --rm -v "$(pwd)/scraper/pages:/app/pages" scraper \
  python download_page.py "URL" "pages/output_file.html"
```

### From within the container:

```bash
python download_page.py "https://example.com/page" "pages/output.html"
```

## Examples

Download the Regionals 2025 page:
```bash
./download_page.sh \
  "https://co.milesplit.com/meets/694555-colorado-5a-region-4-cross-country-2025/results" \
  "pages/regionals_2025_5a_region_4.html"
```

Download with auto-generated filename:
```bash
./download_page.sh "https://co.milesplit.com/meets/694555-colorado-5a-region-4-cross-country-2025/results"
```

## How It Works

1. Launches a headless Chromium browser using Playwright
2. Navigates to the specified URL
3. Waits for dynamic content to load (3 seconds)
4. Extracts the complete HTML content
5. Saves to the specified file

## Features

- Headless browser execution
- 60-second timeout for slow pages
- Automatic directory creation
- Dynamic content rendering (JavaScript-heavy sites)
- Volume mount to save files on host filesystem

## Requirements

- Docker and docker compose
- Playwright and Chromium (included in Docker image)
- See `scraper/requirements.txt` for Python dependencies
