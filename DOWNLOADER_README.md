# Page Downloader

A standalone script using Playwright to download MileSplit pages for offline processing.

## Files

- `scraper/download_page.py` - Python script that uses Playwright to download web pages

## Setup

Requires the scraper virtual environment with Playwright installed:

```bash
cd scraper
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

## Usage

```bash
cd scraper
source .venv/bin/activate
python download_page.py "URL" "pages/output_file.html"
```

## Examples

Download the Regionals 2025 page:
```bash
python download_page.py \
  "https://co.milesplit.com/meets/694555-colorado-5a-region-4-cross-country-2025/results" \
  "pages/regionals_2025_5a_region_4.html"
```

Download with auto-generated filename:
```bash
python download_page.py "https://co.milesplit.com/meets/694555-colorado-5a-region-4-cross-country-2025/results"
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

## Requirements

- Python 3 with the scraper virtual environment
- Playwright and Chromium (`playwright install chromium`)
- See `scraper/requirements.txt` for Python dependencies
