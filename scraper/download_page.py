#!/usr/bin/env python3
"""
Standalone script to download a MileSplit page using Playwright.
Saves the page to sources/pages/{season}/{meet_slug}/ with a descriptive name.

Usage:
    python download_page.py --url <URL> --season 2025 --meet "Meet Name" --name "boys_varsity"
    python download_page.py <URL>   # Saves to sources/pages/unknown/ with auto-generated name
"""

import sys
import os
import re
import argparse
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright
import requests
import time


def url_to_slug(url: str) -> str:
    """Extract a slug from the URL for the meet name."""
    match = re.search(r'meets/(\d+[^/]*)?', url)
    if match:
        return match.group(1).lower().replace('-', '_')
    return "unknown"


def download_html(url: str, wait_seconds: int = 3) -> str:
    """Download a page into memory and return its rendered HTML string.

    Args:
        url: The URL to download.
        wait_seconds: Seconds to wait for dynamic content to render.

    Returns:
        The full rendered HTML content.
    """
    print(f"Loading page: {url}")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_default_timeout(60000)
        try:
            page.goto(url, wait_until='domcontentloaded', timeout=30000)
            if wait_seconds > 0:
                print(f"Waiting {wait_seconds}s for dynamic content...")
                time.sleep(wait_seconds)
            return page.content()
        finally:
            browser.close()


def embed_api_results(html: str, api_data=None) -> str:
    """Embed captured MileSplit results data into an HTML string.

    MileSplit 'formatted' results pages render their results purely from a
    /api/v1/meets/{id}/performances AJAX call; the raw DOM (and therefore the
    saved page) contains no result rows. To make such saved pages parseable
    offline, we append the captured JSON payload as a tagged <script> block
    that the milesplit_api parser can read back.
    """
    if not api_data:
        return html
    import json as _json
    payload = _json.dumps(api_data, separators=(',', ':'), sort_keys=True)
    block = (
        '\n<!-- CAPTURED MILESPLIT API DATA (appended by downloader) -->\n'
        '<script type="application/json" id="milesplit-api-data">\n'
        f'{payload}\n'
        '</script>\n'
    )
    if '</html>' in html.lower():
        return html.rstrip() + block + '\n'
    return html + block


# Constant marker used by parsers to recognize an embedded-API download.
API_DATA_MARKER = 'id="milesplit-api-data"'


def fetch_milesplit_api(url: str, retries: int = 3, timeout: int = 30):
    """Fetch the /api/v1/meets/{id}/performances JSON for a results URL.

    MileSplit 'formatted' results pages pull their data from this AJAX endpoint.
    The page shell itself contains no result rows, and Playwright capture is
    intermittently blocked (405 'Bots not allowed'), so a direct HTTP GET with
    a browser-esque Referer/User-Agent is far more reliable.

    Returns the parsed JSON dict on success (status 200), else None.
    """
    parsed = urlparse(url)
    m = re.search(r"/meets/(\d+)", parsed.path)
    if not m:
        return None
    meet_id = m.group(1)
    api_base = f"https://{parsed.netloc}/api/v1/meets/{meet_id}/performances"
    params = {
        "fields": API_FIELDS,
        "isMeetPro": "0",
        "teamScores": "true",
        "m": "GET",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": url,
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }
    for attempt in range(retries):
        try:
            r = requests.get(api_base, params=params, headers=headers, timeout=timeout)
            if r.status_code != 200:
                time.sleep(1)
                continue
            data = r.json()
            meta = data.get("_meta", {})
            if meta.get("status_code") in (200, None):
                return data
        except Exception:
            pass
        time.sleep(1)
    return None


API_FIELDS = (
    "id,meetId,meetName,teamId,teamName,athleteId,firstName,lastName,"
    "gender,genderName,divisionId,divisionName,gradYear,eventName,"
    "eventCode,eventDistance,mark,place,units,statusCode"
)


def download_page(url: str, output_file: str = None, season: str = "unknown",
                  meet: str = None, name: str = None) -> str:
    """
    Download a web page using Playwright and save it to a structured path.

    Args:
        url: The URL to download
        output_file: Optional explicit output path (overrides structured path)
        season: Season/year (e.g., "2025", "2026") — for directory structure
        meet: Meet name — for directory structure (e.g., "Liberty Bell")
        name: Descriptive file name (e.g., "boys_varsity") — for file name
    """
    if output_file:
        os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
        final_path = output_file
    else:
        meet_slug = url_to_slug(url) if not meet else re.sub(r'[^a-z0-9]+', '_', meet.lower()).strip('_')
        filename = f"{name}.html" if name else "raw.html"
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        dir_path = os.path.join(project_root, "sources", "pages", season, meet_slug)
        os.makedirs(dir_path, exist_ok=True)
        final_path = os.path.join(dir_path, filename)

    os.makedirs(os.path.dirname(final_path) or '.', exist_ok=True)

    print(f"Downloading: {url}")
    print(f"Output file: {final_path}")

    try:
        html_content = download_html(url, wait_seconds=3)
    except Exception as e:
        print(f"✗ Error downloading the page: {e}")
        raise

    with open(final_path, 'w', encoding='utf-8') as f:
        f.write(html_content)

    print(f"✓ Successfully saved to: {final_path}")
    print(f"  File size: {len(html_content)} bytes")
    print(f"\n  Next step: Add an entry to sources/meets.yaml for this race.")

    return final_path


def main():
    parser = argparse.ArgumentParser(
        description="Download a MileSplit page using Playwright",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python download_page.py --url https://co.milesplit.com/meets/... --season 2025 --meet "Liberty Bell" --name "combined_results"
  python download_page.py https://co.milesplit.com/meets/...
  python download_page.py --url <URL> --output /path/to/file.html
        """
    )
    parser.add_argument("--url", "-u", help="The URL to download")
    parser.add_argument("--output", "-o", help="Explicit output file path")
    parser.add_argument("--season", "-s", default="unknown", help="Season/year (e.g., 2025)")
    parser.add_argument("--meet", "-m", help="Meet name (e.g., 'Liberty Bell')")
    parser.add_argument("--name", "-n", help="Descriptive file name (e.g., 'boys_varsity')")
    parser.add_argument("url_pos", nargs="?", help="URL as positional argument")

    args = parser.parse_args()

    url = args.url or args.url_pos
    if not url:
        print("Error: URL is required")
        print("Usage: python download_page.py --url <URL> [--season 2025] [--meet \"Meet Name\"] [--name \"file_name\"]")
        sys.exit(1)

    download_page(
        url=url,
        output_file=args.output,
        season=args.season,
        meet=args.meet,
        name=args.name,
    )


if __name__ == "__main__":
    main()
