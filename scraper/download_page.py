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
from playwright.sync_api import sync_playwright
import time


def url_to_slug(url: str) -> str:
    """Extract a slug from the URL for the meet name."""
    match = re.search(r'meets/(\d+[^/]*)?', url)
    if match:
        return match.group(1).lower().replace('-', '_')
    return "unknown"


def download_page(url: str, output_file: str = None, season: str = "unknown",
                  meet: str = None, name: str = None):
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

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_default_timeout(60000)

        try:
            print("Loading page...")
            page.goto(url, wait_until='load')
            print("Waiting for dynamic content...")
            time.sleep(3)

            html_content = page.content()

            with open(final_path, 'w', encoding='utf-8') as f:
                f.write(html_content)

            print(f"✓ Successfully saved to: {final_path}")
            print(f"  File size: {len(html_content)} bytes")
            print(f"\n  Next step: Add an entry to sources/meets.yaml for this race.")

        except Exception as e:
            print(f"✗ Error downloading page: {e}")
            raise
        finally:
            browser.close()


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
