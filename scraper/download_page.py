#!/usr/bin/env python3
"""
Standalone script to download a MileSplit page using Playwright.
Usage: python download_page.py [URL] [OUTPUT_FILE]
"""

import sys
import os
from playwright.sync_api import sync_playwright
import time


def download_page(url, output_file=None):
    """
    Download a web page using Playwright and save it to a file.
    
    Args:
        url: The URL to download
        output_file: Optional output filename. If not provided, generates from URL.
    """
    # Generate output filename if not provided
    if output_file is None:
        # Extract a reasonable filename from the URL
        page_name = url.split('/')[-2] if url.endswith('/') else url.split('/')[-1]
        page_name = page_name.replace('-', '_')
        output_file = f"pages/{page_name}.html"
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else '.', exist_ok=True)
    
    print(f"Downloading: {url}")
    print(f"Output file: {output_file}")
    
    with sync_playwright() as p:
        # Launch browser in headless mode
        browser = p.chromium.launch(headless=True)
        
        # Create a new page
        page = browser.new_page()
        
        # Set a longer timeout for slow pages
        page.set_default_timeout(60000)  # 60 seconds
        
        try:
            # Navigate to the URL (use 'load' instead of 'networkidle' for better compatibility)
            print("Loading page...")
            page.goto(url, wait_until='load')
            
            # Wait a bit for any dynamic content to load
            print("Waiting for dynamic content...")
            time.sleep(3)
            
            # Get the full HTML content
            html_content = page.content()
            
            # Save to file
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            print(f"✓ Successfully saved to: {output_file}")
            print(f"  File size: {len(html_content)} bytes")
            
        except Exception as e:
            print(f"✗ Error downloading page: {e}")
            raise
        
        finally:
            browser.close()


def main():
    """Main entry point for the script."""
    if len(sys.argv) < 2:
        print("Usage: python download_page.py <URL> [OUTPUT_FILE]")
        print("\nExample:")
        print("  python download_page.py https://co.milesplit.com/meets/694555-colorado-5a-region-4-cross-country-2025/results")
        print("  python download_page.py https://example.com/page pages/my_page.html")
        sys.exit(1)
    
    url = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    download_page(url, output_file)


if __name__ == "__main__":
    main()
