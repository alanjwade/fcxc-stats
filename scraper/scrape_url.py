#!/usr/bin/env python3
"""
MileSplit URL Scraper

Fetches race results directly from a MileSplit formatted results URL using Playwright.
Intercepts the internal MileSplit API call to get structured JSON data.

Usage:
    python scrape_url.py <URL>
    python scrape_url.py <URL> --output-dir /path/to/output
    python scrape_url.py <URL> --text-only
    python scrape_url.py <URL> --json-only

Examples:
    python scrape_url.py "https://co.milesplit.com/meets/688813-john-martin-xc-invitational-2025/results?type=formatted&event=5000m&gender=Girls&division=Varsity"
"""

import sys
import os
import re
import json
import time
import argparse
from urllib.parse import urlparse, parse_qs

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("Error: playwright not installed. Run: pip install playwright && playwright install chromium")
    sys.exit(1)


def parse_url_params(url: str) -> dict:
    """Extract meet ID and filter params from a MileSplit results URL."""
    parsed = urlparse(url)
    
    # Extract meet ID from path like /meets/688813-john-martin.../results
    meet_match = re.search(r'/meets/(\d+)', parsed.path)
    meet_id = meet_match.group(1) if meet_match else None
    
    # Extract query params
    params = parse_qs(parsed.query)
    
    return {
        'meet_id': meet_id,
        'event': params.get('event', [None])[0],
        'gender': params.get('gender', [None])[0],
        'division': params.get('division', [None])[0],
        'type': params.get('type', [None])[0],
    }


def event_param_to_distance(event: str) -> int | None:
    """Convert URL event param (e.g. '5000m', '2Mile') to distance in meters."""
    if not event:
        return None
    event_lower = event.lower()
    # Map common event names
    event_map = {
        '5000m': 5000,
        '5k': 5000,
        '3000m': 3000,
        '3k': 3000,
        '2mile': 3218,  # approximate 2 mile in meters
        '1600m': 1600,
        '800m': 800,
        '400m': 400,
        '200m': 200,
        '100m': 100,
    }
    return event_map.get(event_lower)


def filter_results(data: list, params: dict) -> list:
    """Filter API results by event, gender, and division from URL params."""
    filtered = data

    # Filter by event distance
    target_distance = event_param_to_distance(params.get('event'))
    if target_distance is not None:
        filtered = [d for d in filtered if d.get('eventDistance') == target_distance]
    
    # Filter by gender
    gender_param = params.get('gender', '').lower() if params.get('gender') else None
    if gender_param:
        gender_name_map = {
            'girls': 'Girls',
            'boys': 'Boys',
            'women': 'Girls',
            'men': 'Boys',
        }
        target_gender = gender_name_map.get(gender_param, params.get('gender'))
        filtered = [d for d in filtered if d.get('genderName') == target_gender]

    # Filter by division
    division_param = params.get('division')
    if division_param:
        filtered = [d for d in filtered if d.get('divisionName') == division_param]

    # Sort by place
    def place_key(entry):
        try:
            return int(entry.get('place') or 0)
        except (ValueError, TypeError):
            return 9999

    return sorted(filtered, key=place_key)


def parse_mark_to_seconds(mark: str) -> float | None:
    """Convert a time mark string (MM:SS.ss, MM:SS) to total seconds."""
    if not mark:
        return None
    mark = mark.strip()
    
    patterns = [
        (r'^(\d{1,2}):(\d{2}):(\d{2})\.(\d{1,2})$', 'h_mm_ss_f'),
        (r'^(\d{1,2}):(\d{2})\.(\d{1,2})$', 'mm_ss_f'),
        (r'^(\d{1,2}):(\d{2})$', 'mm_ss'),
        (r'^(\d{3,4})\.(\d{1,2})$', 'sss_f'),
    ]
    
    for pattern, fmt in patterns:
        m = re.match(pattern, mark)
        if m:
            g = m.groups()
            if fmt == 'h_mm_ss_f':
                h, mm, ss, f = g
                frac = int(f) / (10 ** len(f))
                return int(h) * 3600 + int(mm) * 60 + int(ss) + frac
            elif fmt == 'mm_ss_f':
                mm, ss, f = g
                frac = int(f) / (10 ** len(f))
                return int(mm) * 60 + int(ss) + frac
            elif fmt == 'mm_ss':
                mm, ss = g
                return int(mm) * 60 + int(ss)
            elif fmt == 'sss_f':
                ss, f = g
                frac = int(f) / (10 ** len(f))
                return int(ss) + frac
    return None


def seconds_to_mark(seconds: float) -> str:
    """Convert seconds to MM:SS.ss string."""
    mins = int(seconds) // 60
    secs = seconds % 60
    if secs != int(secs):
        return f"{mins}:{secs:05.2f}"
    return f"{mins}:{int(secs):02d}.00"


def fetch_results(url: str, wait_seconds: int = 8) -> dict:
    """
    Load the MileSplit page via Playwright, intercept the API call,
    and return the raw API JSON data.
    """
    api_data = None
    api_url = None

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=['--disable-blink-features=AutomationControlled'],
        )
        context = browser.new_context(
            user_agent=(
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ),
        )
        page = context.new_page()
        # Hide automation marker
        page.add_init_script(
            'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'
        )

        def handle_response(response):
            nonlocal api_data, api_url
            if 'milesplit.com/api' in response.url and 'performances' in response.url:
                api_url = response.url
                try:
                    api_data = response.json()
                except Exception:
                    try:
                        api_data = {'_raw': response.body().decode('utf-8', errors='replace')}
                    except Exception:
                        pass

        page.on('response', handle_response)
        page.set_default_timeout(60000)
        
        print(f"Loading: {url}", file=sys.stderr)
        page.goto(url, wait_until='load')
        
        print(f"Waiting {wait_seconds}s for API response...", file=sys.stderr)
        time.sleep(wait_seconds)
        
        browser.close()

    if api_data is None:
        raise RuntimeError("No API data captured. The page may not have loaded results.")
    
    if api_data.get('_meta', {}).get('status_code') not in (200, None):
        status = api_data.get('_meta', {}).get('status_code')
        error = api_data.get('error', {}).get('message', 'Unknown error')
        raise RuntimeError(f"API returned status {status}: {error}")

    return api_data


def format_results_text(results: list, params: dict, meet_name: str = '') -> str:
    """Format filtered results as a human-readable text table."""
    gender = params.get('gender', '')
    division = params.get('division', '')
    event = params.get('event', '')

    header_parts = [p for p in [meet_name, gender, division, event] if p]
    header = ' - '.join(header_parts)

    lines = [header, '=' * max(len(header), 60), '']
    lines.append(f"{'Place':<6} {'Name':<30} {'School':<35} {'Time':<10}")
    lines.append('-' * 85)

    for entry in results:
        place = entry.get('place', '')
        name = f"{entry.get('firstName', '')} {entry.get('lastName', '')}".strip()
        school = entry.get('teamName', '')
        mark = entry.get('mark', '')
        lines.append(f"{place:<6} {name:<30} {school:<35} {mark:<10}")

    lines.append('')
    lines.append(f"Total: {len(results)} results")
    return '\n'.join(lines)


def build_results_json(results: list, params: dict, meet_name: str = '') -> list:
    """Build a list of structured result dicts for JSON output."""
    output = []
    for entry in results:
        first_name = entry.get('firstName', '').strip()
        last_name = entry.get('lastName', '').strip()
        mark = entry.get('mark', '')
        time_seconds = parse_mark_to_seconds(mark)
        output.append({
            'place': entry.get('place'),
            'first_name': first_name,
            'last_name': last_name,
            'name': f"{first_name} {last_name}".strip(),
            'school': entry.get('teamName', ''),
            'mark': mark,
            'time_seconds': time_seconds,
            'gender': entry.get('genderName', ''),
            'division': entry.get('divisionName', ''),
            'event': entry.get('eventName', ''),
            'grad_year': entry.get('gradYear'),
            'athlete_id': entry.get('athleteId'),
            'team_id': entry.get('teamId'),
        })
    return output


def scrape(url: str, output_dir: str = None, text_only: bool = False, json_only: bool = False) -> dict:
    """
    Main scrape function. Returns dict with 'text' and 'json' keys.
    Optionally writes files to output_dir.
    """
    params = parse_url_params(url)
    
    if not params['meet_id']:
        raise ValueError(f"Could not extract meet ID from URL: {url}")
    
    print(f"Meet ID: {params['meet_id']}", file=sys.stderr)
    print(f"Filters: event={params['event']}, gender={params['gender']}, division={params['division']}", file=sys.stderr)

    api_data = fetch_results(url)
    
    all_results = api_data.get('data', [])
    print(f"Total API results: {len(all_results)}", file=sys.stderr)
    
    filtered = filter_results(all_results, params)
    print(f"Filtered results: {len(filtered)}", file=sys.stderr)

    # Try to get meet name from first result
    meet_name = filtered[0].get('meetName', '') if filtered else ''

    text_output = format_results_text(filtered, params, meet_name)
    json_output = build_results_json(filtered, params, meet_name)

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        # Build filename from params
        parts = [
            f"meet_{params['meet_id']}",
            params.get('event', 'event'),
            params.get('gender', ''),
            params.get('division', ''),
        ]
        base_name = '_'.join(p for p in parts if p)
        
        text_path = os.path.join(output_dir, f"{base_name}.txt")
        json_path = os.path.join(output_dir, f"{base_name}.json")
        
        with open(text_path, 'w', encoding='utf-8') as f:
            f.write(text_output)
        print(f"Text saved to: {text_path}", file=sys.stderr)
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(json_output, f, indent=2)
        print(f"JSON saved to: {json_path}", file=sys.stderr)

    return {'text': text_output, 'json': json_output}


def main():
    parser = argparse.ArgumentParser(
        description='Scrape race results from a MileSplit formatted results URL.'
    )
    parser.add_argument('url', help='MileSplit formatted results URL')
    parser.add_argument('--output-dir', '-o', default=None,
                        help='Directory to save output files (default: print to stdout)')
    parser.add_argument('--text-only', action='store_true',
                        help='Only output text table')
    parser.add_argument('--json-only', action='store_true',
                        help='Only output JSON')
    parser.add_argument('--wait', type=int, default=8,
                        help='Seconds to wait for page to load (default: 8)')
    args = parser.parse_args()

    try:
        result = scrape(
            args.url,
            output_dir=args.output_dir,
            text_only=args.text_only,
            json_only=args.json_only,
        )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    if not args.output_dir:
        if not args.json_only:
            print(result['text'])
        if not args.text_only:
            if not args.json_only:
                print('\n--- JSON ---')
            print(json.dumps(result['json'], indent=2))


if __name__ == '__main__':
    main()
