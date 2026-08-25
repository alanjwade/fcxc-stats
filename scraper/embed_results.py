#!/usr/bin/env python3
"""
Embed captured MileSplit results into an already-downloaded results page.

download_and_add captures the /api/v1/meets/{id}/performances payload and
embeds it automatically for new downloads. For pages downloaded before that
change (which contain no results rows), run this to fetch the API and inject
the (race-filtered) results into the existing HTML file in place.

Usage:
    python embed_results.py <downloaded.html> <results_url>
    python embed_results.py sources/pages/2026/foo/boys_varsity.html \
        "https://co.milesplit.com/meets/<id>-<slug>/results?event=5000m&gender=Boys&division=Varsity"

Run from the scraper/ directory.
"""

import argparse
import os
import sys

from download_page import fetch_milesplit_api, embed_api_results
import download_and_add as dnu


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("html_file", help="Existing downloaded results HTML file")
    ap.add_argument("url", help="The MileSplit results URL used for that file "
                                "(provides the race filters + meet id)")
    args = ap.parse_args(argv)

    if not os.path.exists(args.html_file):
        print(f"Error: file not found: {args.html_file}", file=sys.stderr)
        return 1

    with open(args.html_file, "r", encoding="utf-8", errors="replace") as f:
        html = f.read()

    api = fetch_milesplit_api(args.url)
    if not (api and isinstance(api.get("data"), list) and api["data"]):
        print("Error: could not fetch results API; file left unchanged.", file=sys.stderr)
        return 2

    filtered = dnu.filter_api_for_url(api, args.url)
    results = filtered.get("data", [])
    updated = embed_api_results(html, {"data": results})

    with open(args.html_file, "w", encoding="utf-8") as f:
        f.write(updated)

    print(f"Embedded {len(results)} results into {args.html_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())