# MileSplit URL Scraper

A Playwright-based scraper to fetch race results directly from MileSplit formatted results URLs.

## Installation

The scraper requires Playwright:

```bash
pip install playwright
playwright install chromium
```

Both are already installed in the project's venv.

## Usage

### URL Format Requirements

The script works with MileSplit **formatted results** URLs that include:
- A valid meet ID (e.g., `688813`, `686286`)
- The query parameter `type=formatted`
- Optional filters: `event` (e.g., `5000m`, `2Mile`), `gender` (e.g., `Girls`, `Boys`), `division` (e.g., `Varsity`, `JV`)

**Example URL structure:**
```
https://co.milesplit.com/meets/{MEET_ID}-{meet-name}/results?type=formatted&event={EVENT}&gender={GENDER}&division={DIVISION}
```

### Quick Start

```bash
cd scraper
./scrape "https://co.milesplit.com/meets/688813-john-martin-xc-invitational-2025/results?type=formatted&event=5000m&gender=Girls&division=Varsity"
```

**Important:** Always quote the URL to prevent the shell from parsing the `&` characters as background operators.

### Output Options

**Print text table (default):**
```bash
./scrape "<URL>"
```

**Print JSON only:**
```bash
./scrape "<URL>" --json-only
```

**Print text only:**
```bash
./scrape "<URL>" --text-only
```

**Save to files:**
```bash
./scrape "https://co.milesplit.com/meets/688813-john-martin-xc-invitational-2025/results?type=formatted&event=5000m&gender=Girls&division=Varsity" --output-dir ./results
```

This creates files like `meet_688813_5000m_Girls_Varsity.txt` and `.json`.

## How It Works

1. Loads the MileSplit results page in a headless Chromium browser
2. Uses stealth mode to bypass bot detection (hides `navigator.webdriver` and uses realistic user-agent)
3. Intercepts the internal `/api/v1/meets/{id}/performances` API call
4. Filters results by event distance, gender, and division from URL params
5. Outputs human-readable table and structured JSON

## Examples

**John Martin XC Invitational 2025 - Girls Varsity 5K:**
```bash
./scrape "https://co.milesplit.com/meets/688813-john-martin-xc-invitational-2025/results?type=formatted&event=5000m&gender=Girls&division=Varsity"
```

**Thornton Cross Country Invitational 2025 - Girls Varsity 5K:**
```bash
./scrape "https://co.milesplit.com/meets/686286-thornton-cross-country-invitational-2025/results?type=formatted&event=5000m&gender=Girls&division=Varsity"
```

## Output Example

Text format shows:
- Meet name
- Filters applied (event, gender, division)
- Place, name, school, and time for each runner
- Total count of results

JSON format includes:
- Place, first/last name, full name
- School, time (as mark string and parsed seconds)
- Gender, division, event, grad year
- Athlete ID and team ID for reference

## Notes

- The scraper includes a wait period (default 8 seconds) for the page to fully load and the API to respond
- Results are sorted by place
- Times are parsed from the MileSplit mark format (MM:SS.ss) to total seconds for data consistency
- The script handles different time formats (with/without fractional seconds)
