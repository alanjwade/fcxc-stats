# Scraper Redone — Test Results vs Production Database

**Test Date:** 2026-08-12  
**Test Command:** `python3 test_scraper_vs_db.py`  
**Database:** Read-only comparison against `/data/fcxc_stats.db`  
**Config Source:** `sources/meets.yaml` (44 races across 17 meets)

---

## Executive Summary

| Metric | Value |
|--------|-------|
| **Total races tested** | 44 |
| **Races matching DB exactly** | 0 |
| **Races with issues** | 44 (100%) |
| **Total discrepancies reported** | 2,350+ |
| **DB results affected** | 5,823 out of 6,389 (91%) |

**Status:** ❌ **Scraper redone has critical regressions across multiple parsers and formats.**

---

## Issue Categories

### CATEGORY 1: Complete Parse Failure (No Results Returned)

Parser runs but `extract_races()` returns empty dict. **7 meets affected, 2,152 DB results lost.**

#### Rocky Mountain Lobo Invitational 2025 (4 races, 460 results in DB)

- **Races affected:**
  - Varsity Boys (124 results)
  - Varsity Girls (101 results)
  - JV Boys (129 results)
  - JV Girls (106 results)

- **Parser detected:** `DefaultParser`
- **Issue:** Parser detects content but `extract_races()` returns `{}` (empty dict)
  - `_parse_pre_text()` doesn't match the `<pre>` structure (if present)
  - `_parse_table()` doesn't find tables with times in the expected format
  - Falls back to `_parse_text_results()` which returns nothing
- **Root cause:** HTML structure doesn't match parser assumptions. Need to examine actual HTML format.

**File:** `sources/pages/2025/rocky_mountain_lobo/varsity_boys.html` (and related)

---

#### Vista Nation XC 2-Mile Invitational 2025 (4 races, 271 results in DB)

- **Races affected:**
  - Boys Varsity (37 results)
  - Girls Varsity (26 results)
  - Boys Freshman/Sophomore (106 results)
  - Girls Freshman/Sophomore (102 results)

- **Parser detected:** `DefaultParser`
- **Issue:** Same root cause as Rocky Mountain Lobo
- **Note:** 2-mile distance may affect time parsing if format differs from 5K events

**File:** `sources/pages/2025/vista_nation/boys_varsity.html` (and related)

---

#### Thornton Cross Country Invitational 2025 (4 races, 755 results in DB)

- **Races affected:**
  - JV Boys (214 results)
  - JV Girls (170 results)
  - Varsity Boys (197 results)
  - Varsity Girls (174 results)

- **Parser detected:** `ThorntonCombinedParser` ✓
- **Issue:** Parser correctly identified but `extract_races()` returns `{}`
  - Section header pattern looking for: `"Thornton High School Invitational - JV Boys"` (etc.)
  - Pattern isn't matching the actual headers in the file
  - `_parse_race_text()` regex doesn't match the data line format

- **Root cause:** Mismatch between expected header format and actual file content; line-parsing regex doesn't account for actual whitespace/field layout

**File:** `sources/pages/2025/thornton/combined_results.html`

**Old format notes:** Scraper.py has `scrape_thornton_combined_format()` that parses this successfully. New parser architecture may use different assumptions.

---

#### Desert Twilight XC Invite 2025 (2 races, 570 results in DB)

- **Races affected:**
  - Boys Sweepstakes (330 results)
  - Girls Sweepstakes (240 results)

- **Parser detected:** `RawCombinedParser` ✗ (WRONG)
- **Issue:** `DesertTwilightParser` exists in registry but `RawCombinedParser` is chosen first
  - Parser registry order/precedence issue
  - `RawCombinedParser.can_parse()` is too permissive, matches Desert Twilight content
  - `DesertTwilightParser.can_parse()` looks for `"Desert Twilight"` in content; needs to be checked first

- **Root cause:** Parser registry discovery order needs re-evaluation. `DesertTwilightParser` should be registered before or after `RawCombinedParser` to ensure correct match.

- **Data format:** Multiline per-athlete format:
  ```
  Place
  Optional abbreviation (2 letters) or malformed entry
  Name
  School/Team
  Time
  Year info (optional PR/gap indicators)
  ```

**File:** `sources/pages/2025/desert_twilight/boys.txt` (and girls.txt)

---

#### Loveland Sweetheart 2025 (4 races, 498 results in DB)

- **Races affected:**
  - Varsity Boys (140 results)
  - Varsity Girls (121 results)
  - JV Boys (143 results)
  - JV Girls (94 results)

- **Parser detected:** `LovelandSweetheartParser` ✓
- **Issue:** Parser correctly identified but `extract_races()` returns `{}`
  - Section header detection regex: `r'^(.+? (?:Boys|Girls) .+?)\s*$'`
  - Not matching actual section titles in file (e.g., `"HS Varsity Boys 5K"`, `"Boys HS Open 5K"`)
  - Result: no sections are recognized, all lines skipped

- **Root cause:** Section header regex too strict or doesn't account for actual section title format

**File:** `sources/pages/2025/loveland_sweetheart/combined_results.txt`

**Old format notes:** File has race sections like "HS Varsity Boys 5K" that should be parsed into separate sections.

---

#### Longs Peak Invitational 2025 (2 races, 278 results in DB)

- **Races affected:**
  - Varsity Boys (160 results)
  - Varsity Girls (118 results)

- **Parser detected:** `LongsPeakParser` ✓
- **Issue:** Parser correctly identified but `extract_races()` returns `{}` (empty dict)
  - `_parse_race_text()` regex pattern: `r'^\s*(\d+)\s+([A-Z\'\-]+),\s+([A-Za-z\'\-]+)\s+(\d{1,2})\s+(.+?)\s+(\d{1,2}:\d{2}(?:\.\d{2})?)'`
  - Expects: `Place LASTNAME, Firstname Grade School Time`
  - Not matching actual line format in file

- **Root cause:** Old scraper expected `"Firstname LASTNAME"` (LASTNAME in ALL CAPS) with pace + time; new parser regex uses different assumption

**File:** `sources/pages/2025/longs_peak/boys.txt` (and girls.txt)

**Old format notes:** Scraper.py has `scrape_longs_peak_format()` that successfully parses this with a different regex pattern.

---

#### Northern Conference Championships 2025 (4 races, 320 results in DB)

- **Races affected:**
  - Varsity Boys (61 results)
  - Varsity Girls (60 results)
  - JV Boys (109 results)
  - JV Girls (90 results)

- **Parser detected:** `LovelandSweetheartParser` ✗ (WRONG)
- **Issue:** `NorthernConferenceParser` exists but `LovelandSweetheartParser` is chosen first
  - Parser registry precedence issue again
  - `LovelandSweetheartParser` can_parse returns true but then `extract_races()` returns nothing

- **Root cause:** 
  1. Parser registry order — `LovelandSweetheartParser` fires before `NorthernConferenceParser`
  2. Even if correct parser fires, section detection doesn't work

- **Data format:** Tab-separated with race sections identified by "Race #N" headers

**File:** `sources/pages/2025/northern_conference/combined_results.txt`

---

### CATEGORY 2: Count Mismatch (Partial Results)

Parser returns results but count doesn't match DB. **4 meets affected, varying deltas.**

#### Windsor Wizards Invitational 2025 (4 races)

| Race | Scraped | DB | Delta |
|------|---------|-----|-------|
| Varsity Boys | 92 | 153 | **−61 (−40%)** |
| Varsity Girls | 57 | 108 | **−51 (−47%)** |
| JV Boys | 29 | 70 | **−41 (−59%)** |
| JV Girls | 28 | 51 | **−45 (−88%)** |

- **Parser detected:** Scraper uses `scrape_raw_windsor_combined_format()` from scraper.py (not modular parser)
- **Issue:** Parser stops early, missing 60–88% of results per race
  - Athlete results table header detection failing
  - Stops after team results section
  - Regex for athlete lines doesn't match format

- **Data format:** Combined file with team scores followed by athlete results (NAME in "LASTNAME, Firstname" format, capitalized normally)

**File:** `sources/pages/2025/windsor_wizards/combined_results.html`

**Observations:** This uses the scraper.py's old-style method, not the modular parser. May need inspection of the HTML structure.

---

#### Hawk JV Championships 2025 (2 races)

| Race | Scraped | DB | Delta |
|------|---------|-----|-------|
| JV Boys | 43 | 327 | **−284 (−87%)** |
| JV Girls | 43 | 248 | **−205 (−83%)** |

- **Parser detected:** `DefaultParser`
- **Critical issue:** 43 "results" are **NOT athlete results** — they are **team score rows**
  - Example: Place 1 = "Niwot High" (school name, not athlete)
  - Example: Time = "0:38.00" (team score, not athlete time)
  - Example: School = "Adams County Regional Park XC Course" (venue, not school)

- **Root cause:** Parser is matching and returning team score section instead of athlete section. Format detection or section parsing fundamentally broken.

- **Data format:** Should use `LovelandSweetheartParser` (same format as Loveland Sweetheart 2025)

**File:** `sources/pages/2025/hawk_jv_champs/combined_results.txt`

---

#### Colorado 5A Region 4 Cross Country 2025 (2 races)

| Race | Scraped | DB | Delta |
|------|---------|-----|-------|
| Varsity Boys | 99 | 96 | **+3 (over-count)** |
| Varsity Girls | 99 | 85 | **+14 (over-count)** |

- **Parser detected:** `RegionalsTableParser` 
- **Issue:** Over-counting; 99 rows matched per race when DB has 96 & 85
  - Likely including table header rows or team score rows as athlete results
  - Generic `_parse_table_row()` is too permissive

- **Root cause:** Table parsing doesn't distinguish between athlete rows and metadata/summary rows

**File:** `sources/pages/2025/region_4/results.html`

---

#### Colorado 5A State Cross Country Championships 2025 (2 races)

| Race | Scraped | DB | Delta | Notes |
|------|---------|-----|-------|-------|
| Varsity Boys | 160 | 160 | **MATCH COUNT** | But values wrong (see Category 3) |
| Varsity Girls | 151 | 151 | **MATCH COUNT** | But values wrong (see Category 3) |

- **Parser detected:** `RegionalsTableParser`
- **Count matches DB**, but all athlete names are **blank** and **school = venue**
- See Category 3 below for details

---

### CATEGORY 3: Wrong Values (Correct Count, Wrong Data)

Parser returns correct number of results but data values don't match DB.

#### John Martin XC Invitational 2025 (4 races, ~630 results total)

**All 4 races affected:**
- High School Boys Varsity (189 results)
- High School Boys JV (151 results)
- High School Girls Varsity (148 results)
- High School Girls JV (97 results)

##### Issue 3A: Time Parsing Regression

**Problem:** Times ≥ 24 minutes are parsed incorrectly

**Example from Boys Varsity:**
```
Place 185: 
  Scraped time: 1440:00.00 (24 hours!)
  DB time:     24:00.00 (24 minutes)
  
Place 189:
  Scraped time: 1543:00.00
  DB time:     25:43.00
```

**Root cause:** 
- HTML contains times in format `MM:SS:HH` (minutes:seconds:hundredths)
  - Example: `24:00:00` = 24 min 0 sec 0 hundredths
- Old scraper pattern matched as: `\d{1,2}:\d{2}:\d{2}` → **minutes:seconds:hundredths** → 1440 seconds
- New `BaseParser.parse_time_to_seconds()` pattern matches as: **H:MM:SS** → hours:minutes:seconds → 86400 seconds

**Affected times:** Any athlete running ≥ 24 minutes (slow runners, additional category results, etc.)

**File:** `sources/pages/2025/john_martin/boys_varsity.html` (and related)

**Code location:** `scraper/parsers/base.py`, lines 97–114 in `parse_time_to_seconds()`

##### Issue 3B: School Name Not Normalized

**Problem:** `'Resurrection Christian HS'` not expanded to `'Resurrection Christian High School'`

**Affected:** ~10 results across all 4 races

**Root cause:** School name normalization missing. `LovelandSweetheartParser._normalize_school_name()` is empty (no mappings defined).

---

#### Liberty Bell Cross Country Invitational 2025 (6 races, ~867 results)

**All 6 races affected:**
- Varsity Boys (148 results)
- Varsity Girls (155 results)
- JV Boys (135 results)
- JV Girls (134 results)
- Open Boys (341 results)
- Open Girls (254 results)

**Issue:** Short school names not expanded

| Scraped | DB |
|---------|-----|
| `'Fort Collins'` | `'Fort Collins High School'` |
| `'Fossil Ridge'` | `'Fossil Ridge High School'` |
| `'Rocky Mountain'` | `'Rocky Mountain High School'` |

**Root cause:** `raw_combined.py` parser's school name normalization is not applied or missing mappings

**Code location:** `scraper/parsers/raw_combined.py` or base parser

**Affected count:** ~60+ results across 6 races

---

#### Windsor Wizards Invitational 2025 (4 races, in addition to count issues)

**All 4 races have value discrepancies:**

##### Issue 4A: Fractional Seconds Dropped

**Problem:** Windsor times use 1-decimal format; scraper drops the fraction

**Example from Varsity Boys:**
```
Place 1:
  Scraped: 15:52.00 (fraction dropped)
  DB:      15:52.30

Place 2:
  Scraped: 15:54.00
  DB:      15:54.90
```

**Root cause:** 
- Times in source: `15:52.3` (1 decimal)
- Pattern `MM:SS.ss` requires exactly 2 decimals
- Falls through to `MM:SS` pattern and loses fractional part

**Code location:** `scraper/parsers/base.py`, line 105 — pattern `r'(\d{1,2}):(\d{2})\.(\d{2})'` should accept variable decimal places

**Affected:** All Windsor results with fractional seconds (hundreds)

##### Issue 4B: School Names Truncated

**Problem:** `'Cheyenne East High S'` not expanded to `'Cheyenne East High School'`

**Root cause:** School name mapping not applied. The old scraper had `fix_thornton_school_name()` mappings; new parser doesn't use them.

---

#### Colorado 5A State Cross Country Championships 2025 (2 races, ~311 results)

**Both races critically broken:**

##### Issue 5A: Athlete Names Completely Missing

**Example from Varsity Boys, Place 1:**
```
Scraped:
  Name:   ' ' (blank)
  School: 'Norris Penrose Event Center' (venue, not school)

DB:
  Name:   'Benjamin Adams'
  School: 'Mountain Vista High School'
```

**Affected:** All ~311 results in both races — 100% data loss for athlete identity

**Root cause:** 
- Old `scrape_regionals_table_format()` in scraper.py uses CSS selectors:
  - `.athlete` → athlete name link
  - `.team` → school name link
  - `.finish` → time
- New `RegionalsTableParser._parse_table_row()` guesses cells by content type:
  - First non-numeric, non-time cell → assumed to be name
  - Gets first column value → often a section header or label

**Code location:** `scraper/parsers/regionals_table.py`, lines 48–80

**Fix required:** Use CSS class detection like original scraper, or provide correct parsing logic for MileSplit table structure

---

## Summary of Root Causes

### Parser Registry Issues
1. **Wrong parser selected:**
   - Desert Twilight: `RawCombinedParser` chosen over `DesertTwilightParser`
   - Northern Conference: `LovelandSweetheartParser` chosen over `NorthernConferenceParser`
   - Hawk JV: `DefaultParser` chosen over `LovelandSweetheartParser`

2. **Registry order:** Parsers need re-ordered or precedence logic refined

### Pattern Matching Issues
1. **Section headers not matched:** Thornton, Loveland, Longs Peak parsers' regex patterns don't match actual file format
2. **Line format mismatch:** Data line regex patterns built on wrong assumptions about format
3. **Time format:** MM:SS:HH treated as H:MM:SS; 1-decimal times rejected

### Parser Implementation Issues
1. **DefaultParser fallback too broad:** Catches files it can't actually parse
2. **RegionalsTableParser too generic:** Doesn't handle MileSplit-specific CSS selectors
3. **Missing normalization:** School name mappings not applied in modular parsers
4. **Early exit:** Some parsers stop parsing mid-file (Windsor, Hawk JV)

### Data Format Gaps
1. **Desktop app vs modular parsers:** Old scraper.py has custom `scrape_*_format()` methods; new parsers are generic
2. **HTML structure assumptions:** DefaultParser assumes specific `<pre>` or table structures
3. **Section identification:** Combined files need better header/section detection

---

## Next Steps for Fixing Agent

### Priority 1: Complete Failures (7 meets, 2,152 results)
1. **Rocky Mountain Lobo + Vista Nation:** Debug DefaultParser with actual HTML; add logging to identify format
2. **Thornton:** Check file format; update section header and line parsing regex
3. **Desert Twilight:** Fix parser registry order; validate DesertTwilightParser against file
4. **Loveland Sweetheart:** Fix section header regex to match "HS Varsity Boys 5K" format
5. **Longs Peak:** Fix line parsing regex; validate against actual file format
6. **Northern Conference:** Fix parser registry order; ensure NorthernConferenceParser fires first

### Priority 2: Count Mismatches (4 meets, 489+ results)
1. **Windsor Wizards:** Debug early exit; check athlete results table header detection
2. **Hawk JV:** Fix parser selection; ensure LovelandSweetheartParser is used
3. **Region 4:** Reduce over-count; filter out non-athlete rows in table parsing
4. **State Championships:** See Priority 3A

### Priority 3: Value Mismatches
1. **John Martin times:** Fix `parse_time_to_seconds()` to handle MM:SS:HH format correctly
2. **School normalization:** Add school name mappings to all parsers that need them
3. **Windsor fractional seconds:** Update time regex to accept variable decimal places
4. **State Championships:** Rewrite RegionalsTableParser to use CSS selectors or provide correct cell detection

---

## Test Artifacts

- **Full output:** `/tmp/scraper_test_results.txt` (2,350+ discrepancies listed line-by-line)
- **Test script:** `/home/alan/Projects/fcxc_stats/test_scraper_vs_db.py`
- **Database:** `/home/alan/Projects/fcxc_stats/data/fcxc_stats.db` (read-only in test)
- **Config:** `sources/meets.yaml`

---

## How to Re-Run Test

```bash
cd /home/alan/Projects/fcxc_stats
scraper/.venv/bin/python3 test_scraper_vs_db.py 2>/dev/null > /tmp/test_results.txt
# Or with logging:
scraper/.venv/bin/python3 test_scraper_vs_db.py 2>&1 | tee /tmp/test_results_verbose.txt
```

Expected output: Summary line + discrepancies list, no database modifications.

---

**Generated:** 2026-08-12  
**Test Status:** FAIL (0/44 races pass)
