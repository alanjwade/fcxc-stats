# Scraper Workflow

The scraper runs **locally** (not in Docker) from `scraper/` using its venv,
and writes to `data/fcxc_stats.db` (root of the repo) — the same database the
webapp reads.

## Setup (once)

```bash
cd scraper
python3 -m venv --system-site-packages .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Running the scraper

```bash
cd scraper
source .venv/bin/activate

# Incremental run — skips existing data, adds only new results
DATABASE_URL=sqlite:///$(pwd)/../data/fcxc_stats.db \
  python scraper.py --sources ../sources/meets.yaml

# Full refresh — clear and reload all data (destructive)
DATABASE_URL=sqlite:///$(pwd)/../data/fcxc_stats.db \
  python scraper.py --clear-db --sources ../sources/meets.yaml
```

Use `--sources ../sources/meets.yaml` (the canonical source). The older
`--config ../config/races.yaml` path is legacy and only kept for backward
compatibility.

## Adding a new race (the canonical flow)

1. **Download the page** (some MileSplit pages need JS rendering):
   ```bash
   cd scraper && source .venv/bin/activate
   python download_page.py --url "<URL>" --season 2025 \
     --meet "Meet Name" --name "boys_varsity"
   ```
   Saves to `sources/pages/<season>/<meet_slug>/<name>.html`.

2. **Add metadata** to `sources/meets.yaml` (NOT `config/races.yaml`):
   ```yaml
   - name: "Meet Name 2025"
     date: "2025-09-01"
     venue: "Venue Name"
     season: "2025"
     source:
       type: file
       path: "pages/2025/meet_name/boys_varsity.html"
     races:
       - name: "Varsity Boys"
         distance: "5K"
         class: "varsity"
         gender: "boys"
   ```

3. **If the page uses a new format**, create a parser in `scraper/parsers/`
   (subclass `BaseParser`, see `03-code-conventions.md`). If an existing parser
   already handles the format, skip this step.

4. **Run the scraper** to ingest:
   ```bash
   cd scraper && source .venv/bin/activate
   DATABASE_URL=sqlite:///$(pwd)/../data/fcxc_stats.db \
     python scraper.py --sources ../sources/meets.yaml
   ```

## Notes

- The downloader has both new (named `--season/--meet/--name`) and legacy
  positional (`download_page.py "<url>" "<output.html>"`) arguments.
- After ingesting new data locally, the SQLite file can be copied to the server
  (see `06-deployment-and-cross-workspace.md`).