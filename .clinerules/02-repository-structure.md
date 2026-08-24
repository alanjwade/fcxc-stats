# Repository Structure & Sources of Truth

## Layout

```
fcxc_stats/
├── sources/               # ★ Canonical data organization
│   ├── meets.yaml         #   Source of truth for race metadata (add races here)
│   ├── pages/             #   Raw downloaded MileSplit pages (HTML/TXT), gitignored
│   │   └── <season>/<meet_slug>/<name>.html
│   └── archive/           #   Past seasons archive
├── scraper/               # Standalone scraping module (local venv)
│   ├── parsers/           # ★ Modular per-format parsers
│   │   ├── base.py        #   BaseParser + ParsedResult + auto-registry
│   │   ├── default_parser.py
│   │   └── <meet>_*.py    #   One parser per distinct page format
│   ├── scraper.py         #   Main scraping logic
│   ├── download_page.py   #   Playwright downloader for JS-rendered pages
│   ├── requirements.txt
│   └── .venv/             #   venv (gitignored)
├── webapp/                # Flask web app (Docker)
│   ├── app.py             #   Main Flask application
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── templates/         #   HTML templates
│   └── static/            #   CSS/favicons/images
├── database/
│   ├── init.sql           #   SQLite schema
│   └── migrations/        #   Versioned schema migrations (e.g. 001_*.sql)
├── config/
│   └── races.yaml         #   LEGACY config — migrated to sources/meets.yaml
├── data/                  #   SQLite db shared by scraper & webapp (gitignored)
├── homelab-deployment/    #   The 3 files that mirror into homelab-infra
│   ├── docker-compose.yml
│   ├── .env.example
│   └── backup.yml
├── docker-compose*.yml    #   compose variants (see 06)
├── bump_version_homelab01.sh
├── deploy_homelab01.sh
└── run_dev.sh
```

## Sources of truth — read these first

- **`sources/meets.yaml`** — canonical, single source of truth for race
  metadata. New races go here. **Do not add new races to `config/races.yaml`**;
  that file is legacy.
- **`scraper/parsers/base.py`** — defines `ParsedResult`, `BaseParser`, auto-
  registry, time parsing, and school-name normalization. New parsers build on it.
- **`database/*.sql`** — the schema and any schema changes.
- **`homelab-deployment/docker-compose.yml`** — the current pinned image tag is
  the deployed version source of truth on the app side.

## Gitignored (never commit)

`.env`, `data/*.db`, `scraper/pages/` + `*.html`, virtual environments
(`.venv/`, `venv/`), and local scratch test files (`test_*.py`).