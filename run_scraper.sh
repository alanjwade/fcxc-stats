#!/usr/bin/env bash
#
# run_scraper.sh  (#1 way to run the fcxc_stats scraper)
#
# "Sets everything up" for you so you never have to think about the working
# directory or which Python to use. Running this is equivalent to:
#
#     cd  <project root>/scraper
#     <project root>/scraper/.venv/bin/python  scraper.py --sources <...>
#
# ...but it figures out all of those paths for you. The scraper expects to run
# from inside the `scraper/` folder (so the `parsers` package is importable),
# which is where the "I can never get the directory/python call right" trouble
# comes from. This script handles it.
#
# Usage (run from ANYWHERE; the project root is found automatically):
#   ./run_scraper.sh                 # scrape sources/meets.yaml into local DB
#   ./run_scraper.sh --clear-db      # wipe the DB first, then scrape
#   ./run_scraper.sh --sources sources/meets.yaml
#   ./run_scraper.sh --help          # show scraper.py's full flag list
#
# Environment overrides:
#   DATABASE_URL   DB connection string (default: sqlite:///<root>/data/fcxc_stats.db)
#   FCXC_SOURCES   Path to the sources YAML (default: sources/meets.yaml, repo root)
#
# If the scraper virtual environment doesn't exist yet, this script creates it
# and installs requirements.txt + Playwright's Chromium automatically, then runs.

set -eu

# --- locate the project root (fcxc_stats), working from any directory ---------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"                 # this file lives at the repo root
SCRAPER_DIR="${PROJECT_ROOT}/scraper"
VENV_PYTHON="${SCRAPER_DIR}/.venv/bin/python"

# --- one-time setup: create the venv if it's missing --------------------------
if [ ! -x "$VENV_PYTHON" ]; then
    echo "Setting up the scraper virtual environment (first run)..." >&2
    python3 -m venv "${SCRAPER_DIR}/.venv"
    "$VENV_PYTHON" -m pip install --upgrade pip >&2
    "$VENV_PYTHON" -m pip install -r "${SCRAPER_DIR}/requirements.txt" >&2
    "$VENV_PYTHON" -m playwright install chromium >&2
    echo "Scraper environment ready at ${SCRAPER_DIR}/.venv" >&2
fi

# --- sensible default database URL --------------------------------------------
if [ -z "${DATABASE_URL:-}" ]; then
    DB_PATH="${PROJECT_ROOT}/data/fcxc_stats.db"
    if [ ! -f "$DB_PATH" ]; then
        echo "Note: No database found at ${DB_PATH}; the scraper will create it." >&2
    fi
    export DATABASE_URL="sqlite:///${DB_PATH}"
fi

# --- default sources file unless the caller passed one ------------------------
FCXC_SOURCES="${FCXC_SOURCES:-sources/meets.yaml}"
HAVE_SOURCE_FLAG=""
for arg in "$@"; do
    if [ "$arg" = "--sources" ] || [ "$arg" = "--config" ]; then
        HAVE_SOURCE_FLAG="1"
        break
    fi
done

args=()
if [ -z "$HAVE_SOURCE_FLAG" ]; then
    # Resolve relative to the repo root so cwd doesn't matter.
    if [[ "$FCXC_SOURCES" != /* ]]; then
        FCXC_SOURCES="${PROJECT_ROOT}/${FCXC_SOURCES}"
    fi
    args+=(--sources "$FCXC_SOURCES")
fi
args+=("$@")

# --- run the scraper from the scraper dir so `parsers` is importable ----------
cd "$SCRAPER_DIR"
echo "Using DATABASE_URL: ${DATABASE_URL}" >&2
echo "Running: ${VENV_PYTHON} scraper.py ${args[*]}" >&2
exec "$VENV_PYTHON" "scraper.py" "${args[@]}"