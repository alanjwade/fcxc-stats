#!/bin/bash
# Wrapper script to run the fcxc_stats scraper (scraper.py) with the correct
# Python environment, working directory, and database URL — mirroring the
# download_and_add wrapper for the "download -> add to meets.yaml -> scrape"
# workflow.
#
# Usage:
#   ./run_scraper                 # scrape sources/meets.yaml into local DB
#   ./run_scraper --clear-db      # wipe DB first, then scrape
#   ./run_scraper --sources sources/meets.yaml
#
# Environment overrides:
#   DATABASE_URL   DB connection string (defaults to the repo-local SQLite DB)
#   FCXC_SOURCES   Path to the sources file (default: sources/meets.yaml)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_PYTHON="${SCRIPT_DIR}/.venv/bin/python"

if [ ! -f "$VENV_PYTHON" ]; then
    echo "Error: Virtual environment not found at ${VENV_PYTHON}" >&2
    echo "Run: cd scraper && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt" >&2
    exit 1
fi

# Set a sensible database URL if the caller didn't provide one.
if [ -z "$DATABASE_URL" ]; then
    DB_PATH="${PROJECT_ROOT}/data/fcxc_stats.db"
    if [ ! -f "$DB_PATH" ]; then
        echo "Note: No database found at ${DB_PATH}; the scraper will create it." >&2
    fi
    export DATABASE_URL="sqlite:///${DB_PATH}"
fi

# Default to sources/meets.yaml unless the caller passed --sources / --config.
FCXC_SOURCES="${FCXC_SOURCES:-sources/meets.yaml}"
HAVE_SOURCE_FLAG=""
for arg in "$@"; do
    if [ "$arg" = "--sources" ] || [ "$arg" = "--config" ]; then
        HAVE_SOURCE_FLAG="1"
        break
    fi
done

args=("$@")
if [ -z "$HAVE_SOURCE_FLAG" ]; then
    # Resolve relative to the repo root so cwd doesn't matter.
    if [[ "$FCXC_SOURCES" != /* ]]; then
        FCXC_SOURCES="${PROJECT_ROOT}/${FCXC_SOURCES}"
    fi
    args+=(--sources "$FCXC_SOURCES")
fi

# --- validate the sources file before touching the database -------------------
# A bad entry used to be silently mis-filed (a missing `gender:` was defaulted
# to boys). validate_meets.py fails loudly instead. Skipped for the legacy
# --config format, and skippable with --skip-validate (not passed to scraper.py).
VALIDATE_TARGET=""
USE_LEGACY_CONFIG=""
SKIP_VALIDATE=""
prev=""
for arg in "$@"; do
    case "$prev" in
        --sources) VALIDATE_TARGET="$arg" ;;
        --config)  USE_LEGACY_CONFIG="1" ;;
    esac
    case "$arg" in
        --sources=*)     VALIDATE_TARGET="${arg#*=}" ;;
        --config=*)      USE_LEGACY_CONFIG="1" ;;
        --skip-validate) SKIP_VALIDATE="1" ;;
    esac
    prev="$arg"
done
[ -n "$VALIDATE_TARGET" ] || VALIDATE_TARGET="$FCXC_SOURCES"

if [ -z "$SKIP_VALIDATE" ] && [ -z "$USE_LEGACY_CONFIG" ]; then
    echo "Validating ${VALIDATE_TARGET} ..." >&2
    if ! "$VENV_PYTHON" "${SCRIPT_DIR}/validate_meets.py" "$VALIDATE_TARGET" >&2; then
        echo "Refusing to scrape: fix the problems above (or re-run with --skip-validate)." >&2
        exit 1
    fi
fi

# Run from the scraper directory so the `parsers` package is importable.
cd "$SCRIPT_DIR"

# Drop our own --skip-validate flag before handing the rest to scraper.py.
scraper_args=()
for arg in "${args[@]}"; do
    if [ "$arg" != "--skip-validate" ]; then
        scraper_args+=("$arg")
    fi
done

echo "Using DATABASE_URL: ${DATABASE_URL}" >&2
echo "Launching scraper: ${VENV_PYTHON} scraper.py ${scraper_args[*]}" >&2
exec "$VENV_PYTHON" "scraper.py" "${scraper_args[@]}"