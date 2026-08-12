#!/bin/bash
# Run the webapp locally without Docker — for quick development and testing.
#
# Usage:
#   ./run_dev.sh          # Uses data/fcxc_stats.db
#   ./run_dev.sh --debug  # Enables Flask debug mode / auto-reload

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${SCRIPT_DIR}/webapp/.venv"

# Create webapp venv and install dependencies if not already done
if [ ! -f "${VENV_DIR}/bin/python3" ]; then
    echo "Creating webapp virtual environment..."
    python3 -m venv "${VENV_DIR}"
    "${VENV_DIR}/bin/pip" install -q -r "${SCRIPT_DIR}/webapp/requirements.txt"
fi

PYTHON="${VENV_DIR}/bin/python3"
DB_PATH="${SCRIPT_DIR}/data/fcxc_stats.db"

if [ ! -f "${DB_PATH}" ]; then
    echo "Warning: No database found at ${DB_PATH}"
    echo "The app will start but will have no data until you run the scraper."
fi

DEBUG_FLAG=""
if [[ "$1" == "--debug" ]]; then
    DEBUG_FLAG="1"
    echo "Starting webapp in DEBUG mode (auto-reload enabled)..."
else
    echo "Starting webapp..."
    echo "  Tip: use './run_dev.sh --debug' for auto-reload"
fi

echo "  URL: http://localhost:5000"
echo "  DB:  ${DB_PATH}"
echo ""

export DATABASE_URL="sqlite:///${DB_PATH}"
export SECRET_KEY="local-dev-secret-not-for-production"
export FLASK_DEBUG="${DEBUG_FLAG}"

exec "${PYTHON}" "${SCRIPT_DIR}/webapp/app.py"
