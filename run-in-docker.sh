#!/usr/bin/env bash
# Run Kiploks integration INSIDE the Freqtrade container (so Freqtrade is available for OHLCV/DQG).
# Use this when Freqtrade is only in Docker and you have no host venv.
# Run from project root (where docker-compose.yml and user_data/ are).
set -e
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")
cd "$PROJECT_ROOT"
exec docker compose run --rm --entrypoint python -v "$(pwd)":/app -w /app freqtrade kiploks-freqtrade/run.py "$@"
