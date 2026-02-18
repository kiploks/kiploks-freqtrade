@echo off
REM Run Kiploks integration INSIDE the Freqtrade container (Freqtrade only in Docker).
REM Run from project root (where docker-compose.yml and user_data\ are).
cd /d "%~dp0.."
docker compose run --rm --entrypoint python -v "%cd%":/app -w /app freqtrade kiploks-freqtrade/run.py %*
