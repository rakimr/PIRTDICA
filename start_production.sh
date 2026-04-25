#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

LOG_DIR="/tmp/pirtdica_logs"
mkdir -p "$LOG_DIR"

echo "============================================"
echo "PIRTDICA Reserved VM — production launcher"
echo "Started: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "============================================"

start_bg() {
    local name="$1"
    local script="$2"
    echo "  -> launching $name ($script)"
    python -u "$script" >> "$LOG_DIR/${name}.log" 2>&1 &
    echo "     pid $!"
}

start_bg "chart_refresh"     "scheduler_charts.py"
start_bg "pregame_refresh"   "scheduler_pregame.py"
start_bg "postgame_pipeline" "scheduler_postgame.py"

trap 'echo "Shutting down..."; kill $(jobs -p) 2>/dev/null || true; exit 0' SIGTERM SIGINT

echo "  -> launching web app (uvicorn :5000)"
exec python -u -m uvicorn backend.main:app \
    --host 0.0.0.0 \
    --port 5000 \
    --workers 1 \
    --proxy-headers \
    --forwarded-allow-ips '*'
