#!/usr/bin/env bash
set -uo pipefail

cd "$(dirname "$0")"

LOG_DIR="/tmp/pirtdica_logs"
mkdir -p "$LOG_DIR"

# Task #34: Schedulers are env-gated so dev workspaces don't double-fire
# alongside this production deployment. The gate is set HERE so the
# deployment is the single source of truth for live scheduler runs.
export SCHEDULERS_ENABLED=1

echo "============================================"
echo "PIRTDICA Reserved VM — production launcher"
echo "Started: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "SCHEDULERS_ENABLED=$SCHEDULERS_ENABLED"
echo "============================================"

PIDS=()

cleanup() {
    echo "Shutting down child processes..."
    for pid in "${PIDS[@]}"; do
        # Negative pid signals the whole process group. With `set -m` below,
        # each backgrounded subshell becomes its own pgroup, so SIGTERM
        # reaches every member of the python|awk|tee pipeline (not just the
        # subshell). Falls back to plain TERM if pgroup signaling fails
        # (e.g. for the uvicorn process which isn't in a subshell).
        kill -TERM -"$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
    done
    wait
    exit 0
}
trap cleanup SIGTERM SIGINT

# Job-control / monitor mode: each `( ... ) &` gets its own process group,
# which makes the cleanup trap above able to kill the full pipeline cleanly.
set -m

start_bg() {
    local name="$1"
    local script="$2"
    echo "  -> launching $name ($script)"
    # Tee to both stdout (deployment logs) AND local file so we can debug
    # remotely via fetch_deployment_logs OR via the admin scheduler-status panel.
    # Each line is prefixed with [<name>] so different schedulers can be told
    # apart in the combined deployment log stream.
    ( python -u "$script" 2>&1 | awk -v tag="[$name]" '{ print tag, $0; fflush(); }' | tee -a "$LOG_DIR/${name}.log" ) &
    local pid=$!
    PIDS+=("$pid")
    echo "     pid $pid (logs: $LOG_DIR/${name}.log)"
}

start_bg "chart_refresh"     "scheduler_charts.py"
start_bg "pregame_refresh"   "scheduler_pregame.py"
start_bg "postgame_pipeline" "scheduler_postgame.py"
start_bg "props_refresh"     "scheduler_props.py"

echo "  -> launching web app (uvicorn :5000)"
python -u -m uvicorn backend.main:app \
    --host 0.0.0.0 \
    --port 5000 \
    --workers 1 \
    --proxy-headers \
    --forwarded-allow-ips '*' &
WEB_PID=$!
PIDS+=("$WEB_PID")
echo "     pid $WEB_PID"

echo "All processes launched. Waiting..."
wait -n
echo "A child process exited unexpectedly. Shutting down the rest..."
cleanup
