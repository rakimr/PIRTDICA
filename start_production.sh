#!/usr/bin/env bash
set -uo pipefail

cd "$(dirname "$0")"

LOG_DIR="/tmp/pirtdica_logs"
mkdir -p "$LOG_DIR"

# How long to wait before restarting a scheduler that exited/crashed.
RESTART_DELAY="${SCHEDULER_RESTART_DELAY:-15}"

# Web port (prod default 5000; overridable for local testing).
PORT="${PORT:-5000}"

# Max seconds to wait for the web tier to answer before starting schedulers
# anyway. The gate exists to keep boot CPU free for uvicorn so the
# deployment readiness probe passes quickly; it must never permanently
# block the schedulers.
WEB_READY_TIMEOUT="${WEB_READY_TIMEOUT:-180}"
# Harden: must be a non-negative integer — anything else (e.g. "180s") would
# crash the (( )) arithmetic under set -u and the schedulers would never
# start. Fall back to the default instead.
case "$WEB_READY_TIMEOUT" in
    ''|*[!0-9]*)
        echo "!! invalid WEB_READY_TIMEOUT='$WEB_READY_TIMEOUT' — using 180"
        WEB_READY_TIMEOUT=180
        ;;
esac

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
        # reaches every member of the supervisor|python|awk|tee pipeline (not
        # just the subshell). Falls back to plain TERM if pgroup signaling
        # fails (e.g. for the uvicorn process which isn't in a subshell).
        kill -TERM -"$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
    done
    wait
    exit 0
}
trap cleanup SIGTERM SIGINT

# Job-control / monitor mode: each `( ... ) &` gets its own process group,
# which makes the cleanup trap above able to kill the full pipeline cleanly.
set -m

# start_bg runs a scheduler under a SUPERVISOR loop. If the scheduler exits or
# crashes, it is restarted after RESTART_DELAY rather than taking the whole
# deployment down with it. This is the critical reason the web tier (uvicorn)
# is decoupled from the schedulers: a single scraper hiccup during the
# deployment readiness probe must NOT kill the web server (which previously
# happened with a shared `wait -n` and caused publishes to fail the health
# check / never become ready).
start_bg() {
    local name="$1"
    local script="$2"
    echo "  -> launching $name ($script) [supervised]"
    (
        # On shutdown, exit the supervisor promptly instead of restarting.
        trap 'exit 0' SIGTERM SIGINT
        while true; do
            # Tee to both stdout (deployment logs) AND a local file so we can
            # debug remotely via fetch_deployment_logs OR the admin
            # scheduler-status panel. Each line is prefixed with [<name>] so
            # the different schedulers can be told apart in the combined log.
            python -u "$script" 2>&1 | awk -v tag="[$name]" '{ print tag, $0; fflush(); }' | tee -a "$LOG_DIR/${name}.log"
            rc=$?
            echo "[$name] process exited (rc=$rc) — restarting in ${RESTART_DELAY}s" | tee -a "$LOG_DIR/${name}.log"
            sleep "$RESTART_DELAY"
        done
    ) &
    local pid=$!
    PIDS+=("$pid")
    echo "     pid $pid (logs: $LOG_DIR/${name}.log)"
}

# BOOT ORDER MATTERS: uvicorn starts FIRST and must be answering HTTP before
# any scheduler is allowed to boot. Previously all 4 schedulers launched
# before the web app; five python processes cold-importing pandas on the
# 0.5-vCPU VM pushed readiness to ~2m45s and the 2026-08-07 publish failed
# the health probe outright, taking the site down. Schedulers can wait a
# minute; the readiness probe cannot.
echo "  -> launching web app (uvicorn :$PORT)"
python -u -m uvicorn backend.main:app \
    --host 0.0.0.0 \
    --port "$PORT" \
    --workers 1 \
    --proxy-headers \
    --forwarded-allow-ips '*' &
WEB_PID=$!
PIDS+=("$WEB_PID")
echo "     pid $WEB_PID"

# Gate: wait until the web tier answers 200 on / (same path the platform
# probes), a bounded wait so schedulers always start eventually.
web_gate_start=$(date +%s)
if command -v curl >/dev/null 2>&1; then
    echo "  -> waiting for web tier to answer on :$PORT (max ${WEB_READY_TIMEOUT}s)..."
    while true; do
        if ! kill -0 "$WEB_PID" 2>/dev/null; then
            echo "  !! web tier died during boot — skipping scheduler launch; supervisor will exit for platform restart"
            break
        fi
        if curl -sf -o /dev/null --max-time 5 "http://127.0.0.1:$PORT/"; then
            echo "  -> web tier is up ($(( $(date +%s) - web_gate_start ))s) — starting schedulers"
            break
        fi
        if (( $(date +%s) - web_gate_start >= WEB_READY_TIMEOUT )); then
            echo "  !! web tier not confirmed after ${WEB_READY_TIMEOUT}s — starting schedulers anyway"
            break
        fi
        sleep 2
    done
else
    echo "  !! curl not found — falling back to fixed 45s head start for the web tier"
    # 1s increments so a SIGTERM trap fires promptly and a dead web tier
    # cuts the wait short (matching the curl path's behavior).
    for _ in $(seq 1 45); do
        kill -0 "$WEB_PID" 2>/dev/null || break
        sleep 1
    done
fi

if kill -0 "$WEB_PID" 2>/dev/null; then
    start_bg "chart_refresh"     "scheduler_charts.py"
    start_bg "pregame_refresh"   "scheduler_pregame.py"
    start_bg "postgame_pipeline" "scheduler_postgame.py"
    start_bg "props_refresh"     "scheduler_props.py"
fi

echo "All processes launched. Waiting on web tier (pid $WEB_PID)..."
# The deployment's health is tied to the WEB SERVER only. Schedulers are
# supervised above and restart themselves, so they can never bring the
# deployment down. If uvicorn itself exits, the deployment has lost its web
# tier and there is nothing left to serve, so we shut everything down and let
# the platform restart the VM.
wait "$WEB_PID"
echo "Web tier (uvicorn) exited. Shutting down the rest..."
cleanup
