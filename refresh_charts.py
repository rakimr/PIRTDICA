"""
Lightweight Chart Gallery refresh — keeps the static chart images current
between pipeline runs. Re-scrapes time-sensitive data (referees, injuries,
game odds) and regenerates ONLY the gallery PNGs (no projection rebuild,
no recommendation CSV rewrites). Pushes to GitHub only when chart bytes
actually changed.

Coordination: respects PIPELINE_LOCK_FILE so it won't clash with the full
pre-game / post-game pipelines if they happen to overlap.

Designed to be called hourly by scheduler_charts.py.
"""
import subprocess
import sys
import os
import hashlib
import time
import sqlite3
from datetime import datetime

WORKSPACE = "/home/runner/workspace"
PIPELINE_LOCK_FILE = os.path.join(WORKSPACE, ".pipeline.lock")
CHART_LOCK_FILE = os.path.join(WORKSPACE, ".chart_refresh.lock")

DATA_SCRIPTS = [
    ("scrape_injury_alerts.py", "Injury Alerts (RotoGrinders)"),
    ("scrape_espn_injuries.py", "Injury Alerts (ESPN Backup)"),
    ("scrape_referee_assignments.py", "Referee Assignments"),
    ("etl_game_foul_environment.py", "Game Foul Environment"),
    ("scrape_game_odds.py", "Game Odds"),
]

CHART_FILES = [
    "static/images/value_chart.png",
    "static/images/upside_chart.png",
    "static/images/dvp_heatmap.png",
    "static/images/ref_foul_chart.png",
    "static/images/wnba_value_chart.png",
    "static/images/wnba_upside_chart.png",
    "static/images/wnba_dvp_heatmap.png",
]


def file_hash(path):
    if not os.path.exists(path):
        return None
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def chart_hashes():
    return {f: file_hash(os.path.join(WORKSPACE, f)) for f in CHART_FILES}


def wait_for_pipeline_lock(max_wait_secs=1800):
    """If a full pipeline is running, wait up to max_wait_secs for it to finish."""
    if not os.path.exists(PIPELINE_LOCK_FILE):
        return True
    print(f"  Pipeline lock present — waiting up to {max_wait_secs}s...", flush=True)
    waited = 0
    while waited < max_wait_secs:
        if not os.path.exists(PIPELINE_LOCK_FILE):
            print(f"  Pipeline lock cleared after {waited}s. Proceeding.", flush=True)
            return True
        time.sleep(15)
        waited += 15
    print(f"  Pipeline lock still held after {max_wait_secs}s. Skipping this cycle.", flush=True)
    return False


def acquire_chart_lock():
    if os.path.exists(CHART_LOCK_FILE):
        try:
            mtime = os.path.getmtime(CHART_LOCK_FILE)
            age = time.time() - mtime
            if age < 1800:
                print(f"  Another chart refresh is running (lock age {age:.0f}s). Skipping.", flush=True)
                return False
            print(f"  Stale chart lock (age {age:.0f}s) — overwriting.", flush=True)
        except OSError:
            pass
    with open(CHART_LOCK_FILE, "w") as f:
        f.write(f"{os.getpid()}\n{datetime.now().isoformat()}\n")
    return True


def release_chart_lock():
    try:
        os.remove(CHART_LOCK_FILE)
    except OSError:
        pass


def run_script(script, description, timeout=240):
    print(f"\n  -> {description}", flush=True)
    parts = script.split()
    cmd = [sys.executable, "-u"] + parts
    try:
        result = subprocess.run(
            cmd,
            cwd=WORKSPACE,
            capture_output=True,
            text=True,
            timeout=timeout,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        if result.returncode == 0:
            print(f"     OK", flush=True)
            return True
        tail = (result.stderr or result.stdout or "").splitlines()[-5:]
        print(f"     FAILED (exit {result.returncode})", flush=True)
        for line in tail:
            print(f"     {line}", flush=True)
        return False
    except subprocess.TimeoutExpired:
        print(f"     TIMEOUT after {timeout}s", flush=True)
        return False
    except Exception as e:
        print(f"     ERROR: {e}", flush=True)
        return False


def regenerate_charts():
    """Call chart-generating functions DIRECTLY — does not rewrite recommendation CSVs."""
    print("\n  -> Chart Regeneration (charts-only)", flush=True)
    try:
        sys.path.insert(0, WORKSPACE)
        os.chdir(WORKSPACE)
        import pandas as pd
        from analysis.player_value import (
            calculate_value_metrics,
            generate_value_chart,
            generate_upside_chart,
            generate_dvp_heatmap,
            generate_ref_foul_chart,
        )

        try:
            players_df = pd.read_csv(os.path.join(WORKSPACE, "dfs_players.csv"))
            players_df = calculate_value_metrics(players_df)
            generate_value_chart(players_df)
            generate_upside_chart(players_df)
            print(f"     value_chart + upside_chart OK", flush=True)
        except Exception as e:
            print(f"     value/upside FAILED: {e}", flush=True)

        try:
            conn = sqlite3.connect(os.path.join(WORKSPACE, "dfs_nba.db"))
            dvp_df = pd.read_sql_query("SELECT * FROM dvp_blended", conn)
            conn.close()
            generate_dvp_heatmap(dvp_df)
            print(f"     dvp_heatmap OK", flush=True)
        except Exception as e:
            print(f"     dvp_heatmap FAILED: {e}", flush=True)

        try:
            generate_ref_foul_chart()
            print(f"     ref_foul_chart OK", flush=True)
        except Exception as e:
            print(f"     ref_foul_chart FAILED: {e}", flush=True)

        try:
            import generate_wnba_charts as wnba_charts
            wnba_df = wnba_charts._load_value()
            wnba_charts.generate_value_chart(wnba_df)
            wnba_charts.generate_upside_chart(wnba_df)
            wnba_charts.generate_dvp_heatmap()
            print(f"     wnba value/upside/dvp OK", flush=True)
        except Exception as e:
            print(f"     wnba charts FAILED: {e}", flush=True)

        return True
    except Exception as e:
        print(f"     Chart regeneration setup failed: {e}", flush=True)
        return False


def main():
    start = datetime.now()
    print("=" * 50)
    print(f"CHART REFRESH @ {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50, flush=True)

    if not wait_for_pipeline_lock():
        return

    if not acquire_chart_lock():
        return

    try:
        before = chart_hashes()

        failed = 0
        for script, desc in DATA_SCRIPTS:
            if not run_script(script, desc):
                failed += 1

        regenerate_charts()

        after = chart_hashes()
        changed = [f for f in CHART_FILES if before.get(f) != after.get(f)]

        print(f"\n  Scrapers: {len(DATA_SCRIPTS) - failed} OK, {failed} failed")
        print(f"  Charts changed: {len(changed)}/{len(CHART_FILES)}", flush=True)
        for f in changed:
            print(f"    - {f}")

        if changed:
            print("\n  Pushing updated charts to GitHub...", flush=True)
            run_script("push_to_github.py", "Push to GitHub", timeout=180)
        else:
            print("\n  No chart changes — skipping push.", flush=True)

        elapsed = (datetime.now() - start).total_seconds()
        print(f"\nChart refresh done in {elapsed:.0f}s", flush=True)
    finally:
        release_chart_lock()


if __name__ == "__main__":
    main()
