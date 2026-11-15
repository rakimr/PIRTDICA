"""
Pre-Game Article Refresh — scheduled relative to each tipoff
Re-scrapes injuries, rebuilds projections, regenerates the article,
and syncs directly to the serving database, without requiring a GitHub backup.
"""
import subprocess
import sys
import os
import time
from datetime import datetime
RUN_DEADLINE = None

REFRESH_SCRIPTS = [
    ("scrape_nba_rosters.py", "Current NBA Player Teams (ESPN)"),
    # Salary scrape must run first so the slate (game_time, players, teams)
    # exists before downstream steps depend on it. Without this, production
    # never re-scrapes the daily FanDuel slate and player_salaries_live goes
    # stale (the symptom: home page shows "no contests today" even when
    # games are scheduled).
    ("scrape_player_salaries.py", "Player Salaries (FanDuel via RotoGrinders)"),
    ("scrape_standings.py", "NBA Standings"),
    ("scrape_wnba_standings.py", "WNBA Standings"),
    ("scrape_injury_alerts.py", "Injury Alerts (RotoGrinders)"),
    ("scrape_espn_injuries.py", "Injury Alerts (ESPN Backup)"),
    ("scrape_depth_charts.py", "Depth Charts (Rotation/Injury)"),
    ("manual_injuries.py sync", "Manual Injury Overrides"),
    ("detect_rotation_changes.py", "Rotation Detection"),
    ("scrape_game_odds.py", "Game Odds (Line Movements)"),
    ("scrape_player_props.py --force", "Player Prop Odds (The Odds API)"),
    ("scrape_referee_assignments.py", "Referee Assignments"),
    ("etl_game_foul_environment.py", "Game Foul Environment"),
    ("build_player_archetypes.py", "Player Archetype Classification"),
    ("build_dva.py", "Defense vs Archetype (DVA)"),
    ("matchup_engine.py", "Matchup Interaction Layer"),
    ("dfs_players.py", "DFS Player Projections"),
    ("analysis/player_value.py", "Player Value Analysis"),
    ("generate_house_lineup.py --force", "House Lineup Generation"),
    ("generate_article.py", "Generate Daily Article"),
    # WNBA pre-game refresh: re-scrape props/projections and regenerate the WNBA
    # article inside the T-60 wave so the official-call lock actually fires 60 min
    # before tip (previously generate_wnba_article only ran in the ~1 AM daily
    # update, so its lock window around tip-off was never hit). No-ops cleanly out
    # of WNBA season when there are no games on the slate.
    ("scrape_wnba_props.py --force", "WNBA Games & Props (The Odds API)"),
    ("build_wnba_projections.py", "WNBA Projections & Prop Recommendations"),
    ("build_wnba_slump_risk.py", "WNBA Slump Risk Engine (cool-off warning signals)"),
    ("generate_wnba_article.py", "Generate WNBA Daily Article"),
    ("sync_to_postgres.py", "Sync Pipeline Data to PostgreSQL"),
]

def run_script(script_name, description):
    print(f"\n{'='*50}")
    print(f"Running: {description}")
    print(f"{'='*50}", flush=True)

    parts = script_name.split()
    cmd = [sys.executable, "-u"] + parts

    timeout = 600
    if RUN_DEADLINE is not None:
        timeout = min(timeout, int(RUN_DEADLINE - time.monotonic()))
        if timeout <= 0:
            print("Pre-tipoff publication budget exhausted; leaving refresh pending.")
            return False

    try:
        result = subprocess.run(
            cmd,
            text=True,
            timeout=timeout,
            env={**os.environ, "PYTHONUNBUFFERED": "1"}
        )
    except subprocess.TimeoutExpired:
        print(f"TIMEOUT: {script_name} exceeded {timeout}s limit - skipping")
        return False

    if result.returncode != 0:
        print(f"ERROR in {script_name} (exit code {result.returncode})")
        return False

    return True

def push_to_github():
    print(f"\n{'='*50}")
    print("Auto-Push to GitHub (Pre-Game Refresh)")
    print(f"{'='*50}")
    try:
        date_str = datetime.now().strftime("%b %d")
        result = subprocess.run(
            [sys.executable, "push_to_github.py", f"Pre-game refresh — {date_str} 6:20 PM"],
            cwd="/home/runner/workspace",
            capture_output=True, text=True, timeout=120
        )
        output = result.stdout.strip()
        if output:
            for line in output.split('\n'):
                if line.strip() and not line.startswith('='):
                    print(f"  {line.strip()}")
        if result.returncode != 0 and result.stderr:
            safe_err = result.stderr.strip()[-200:]
            if "x-access-token" in safe_err or "Bearer" in safe_err:
                safe_err = "[error redacted — contains token]"
            print(f"  Push error: {safe_err}")
    except subprocess.TimeoutExpired:
        print("  Push timed out — will retry next run")
    except Exception as e:
        print(f"  Push error: {e}")

PIPELINE_LOCK_FILE = "/home/runner/workspace/.pipeline.lock"
RESEARCH_LOCK_FILE = "/tmp/pirtdica_recommendation_refresh.lock"
QUALITY_PATH = "/tmp/pirtdica_pregame_quality.json"


def main():
    global RUN_DEADLINE
    RUN_DEADLINE = time.monotonic() + 12 * 60
    from scheduler_pregame import get_all_game_times_today_et, get_et_now
    started = get_et_now()
    next_tips = [t for t in get_all_game_times_today_et() if t > started]
    next_tip = min(next_tips) if next_tips else None
    if next_tip:
        available = (next_tip - started).total_seconds() - 15 * 60
        if available <= 0:
            print("Inside the pre-tipoff publication deadline; no pregame rewrite started.")
            return 1
        RUN_DEADLINE = min(RUN_DEADLINE, time.monotonic() + available)
    print("=" * 50)
    print("PRE-GAME ARTICLE REFRESH")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    import fcntl
    from utils.pregame_quality import audit
    from datetime import timezone
    import json
    # Serialize competing refreshes, and do not overwrite another pipeline's marker.
    lock = open(RESEARCH_LOCK_FILE, "a")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock.close()
        print("Refresh already running; scheduler will retry.")
        return 1
    if os.path.exists(PIPELINE_LOCK_FILE):
        lock.close()
        print("Full pipeline is running; scheduler will retry.")
        return 1
    with open(PIPELINE_LOCK_FILE, "w") as lf:
        lf.write(f"pregame {os.getpid()} {datetime.now().isoformat()}\n")

    success_count = 0
    fail_count = 0

    try:
        # These daily computations must not delay live quotes and publication.
        deferred = {
            "scrape_standings.py", "scrape_wnba_standings.py",
            "build_player_archetypes.py", "build_dva.py", "generate_house_lineup.py --force",
        }
        steps = [(s, d) for s, d in REFRESH_SCRIPTS if s not in deferred]
        # Rotation modelling consumes current odds; fetch those before modelling.
        odds = next(step for step in steps if step[0] == "scrape_game_odds.py")
        steps.remove(odds)
        steps.insert(0, odds)
        props = next(step for step in steps if step[0] == "scrape_player_props.py --force")
        steps.remove(props)
        steps.insert(next(i for i, step in enumerate(steps) if step[0] == "detect_rotation_changes.py"), props)
        # Refresh real current-season baselines, not heavyweight game-log/chart jobs.
        steps.insert(next(i for i, step in enumerate(steps) if step[0] == "detect_rotation_changes.py"),
                     ("scrape_per100_stats.py", "Current-season minutes and usage"))
        has_nba = True
        report = None
        for script, description in steps:
            if not has_nba and "wnba" not in script and script != "sync_to_postgres.py":
                continue
            if script == "detect_rotation_changes.py":
                report = audit()
                print("[pregame_quality] " + json.dumps(report), flush=True)
                with open(QUALITY_PATH, "w") as f:
                    json.dump(report, f)
                if report["errors"]:
                    print("Critical sources incomplete; preserving published analysis.")
                    return 1
            if run_script(script, description):
                success_count += 1
                if script == "scrape_game_odds.py":
                    import sqlite3
                    with sqlite3.connect("dfs_nba.db") as db:
                        has_nba = db.execute("SELECT COUNT(*) FROM game_odds").fetchone()[0] > 0
            else:
                fail_count += 1
                # RotoGrinders has an independent ESPN backup. Everything else
                # on the publication path must succeed before downstream writes.
                if script != "scrape_injury_alerts.py":
                    print(f"Publication stopped at {script}; no success marker recorded.")
                    return 1

        print("\n" + "=" * 50)
        print(f"Pre-Game Refresh Complete: {success_count} succeeded, {fail_count} failed")
        print("=" * 50)

        print("[pregame_summary] " + json.dumps({
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "started_at": started.isoformat(),
            "next_tip": next_tip.isoformat() if next_tip else None,
            "minutes_before_tip": round((next_tip - get_et_now()).total_seconds() / 60, 1) if next_tip else None,
            "successful_steps": success_count, "failed_steps": fail_count,
            "source_quality": report["status"] if report else "no_nba_slate",
        }), flush=True)
        # Preserve the ancillary services, but never let them delay the article.
        # Leave a budget for the tail's final sync; daily refresh remains its
        # fallback if the deadline is too close.
        tail_changed = False
        for script, description in REFRESH_SCRIPTS:
            if script not in deferred or (not has_nba and "wnba" not in script):
                continue
            if RUN_DEADLINE - time.monotonic() < 180:
                print(f"Deferred to daily refresh: {description} (publication deadline)")
                continue
            tail_changed = run_script(script, description) or tail_changed
        if tail_changed and RUN_DEADLINE - time.monotonic() > 120:
            if not run_script("sync_to_postgres.py", "Publish ancillary refresh"):
                print("Ancillary sync incomplete; primary article refresh already published.")
        return 0
    finally:
        try:
            os.remove(PIPELINE_LOCK_FILE)
        except OSError:
            pass
        lock.close()

if __name__ == "__main__":
    sys.exit(main())
