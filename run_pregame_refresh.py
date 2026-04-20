"""
Pre-Game Article Refresh — runs at 6:20 PM ET
Re-scrapes injuries, rebuilds projections, regenerates the article,
and pushes to GitHub so the live site reflects late-breaking news.
"""
import subprocess
import sys
import os
import time
from datetime import datetime

REFRESH_SCRIPTS = [
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
    ("sync_to_postgres.py", "Sync Pipeline Data to PostgreSQL"),
]

def run_script(script_name, description):
    print(f"\n{'='*50}")
    print(f"Running: {description}")
    print(f"{'='*50}", flush=True)

    parts = script_name.split()
    cmd = [sys.executable, "-u"] + parts

    timeout = 600

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

def main():
    print("=" * 50)
    print("PRE-GAME ARTICLE REFRESH (6:20 PM ET)")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    success_count = 0
    fail_count = 0

    for script, description in REFRESH_SCRIPTS:
        if run_script(script, description):
            success_count += 1
        else:
            fail_count += 1

    print("\n" + "=" * 50)
    print(f"Pre-Game Refresh Complete: {success_count} succeeded, {fail_count} failed")
    print("=" * 50)

    push_to_github()

if __name__ == "__main__":
    main()
