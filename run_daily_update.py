import subprocess
import sys
import os
import json
import urllib.request
from datetime import datetime

NBA_COM_SCRIPTS = {
    "scrape_nba_gamelogs.py",
    "scrape_shot_zones.py",
    "scrape_team_defense_zones.py",
    "scrape_play_types.py",
}

SCRIPTS = [
    ("scrape_player_salaries.py", "Player Salaries"),
    ("scrape_depth_charts.py", "Depth Charts"),
    ("scrape_game_odds.py", "Game Odds"),
    ("scrape_dvp.py", "DVP Stats"),
    ("scrape_per100_stats.py", "Per-100 Possession Stats"),
    ("scrape_referee_stats.py", "Referee Stats"),
    ("scrape_referee_assignments.py", "Referee Assignments"),
    ("scrape_injury_alerts.py", "Injury Alerts (RotoGrinders)"),
    ("scrape_espn_injuries.py", "Injury Alerts (ESPN Backup)"),
    ("manual_injuries.py sync", "Manual Injury Overrides"),
    ("scrape_nba_gamelogs.py", "NBA Game Logs (Volatility)"),
    ("scrape_standings.py", "Team Standings"),
    ("etl_referee_stats_agg.py", "Referee Stats Aggregation"),
    ("etl_game_foul_environment.py", "Game Foul Environment"),
    ("detect_rotation_changes.py", "Rotation Detection"),
    ("scrape_shot_zones.py", "Shot Zones, Creation, Hustle & Tracking Stats"),
    ("scrape_team_defense_zones.py", "Team Defensive Shot Zones"),
    ("scrape_play_types.py", "Team Play Type Schemes (Synergy)"),
    ("scrape_measurements.py", "Player Physical Measurements"),
    ("build_player_archetypes.py", "Player Archetype Classification"),
    ("build_dva.py", "Defense vs Archetype (DVA)"),
    ("matchup_engine.py", "Matchup Interaction Layer"),
    ("dfs_players.py", "DFS Player Projections"),
    ("scrape_player_props.py", "Player Prop Odds (The Odds API)"),
    ("scrape_fta_ownership.py", "FTA Ownership Projections"),
    ("analysis/player_value.py", "Player Value Analysis"),
    ("estimate_ownership.py --iterations 500 --update-calibration", "Ownership Estimation (with FTA calibration)"),
    ("generate_house_lineup.py --force", "House Lineup Generation"),
    ("score_contest.py --update-factors", "Score Yesterday's Contest + Update ML Factors"),
    ("sync_to_postgres.py", "Sync Pipeline Data to PostgreSQL"),
]

def run_script(script_name, description):
    print(f"\n{'='*50}")
    print(f"Running: {description}")
    print(f"{'='*50}", flush=True)
    
    parts = script_name.split()
    base_script = parts[0]
    cmd = [sys.executable, "-u"] + parts
    
    timeout = 300 if base_script in NBA_COM_SCRIPTS else 600
    
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

def main():
    print("="*50)
    print("NBA DFS Daily Update")
    print("="*50)
    
    from utils.nba_api_helpers import reset_circuit, get_circuit_info
    reset_circuit()
    
    success_count = 0
    fail_count = 0
    
    for script, description in SCRIPTS:
        if run_script(script, description):
            success_count += 1
        else:
            fail_count += 1
    
    circuit_info = get_circuit_info()
    if circuit_info and circuit_info.get('tripped'):
        print(f"\nCIRCUIT BREAKER: NBA.com was unreachable (tripped by: {circuit_info.get('tripped_by', 'unknown')})")
        print("All subsequent NBA.com calls used cached data.")
    
    print("\n" + "="*50)
    print(f"Daily Update Complete: {success_count} succeeded, {fail_count} failed")
    print("="*50)
    
    push_to_github()

def get_github_token():
    hostname = os.environ.get("REPLIT_CONNECTORS_HOSTNAME", "")
    identity = os.environ.get("REPL_IDENTITY", "")
    if not hostname or not identity:
        return None
    
    url = f"https://{hostname}/api/v2/connection?include_secrets=true&connector_names=github"
    req = urllib.request.Request(url, headers={
        "Accept": "application/json",
        "X-Replit-Token": f"repl {identity}"
    })
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            items = data.get("items", [])
            if items:
                return items[0]["settings"]["access_token"]
    except Exception as e:
        print(f"  Could not retrieve GitHub token: {e}")
    return None

PIPELINE_OUTPUT_FILES = [
    "dfs_players.csv",
    "dfs_players_valued.csv",
    "ownership_projections.csv",
    "prop_recommendations.csv",
    "targeted_plays.csv",
    "static/images/dvp_heatmap.png",
    "static/images/ref_foul_chart.png",
    "static/images/upside_chart.png",
    "static/images/value_chart.png",
]

def push_to_github():
    print(f"\n{'='*50}")
    print("Auto-Push to GitHub")
    print(f"{'='*50}")
    
    token = get_github_token()
    if not token:
        print("  SKIPPED: GitHub token not available")
        return
    
    today = datetime.now().strftime("%Y-%m-%d")
    remote_url = f"https://x-access-token:{token}@github.com/rakimr/PIRTDICA.git"
    
    existing = [f for f in PIPELINE_OUTPUT_FILES if os.path.exists(os.path.join("/home/runner/workspace", f))]
    if not existing:
        print("  No pipeline output files found to push")
        return
    
    add_files = " ".join(existing)
    cmd = (
        f'cd /home/runner/workspace && '
        f'rm -f .git/index.lock && '
        f'git add {add_files} && '
        f'git diff --cached --quiet && echo "NO_CHANGES" || '
        f'(git commit -m "Daily pipeline update {today}" && '
        f'git push {remote_url} main --no-thin 2>&1 && echo "PUSH_OK")'
    )
    
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=90,
            env={**os.environ}
        )
        output = result.stdout.strip()
        
        if "NO_CHANGES" in output:
            print("  No changes to push — GitHub is up to date")
        elif "PUSH_OK" in output:
            print(f"  Pushed daily update for {today} to GitHub")
        else:
            stderr = result.stderr.strip() if result.stderr else ""
            safe_out = output[-200:] if output else ""
            safe_err = stderr[-200:] if stderr else ""
            for s in [safe_out, safe_err]:
                if "x-access-token" in s:
                    s = "[output redacted — contains token]"
            print(f"  Push result: {safe_out or safe_err}")
    except subprocess.TimeoutExpired:
        print("  Push timed out — will retry next run")
    except Exception as e:
        print(f"  Push error: {e}")

if __name__ == "__main__":
    main()
