import subprocess
import sys
import os

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
    
    timeout = 900 if base_script in NBA_COM_SCRIPTS else 600
    
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
    
    success_count = 0
    fail_count = 0
    
    for script, description in SCRIPTS:
        if run_script(script, description):
            success_count += 1
        else:
            fail_count += 1
    
    print("\n" + "="*50)
    print(f"Daily Update Complete: {success_count} succeeded, {fail_count} failed")
    print("="*50)

if __name__ == "__main__":
    main()
