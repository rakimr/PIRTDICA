import subprocess
import sys

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
    ("build_player_archetypes.py", "Player Archetype Classification"),
    ("build_dva.py", "Defense vs Archetype (DVA)"),
    ("dfs_players.py", "DFS Player Projections"),
    ("scrape_player_props.py", "Player Prop Odds (The Odds API)"),
    ("scrape_fta_ownership.py", "FTA Ownership Projections"),
    ("analysis/player_value.py", "Player Value Analysis"),
    ("estimate_ownership.py --iterations 500 --update-calibration", "Ownership Estimation (with FTA calibration)"),
    ("generate_house_lineup.py --force", "House Lineup Generation"),
    ("score_contest.py --update-factors", "Score Yesterday's Contest + Update ML Factors"),
]

def run_script(script_name, description):
    print(f"\n{'='*50}")
    print(f"Running: {description}")
    print(f"{'='*50}")
    
    parts = script_name.split()
    cmd = [sys.executable] + parts
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True
    )
    
    if result.stdout:
        print(result.stdout)
    
    if result.returncode != 0:
        print(f"ERROR in {script_name}:")
        print(result.stderr)
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
