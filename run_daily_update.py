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
    ("etl_referee_stats_agg.py", "Referee Stats Aggregation"),
    ("etl_game_foul_environment.py", "Game Foul Environment"),
    ("detect_rotation_changes.py", "Rotation Detection"),
    ("dfs_players.py", "DFS Player Projections"),
]

def run_script(script_name, description):
    print(f"\n{'='*50}")
    print(f"Running: {description}")
    print(f"{'='*50}")
    
    result = subprocess.run(
        [sys.executable, script_name],
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
