import sqlite3
import pandas as pd

def get_game_foul_diff(home, away):
    conn = sqlite3.connect("dfs_nba.db")
    query = """
        SELECT *
        FROM game_foul_environment
        WHERE home_team = ?
          AND away_team = ?
    """
    df = pd.read_sql(query, conn, params=[home, away])
    conn.close()
    return df

# Example usage:
print(get_game_foul_diff("DET", "HOU"))