"""
Scrape all player game logs from NBA.com stats API.
Calculates minutes standard deviation for volatility analysis.
"""

from nba_api.stats.endpoints import leaguegamelog
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

def scrape_gamelogs():
    print("Fetching all player game logs from NBA.com...")
    
    gamelog = leaguegamelog.LeagueGameLog(
        season='2025-26',
        player_or_team_abbreviation='P',
        season_type_all_star='Regular Season'
    )
    df = gamelog.get_data_frames()[0]
    print(f"Got {len(df)} game log entries for {df['PLAYER_NAME'].nunique()} players")
    
    df = df[df['MIN'] > 0]
    print(f"After filtering DNPs: {len(df)} entries")
    
    stats = df.groupby('PLAYER_NAME').agg(
        games_played=('MIN', 'count'),
        avg_min=('MIN', 'mean'),
        min_sd=('MIN', 'std')
    ).reset_index()
    
    stats = stats.rename(columns={'PLAYER_NAME': 'player_name'})
    stats['min_sd'] = stats['min_sd'].fillna(10.0)
    stats['min_sd'] = stats['min_sd'].round(2)
    stats['avg_min'] = stats['avg_min'].round(2)
    
    max_games = 50
    def calc_omega(row):
        gp = min(row['games_played'] / max_games, 1.0)
        sd = row['min_sd']
        sd_factor = max(0, min(1, 1 - (sd - 3) / 7))
        return round(max(0.10, min(0.90, (gp * 0.5) + (sd_factor * 0.5))), 3)
    
    stats['omega'] = stats.apply(calc_omega, axis=1)
    stats['scraped_at'] = datetime.now().isoformat()
    
    conn = sqlite3.connect('dfs_nba.db')
    stats.to_sql('player_volatility', conn, if_exists='replace', index=False)
    conn.close()
    
    print(f"Saved volatility data for {len(stats)} players.")
    return stats

if __name__ == "__main__":
    stats = scrape_gamelogs()
    print("\n=== Most Stable (Low SD) ===")
    print(stats.nsmallest(5, 'min_sd')[['player_name', 'games_played', 'min_sd']].to_string(index=False))
    print("\n=== Most Volatile (High SD) ===")
    print(stats.nlargest(5, 'min_sd')[['player_name', 'games_played', 'min_sd']].to_string(index=False))
