"""
Scrape all player game logs from NBA.com stats API.
Calculates minutes SD, fantasy point SD, and volatility metrics.
"""

from nba_api.stats.endpoints import leaguegamelog
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime


def calc_fanduel_fp(row):
    """Calculate FanDuel fantasy points from box score stats."""
    pts = row.get('PTS', 0) or 0
    reb = row.get('REB', 0) or 0
    ast = row.get('AST', 0) or 0
    stl = row.get('STL', 0) or 0
    blk = row.get('BLK', 0) or 0
    tov = row.get('TOV', 0) or 0
    return pts + (reb * 1.2) + (ast * 1.5) + (stl * 3) + (blk * 3) - tov


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
    
    df['FP'] = df.apply(calc_fanduel_fp, axis=1)
    df['FPPM'] = df['FP'] / df['MIN']
    
    stats = df.groupby('PLAYER_NAME').agg(
        games_played=('MIN', 'count'),
        avg_min=('MIN', 'mean'),
        min_sd=('MIN', 'std'),
        avg_fp=('FP', 'mean'),
        fp_sd=('FP', 'std'),
        avg_fppm=('FPPM', 'mean'),
        fppm_sd=('FPPM', 'std'),
        max_fp=('FP', 'max'),
        min_fp=('FP', 'min')
    ).reset_index()
    
    stats = stats.rename(columns={'PLAYER_NAME': 'player_name'})
    stats['min_sd'] = stats['min_sd'].fillna(10.0).round(2)
    stats['avg_min'] = stats['avg_min'].round(2)
    stats['fp_sd'] = stats['fp_sd'].fillna(15.0).round(2)
    stats['avg_fp'] = stats['avg_fp'].round(2)
    stats['avg_fppm'] = stats['avg_fppm'].round(3)
    stats['fppm_sd'] = stats['fppm_sd'].fillna(0.5).round(3)
    stats['max_fp'] = stats['max_fp'].round(1)
    stats['min_fp'] = stats['min_fp'].round(1)
    
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
    print(f"New columns: avg_fp, fp_sd, avg_fppm, fppm_sd, max_fp, min_fp")
    return stats

if __name__ == "__main__":
    stats = scrape_gamelogs()
    print("\n=== Most Stable (Low SD) ===")
    print(stats.nsmallest(5, 'min_sd')[['player_name', 'games_played', 'min_sd']].to_string(index=False))
    print("\n=== Most Volatile (High SD) ===")
    print(stats.nlargest(5, 'min_sd')[['player_name', 'games_played', 'min_sd']].to_string(index=False))
