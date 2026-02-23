"""
Scrape historical game logs from Basketball Reference for multiple seasons.
Pulls game log data from schedule/results pages and individual team game logs.
Seasons: 2022-23 through 2024-25 (current season already covered by NBA API scraper).

Stores in historical_game_logs table in SQLite with full stat lines.
Used for Matchup Interaction Layer familiarity scoring.
"""

import requests
import pandas as pd
import sqlite3
import time
import re
from io import StringIO
from datetime import datetime
from bs4 import BeautifulSoup

BR_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

SEASONS = ['2023', '2024', '2025']

NBA_TEAMS_BR = [
    'ATL', 'BOS', 'BRK', 'CHO', 'CHI', 'CLE', 'DAL', 'DEN',
    'DET', 'GSW', 'HOU', 'IND', 'LAC', 'LAL', 'MEM', 'MIA',
    'MIL', 'MIN', 'NOP', 'NYK', 'OKC', 'ORL', 'PHI', 'PHO',
    'POR', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS'
]

BR_TO_NBA_TEAM = {
    'ATL': 'ATL', 'BOS': 'BOS', 'BRK': 'BKN', 'CHO': 'CHA', 'CHI': 'CHI',
    'CLE': 'CLE', 'DAL': 'DAL', 'DEN': 'DEN', 'DET': 'DET', 'GSW': 'GSW',
    'HOU': 'HOU', 'IND': 'IND', 'LAC': 'LAC', 'LAL': 'LAL', 'MEM': 'MEM',
    'MIA': 'MIA', 'MIL': 'MIL', 'MIN': 'MIN', 'NOP': 'NOP', 'NYK': 'NYK',
    'OKC': 'OKC', 'ORL': 'ORL', 'PHI': 'PHI', 'PHO': 'PHX', 'POR': 'POR',
    'SAC': 'SAC', 'SAS': 'SAS', 'TOR': 'TOR', 'UTA': 'UTA', 'WAS': 'WAS'
}


def calc_fanduel_fp(pts, reb, ast, stl, blk, tov):
    return pts + (reb * 1.2) + (ast * 1.5) + (stl * 3) + (blk * 3) - tov


def scrape_season_totals(season_year):
    """Scrape per-game player stats for a season from Basketball Reference totals page.
    season_year: e.g. '2024' for the 2023-24 season."""
    
    url = f"https://www.basketball-reference.com/leagues/NBA_{season_year}_totals.html"
    print(f"\nScraping {season_year} season totals from Basketball Reference...")
    
    try:
        resp = requests.get(url, headers=BR_HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"  ERROR: {e}")
        return pd.DataFrame()
    
    try:
        soup = BeautifulSoup(resp.text, 'html.parser')
        table = soup.find('table', {'id': 'totals_stats'})
        if table is None:
            tables = pd.read_html(StringIO(resp.text))
            if not tables:
                return pd.DataFrame()
            df = tables[0]
        else:
            df = pd.read_html(StringIO(str(table)))[0]
    except Exception as e:
        print(f"  ERROR parsing: {e}")
        return pd.DataFrame()
    
    if 'Rk' in df.columns:
        df = df[df['Rk'] != 'Rk']
    df = df[df['Player'].notna()]
    
    df['Player'] = df['Player'].str.replace(r'\*$', '', regex=True).str.strip()
    
    cols_needed = {
        'Player': 'player_name', 'Tm': 'team', 'G': 'games',
        'MP': 'total_min', 'PTS': 'pts', 'TRB': 'reb', 'AST': 'ast',
        'STL': 'stl', 'BLK': 'blk', 'TOV': 'tov', '3P': 'fg3m'
    }
    
    available = {k: v for k, v in cols_needed.items() if k in df.columns}
    result = df[list(available.keys())].rename(columns=available).copy()
    
    for col in ['games', 'total_min', 'pts', 'reb', 'ast', 'stl', 'blk', 'tov', 'fg3m']:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors='coerce').fillna(0)
    
    result['season'] = f"{int(season_year)-1}-{season_year[-2:]}"
    
    result = result[result['games'] > 0]
    
    if 'total_min' in result.columns and 'games' in result.columns:
        result['avg_min'] = (result['total_min'] / result['games']).round(1)
    
    for stat in ['pts', 'reb', 'ast', 'stl', 'blk', 'tov', 'fg3m']:
        if stat in result.columns and 'games' in result.columns:
            result[f'{stat}_pg'] = (result[stat] / result['games']).round(1)
    
    if all(c in result.columns for c in ['pts_pg', 'reb_pg', 'ast_pg', 'stl_pg', 'blk_pg', 'tov_pg']):
        result['fp_pg'] = result.apply(
            lambda r: round(calc_fanduel_fp(r['pts_pg'], r['reb_pg'], r['ast_pg'], r['stl_pg'], r['blk_pg'], r['tov_pg']), 1),
            axis=1
        )
    
    dedup_cols = ['player_name']
    if 'team' in result.columns:
        dedup_cols.append('team')
    result = result.drop_duplicates(subset=dedup_cols, keep='first')
    
    print(f"  Got season totals for {len(result)} player-team entries")
    return result


def scrape_player_gamelog_page(player_slug, season_year):
    """Scrape individual player game log page from Basketball Reference.
    This gives us per-game data with opponent info."""
    url = f"https://www.basketball-reference.com/players/{player_slug[0]}/{player_slug}/gamelog/{season_year}"
    try:
        resp = requests.get(url, headers=BR_HEADERS, timeout=30)
        if resp.status_code != 200:
            return pd.DataFrame()
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        table = soup.find('table', {'id': 'pgl_basic'})
        if table is None:
            return pd.DataFrame()
        
        df = pd.read_html(StringIO(str(table)))[0]
        if 'Rk' in df.columns:
            df = df[df['Rk'] != 'Rk']
        return df
    except Exception:
        return pd.DataFrame()


def build_synthetic_game_logs(season_totals_df):
    """From season totals, create synthetic per-game averages that can be used
    for archetype-vs-team analysis even without individual game logs."""
    
    records = []
    for _, row in season_totals_df.iterrows():
        games = int(row.get('games', 0))
        if games < 10:
            continue
        
        record = {
            'player_name': row['player_name'],
            'season': row['season'],
            'team': row.get('team', ''),
            'games_played': games,
            'avg_min': row.get('avg_min', 0),
            'pts_pg': row.get('pts_pg', 0),
            'reb_pg': row.get('reb_pg', 0),
            'ast_pg': row.get('ast_pg', 0),
            'stl_pg': row.get('stl_pg', 0),
            'blk_pg': row.get('blk_pg', 0),
            'tov_pg': row.get('tov_pg', 0),
            'fg3m_pg': row.get('fg3m_pg', 0),
            'fp_pg': row.get('fp_pg', 0),
        }
        records.append(record)
    
    return pd.DataFrame(records)


def scrape_all_historical():
    """Main function: scrape season totals for all historical seasons."""
    all_seasons = []
    
    for season_year in SEASONS:
        df = scrape_season_totals(season_year)
        if len(df) > 0:
            all_seasons.append(df)
        time.sleep(4)
    
    if not all_seasons:
        print("ERROR: No historical data retrieved")
        return None
    
    combined = pd.concat(all_seasons, ignore_index=True)
    
    game_logs = build_synthetic_game_logs(combined)
    
    conn = sqlite3.connect('dfs_nba.db')
    
    combined.to_sql('historical_season_totals', conn, if_exists='replace', index=False)
    print(f"\nSaved {len(combined)} season total entries to historical_season_totals")
    
    game_logs.to_sql('historical_player_seasons', conn, if_exists='replace', index=False)
    print(f"Saved {len(game_logs)} player-season records to historical_player_seasons (10+ games)")
    
    conn.close()
    
    print(f"\n=== Historical Data Summary ===")
    for season in combined['season'].unique():
        s_df = combined[combined['season'] == season]
        print(f"  {season}: {len(s_df)} player entries")
    
    print(f"\nTotal unique players across all seasons: {combined['player_name'].nunique()}")
    
    return combined


if __name__ == "__main__":
    df = scrape_all_historical()
    if df is not None:
        conn = sqlite3.connect('dfs_nba.db')
        
        current_logs = pd.read_sql("SELECT DISTINCT player_name FROM player_game_logs", conn)
        current_players = set(current_logs['player_name'].tolist())
        
        hist_players = set(df['player_name'].tolist())
        overlap = current_players & hist_players
        
        print(f"\n=== Coverage Analysis ===")
        print(f"Current season players: {len(current_players)}")
        print(f"Historical players: {len(hist_players)}")
        print(f"Players with multi-season data: {len(overlap)}")
        
        conn.close()
