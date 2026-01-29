"""
Scrape player game logs from Basketball Reference to calculate minutes volatility (SD).
Gets last 20-40 games per player to compute:
- σᵢ (player's minutes SD)
- Used for star weight ω calculation
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import time
import re
from team_map import normalize_name

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

def get_player_slug(player_name):
    """Convert player name to Basketball Reference slug format."""
    parts = normalize_name(player_name).split()
    if len(parts) < 2:
        return None
    last = parts[-1][:5].lower()
    first = parts[0][:2].lower()
    slug = f"{last}{first}01"
    return slug

def scrape_player_game_log(player_name, season='2025'):
    """Scrape game log for a player, return list of minutes played."""
    slug = get_player_slug(player_name)
    if not slug:
        return []
    
    url = f"https://www.basketball-reference.com/players/{slug[0]}/{slug}/gamelog/{season}"
    
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            return []
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        table = soup.find('table', {'id': 'player_game_log_reg'})
        if not table:
            return []
        
        minutes_list = []
        rows = table.find('tbody').find_all('tr')
        
        for row in rows:
            if 'thead' in row.get('class', []):
                continue
            
            mp_cell = row.find('td', {'data-stat': 'mp'})
            if mp_cell and mp_cell.text:
                try:
                    mp_text = mp_cell.text.strip()
                    if ':' in mp_text:
                        mins, secs = mp_text.split(':')
                        minutes = int(mins) + int(secs) / 60
                    else:
                        minutes = float(mp_text)
                    minutes_list.append(minutes)
                except:
                    pass
        
        return minutes_list[-30:]  # Last 30 games
        
    except Exception as e:
        print(f"Error scraping {player_name}: {e}")
        return []

def calculate_volatility_stats(minutes_list):
    """Calculate SD and CV from minutes history."""
    if len(minutes_list) < 5:
        return None, None, None
    
    import numpy as np
    arr = np.array(minutes_list)
    mean_min = np.mean(arr)
    sd = np.std(arr)
    cv = sd / mean_min if mean_min > 0 else 0
    
    return round(mean_min, 2), round(sd, 2), round(cv, 3)

def main():
    conn = sqlite3.connect('dfs_nba.db')
    
    # Get players from salaries
    players_df = pd.read_sql("SELECT DISTINCT player_name FROM player_salaries", conn)
    
    results = []
    
    print(f"Scraping game logs for {len(players_df)} players...")
    
    for i, row in players_df.iterrows():
        player = row['player_name']
        
        if i > 0 and i % 10 == 0:
            print(f"  Processed {i}/{len(players_df)}...")
            time.sleep(3)  # Rate limit
        
        minutes = scrape_player_game_log(player)
        
        if minutes:
            mean_min, sd, cv = calculate_volatility_stats(minutes)
            if mean_min is not None:
                results.append({
                    'player_name': player,
                    'games_sampled': len(minutes),
                    'avg_minutes': mean_min,
                    'minutes_sd': sd,
                    'minutes_cv': cv
                })
        
        time.sleep(1)  # Be nice to BBRef
    
    if results:
        df = pd.DataFrame(results)
        df.to_sql('player_volatility', conn, if_exists='replace', index=False)
        print(f"\nSaved volatility stats for {len(df)} players")
        print(df.head(20).to_string(index=False))
    
    conn.close()

if __name__ == "__main__":
    main()
