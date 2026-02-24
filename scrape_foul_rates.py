import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from utils.timezone import get_eastern_now
import time
import re

def scrape_foul_rates():
    """
    Scrape player foul rates from Basketball Reference.
    Gets fouls per game for recent games.
    """
    url = "https://www.basketball-reference.com/leagues/NBA_2026_per_game.html"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"Error fetching foul rates: {e}")
        return pd.DataFrame()
    
    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", {"id": "per_game_stats"})
    
    if not table:
        print("Could not find per game stats table")
        return pd.DataFrame()
    
    rows = []
    tbody = table.find("tbody")
    if not tbody:
        return pd.DataFrame()
    
    for tr in tbody.find_all("tr"):
        if tr.get("class") and "thead" in tr.get("class"):
            continue
        
        cells = tr.find_all(["th", "td"])
        if len(cells) < 20:
            continue
        
        try:
            player_cell = tr.find("td", {"data-stat": "player"})
            if not player_cell:
                continue
            
            player_name = player_cell.get_text(strip=True)
            team = tr.find("td", {"data-stat": "team_id"})
            team = team.get_text(strip=True) if team else ""
            
            games = tr.find("td", {"data-stat": "g"})
            games = int(games.get_text(strip=True)) if games and games.get_text(strip=True) else 0
            
            mpg = tr.find("td", {"data-stat": "mp_per_g"})
            mpg = float(mpg.get_text(strip=True)) if mpg and mpg.get_text(strip=True) else 0.0
            
            pf = tr.find("td", {"data-stat": "pf_per_g"})
            fouls_per_game = float(pf.get_text(strip=True)) if pf and pf.get_text(strip=True) else 0.0
            
            if games > 0 and mpg > 0:
                fouls_per_36 = (fouls_per_game / mpg) * 36 if mpg > 0 else 0
                
                rows.append({
                    "player_name": player_name,
                    "team": team,
                    "games": games,
                    "mpg": mpg,
                    "fouls_per_game": fouls_per_game,
                    "fouls_per_36": round(fouls_per_36, 2),
                    "scraped_at": get_eastern_now().isoformat()
                })
        except Exception as e:
            continue
    
    df = pd.DataFrame(rows)
    
    if not df.empty:
        df = df.drop_duplicates(subset=["player_name", "team"], keep="first")
    
    return df

def save_foul_rates(df):
    """Save foul rates to database."""
    if df.empty:
        print("No foul rate data to save")
        return
    
    conn = sqlite3.connect("dfs_nba.db")
    
    df.to_sql("player_foul_rates", conn, if_exists="replace", index=False)
    
    conn.close()
    print(f"Saved {len(df)} player foul rates")

if __name__ == "__main__":
    print("Scraping player foul rates...")
    df = scrape_foul_rates()
    
    if not df.empty:
        print(f"\nTop foul-prone players (per game):")
        print(df.nlargest(10, "fouls_per_game")[["player_name", "team", "fouls_per_game", "fouls_per_36"]].to_string(index=False))
        
        save_foul_rates(df)
    else:
        print("Failed to scrape foul rates")
