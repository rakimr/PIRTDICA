import requests
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import re
from team_map import TEAM_MAP

# ============================
# 1. CONNECT TO DATABASE
# ============================

conn = sqlite3.connect("dfs_nba.db")
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS game_odds")
cursor.execute("""
CREATE TABLE IF NOT EXISTS game_odds (
    away_team TEXT,
    home_team TEXT,
    spread REAL,
    total REAL,
    scraped_at TEXT
)
""")
conn.commit()

# ============================
# 2. SCRAPE TEAMRANKINGS
# ============================

URL = "https://www.teamrankings.com/nba/odds/"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

print("Fetching NBA odds from TeamRankings...")
response = requests.get(URL, headers=headers, timeout=30)

if response.status_code != 200:
    print(f"Error fetching page: {response.status_code}")
    conn.close()
    exit(1)

soup = BeautifulSoup(response.text, "html.parser")

games = {}

tables = soup.find_all("table", class_="tr-table")

for table in tables:
    header = table.find_previous_sibling("h2")
    if not header:
        continue
    
    header_text = header.get_text(strip=True)
    
    is_spread = "Spread" in header_text
    is_total = "Total" in header_text
    
    if not is_spread and not is_total:
        continue
    
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        
        matchup = cells[0].get_text(strip=True)
        value = cells[1].get_text(strip=True)
        
        # Parse matchup: "Detroit vs. Sacramento"
        match = re.match(r'(.+?)\s+vs\.?\s+(.+)', matchup)
        if not match:
            continue
        
        away_name = match.group(1).strip()
        home_name = match.group(2).strip()
        
        # Normalize team names
        away_team = TEAM_MAP.get(away_name, away_name)
        home_team = TEAM_MAP.get(home_name, home_name)
        
        game_key = f"{away_team}@{home_team}"
        
        if game_key not in games:
            games[game_key] = {
                "away_team": away_team,
                "home_team": home_team,
                "spread": None,
                "total": None
            }
        
        try:
            val = float(value)
            if is_spread:
                games[game_key]["spread"] = val
            elif is_total:
                games[game_key]["total"] = val
        except:
            pass

# ============================
# 3. SAVE TO DATABASE
# ============================

rows = []
for game_key, game in games.items():
    game["scraped_at"] = datetime.utcnow().isoformat()
    rows.append(game)

df = pd.DataFrame(rows)

if not df.empty:
    df.to_sql("game_odds", conn, if_exists="append", index=False)
    print(f"Game odds scraped successfully. {len(df)} games found.")
    print(df.to_string(index=False))
else:
    print("No game odds found.")

conn.close()
