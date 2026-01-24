import requests
import pandas as pd
import sqlite3
from datetime import datetime
from team_map import TEAM_MAP

API_KEY = "fd1e867a3a7f4a5480fa9344140fd201"

# ============================
# 1. CONNECT TO DATABASE
# ============================

conn = sqlite3.connect("dfs_nba.db")
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS player_salaries")
cursor.execute("""
CREATE TABLE IF NOT EXISTS player_salaries (
    player_id INTEGER,
    player_name TEXT,
    team TEXT,
    position TEXT,
    salary REAL,
    slate_id INTEGER,
    slate_name TEXT,
    game_date TEXT,
    scraped_at TEXT
)
""")

conn.commit()

# ============================
# 2. SCRAPE DFS SLATES
# ============================

def scrape_salaries(date_str, operator="FanDuel", slate_type="Main"):
    url = f"https://api.sportsdata.io/v3/nba/projections/json/DfsSlatesByDate/{date_str}?key={API_KEY}"
    response = requests.get(url)

    if response.status_code != 200:
        print("Error fetching DFS slates:", response.text)
        return None

    slates = response.json()
    rows = []
    
    print(f"Found {len(slates)} total slates")
    for s in slates:
        print(f"  - {s.get('Operator')}: {s.get('SlateName')} (ID: {s.get('SlateID')})")

    target_slates = [s for s in slates if s.get("Operator") == operator]
    if not target_slates:
        print(f"No {operator} slates available for this date.")
        return pd.DataFrame()
    
    for slate in target_slates:
        slate_name = slate.get("SlateName", "") or ""
        
        if slate_type and slate_type.lower() not in slate_name.lower():
            continue
            
        slate_id = slate.get("SlateID")
        print(f"Processing slate: {slate_name} (ID: {slate_id})")

        dfs_slate_players = slate.get("DfsSlatePlayers", [])
        
        for player in dfs_slate_players:
            player_id = player.get("PlayerID")
            player_name = player.get("OperatorPlayerName")
            team = player.get("Team")
            team = TEAM_MAP.get(team, team) if team else None
            
            salary = player.get("OperatorSalary")
            position = player.get("OperatorPosition")

            rows.append({
                "player_id": player_id,
                "player_name": player_name,
                "team": team,
                "position": position,
                "salary": salary,
                "slate_id": slate_id,
                "slate_name": slate_name,
                "game_date": date_str,
                "scraped_at": datetime.utcnow().isoformat()
            })

    return pd.DataFrame(rows)

# ============================
# 3. RUN + SAVE
# ============================

today = "2026-01-23"
print(f"Fetching FanDuel main slate for {today}...")
df = scrape_salaries(today, operator="FanDuel", slate_type="Main")

if df is not None and not df.empty:
    df.to_sql("player_salaries", conn, if_exists="append", index=False)
    print(f"Player salaries scraped successfully. {len(df)} players found.")
    print(df.head(10))
else:
    print("No salary data found for today's FanDuel main slate.")

conn.close()
