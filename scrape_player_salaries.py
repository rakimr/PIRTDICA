import requests
import pandas as pd
import sqlite3
from datetime import datetime
from team_map import TEAM_MAP

API_KEY = "YOUR_API_KEY_HERE"

# ============================
# 1. CONNECT TO DATABASE
# ============================

conn = sqlite3.connect("dfs_nba.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS player_salaries (
    player_id INTEGER,
    player_name TEXT,
    team TEXT,
    opponent TEXT,
    dk_salary REAL,
    fd_salary REAL,
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

def scrape_salaries(date_str):
    url = f"https://api.sportsdata.io/v3/nba/projections/json/DfsSlatesByDate/{date_str}?key={API_KEY}"
    response = requests.get(url)

    if response.status_code != 200:
        print("Error fetching DFS slates:", response.text)
        return None

    slates = response.json()
    rows = []

    for slate in slates:
        slate_id = slate.get("SlateID")
        slate_name = slate.get("SlateName")

        for player in slate.get("DfsPlayers", []):
            player_id = player.get("PlayerID")
            player_name = player.get("Name")
            team = TEAM_MAP.get(player.get("Team"), player.get("Team"))
            opponent = TEAM_MAP.get(player.get("Opponent"), player.get("Opponent"))

            operator = player.get("Operator")
            salary = player.get("OperatorSalary")

            dk_salary = salary if operator == "DraftKings" else None
            fd_salary = salary if operator == "FanDuel" else None

            rows.append([
                player_id,
                player_name,
                team,
                opponent,
                dk_salary,
                fd_salary,
                slate_id,
                slate_name,
                date_str,
                datetime.utcnow().isoformat()
            ])

    return pd.DataFrame(rows, columns=[
        "player_id", "player_name", "team", "opponent",
        "dk_salary", "fd_salary",
        "slate_id", "slate_name",
        "game_date", "scraped_at"
    ])

# ============================
# 3. RUN + SAVE
# ============================

today = datetime.utcnow().date().isoformat()
df = scrape_salaries(today)

if df is not None and not df.empty:
    cursor.execute("DELETE FROM player_salaries WHERE game_date = ?", (today,))
    conn.commit()

    df.to_sql("player_salaries", conn, if_exists="append", index=False)
    print("Player salaries scraped successfully.")
    print(df.head())
else:
    print("No salary data found.")

conn.close()
