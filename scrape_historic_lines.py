import requests
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
from team_map import TEAM_MAP

conn = sqlite3.connect("dfs_nba.db")
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS historic_lines")
cursor.execute("""
CREATE TABLE IF NOT EXISTS historic_lines (
    game_date TEXT,
    team TEXT,
    site TEXT,
    spread REAL,
    total REAL,
    team_line REAL,
    opponent TEXT,
    season INTEGER,
    scraped_at TEXT
)
""")
conn.commit()

URL = "https://sportsdatabase.com/NBA/query.html"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

SEASONS = [2023, 2024, 2025]

all_rows = []

for season in SEASONS:
    query = f"date, team, site, line, total, o:team@(team and season) and season={season}"
    
    print(f"Fetching season {season}...")
    
    response = requests.get(URL, params={"sdql": query}, headers=headers, timeout=60)
    
    if response.status_code != 200:
        print(f"Error fetching season {season}: {response.status_code}")
        continue
    
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", id="pyql_id")
    
    if not table:
        print(f"No table found for season {season}")
        continue
    
    tbody = table.find("tbody")
    if not tbody:
        trs = table.find_all("tr")[1:]
    else:
        trs = tbody.find_all("tr")
    
    season_count = 0
    for tr in trs:
        tds = tr.find_all("td")
        if len(tds) < 6:
            continue
        
        try:
            game_date = tds[0].get_text(strip=True)
            team_name = tds[1].get_text(strip=True)
            site = tds[2].get_text(strip=True)
            spread = float(tds[3].get_text(strip=True))
            total = float(tds[4].get_text(strip=True))
            opponent = tds[5].get_text(strip=True)
            
            team = TEAM_MAP.get(team_name, team_name)
            opponent = TEAM_MAP.get(opponent, opponent)
            
            team_line = (total / 2) - (spread / 2)
            
            all_rows.append({
                "game_date": game_date,
                "team": team,
                "site": site,
                "spread": spread,
                "total": total,
                "team_line": round(team_line, 2),
                "opponent": opponent,
                "season": season,
                "scraped_at": datetime.utcnow().isoformat()
            })
            season_count += 1
        except (ValueError, IndexError) as e:
            continue
    
    print(f"  Found {season_count} games for season {season}")

df = pd.DataFrame(all_rows)

if not df.empty:
    df.to_sql("historic_lines", conn, if_exists="append", index=False)
    print(f"\nHistoric lines scraped successfully. {len(df)} total rows saved.")
    
    print("\n=== Average Team Line (Implied Score) by Team ===")
    avg_team_lines = df.groupby("team")["team_line"].mean().round(2).sort_values(ascending=False)
    print(avg_team_lines.head(10))
    print("...")
    print(avg_team_lines.tail(10))
else:
    print("No historic line data found.")

conn.close()
