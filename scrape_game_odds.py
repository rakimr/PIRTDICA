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
    team TEXT,
    opponent TEXT,
    spread REAL,
    location TEXT,
    game_date TEXT,
    game_time TEXT,
    scraped_at TEXT
)
""")
conn.commit()

# ============================
# 2. SCRAPE TEAMRANKINGS
# ============================

URL = "https://www.teamrankings.com/nba-ats-picks/"
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

rows = []

table = soup.find("table", class_="tr-table")
if not table:
    print("No odds table found")
    conn.close()
    exit(1)

tbody = table.find("tbody")
game_rows = tbody.find_all("tr") if tbody else []

print(f"Found {len(game_rows)} games")

for row in game_rows:
    cells = row.find_all("td")
    if len(cells) < 6:
        continue
    
    day = cells[0].get_text(strip=True)
    status = cells[1].get_text(strip=True)
    pick = cells[2].get_text(strip=True)
    opponent_text = cells[3].get_text(strip=True)
    
    # Skip games that already happened (status shows result)
    if status in ["Right", "Wrong", "Push"]:
        continue
    
    # Status contains game time for upcoming games (e.g., "9:30pm")
    game_time = status if ":" in status or "pm" in status.lower() or "am" in status.lower() else None
    
    # Parse team and spread from pick (e.g., "546 Detroit -4.5")
    match = re.search(r'\d+\s+(.+?)\s+([-+]?\d+\.?\d*)', pick)
    if not match:
        continue
    
    team_name = match.group(1).strip()
    spread = float(match.group(2))
    
    # Parse opponent and location (e.g., "vs Cleveland" or "at Charlotte")
    location_match = re.match(r'(vs|at)\s+(.+)', opponent_text)
    if location_match:
        location = "Home" if location_match.group(1) == "vs" else "Away"
        opponent_name = location_match.group(2).strip()
    else:
        location = "Unknown"
        opponent_name = opponent_text
    
    # Normalize team names
    team = TEAM_MAP.get(team_name, team_name)
    opponent = TEAM_MAP.get(opponent_name, opponent_name)
    
    # Parse date (assumes current year)
    current_year = datetime.now().year
    game_date = f"{current_year}-{day.replace('/', '-')}"
    
    rows.append({
        "team": team,
        "opponent": opponent,
        "spread": spread,
        "location": location,
        "game_date": game_date,
        "game_time": game_time,
        "scraped_at": datetime.utcnow().isoformat()
    })

# ============================
# 3. SAVE TO DATABASE
# ============================

df = pd.DataFrame(rows)

if not df.empty:
    df.to_sql("game_odds", conn, if_exists="append", index=False)
    print(f"Game odds scraped successfully. {len(df)} games found.")
    print(df.to_string(index=False))
else:
    print("No upcoming game odds found.")

conn.close()
