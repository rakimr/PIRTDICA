import requests
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
from team_map import TEAM_MAP  # shared mapping file

# ============================
# 1. CONNECT TO DATABASE
# ============================

conn = sqlite3.connect("dfs_nba.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS player_positions (
    player_name TEXT PRIMARY KEY,
    team TEXT,
    true_position TEXT,
    pg_pct REAL,
    sg_pct REAL,
    sf_pct REAL,
    pf_pct REAL,
    c_pct REAL,
    scraped_at TEXT
)
""")

conn.commit()

# ============================
# 2. SCRAPE LEAGUE-WIDE PLAY-BY-PLAY PAGE
# ============================

url = "https://www.basketball-reference.com/leagues/NBA_2026_play-by-play.html"
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
html = requests.get(url, headers=headers, timeout=30).text

import re as regex
comments = regex.findall(r'<!--(.*?)-->', html, regex.DOTALL)
for comment in comments:
    if 'id="pbp_stats"' in comment:
        html = comment
        break

soup = BeautifulSoup(html, "html.parser")
table = soup.find("table", {"id": "pbp_stats"})

if table is None:
    print("Error: Could not find pbp_stats table")
    conn.close()
    exit(1)

rows = []
tbody = table.find("tbody")
for tr in tbody.find_all("tr"):
    if tr.get("class") and "thead" in tr.get("class"):
        continue
    
    cells = tr.find_all(["th", "td"])
    if len(cells) < 13:
        continue
    
    player_cell = tr.find("td", {"data-stat": "name_display"})
    team_cell = tr.find("td", {"data-stat": "team_name_abbr"})
    pct1 = tr.find("td", {"data-stat": "pct_1"})
    pct2 = tr.find("td", {"data-stat": "pct_2"})
    pct3 = tr.find("td", {"data-stat": "pct_3"})
    pct4 = tr.find("td", {"data-stat": "pct_4"})
    pct5 = tr.find("td", {"data-stat": "pct_5"})
    
    if not player_cell:
        continue
    
    player_name = player_cell.get_text(strip=True)
    team = team_cell.get_text(strip=True) if team_cell else None
    pg_pct = float(pct1.get_text(strip=True) or 0) if pct1 else 0
    sg_pct = float(pct2.get_text(strip=True) or 0) if pct2 else 0
    sf_pct = float(pct3.get_text(strip=True) or 0) if pct3 else 0
    pf_pct = float(pct4.get_text(strip=True) or 0) if pct4 else 0
    c_pct = float(pct5.get_text(strip=True) or 0) if pct5 else 0
    
    rows.append({
        "player_name": player_name,
        "team": TEAM_MAP.get(team, team) if team else None,
        "pg_pct": pg_pct,
        "sg_pct": sg_pct,
        "sf_pct": sf_pct,
        "pf_pct": pf_pct,
        "c_pct": c_pct
    })

df = pd.DataFrame(rows)

# ============================
# 3. DETERMINE TRUE POSITION
# ============================

def get_true_position(row):
    pos_map = {
        "PG": row["pg_pct"],
        "SG": row["sg_pct"],
        "SF": row["sf_pct"],
        "PF": row["pf_pct"],
        "C": row["c_pct"]
    }
    return max(pos_map, key=lambda k: pos_map[k])

df["true_position"] = df.apply(get_true_position, axis=1)

# ============================
# 4. SELECT FINAL COLUMNS
# ============================

final = df[["player_name", "team", "true_position", "pg_pct", "sg_pct", "sf_pct", "pf_pct", "c_pct"]].copy()

final = final.drop_duplicates(subset=["player_name"], keep="first")

final["scraped_at"] = datetime.utcnow().isoformat()

# ============================
# 5. WRITE TO DATABASE
# ============================

cursor.execute("DELETE FROM player_positions")
conn.commit()

final.to_sql("player_positions", conn, if_exists="append", index=False)

conn.close()

print("Player positions scraped successfully.")
print(final.head())