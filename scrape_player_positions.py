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
html = requests.get(url).text
soup = BeautifulSoup(html, "html.parser")

table = soup.find("table", {"id": "pbp_stats"})

df = pd.read_html(str(table))[0]

# Drop header rows that BR sometimes repeats
df = df[df["Rk"].notna()]

# ============================
# 3. CLEAN + EXTRACT POSITION DATA
# ============================

df["player_name"] = df["Player"]
df["team"] = df["Tm"]

# Convert team names to acronyms (BR already uses acronyms, but safe)
df["team"] = df["team"].map(lambda x: TEAM_MAP.get(x, x))

# Extract positional percentages
df["pg_pct"] = df["pct_1"].astype(float)
df["sg_pct"] = df["pct_2"].astype(float)
df["sf_pct"] = df["pct_3"].astype(float)
df["pf_pct"] = df["pct_4"].astype(float)
df["c_pct"] = df["pct_5"].astype(float)

# Determine true position
def get_true_position(row):
    pos_map = {
        "PG": row["pg_pct"],
        "SG": row["sg_pct"],
        "SF": row["sf_pct"],
        "PF": row["pf_pct"],
        "C": row["c_pct"]
    }
    return max(pos_map, key=pos_map.get)

df["true_position"] = df.apply(get_true_position, axis=1)

# ============================
# 4. SELECT FINAL COLUMNS
# ============================

final = df[[
    "player_name",
    "team",
    "true_position",
    "pg_pct",
    "sg_pct",
    "sf_pct",
    "pf_pct",
    "c_pct"
]]

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