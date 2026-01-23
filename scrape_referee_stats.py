import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3

# ============================
# 1. CONNECT TO DATABASE
# ============================

conn = sqlite3.connect("dfs_nba.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS referee_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rank INTEGER,
    referee TEXT,
    role TEXT,
    gender TEXT,
    experience_years INTEGER,
    games_officiated INTEGER,
    home_win_pct REAL,
    home_point_diff REAL,
    total_points_pg REAL,
    fouls_pg REAL,
    foul_pct_road REAL,
    foul_pct_home REAL,
    foul_diff REAL,
    season TEXT,
    scraped_at TEXT
)
""")

conn.commit()

# ============================
# 2. SCRAPE HTML
# ============================

url = "https://www.nbastuffer.com/2025-2026-nba-referee-stats/"
html = requests.get(url).text
soup = BeautifulSoup(html, "html.parser")

table = soup.find("tbody", class_="row-striping")

rows = []
for tr in table.find_all("tr"):
    cells = [td.text.strip() for td in tr.find_all("td")]
    if len(cells) == 14:  # expected number of columns
        rows.append(cells)

# ============================
# 3. BUILD DATAFRAME
# ============================

columns = [
    "rank",
    "referee",
    "role",
    "gender",
    "experience_years",
    "games_officiated",
    "home_win_pct",
    "home_point_diff",
    "total_points_pg",
    "fouls_pg",
    "foul_pct_road",
    "foul_pct_home",
    "foul_diff",
    "blank"
]

df = pd.DataFrame(rows, columns=columns)
df = df.drop(columns=["blank"])

# ============================
# 4. CLEAN DATA
# ============================

# Convert numeric columns
numeric_cols = [
    "rank", "experience_years", "games_officiated",
    "home_point_diff", "total_points_pg", "fouls_pg", "foul_diff"
]

for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# Convert percentages (already decimals like 0.529)
pct_cols = ["home_win_pct", "foul_pct_road", "foul_pct_home"]

for col in pct_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

df["season"] = "2025-2026"
df["scraped_at"] = datetime.utcnow().isoformat()

# ============================
# 5. WRITE TO DATABASE
# ============================

df.to_sql("referee_stats", conn, if_exists="append", index=False)
conn.close()

print("Scrape complete. Sample:")
print(df.head())