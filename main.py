import pandas as pd
import sqlite3
from datetime import datetime

# ============================
# 1. CREATE / CONNECT DATABASE
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
# 2. SCRAPE REFEREE TABLE
# ============================

url = "https://www.nbastuffer.com/2025-2026-nba-referee-stats/"
tables = pd.read_html(url)
df = tables[0]  # first table on the page

# ============================
# 3. CLEAN + NORMALIZE COLUMNS
# ============================

df = df.rename(columns={
    "RANK": "rank",
    "REFEREE": "referee",
    "ROLE": "role",
    "GENDER": "gender",
    "EXPERIENCE (YEARS)": "experience_years",
    "GAMES OFFICIATED": "games_officiated",
    "HOME TEAM WIN%": "home_win_pct",
    "HOME TEAM POINTS DIFFERENTIAL": "home_point_diff",
    "TOTAL POINTS PER GAME": "total_points_pg",
    "CALLED FOULS PER GAME": "fouls_pg",
    "FOUL% AGAINST ROAD TEAMS": "foul_pct_road",
    "FOUL% AGAINST HOME TEAMS": "foul_pct_home",
    "FOUL DIFFERENTIAL (Ag.Rd Tm) - (Ag. Hm Tm)": "foul_diff"
})

# Convert percentage strings → decimals
pct_cols = ["home_win_pct", "foul_pct_road", "foul_pct_home"]

for col in pct_cols:
    df[col] = (
        df[col]
        .astype(str)
        .str.replace("%", "", regex=False)
        .astype(float) / 100
    )

# Add metadata
df["season"] = "2025-2026"
df["scraped_at"] = datetime.utcnow().isoformat()

# ============================
# 4. WRITE TO DATABASE
# ============================

df.to_sql(
    "referee_stats",
    conn,
    if_exists="append",
    index=False
)

conn.close()

print("Referee stats scraped and stored successfully.")

print(df.head(5))