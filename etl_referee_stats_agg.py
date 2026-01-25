import sqlite3
import pandas as pd
from datetime import datetime

# ============================
# 1. CONNECT TO DATABASE
# ============================

conn = sqlite3.connect("dfs_nba.db")
cursor = conn.cursor()

# Create aggregated table if not exists (with role-specific stats)
cursor.execute("""
CREATE TABLE IF NOT EXISTS referee_stats_agg (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    referee TEXT,
    role TEXT,
    true_foul_diff REAL,
    true_foul_pct_road REAL,
    true_foul_pct_home REAL,
    true_fouls_pg REAL,
    true_total_points_pg REAL,
    updated_at TEXT,
    UNIQUE(referee, role)
)
""")

conn.commit()

# ============================
# 2. LOAD RAW REFEREE STATS
# ============================

df = pd.read_sql("SELECT * FROM referee_stats", conn)

if df.empty:
    print("No referee stats found. Run scrape_referee_stats.py first.")
    conn.close()
    exit()

# ============================
# 3. AGGREGATE BY REFEREE + ROLE
# ============================

agg = df.groupby(["referee", "role"]).agg({
    "foul_diff": "mean",
    "foul_pct_road": "mean",
    "foul_pct_home": "mean",
    "fouls_pg": "mean",
    "total_points_pg": "mean"
}).reset_index()

agg = agg.rename(columns={
    "foul_diff": "true_foul_diff",
    "foul_pct_road": "true_foul_pct_road",
    "foul_pct_home": "true_foul_pct_home",
    "fouls_pg": "true_fouls_pg",
    "total_points_pg": "true_total_points_pg"
})

agg["updated_at"] = datetime.utcnow().isoformat()

# ============================
# 4. WRITE AGGREGATED RESULTS
# ============================

# Clear old aggregated data
cursor.execute("DELETE FROM referee_stats_agg")
conn.commit()

# Insert new aggregated data
agg.to_sql("referee_stats_agg", conn, if_exists="append", index=False)

conn.close()

print("Referee stats aggregated successfully.")
print(agg.head())