import sqlite3
import pandas as pd
from datetime import datetime

# ============================
# 1. CONNECT TO DATABASE
# ============================

conn = sqlite3.connect("dfs_nba.db")
cursor = conn.cursor()

# Create output table if not exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS game_foul_environment (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game TEXT,
    home_team TEXT,
    away_team TEXT,
    crew_chief TEXT,
    referee TEXT,
    umpire TEXT,
    avg_foul_diff REAL,
    avg_foul_pct_road REAL,
    avg_foul_pct_home REAL,
    avg_fouls_pg REAL,
    avg_total_points_pg REAL,
    game_date TEXT,
    updated_at TEXT
)
""")

conn.commit()

# ============================
# 2. LOAD TODAY'S ASSIGNMENTS
# ============================

assignments = pd.read_sql("SELECT * FROM referee_assignments", conn)

if assignments.empty:
    print("No referee assignments found. Run scrape_referee_assignments.py first.")
    conn.close()
    exit()

# ============================
# 3. LOAD AGGREGATED REF STATS
# ============================

agg = pd.read_sql("SELECT * FROM referee_stats_agg", conn)

if agg.empty:
    print("No aggregated referee stats found. Run etl_referee_stats_agg.py first.")
    conn.close()
    exit()

# ============================
# 4. JOIN ASSIGNMENTS WITH REF TENDENCIES (ROLE-MATCHED)
# ============================

# Filter agg by role for role-specific matching
agg_chief = agg[agg["role"] == "CHIEF"].copy()
agg_crew = agg[agg["role"] == "CREW"].copy()

# Merge crew_chief with CHIEF role stats
merged = assignments.merge(
    agg_chief.add_prefix("chief_"),
    left_on="crew_chief",
    right_on="chief_referee",
    how="left"
).merge(
    agg_crew.add_prefix("ref_"),
    left_on="referee",
    right_on="ref_referee",
    how="left"
).merge(
    agg_crew.add_prefix("ump_"),
    left_on="umpire",
    right_on="ump_referee",
    how="left"
)

# ============================
# 5. COMPUTE GAME-LEVEL AVERAGES
# ============================

merged["avg_foul_diff"] = merged[[
    "chief_true_foul_diff",
    "ref_true_foul_diff",
    "ump_true_foul_diff"
]].mean(axis=1)

merged["avg_foul_pct_road"] = merged[[
    "chief_true_foul_pct_road",
    "ref_true_foul_pct_road",
    "ump_true_foul_pct_road"
]].mean(axis=1)

merged["avg_foul_pct_home"] = merged[[
    "chief_true_foul_pct_home",
    "ref_true_foul_pct_home",
    "ump_true_foul_pct_home"
]].mean(axis=1)

merged["avg_fouls_pg"] = merged[[
    "chief_true_fouls_pg",
    "ref_true_fouls_pg",
    "ump_true_fouls_pg"
]].mean(axis=1)

merged["avg_total_points_pg"] = merged[[
    "chief_true_total_points_pg",
    "ref_true_total_points_pg",
    "ump_true_total_points_pg"
]].mean(axis=1)

# ============================
# 6. SELECT FINAL COLUMNS
# ============================

final = merged[[
    "game",
    "home_team",
    "away_team",
    "crew_chief",
    "referee",
    "umpire",
    "avg_foul_diff",
    "avg_foul_pct_road",
    "avg_foul_pct_home",
    "avg_fouls_pg",
    "avg_total_points_pg",
    "game_date"
]]

final["updated_at"] = datetime.utcnow().isoformat()

# ============================
# 7. WRITE TO DATABASE
# ============================

cursor.execute("DELETE FROM game_foul_environment")
conn.commit()

final.to_sql("game_foul_environment", conn, if_exists="append", index=False)

conn.close()

print("Game foul environment computed successfully.")
print(final.head())