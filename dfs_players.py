import sqlite3
import pandas as pd
import numpy as np
import re

conn = sqlite3.connect("dfs_nba.db")

salaries = pd.read_sql("SELECT * FROM player_salaries", conn)

if salaries.empty:
    print("No player salary data available. DFS players table not created.")
    conn.close()
    exit(0)

rotation = pd.read_sql("SELECT * FROM rotation_minutes", conn)
game_odds = pd.read_sql("SELECT * FROM game_odds", conn)
dvp = pd.read_sql("SELECT * FROM dvp_stats", conn)
game_foul_env = pd.read_sql("SELECT * FROM game_foul_environment", conn)
hist_lines = pd.read_sql("SELECT team, AVG(team_line) as avg_team_line FROM historic_lines GROUP BY team", conn)
per100 = pd.read_sql("SELECT * FROM player_per100", conn)
team_pace = pd.read_sql("SELECT * FROM team_pace", conn)

def normalize_name(name):
    if pd.isna(name):
        return ""
    name = str(name).strip().lower()
    name = re.sub(r'\.', '', name)
    name = re.sub(r'-', ' ', name)
    name = re.sub(r'\s+(jr|sr|ii|iii|iv|v)\.?$', '', name)
    name = re.sub(r'\s+', ' ', name)
    return name.strip()

salaries["player_name"] = salaries["player_name"].str.strip()
rotation["player_name"] = rotation["player_name"].str.strip()
per100["player_name"] = per100["player_name"].str.strip()

salaries["norm_name"] = salaries["player_name"].apply(normalize_name)
rotation["norm_name"] = rotation["player_name"].apply(normalize_name)
per100["norm_name"] = per100["player_name"].apply(normalize_name)

df = salaries.merge(
    rotation[["team", "norm_name", "espn_slot", "projected_min"]],
    on=["team", "norm_name"],
    how="left"
)

df = df.merge(
    per100[["norm_name", "fp_per100"]],
    on="norm_name",
    how="left"
)

df = df.merge(
    team_pace,
    on="team",
    how="left"
)

def get_opponent_and_location(row):
    team = row["team"]
    away_match = game_odds[game_odds["away_team"] == team]
    home_match = game_odds[game_odds["home_team"] == team]
    
    if not away_match.empty:
        return pd.Series({
            "opponent": away_match.iloc[0]["home_team"],
            "location": "away",
            "spread": away_match.iloc[0]["spread"],
            "total": away_match.iloc[0]["total"]
        })
    elif not home_match.empty:
        return pd.Series({
            "opponent": home_match.iloc[0]["away_team"],
            "location": "home",
            "spread": -home_match.iloc[0]["spread"],
            "total": home_match.iloc[0]["total"]
        })
    return pd.Series({"opponent": None, "location": None, "spread": None, "total": None})

game_info = df.apply(get_opponent_and_location, axis=1)
df = pd.concat([df, game_info], axis=1)

df["team_line"] = (df["total"] / 2) - (df["spread"] / 2)

df = df.merge(hist_lines, on="team", how="left")

df["line_weight"] = df["team_line"] / df["avg_team_line"]
df["line_weight"] = df["line_weight"].fillna(1.0)

POSITION_MAP = {
    "PG": "PG", "SG": "SG", "SF": "SF", "PF": "PF", "C": "C",
    "G": "PG", "F": "SF"
}

def get_true_position(espn_slot):
    if pd.isna(espn_slot):
        return None
    pos = ''.join([c for c in espn_slot if c.isalpha()])
    return POSITION_MAP.get(pos, pos)

df["true_position"] = df["espn_slot"].apply(get_true_position)

def get_dvp_weight(row):
    if pd.isna(row["opponent"]) or pd.isna(row["true_position"]):
        return 1.0
    
    match = dvp[(dvp["team"] == row["opponent"]) & (dvp["position"] == row["true_position"])]
    if match.empty:
        return 1.0
    
    return match.iloc[0]["dvp_score"]

df["dvp_raw"] = df.apply(get_dvp_weight, axis=1)

dvp_mean = df["dvp_raw"].mean()
dvp_std = df["dvp_raw"].std()
df["dvp_weight"] = 1 + ((df["dvp_raw"] - dvp_mean) / dvp_std) * 0.10
df["dvp_weight"] = df["dvp_weight"].clip(0.85, 1.15)

def get_ref_weight(row):
    if pd.isna(row["location"]) or pd.isna(row["team"]):
        return 1.0
    
    match = game_foul_env[
        (game_foul_env["home_team"] == row["team"]) | 
        (game_foul_env["away_team"] == row["team"])
    ]
    
    if match.empty:
        return 1.0
    
    crew_foul_diff = match.iloc[0]["avg_foul_diff"]
    if pd.isna(crew_foul_diff):
        return 1.0
    
    if row["location"] == "home":
        return 1 + (crew_foul_diff / 2) * 0.05
    else:
        return 1 - (crew_foul_diff / 2) * 0.05

df["ref_weight"] = df.apply(get_ref_weight, axis=1)

DEFAULT_FP_PER100 = 30.0
DEFAULT_PACE = 100.0

df["fp_per100"] = df["fp_per100"].fillna(DEFAULT_FP_PER100)
df["pace"] = df["pace"].fillna(DEFAULT_PACE)

df["fp_per_poss"] = df["fp_per100"] / 100
df["poss_per_min"] = df["pace"] / 48
df["fp_per_min"] = df["fp_per_poss"] * df["poss_per_min"]

df["base_fp"] = df["fp_per_min"] * df["projected_min"].fillna(0)

df["proj_fp"] = df["base_fp"] * df["line_weight"] * df["dvp_weight"] * df["ref_weight"]
df["proj_fp"] = df["proj_fp"].round(2)

output_cols = [
    "player_name", "position", "true_position", "projected_min", "salary",
    "team", "opponent", "location", "fp_per100", "fp_per_min", 
    "ref_weight", "dvp_weight", "line_weight", "proj_fp"
]

df_output = df[output_cols].copy()
df_output = df_output.rename(columns={"position": "fd_position"})

df_output = df_output.dropna(subset=["projected_min"])
df_output = df_output.sort_values("proj_fp", ascending=False)

df_output.to_sql("dfs_players", conn, if_exists="replace", index=False)

conn.close()

print(f"DFS Players table created with {len(df_output)} players.\n")
print("=== Top 20 Projected Players ===")
print(df_output.head(20).to_string(index=False))

print("\n=== Weight Summary ===")
print(f"Line Weight Range: {df_output['line_weight'].min():.3f} - {df_output['line_weight'].max():.3f}")
print(f"DVP Weight Range: {df_output['dvp_weight'].min():.3f} - {df_output['dvp_weight'].max():.3f}")
print(f"Ref Weight Range: {df_output['ref_weight'].min():.3f} - {df_output['ref_weight'].max():.3f}")
