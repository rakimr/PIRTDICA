import sqlite3
import pandas as pd
import numpy as np
import re
import unicodedata

conn = sqlite3.connect("dfs_nba.db")

salaries = pd.read_sql("SELECT * FROM player_salaries", conn)

if salaries.empty:
    print("No player salary data available. DFS players table not created.")
    conn.close()
    exit(0)

rotation = pd.read_sql("SELECT * FROM rotation_minutes", conn)
game_odds = pd.read_sql("SELECT * FROM game_odds", conn)
dvp = pd.read_sql("SELECT * FROM dvp_blended", conn)
game_foul_env = pd.read_sql("SELECT * FROM game_foul_environment", conn)
hist_lines = pd.read_sql("SELECT team, AVG(team_line) as avg_team_line FROM historic_lines GROUP BY team", conn)
player_stats = pd.read_sql("SELECT * FROM player_stats", conn)
player_positions = pd.read_sql("SELECT player_name, true_position FROM player_positions", conn)

NAME_ALIASES = {
    "ryan nembhard": "rj nembhard",
    "nicolas claxton": "nic claxton",
    "cameron thomas": "cam thomas",
    "kenneth lofton": "kenneth lofton",
    "nicolas batum": "nic batum",
    "patty mills": "patrick mills",
    "egor dmin": "egor demin",
    "ronald holland": "ron holland",
}

def normalize_name(name):
    if pd.isna(name):
        return ""
    name = str(name).strip()
    try:
        name = name.encode('latin-1').decode('utf-8')
    except (UnicodeDecodeError, UnicodeEncodeError):
        pass
    name = name.lower()
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
    name = re.sub(r'\.', '', name)
    name = re.sub(r'-', ' ', name)
    name = re.sub(r'\s+(jr|sr|ii|iii|iv|v)\.?$', '', name)
    name = re.sub(r'\s+', ' ', name)
    name = name.strip()
    return NAME_ALIASES.get(name, name)

salaries["player_name"] = salaries["player_name"].str.strip()
rotation["player_name"] = rotation["player_name"].str.strip()
player_stats["player_name"] = player_stats["player_name"].str.strip()
player_positions["player_name"] = player_positions["player_name"].str.strip()

salaries["norm_name"] = salaries["player_name"].apply(normalize_name)
rotation["norm_name"] = rotation["player_name"].apply(normalize_name)
player_stats["norm_name"] = player_stats["player_name"].apply(normalize_name)
player_positions["norm_name"] = player_positions["player_name"].apply(normalize_name)

df = salaries.merge(
    rotation[["team", "norm_name", "espn_slot", "projected_min"]],
    on=["team", "norm_name"],
    how="left"
)

df = df.merge(
    player_stats[["norm_name", "fp_pg", "fp_per_min", "games_played", "mpg", "usg_pct"]],
    on="norm_name",
    how="left"
)

injury_alerts = pd.read_sql("SELECT * FROM injury_alerts WHERE status IN ('OUT', 'Doubtful')", conn)
injury_alerts["norm_name"] = injury_alerts["player_name"].apply(normalize_name)

injury_alerts = injury_alerts.merge(
    player_stats[["norm_name", "team", "usg_pct"]].drop_duplicates(subset=["norm_name"]),
    on="norm_name",
    how="left"
)

TEAM_GAMES_PLAYED = 48

df["games_pct"] = (df["games_played"] / TEAM_GAMES_PLAYED * 100).round(1)
df["games_pct"] = df["games_pct"].fillna(0)

GP_THRESHOLD = 60
df["low_gp_flag"] = df["games_pct"] < GP_THRESHOLD

def get_gp_penalty(games_pct):
    if pd.isna(games_pct) or games_pct >= GP_THRESHOLD:
        return 1.0
    elif games_pct >= 50:
        return 0.95
    elif games_pct >= 40:
        return 0.90
    else:
        return 0.85

df["gp_weight"] = df["games_pct"].apply(get_gp_penalty)

USAGE_BETA = 0.7

def calculate_usage_adjustment(row):
    """
    Calculate usage-based FPPM adjustment when teammates are injured.
    
    Formula: FPPM_adj = FPPM_base × (1 + β × (Usage_adj - Usage_base) / Usage_base)
    
    Usage redistribution: When a player is OUT, their usage is redistributed
    proportionally to remaining teammates based on their baseline usage.
    """
    team = row["team"]
    player_norm = row["norm_name"]
    base_usage = row.get("usg_pct", 20.0)
    base_fppm = row.get("fp_per_min", 1.0)
    
    if pd.isna(base_usage) or base_usage <= 0:
        base_usage = 20.0
    if pd.isna(base_fppm) or base_fppm <= 0:
        base_fppm = 1.0
    
    team_injuries = injury_alerts[(injury_alerts["team"] == team) & (injury_alerts["team"].notna())]
    if team_injuries.empty:
        return base_fppm, base_usage, 0.0
    
    injured_usage = 0.0
    for _, inj in team_injuries.iterrows():
        inj_norm = inj["norm_name"]
        if inj_norm == player_norm:
            continue
        inj_usg = inj.get("usg_pct", 0)
        if pd.notna(inj_usg) and inj_usg > 0:
            injured_usage += inj_usg
    
    if injured_usage <= 0:
        return base_fppm, base_usage, 0.0
    
    team_active = df[(df["team"] == team) & (~df["norm_name"].isin(team_injuries["norm_name"]))]
    team_total_usage = team_active["usg_pct"].fillna(20.0).sum()
    
    if team_total_usage <= 0:
        team_total_usage = 100.0
    
    usage_share = base_usage / team_total_usage
    usage_boost = injured_usage * usage_share * 0.6
    
    adj_usage = base_usage + usage_boost
    
    usage_delta_pct = (adj_usage - base_usage) / base_usage
    fppm_adj = base_fppm * (1 + USAGE_BETA * usage_delta_pct)
    
    return fppm_adj, adj_usage, usage_boost

usage_results = df.apply(calculate_usage_adjustment, axis=1, result_type='expand')
df["fppm_adj"] = usage_results[0]
df["usg_adj"] = usage_results[1]
df["usg_boost"] = usage_results[2]

df["usg_pct"] = df["usg_pct"].fillna(20.0)

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

df["implied_total"] = (df["total"] / 2) - (df["spread"] / 2)

df = df.merge(hist_lines, on="team", how="left")

df["line_weight"] = df["implied_total"] / df["avg_team_line"]
df["line_weight"] = df["line_weight"].fillna(1.0)

df = df.merge(
    player_positions[["norm_name", "true_position"]],
    on="norm_name",
    how="left"
)

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

DEFAULT_FP_PER_MIN = 1.0

df["fp_per_min"] = df["fp_per_min"].fillna(DEFAULT_FP_PER_MIN)
df["fppm_adj"] = df["fppm_adj"].fillna(df["fp_per_min"])
df["fp_pg"] = df["fp_pg"].fillna(0)

df["base_fp"] = df["fppm_adj"] * df["projected_min"].fillna(0)

vol_df = pd.read_sql("SELECT player_name, min_sd, fp_sd, avg_fp, max_fp, min_fp, avg_fppm, fppm_sd FROM player_volatility", conn)
df = df.merge(vol_df, on='player_name', how='left')
df['fp_sd'] = df['fp_sd'].fillna(15.0)
df['hist_avg_fp'] = df['avg_fp']
df['hist_max_fp'] = df['max_fp']
df['hist_min_fp'] = df['min_fp']

def calc_omega(row):
    gp = row.get('games_pct', 50) / 100
    sd = row.get('min_sd', None)
    if pd.isna(sd):
        sd = 7.0
    sd_factor = max(0, min(1, 1 - (sd - 3) / 7))
    return round(max(0.10, min(0.90, (gp * 0.5) + (sd_factor * 0.5))), 3)

df['omega'] = df.apply(calc_omega, axis=1)
df['omega_weight'] = 0.95 + df['omega'] * 0.10

try:
    standings_df = pd.read_sql("SELECT team, incentive_score, variance_multiplier FROM team_standings", conn)
    df = df.merge(standings_df, on='team', how='left')
    df['incentive_score'] = df['incentive_score'].fillna(0.0)
    df['variance_multiplier'] = df['variance_multiplier'].fillna(1.0)
    df['fp_sd'] = df['fp_sd'] * df['variance_multiplier']
    print(f"Applied incentive-based variance adjustments to {(df['variance_multiplier'] != 1.0).sum()} players")
except Exception as e:
    df['incentive_score'] = 0.0
    df['variance_multiplier'] = 1.0

df["proj_fp"] = df["base_fp"] * df["line_weight"] * df["dvp_weight"] * df["ref_weight"] * df["gp_weight"] * df["omega_weight"]

try:
    from sqlalchemy import create_engine
    from utils.name_normalize import normalize_player_name
    import os
    database_url = os.environ.get('DATABASE_URL')
    if database_url:
        pg_engine = create_engine(database_url)
        adj_factors = pd.read_sql_query(
            "SELECT player_name_normalized, adjustment_factor, sample_size FROM player_adjustment_factors WHERE sample_size >= 3",
            pg_engine
        )
        if len(adj_factors) > 0:
            adj_dict = dict(zip(adj_factors['player_name_normalized'], adj_factors['adjustment_factor']))
            df['player_name_normalized'] = df['player_name'].apply(normalize_player_name)
            df['ml_adjustment'] = df['player_name_normalized'].map(adj_dict).fillna(1.0)
            df["proj_fp"] = df["proj_fp"] * df["ml_adjustment"]
            adjusted_count = (df['ml_adjustment'] != 1.0).sum()
            print(f"Applied ML adjustments to {adjusted_count} players based on historical performance")
        else:
            df['ml_adjustment'] = 1.0
except Exception as e:
    df['ml_adjustment'] = 1.0

df["proj_fp"] = df["proj_fp"].round(2)

df["ceiling"] = (df["proj_fp"] + 1.5 * df["fp_sd"]).round(1)
df["floor"] = (df["proj_fp"] - 1.0 * df["fp_sd"]).clip(lower=0).round(1)
df["fp_range"] = df["ceiling"] - df["floor"]
df["upside_ratio"] = ((df["ceiling"] - df["proj_fp"]) / df["proj_fp"]).round(3)

def clean_display_name(name):
    """Remove Jr., Sr., II, III suffixes for cleaner display."""
    if pd.isna(name):
        return name
    name = str(name).strip()
    name = re.sub(r'\s+(Jr\.?|Sr\.?|II|III|IV|V)$', '', name, flags=re.IGNORECASE)
    return name.strip()

df["player_name"] = df["player_name"].apply(clean_display_name)

output_cols = [
    "player_name", "position", "true_position", "projected_min", "salary",
    "team", "opponent", "location", "implied_total", "fp_pg", "fp_per_min", "usg_pct", "usg_boost", "fppm_adj",
    "ref_weight", "dvp_weight", "line_weight", "games_pct", "gp_weight", "low_gp_flag", "min_sd", "omega", "omega_weight",
    "proj_fp", "fp_sd", "ceiling", "floor", "fp_range", "upside_ratio", "hist_max_fp", "hist_min_fp"
]

df_output = df[output_cols].copy()
df_output = df_output.rename(columns={"position": "fd_position"})

df_output = df_output.dropna(subset=["projected_min"])
df_output = df_output.dropna(subset=["opponent"])
df_output = df_output.sort_values("proj_fp", ascending=False)

df_output.to_sql("dfs_players", conn, if_exists="replace", index=False)

conn.close()

df_output.to_csv("dfs_players.csv", index=False)

print(f"DFS Players table created with {len(df_output)} players.\n")
print("=== Top 20 Projected Players ===")
print(df_output.head(20).to_string(index=False))

print("\n=== Weight Summary ===")
print(f"Line Weight Range: {df_output['line_weight'].min():.3f} - {df_output['line_weight'].max():.3f}")
print(f"DVP Weight Range: {df_output['dvp_weight'].min():.3f} - {df_output['dvp_weight'].max():.3f}")
print(f"Ref Weight Range: {df_output['ref_weight'].min():.3f} - {df_output['ref_weight'].max():.3f}")
print(f"\nExported to dfs_players.csv")
