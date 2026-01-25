import sqlite3
import pandas as pd
import re
import unicodedata
from baseline_minutes import get_baseline_minutes, project_minutes, get_game_context_label

conn = sqlite3.connect("dfs_nba.db")

depth = pd.read_sql("SELECT * FROM depth_charts", conn)
salaries = pd.read_sql("SELECT * FROM player_salaries", conn)

odds_exists = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table' AND name='game_odds'", conn)
if not odds_exists.empty:
    odds = pd.read_sql("SELECT * FROM game_odds", conn)
else:
    odds = pd.DataFrame()

injury_exists = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table' AND name='injury_alerts'", conn)
if not injury_exists.empty:
    injuries = pd.read_sql("SELECT player_name, status FROM injury_alerts WHERE status = 'OUT'", conn)
else:
    injuries = pd.DataFrame()

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
    return name.strip()

out_players = set()
if not injuries.empty:
    out_players = set(injuries["player_name"].apply(normalize_name).tolist())
    print(f"Players OUT today: {len(out_players)}")

depth["player_name"] = depth["player_name"].str.strip()
salaries["player_name"] = salaries["player_name"].str.strip()
depth["norm_name"] = depth["player_name"].apply(normalize_name)
salaries["norm_name"] = salaries["player_name"].apply(normalize_name)

depth = depth[depth["position_slot"].str.match(r'^[A-Z]{1,2}\d+$', na=False)]

teams = depth["team"].unique()

rotation_rows = []

for team in teams:
    team_depth = depth[depth["team"] == team].copy()
    team_salaries = salaries[salaries["team"] == team].copy()

    starters = set(team_salaries["player_name"].tolist())
    
    bench_players = set()
    if "status" in team_salaries.columns:
        bench_salaries = team_salaries[team_salaries["status"] == "Bench"]
        bench_players = set(bench_salaries["norm_name"].tolist())

    spread = None
    if not odds.empty:
        team_odds = odds[(odds["away_team"] == team) | (odds["home_team"] == team)]
        if not team_odds.empty:
            spread = team_odds.iloc[0]["spread"]

    pos_groups = {}
    for _, row in team_depth.iterrows():
        slot = row["position_slot"]
        match = re.match(r'^([A-Z]{1,2})(\d+)$', slot)
        if not match:
            continue
        pos = match.group(1)
        depth_num = int(match.group(2))
        pos_groups.setdefault(pos, []).append((depth_num, row["player_name"], row["norm_name"]))

    for pos in pos_groups:
        pos_groups[pos] = sorted(pos_groups[pos], key=lambda x: x[0])

    for pos, players in pos_groups.items():
        espn_order = [(p, norm) for _, p, norm in players]
        
        out_at_pos = [norm for _, norm in espn_order if norm in out_players]
        active_order = [(p, norm) for p, norm in espn_order if norm not in out_players]
        
        out_minutes_pool = 0.0
        for norm in out_at_pos:
            orig_idx = [n for _, n in espn_order].index(norm)
            out_minutes_pool += get_baseline_minutes(f"{pos}{orig_idx+1}")

        starting_candidates = [p for p, norm in active_order if p in starters]

        if not starting_candidates and active_order:
            actual_starter = active_order[0][0]
        elif starting_candidates:
            actual_starter = starting_candidates[0]
        else:
            continue

        active_names = [p for p, _ in active_order]
        espn_starter_index = active_names.index(actual_starter) if actual_starter in active_names else 0

        minutes_boost = out_minutes_pool / len(active_order) if active_order else 0

        for i, (player, norm) in enumerate(active_order):
            new_depth = i - espn_starter_index + 1
            if new_depth < 1:
                new_depth = 1

            orig_idx = [n for _, n in espn_order].index(norm)
            espn_slot = f"{pos}{orig_idx+1}"
            inferred_rank = f"{pos}{new_depth}"
            is_promoted = new_depth < (orig_idx + 1)
            is_bench_to_starter = is_promoted and new_depth == 1

            original_baseline = get_baseline_minutes(espn_slot)

            starter_bump = 10.0 if is_bench_to_starter else 0.0
            injury_bump = minutes_boost if out_at_pos else 0.0
            
            bench_penalty = 0.0
            is_espn_starter = orig_idx == 0
            is_bench_labeled = norm in bench_players
            if is_espn_starter and is_bench_labeled:
                bench_penalty = -8.0

            game_context = 0.0
            if spread is not None:
                abs_spread = abs(spread)
                if abs_spread < 5.0:
                    game_context = 2.0
                elif abs_spread >= 10.0:
                    game_context = -2.0

            projected_min = max(0, original_baseline + starter_bump + game_context + injury_bump + bench_penalty)

            rotation_rows.append({
                "team": team,
                "player_name": player,
                "espn_slot": espn_slot,
                "new_depth": inferred_rank,
                "promoted": is_promoted,
                "demoted": new_depth > (orig_idx + 1),
                "baseline_min": original_baseline,
                "starter_bump": starter_bump,
                "injury_bump": round(injury_bump, 2),
                "bench_penalty": bench_penalty,
                "game_context": game_context,
                "projected_min": round(projected_min, 2),
                "spread": spread,
                "game_type": get_game_context_label(spread)
            })

rotation_df = pd.DataFrame(rotation_rows)

if rotation_df.empty:
    print("No rotation data generated (missing salary data for starters)")
    rotation_df = pd.DataFrame(columns=[
        "team", "player_name", "espn_slot", "new_depth", "promoted", "demoted",
        "baseline_min", "starter_bump", "injury_bump", "bench_penalty", "game_context", "projected_min", "spread", "game_type"
    ])
else:
    def extract_depth_num(slot):
        import re
        match = re.search(r'\d+', slot)
        return int(match.group()) if match else 99

    rotation_df["depth_num"] = rotation_df["espn_slot"].apply(extract_depth_num)
    rotation_df = rotation_df.sort_values(["team", "player_name", "depth_num"])
    rotation_df = rotation_df.drop_duplicates(subset=["team", "player_name"], keep="first")
    rotation_df = rotation_df.drop(columns=["depth_num"])

rotation_df.to_sql("rotation_minutes", conn, if_exists="replace", index=False)

conn.close()

print("Rotation detection complete.")
print(f"\nFound {len(rotation_df)} player rotations across {len(teams)} teams")

promoted = rotation_df[rotation_df["promoted"] == True]
if not promoted.empty:
    print(f"\n=== Promoted Players ({len(promoted)}) ===")
    print(promoted[["team", "player_name", "espn_slot", "new_depth", "baseline_min", "starter_bump", "game_context", "projected_min"]].to_string(index=False))
