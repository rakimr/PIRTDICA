import sqlite3
import pandas as pd
import re
import unicodedata
from baseline_minutes import get_baseline_minutes, project_minutes, get_game_context_label, clip_minutes, get_minutes_bounds
from physical_matchups import get_opposing_center_modifier

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

per100_exists = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table' AND name='player_per100'", conn)
if not per100_exists.empty:
    per100 = pd.read_sql("SELECT player_name, mpg, games_played FROM player_per100", conn)
    per100["norm_name"] = per100["player_name"].apply(lambda x: x.strip().lower() if pd.notna(x) else "")
else:
    per100 = pd.DataFrame()

def get_player_mpg(norm_name):
    """Get player's trailing average MPG."""
    if per100.empty:
        return None
    match = per100[per100["norm_name"].str.contains(norm_name.split()[0] if norm_name else "", case=False, na=False)]
    if not match.empty:
        return match.iloc[0]["mpg"]
    return None

def get_omega(depth_rank, mpg, games_played=None):
    """
    Calculate omega (player trust weight) based on role and consistency.
    Higher omega = more trust in player's actual minutes vs role baseline.
    """
    if mpg is None:
        return 0.3
    
    depth_num = int(re.search(r'\d+', depth_rank).group()) if re.search(r'\d+', depth_rank) else 1
    
    if depth_num == 1 and mpg >= 34:
        return 0.7
    elif depth_num == 1 and mpg >= 30:
        return 0.6
    elif depth_num <= 2:
        return 0.5
    elif depth_num <= 3:
        return 0.35
    else:
        return 0.2

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
    opponent = None
    if not odds.empty:
        team_odds = odds[(odds["away_team"] == team) | (odds["home_team"] == team)]
        if not team_odds.empty:
            spread = team_odds.iloc[0]["spread"]
            if team_odds.iloc[0]["away_team"] == team:
                opponent = team_odds.iloc[0]["home_team"]
            else:
                opponent = team_odds.iloc[0]["away_team"]

    opp_center_name, opp_center_physical = get_opposing_center_modifier(opponent) if opponent else (None, 0.0)

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

        foul_risk = 0.0
        foul_mins_lost = 0.0
        if pos == "C" and opp_center_physical > 0:
            base_foul_rate = 4.0
            foul_risk = min(1.0, (base_foul_rate + opp_center_physical) / 7.0)
            starter_baseline = get_baseline_minutes(f"{pos}1")
            foul_mins_lost = foul_risk * starter_baseline * 0.25

        for i, (player, norm) in enumerate(active_order):
            new_depth = i - espn_starter_index + 1
            if new_depth < 1:
                new_depth = 1

            orig_idx = [n for _, n in espn_order].index(norm)
            espn_slot = f"{pos}{orig_idx+1}"
            inferred_rank = f"{pos}{new_depth}"
            is_promoted = new_depth < (orig_idx + 1)
            is_bench_to_starter = is_promoted and new_depth == 1

            role_baseline = get_baseline_minutes(inferred_rank)

            player_mpg = get_player_mpg(norm)
            omega = get_omega(inferred_rank, player_mpg)
            
            if player_mpg is not None:
                weighted_base = (1 - omega) * role_baseline + omega * player_mpg
            else:
                weighted_base = role_baseline

            starter_bump = 10.0 if is_bench_to_starter else 0.0
            injury_bump = minutes_boost if out_at_pos else 0.0
            
            bench_penalty = 0.0
            is_espn_starter = orig_idx == 0
            is_bench_labeled = norm in bench_players
            if is_espn_starter and is_bench_labeled:
                bench_penalty = -6.0

            game_context = 0.0
            if spread is not None:
                abs_spread = abs(spread)
                if abs_spread < 5.0:
                    game_context = 2.0
                elif abs_spread >= 10.0:
                    game_context = -2.0

            foul_boost = 0.0
            if pos == "C" and foul_mins_lost > 0:
                if new_depth == 1:
                    foul_boost = -foul_mins_lost
                elif new_depth == 2:
                    foul_boost = foul_mins_lost * 0.85

            total_adjustments = starter_bump + game_context + injury_bump + bench_penalty + foul_boost
            raw_projected = weighted_base + total_adjustments
            min_floor, max_ceiling = get_minutes_bounds(inferred_rank)
            projected_min = clip_minutes(raw_projected, inferred_rank)

            rotation_rows.append({
                "team": team,
                "player_name": player,
                "espn_slot": espn_slot,
                "new_depth": inferred_rank,
                "promoted": is_promoted,
                "demoted": new_depth > (orig_idx + 1),
                "role_baseline": round(role_baseline, 2),
                "player_mpg": round(player_mpg, 1) if player_mpg else None,
                "omega": omega,
                "weighted_base": round(weighted_base, 2),
                "starter_bump": starter_bump,
                "injury_bump": round(injury_bump, 2),
                "bench_penalty": bench_penalty,
                "game_context": game_context,
                "foul_boost": round(foul_boost, 2),
                "foul_risk": round(foul_risk, 2) if pos == "C" else 0.0,
                "opp_physical": opp_center_name if pos == "C" else None,
                "min_floor": min_floor,
                "max_ceiling": max_ceiling,
                "projected_min": projected_min,
                "spread": spread,
                "game_type": get_game_context_label(spread),
                "opponent": opponent
            })

rotation_df = pd.DataFrame(rotation_rows)

if rotation_df.empty:
    print("No rotation data generated (missing salary data for starters)")
    rotation_df = pd.DataFrame(columns=[
        "team", "player_name", "espn_slot", "new_depth", "promoted", "demoted",
        "role_baseline", "player_mpg", "omega", "weighted_base",
        "starter_bump", "injury_bump", "bench_penalty", "game_context",
        "foul_boost", "foul_risk", "opp_physical",
        "min_floor", "max_ceiling", "projected_min", "spread", "game_type", "opponent"
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
