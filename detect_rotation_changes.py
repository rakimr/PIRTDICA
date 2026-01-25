import sqlite3
import pandas as pd
from baseline_minutes import get_baseline_minutes, project_minutes, get_game_context_label

conn = sqlite3.connect("dfs_nba.db")

depth = pd.read_sql("SELECT * FROM depth_charts", conn)
salaries = pd.read_sql("SELECT * FROM player_salaries", conn)

odds_exists = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table' AND name='game_odds'", conn)
if not odds_exists.empty:
    odds = pd.read_sql("SELECT * FROM game_odds", conn)
else:
    odds = pd.DataFrame()

depth["player_name"] = depth["player_name"].str.strip()
salaries["player_name"] = salaries["player_name"].str.strip()

depth = depth[depth["position_slot"].str.match(r'^[A-Z]{1,2}\d+$', na=False)]

teams = depth["team"].unique()

rotation_rows = []

for team in teams:
    team_depth = depth[depth["team"] == team].copy()
    team_salaries = salaries[salaries["team"] == team].copy()

    starters = set(team_salaries["player_name"].tolist())

    spread = None
    if not odds.empty:
        team_odds = odds[(odds["away_team"] == team) | (odds["home_team"] == team)]
        if not team_odds.empty:
            spread = team_odds.iloc[0]["spread"]

    pos_groups = {}
    for _, row in team_depth.iterrows():
        slot = row["position_slot"]
        import re
        match = re.match(r'^([A-Z]{1,2})(\d+)$', slot)
        if not match:
            continue
        pos = match.group(1)
        depth_num = int(match.group(2))
        pos_groups.setdefault(pos, []).append((depth_num, row["player_name"]))

    for pos in pos_groups:
        pos_groups[pos] = sorted(pos_groups[pos], key=lambda x: x[0])

    for pos, players in pos_groups.items():
        espn_order = [p for _, p in players]

        starting_candidates = [p for p in espn_order if p in starters]

        if not starting_candidates:
            continue

        actual_starter = starting_candidates[0]
        espn_starter_index = espn_order.index(actual_starter)

        for i, player in enumerate(espn_order):
            new_depth = i - espn_starter_index + 1

            if new_depth < 1:
                new_depth = 1

            inferred_rank = f"{pos}{new_depth}"
            is_promoted = new_depth < (i + 1)

            projection = project_minutes(
                position_slot=inferred_rank,
                is_bench_to_starter=is_promoted and new_depth == 1,
                spread=spread
            )

            rotation_rows.append({
                "team": team,
                "player_name": player,
                "espn_slot": f"{pos}{i+1}",
                "new_depth": inferred_rank,
                "promoted": is_promoted,
                "demoted": new_depth > (i + 1),
                "baseline_min": projection["baseline_min"],
                "starter_bump": projection["starter_bump"],
                "game_context": projection["game_context"],
                "projected_min": projection["projected_min"],
                "spread": spread,
                "game_type": get_game_context_label(spread)
            })

rotation_df = pd.DataFrame(rotation_rows)
rotation_df.to_sql("rotation_minutes", conn, if_exists="replace", index=False)

conn.close()

print("Rotation detection complete.")
print(f"\nFound {len(rotation_df)} player rotations across {len(teams)} teams")

promoted = rotation_df[rotation_df["promoted"] == True]
if not promoted.empty:
    print(f"\n=== Promoted Players ({len(promoted)}) ===")
    print(promoted[["team", "player_name", "espn_slot", "new_depth", "baseline_min", "starter_bump", "game_context", "projected_min"]].to_string(index=False))
