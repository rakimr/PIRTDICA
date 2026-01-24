import pandas as pd

df = pd.read_csv("attached_assets/2012-18_playerBoxScore_1769293323500.csv")

print(f"Loaded {len(df)} player box score records")
print(f"Positions: {df['playPos'].unique().tolist()}")

df = df[df['playMin'] > 0].copy()

df = df.sort_values(['gmDate', 'teamAbbr', 'playPos', 'playMin'], ascending=[True, True, True, False])

df['depth_rank'] = df.groupby(['gmDate', 'teamAbbr', 'playPos']).cumcount() + 1

df['inferred_rank'] = df['playPos'] + df['depth_rank'].astype(str)

print(f"\nRanked {len(df)} player-game records")

baseline_minutes = df.groupby('inferred_rank')['playMin'].agg(['mean', 'count']).reset_index()
baseline_minutes.columns = ['inferred_rank', 'baseline_min', 'sample_count']
baseline_minutes = baseline_minutes.sort_values('sample_count', ascending=False)

print("\n=== Baseline Minutes by Depth Rank (top 30 by sample count) ===")
print(baseline_minutes.head(30).to_string(index=False))

baseline_minutes.to_csv("baseline_minutes_by_depth.csv", index=False)
print(f"\nSaved full results to baseline_minutes_by_depth.csv")

print("\n=== Position Breakdown ===")
for pos in ['PG', 'SG', 'SF', 'PF', 'C']:
    pos_data = baseline_minutes[baseline_minutes['inferred_rank'].str.startswith(pos)]
    pos_data = pos_data.sort_values('inferred_rank')
    print(f"\n{pos} Depth:")
    print(pos_data.to_string(index=False))
