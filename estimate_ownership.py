"""
Projected Ownership Estimator

Estimates public ownership by running multiple optimizer variations
and counting player frequency. Logic: most DFS players optimize similarly
(salary + minutes + value), so frequency across many lineups ≈ public ownership.
"""

import pandas as pd
import numpy as np
from pulp import LpMaximize, LpProblem, LpVariable, lpSum, PULP_CBC_CMD
import sqlite3
from collections import Counter

def can_play(fd_pos, slot):
    """Check if player can fill a position slot."""
    if pd.isna(fd_pos):
        return False
    return slot in fd_pos.split('/')

def run_single_optimization(df, salary_cap=60000, noise_scale=0.0):
    """Run a single optimization with optional noise for variation."""
    df = df.copy()
    
    if noise_scale > 0:
        noise = np.random.normal(0, noise_scale, len(df))
        df['proj_fp_opt'] = df['proj_fp'] + noise
    else:
        df['proj_fp_opt'] = df['proj_fp']
    
    prob = LpProblem("Ownership_Sim", LpMaximize)
    x = {i: LpVariable(f"x_{i}", cat="Binary") for i in range(len(df))}
    
    slot_vars = {}
    for slot in ['PG', 'SG', 'SF', 'PF', 'C']:
        for i in range(len(df)):
            if can_play(df.loc[i, 'fd_position'], slot):
                slot_vars[(i, slot)] = LpVariable(f"slot_{i}_{slot}", cat="Binary")
    
    prob += lpSum(df.loc[i, 'proj_fp_opt'] * x[i] for i in range(len(df)))
    prob += lpSum(df.loc[i, 'salary'] * x[i] for i in range(len(df))) <= salary_cap
    prob += lpSum(x[i] for i in range(len(df))) == 9
    
    for slot, count in [('PG', 2), ('SG', 2), ('SF', 2), ('PF', 2), ('C', 1)]:
        eligible = [i for i in range(len(df)) if can_play(df.loc[i, 'fd_position'], slot)]
        prob += lpSum(slot_vars[(i, slot)] for i in eligible if (i, slot) in slot_vars) == count
    
    for i in range(len(df)):
        player_slots = [slot for slot in ['PG', 'SG', 'SF', 'PF', 'C'] if (i, slot) in slot_vars]
        if player_slots:
            prob += lpSum(slot_vars[(i, slot)] for slot in player_slots) == x[i]
    
    prob.solve(PULP_CBC_CMD(msg=0))
    
    selected_idx = [i for i in range(len(df)) if x[i].value() == 1]
    return df.loc[selected_idx, 'player_name'].tolist()

def estimate_ownership(csv_path='dfs_players.csv', num_iterations=100, min_minutes=15):
    """
    Estimate ownership by running optimizer many times with variations.
    
    Variations include:
    - Different noise levels on projections
    - Different salary caps ($58k-$60k)
    - Different minute thresholds
    """
    print(f"Loading players from {csv_path}...")
    df = pd.read_csv(csv_path)
    df = df.dropna(subset=['proj_fp', 'salary'])
    df = df[df['salary'] > 0]
    df = df[df['projected_min'] >= min_minutes].reset_index(drop=True)
    
    print(f"Running {num_iterations} optimizer variations...")
    
    all_players = []
    
    for i in range(num_iterations):
        noise = np.random.uniform(1.0, 4.0)
        cap_variation = np.random.choice([58500, 59000, 59500, 60000])
        
        try:
            players = run_single_optimization(df, salary_cap=cap_variation, noise_scale=noise)
            all_players.extend(players)
        except Exception as e:
            continue
        
        if (i + 1) % 20 == 0:
            print(f"  Completed {i + 1}/{num_iterations} iterations")
    
    player_counts = Counter(all_players)
    total_lineups = num_iterations
    
    ownership = []
    for player, count in player_counts.items():
        pown = (count / total_lineups) * 100
        ownership.append({
            'player_name': player,
            'appearances': count,
            'pown_pct': round(pown, 1)
        })
    
    ownership_df = pd.DataFrame(ownership)
    ownership_df = ownership_df.sort_values('pown_pct', ascending=False)
    
    ownership_df = df[['player_name', 'team', 'salary', 'proj_fp', 'fd_position']].merge(
        ownership_df, on='player_name', how='left'
    )
    ownership_df['pown_pct'] = ownership_df['pown_pct'].fillna(0)
    ownership_df['appearances'] = ownership_df['appearances'].fillna(0).astype(int)
    ownership_df = ownership_df.sort_values('pown_pct', ascending=False)
    
    return ownership_df

def get_ownership_tiers(ownership_df):
    """Categorize players into ownership tiers."""
    df = ownership_df.copy()
    
    def get_tier(pown):
        if pown >= 30:
            return 'Chalk (30%+)'
        elif pown >= 15:
            return 'Popular (15-30%)'
        elif pown >= 5:
            return 'Moderate (5-15%)'
        elif pown > 0:
            return 'Low (1-5%)'
        else:
            return 'Contrarian (0%)'
    
    df['ownership_tier'] = df['pown_pct'].apply(get_tier)
    return df

def print_ownership_report(ownership_df):
    """Print ownership analysis report."""
    df = get_ownership_tiers(ownership_df)
    
    print("\n" + "="*60)
    print("PROJECTED OWNERSHIP REPORT")
    print("="*60)
    
    print("\n=== CHALK PLAYS (30%+) ===")
    chalk = df[df['pown_pct'] >= 30][['player_name', 'team', 'salary', 'proj_fp', 'pown_pct']]
    if len(chalk) > 0:
        print(chalk.to_string(index=False))
    else:
        print("No chalk plays")
    
    print("\n=== POPULAR PLAYS (15-30%) ===")
    popular = df[(df['pown_pct'] >= 15) & (df['pown_pct'] < 30)][['player_name', 'team', 'salary', 'proj_fp', 'pown_pct']]
    if len(popular) > 0:
        print(popular.head(10).to_string(index=False))
    else:
        print("No popular plays")
    
    print("\n=== MODERATE OWNERSHIP (5-15%) ===")
    moderate = df[(df['pown_pct'] >= 5) & (df['pown_pct'] < 15)][['player_name', 'team', 'salary', 'proj_fp', 'pown_pct']]
    if len(moderate) > 0:
        print(moderate.head(10).to_string(index=False))
    else:
        print("No moderate plays")
    
    print("\n=== CONTRARIAN PLAYS (High Value, Low Own) ===")
    df['value'] = df['proj_fp'] / (df['salary'] / 1000)
    contrarian = df[(df['pown_pct'] < 5) & (df['value'] >= 5.5)][['player_name', 'team', 'salary', 'proj_fp', 'value', 'pown_pct']]
    if len(contrarian) > 0:
        print(contrarian.head(10).to_string(index=False))
    else:
        print("No high-value contrarian plays")
    
    print("\n" + "="*60)
    
    return df

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Estimate projected ownership')
    parser.add_argument('--iterations', type=int, default=1000, help='Number of optimizer runs (1000 = stable, 10000 = very good)')
    parser.add_argument('--min-minutes', type=int, default=15, help='Minimum projected minutes')
    parser.add_argument('--output', type=str, default='ownership_projections.csv', help='Output file')
    args = parser.parse_args()
    
    ownership_df = estimate_ownership(
        num_iterations=args.iterations,
        min_minutes=args.min_minutes
    )
    
    report_df = print_ownership_report(ownership_df)
    
    report_df.to_csv(args.output, index=False)
    print(f"\nSaved ownership projections to {args.output}")
