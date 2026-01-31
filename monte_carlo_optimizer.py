"""
Monte Carlo DFS Lineup Optimizer

Instead of optimizing for expected value, this simulates thousands of 
possible outcomes and finds lineups that WIN most often.

For GPPs, you don't need the highest average score - you need to finish 1st.
"""

import pandas as pd
import numpy as np
from pulp import LpMaximize, LpProblem, LpVariable, lpSum, PULP_CBC_CMD
import argparse
from collections import defaultdict

def can_play(fd_pos, slot):
    if pd.isna(fd_pos):
        return False
    return slot in str(fd_pos).split('/')

def generate_lineup(df, salary_cap=60000, excluded_players=None):
    """Generate a single optimal lineup using LP."""
    if excluded_players is None:
        excluded_players = set()
    
    pool = df[~df['player_name'].isin(excluded_players)].reset_index(drop=True)
    
    prob = LpProblem("Lineup", LpMaximize)
    x = {i: LpVariable(f"x_{i}", cat="Binary") for i in range(len(pool))}
    
    slot_vars = {}
    for slot in ['PG', 'SG', 'SF', 'PF', 'C']:
        for i in range(len(pool)):
            if can_play(pool.loc[i, 'fd_position'], slot):
                slot_vars[(i, slot)] = LpVariable(f"slot_{i}_{slot}", cat="Binary")
    
    prob += lpSum(pool.loc[i, 'score'] * x[i] for i in range(len(pool)))
    prob += lpSum(pool.loc[i, 'salary'] * x[i] for i in range(len(pool))) <= salary_cap
    prob += lpSum(x[i] for i in range(len(pool))) == 9
    
    for slot, count in [('PG', 2), ('SG', 2), ('SF', 2), ('PF', 2), ('C', 1)]:
        eligible = [i for i in range(len(pool)) if can_play(pool.loc[i, 'fd_position'], slot)]
        prob += lpSum(slot_vars[(i, slot)] for i in eligible if (i, slot) in slot_vars) == count
    
    for i in range(len(pool)):
        player_slots = [slot for slot in ['PG', 'SG', 'SF', 'PF', 'C'] if (i, slot) in slot_vars]
        if player_slots:
            prob += lpSum(slot_vars[(i, slot)] for slot in player_slots) == x[i]
    
    prob.solve(PULP_CBC_CMD(msg=0))
    
    if prob.status != 1:
        return None
    
    selected_idx = [i for i in range(len(pool)) if x[i].value() == 1]
    return set(pool.loc[selected_idx, 'player_name'].tolist())

def generate_diverse_lineups(df, num_lineups=20, salary_cap=60000):
    """Generate multiple diverse lineups by excluding previous selections."""
    lineups = []
    all_excluded = set()
    
    for i in range(num_lineups):
        df_temp = df.copy()
        if i > 0:
            noise = np.random.normal(0, df['fp_sd'] * 0.3)
            df_temp['score'] = df_temp['score'] + noise
        
        lineup = generate_lineup(df_temp, salary_cap)
        if lineup and lineup not in lineups:
            lineups.append(lineup)
    
    return lineups

def simulate_outcomes(df, num_sims=10000):
    """Simulate player outcomes using normal distribution."""
    players = df['player_name'].tolist()
    means = df['proj_fp'].values
    stds = df['fp_sd'].values
    
    simulations = np.random.normal(
        means.reshape(1, -1),
        stds.reshape(1, -1),
        size=(num_sims, len(players))
    )
    simulations = np.maximum(simulations, 0)
    
    return pd.DataFrame(simulations, columns=players)

def evaluate_lineups(lineups, sim_results):
    """Calculate win rates for each lineup across simulations."""
    num_sims = len(sim_results)
    lineup_scores = np.zeros((num_sims, len(lineups)))
    
    for i, lineup in enumerate(lineups):
        lineup_scores[:, i] = sim_results[list(lineup)].sum(axis=1)
    
    winners = np.argmax(lineup_scores, axis=1)
    
    stats = []
    for i, lineup in enumerate(lineups):
        wins = np.sum(winners == i)
        scores = lineup_scores[:, i]
        stats.append({
            'lineup_id': i + 1,
            'players': lineup,
            'win_rate': wins / num_sims * 100,
            'avg_score': np.mean(scores),
            'median_score': np.median(scores),
            'p90_score': np.percentile(scores, 90),
            'p10_score': np.percentile(scores, 10),
            'max_score': np.max(scores),
            'std_score': np.std(scores)
        })
    
    return sorted(stats, key=lambda x: -x['win_rate'])

def print_lineup_details(df, lineup, lineup_stats):
    """Print detailed breakdown of a lineup."""
    players_df = df[df['player_name'].isin(lineup)].copy()
    players_df = players_df.sort_values('salary', ascending=False)
    
    print(f"\n{'Player':<26} {'Pos':<8} {'Team':<5} {'Salary':>7} {'Proj':>7} {'SD':>6} {'Ceil':>7}")
    print("-" * 75)
    
    total_sal = 0
    total_proj = 0
    
    for _, p in players_df.iterrows():
        ceil = p['proj_fp'] + 1.5 * p['fp_sd']
        print(f"{p['player_name']:<26} {p['fd_position']:<8} {p['team']:<5} ${p['salary']:>6,} {p['proj_fp']:>7.1f} {p['fp_sd']:>6.1f} {ceil:>7.1f}")
        total_sal += p['salary']
        total_proj += p['proj_fp']
    
    print("-" * 75)
    print(f"{'TOTAL':<26} {'':<8} {'':<5} ${total_sal:>6,} {total_proj:>7.1f}")
    print(f"\nSalary: ${total_sal:,} / $60,000")
    print(f"Win Rate: {lineup_stats['win_rate']:.2f}%")
    print(f"Score Distribution: {lineup_stats['p10_score']:.1f} (10th) | {lineup_stats['avg_score']:.1f} (avg) | {lineup_stats['p90_score']:.1f} (90th) | {lineup_stats['max_score']:.1f} (max)")

def main():
    parser = argparse.ArgumentParser(description='Monte Carlo DFS Optimizer')
    parser.add_argument('--lineups', type=int, default=20, help='Number of lineups to generate')
    parser.add_argument('--sims', type=int, default=10000, help='Number of simulations')
    parser.add_argument('--min-minutes', type=int, default=20, help='Minimum projected minutes')
    parser.add_argument('--csv', type=str, default='dfs_players.csv', help='Path to projections CSV')
    parser.add_argument('--top', type=int, default=5, help='Show top N lineups')
    args = parser.parse_args()
    
    print(f"Loading data from {args.csv}...")
    df = pd.read_csv(args.csv)
    df = df.dropna(subset=['proj_fp', 'salary'])
    df = df[df['salary'] > 0]
    df = df[df['projected_min'] >= args.min_minutes].reset_index(drop=True)
    
    df['fp_sd'] = df['fp_sd'].fillna(8.0)
    df['score'] = df['proj_fp'] + 1.0 * df['fp_sd']
    
    print(f"Players in pool: {len(df)}")
    print(f"Generating {args.lineups} diverse lineups...")
    
    lineups = generate_diverse_lineups(df, num_lineups=args.lineups)
    print(f"Generated {len(lineups)} unique lineups")
    
    print(f"Running {args.sims:,} simulations...")
    sim_results = simulate_outcomes(df, num_sims=args.sims)
    
    print("Evaluating lineups...")
    stats = evaluate_lineups(lineups, sim_results)
    
    print("\n" + "=" * 80)
    print("MONTE CARLO RESULTS - LINEUPS RANKED BY WIN PROBABILITY")
    print("=" * 80)
    
    print(f"\n{'Rank':<5} {'Win%':>7} {'Avg':>7} {'P90':>7} {'Max':>7} {'StdDev':>7}")
    print("-" * 50)
    
    for i, s in enumerate(stats[:args.top]):
        print(f"{i+1:<5} {s['win_rate']:>6.2f}% {s['avg_score']:>7.1f} {s['p90_score']:>7.1f} {s['max_score']:>7.1f} {s['std_score']:>7.1f}")
    
    print("\n" + "=" * 80)
    print("TOP LINEUP BY WIN PROBABILITY")
    print("=" * 80)
    
    best = stats[0]
    print_lineup_details(df, best['players'], best)
    
    if len(stats) > 1:
        print("\n" + "=" * 80)
        print("SECOND BEST LINEUP (for diversity)")
        print("=" * 80)
        second = stats[1]
        print_lineup_details(df, second['players'], second)

if __name__ == "__main__":
    main()
