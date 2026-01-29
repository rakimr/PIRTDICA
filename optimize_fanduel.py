"""
FanDuel DFS Lineup Optimizer
Uses linear programming with dual eligibility to find optimal 9-player lineup.
Constraints: $60,000 cap, 2 PG, 2 SG, 2 SF, 2 PF, 1 C
"""

import pandas as pd
import numpy as np
from pulp import LpMaximize, LpProblem, LpVariable, lpSum, PULP_CBC_CMD
import argparse

def calculate_star_weight(row):
    """
    Calculate reliability weight combining:
    - games_pct: availability (50% weight)
    - min_sd: minutes stability (50% weight)
    
    Formula: ω = (games_pct_factor * 0.5) + (sd_factor * 0.5)
    """
    gp = row.get('games_pct', 50) / 100  # 0-1 scale
    sd = row.get('min_sd', None)
    
    # If no SD data, assume moderate-high volatility
    if pd.isna(sd) or sd is None:
        sd = 7.0
    
    # SD factor: SD of 3 = 1.0 (perfect), SD of 10+ = 0.0 (volatile)
    sd_factor = np.clip(1 - (sd - 3) / 7, 0, 1)
    
    # Combined: 50% availability, 50% stability
    omega = (gp * 0.5) + (sd_factor * 0.5)
    
    return round(np.clip(omega, 0.10, 0.90), 3)

def can_play(fd_pos, slot):
    """Check if player can fill a position slot (handles dual eligibility)."""
    if pd.isna(fd_pos):
        return False
    return slot in fd_pos.split('/')

def optimize_lineup(csv_path='dfs_players.csv', min_minutes=20, use_reliability=False, salary_cap=60000):
    """Run the optimizer and return optimal lineup."""
    import sqlite3
    
    df = pd.read_csv(csv_path)
    df = df.dropna(subset=['proj_fp', 'salary'])
    df = df[df['salary'] > 0]
    df = df[df['projected_min'] >= min_minutes].reset_index(drop=True)
    
    # Load volatility data if available
    try:
        conn = sqlite3.connect('dfs_nba.db')
        vol = pd.read_sql("SELECT player_name, min_sd FROM player_volatility", conn)
        conn.close()
        df = df.merge(vol, on='player_name', how='left')
    except:
        df['min_sd'] = None
    
    print(f"Players after {min_minutes}-min filter: {len(df)}")
    
    if use_reliability:
        df['star_weight'] = df.apply(calculate_star_weight, axis=1)
        df['reliability_adj'] = 1 + (df['star_weight'] - 0.5) * 0.25
        df['proj_fp_opt'] = df['proj_fp'] * df['reliability_adj']
    else:
        df['proj_fp_opt'] = df['proj_fp']
        df['star_weight'] = 0.5
    
    prob = LpProblem("FanDuel_Optimizer", LpMaximize)
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
    selected = df.loc[selected_idx].copy()
    
    assigned_slots = []
    for i in selected_idx:
        for slot in ['PG', 'SG', 'SF', 'PF', 'C']:
            if (i, slot) in slot_vars and slot_vars[(i, slot)].value() == 1:
                assigned_slots.append(slot)
                break
    selected['assigned_slot'] = assigned_slots
    
    order = {'PG': 0, 'SG': 1, 'SF': 2, 'PF': 3, 'C': 4}
    selected['slot_order'] = selected['assigned_slot'].map(order)
    selected = selected.sort_values(['slot_order', 'salary'], ascending=[True, False])
    
    return selected

def print_lineup(selected, use_reliability=False):
    """Pretty print the optimal lineup."""
    mode = "Reliability-Adjusted" if use_reliability else "Raw Projections"
    
    print("\n" + "=" * 95)
    print(f"OPTIMAL FANDUEL LINEUP ({mode})")
    print("=" * 95)
    
    if use_reliability:
        print(f"{'Slot':<4} {'Player':<26} {'FD Pos':<7} {'Team':<5} {'Salary':>7} {'ω':>5} {'Proj FP':>8}")
    else:
        print(f"{'Slot':<4} {'Player':<26} {'FD Pos':<7} {'Team':<5} {'Opp':<5} {'Salary':>7} {'Proj FP':>8}")
    print("-" * 95)
    
    total_salary = 0
    total_fp = 0
    
    for _, p in selected.iterrows():
        if use_reliability:
            print(f"{p['assigned_slot']:<4} {p['player_name']:<26} {p['fd_position']:<7} {p['team']:<5} ${p['salary']:>6,.0f} {p['star_weight']:>5.2f} {p['proj_fp']:>8.2f}")
        else:
            opp = str(p.get('opponent', ''))[:5]
            print(f"{p['assigned_slot']:<4} {p['player_name']:<26} {p['fd_position']:<7} {p['team']:<5} {opp:<5} ${p['salary']:>6,.0f} {p['proj_fp']:>8.2f}")
        total_salary += p['salary']
        total_fp += p['proj_fp']
    
    print("-" * 95)
    print(f"{'TOTAL':<4} {'':<26} {'':<7} {'':<5} {'':<5} ${total_salary:>6,.0f} {total_fp:>8.2f}")
    print(f"\nSalary: ${total_salary:,.0f} / $60,000  |  Remaining: ${60000 - total_salary:,.0f}")

def main():
    parser = argparse.ArgumentParser(description='FanDuel DFS Lineup Optimizer')
    parser.add_argument('--min-minutes', type=int, default=20, help='Minimum projected minutes filter')
    parser.add_argument('--reliability', action='store_true', help='Use reliability-adjusted projections')
    parser.add_argument('--csv', type=str, default='dfs_players.csv', help='Path to projections CSV')
    args = parser.parse_args()
    
    selected = optimize_lineup(
        csv_path=args.csv,
        min_minutes=args.min_minutes,
        use_reliability=args.reliability
    )
    
    print_lineup(selected, use_reliability=args.reliability)

if __name__ == "__main__":
    main()
