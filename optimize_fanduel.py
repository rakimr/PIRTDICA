"""
FanDuel DFS Lineup Optimizer
Uses linear programming with dual eligibility to find optimal 9-player lineup.
Constraints: $60,000 cap, 2 PG, 2 SG, 2 SF, 2 PF, 1 C

Slate-Size Aware Strategy:
- Small slates (1-3 games): Maximize floor (μ - σ) — eat chalk
- Medium slates (4-7 games): Balanced approach (μ + 0.5σ)
- Large slates (8+ games): Maximize ceiling (μ + 1.5σ) — chase upside
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
    gp = row.get('games_pct', 50) / 100
    sd = row.get('min_sd', None)
    
    if pd.isna(sd) or sd is None:
        sd = 7.0
    
    sd_factor = np.clip(1 - (sd - 3) / 7, 0, 1)
    omega = (gp * 0.5) + (sd_factor * 0.5)
    
    return round(np.clip(omega, 0.10, 0.90), 3)

def get_slate_lambda(num_games, mode='auto'):
    """
    Determine variance multiplier (λ) based on slate size and mode.
    
    λ < 0: Floor-focused (cash games, small slates)
    λ = 0: Raw projections
    λ > 0: Ceiling-focused (GPP, large slates)
    """
    if mode == 'cash':
        return -1.0
    elif mode == 'gpp':
        if num_games <= 3:
            return 0.5
        elif num_games <= 7:
            return 1.0
        else:
            return 1.5
    else:
        if num_games <= 3:
            return -0.5
        elif num_games <= 7:
            return 0.5
        else:
            return 1.5

def get_slate_description(num_games):
    """Return slate size category."""
    if num_games <= 3:
        return "SMALL"
    elif num_games <= 7:
        return "MEDIUM"
    else:
        return "LARGE"

def can_play(fd_pos, slot):
    """Check if player can fill a position slot (handles dual eligibility)."""
    if pd.isna(fd_pos):
        return False
    return slot in fd_pos.split('/')

def optimize_lineup(csv_path='dfs_players.csv', min_minutes=20, mode='auto', salary_cap=60000):
    """Run the optimizer and return optimal lineup."""
    import sqlite3
    
    df = pd.read_csv(csv_path)
    df = df.dropna(subset=['proj_fp', 'salary'])
    df = df[df['salary'] > 0]
    df = df[df['projected_min'] >= min_minutes].reset_index(drop=True)
    
    unique_teams = df['team'].nunique()
    num_games = unique_teams // 2 if unique_teams > 1 else 1
    
    lam = get_slate_lambda(num_games, mode)
    slate_size = get_slate_description(num_games)
    
    try:
        conn = sqlite3.connect('dfs_nba.db')
        vol = pd.read_sql("SELECT player_name, min_sd FROM player_volatility", conn)
        conn.close()
        df = df.merge(vol, on='player_name', how='left')
    except:
        df['min_sd'] = None
    
    df['star_weight'] = df.apply(calculate_star_weight, axis=1)
    
    if 'fp_sd' not in df.columns:
        df['fp_sd'] = 8.0
    df['fp_sd'] = df['fp_sd'].fillna(8.0)
    
    df['proj_fp_opt'] = df['proj_fp'] + (lam * df['fp_sd'])
    
    print(f"Slate: {num_games} games ({slate_size}) | Mode: {mode.upper()} | λ = {lam:+.1f}")
    print(f"Objective: μ {'+' if lam >= 0 else ''}{lam:.1f}σ")
    print(f"Players after {min_minutes}-min filter: {len(df)}")
    
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
    
    return selected, num_games, lam, mode

def print_lineup(selected, num_games, lam, mode):
    """Pretty print the optimal lineup with slate context."""
    slate_size = get_slate_description(num_games)
    
    strategy_tips = {
        "SMALL": "Floor-focused: Eat chalk, prioritize stable minutes",
        "MEDIUM": "Balanced: Mix chalk with controlled variance",
        "LARGE": "Ceiling-focused: Chase upside, embrace volatility"
    }
    
    print("\n" + "=" * 100)
    print(f"OPTIMAL FANDUEL LINEUP | {slate_size} SLATE ({num_games} games) | {mode.upper()} MODE")
    print(f"Strategy: {strategy_tips[slate_size]}")
    print("=" * 100)
    
    print(f"{'Slot':<4} {'Player':<26} {'FD Pos':<7} {'Team':<5} {'Salary':>7} {'Proj':>7} {'σ':>6} {'Ceil':>7} {'Floor':>7}")
    print("-" * 100)
    
    total_salary = 0
    total_fp = 0
    total_ceil = 0
    total_floor = 0
    
    for _, p in selected.iterrows():
        fp_sd = p.get('fp_sd', 8.0)
        ceiling = p['proj_fp'] + 1.5 * fp_sd
        floor = max(0, p['proj_fp'] - 1.0 * fp_sd)
        
        print(f"{p['assigned_slot']:<4} {p['player_name']:<26} {p['fd_position']:<7} {p['team']:<5} ${p['salary']:>6,.0f} {p['proj_fp']:>7.1f} {fp_sd:>6.1f} {ceiling:>7.1f} {floor:>7.1f}")
        total_salary += p['salary']
        total_fp += p['proj_fp']
        total_ceil += ceiling
        total_floor += floor
    
    print("-" * 100)
    print(f"{'TOTAL':<4} {'':<26} {'':<7} {'':<5} ${total_salary:>6,.0f} {total_fp:>7.1f} {'':<6} {total_ceil:>7.1f} {total_floor:>7.1f}")
    print(f"\nSalary: ${total_salary:,.0f} / $60,000  |  Remaining: ${60000 - total_salary:,.0f}")
    print(f"Projection Range: {total_floor:.1f} (floor) — {total_fp:.1f} (mean) — {total_ceil:.1f} (ceiling)")

def main():
    parser = argparse.ArgumentParser(description='FanDuel DFS Lineup Optimizer')
    parser.add_argument('--min-minutes', type=int, default=20, help='Minimum projected minutes filter')
    parser.add_argument('--mode', type=str, default='auto', choices=['auto', 'cash', 'gpp'],
                        help='Optimization mode: auto (slate-aware), cash (floor), gpp (ceiling)')
    parser.add_argument('--csv', type=str, default='dfs_players.csv', help='Path to projections CSV')
    parser.add_argument('--reliability', action='store_true', help='(Deprecated) Use --mode instead')
    args = parser.parse_args()
    
    if args.reliability:
        print("Note: --reliability is deprecated. Use --mode cash/gpp/auto instead.")
    
    selected, num_games, lam, mode = optimize_lineup(
        csv_path=args.csv,
        min_minutes=args.min_minutes,
        mode=args.mode
    )
    
    print_lineup(selected, num_games, lam, mode)

if __name__ == "__main__":
    main()
