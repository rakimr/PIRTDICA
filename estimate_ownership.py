"""
Projected Ownership Estimator

Estimates public ownership by running multiple optimizer variations
and counting player frequency. Logic: most DFS players optimize similarly
(salary + minutes + value), so frequency across many lineups ≈ public ownership.

ML Calibration: Learns correction factors from FTA (FantasyTeamAdvice) ownership
data by salary tier. Over time, adjusts Monte Carlo output to match market consensus.
"""

import pandas as pd
import numpy as np
from pulp import LpMaximize, LpProblem, LpVariable, lpSum, PULP_CBC_CMD
import sqlite3
from collections import Counter
from utils.timezone import get_eastern_date_str, get_eastern_now
import unicodedata
import re
import os

def _normalize_name(name):
    """Normalize player name for matching (local copy to avoid backend import)."""
    name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII')
    name = re.sub(r'\s+(Jr\.?|Sr\.?|II|III|IV)$', '', name, flags=re.IGNORECASE)
    return name.strip()

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

def get_salary_tier(salary):
    """Categorize player by salary tier for calibration."""
    if salary >= 9000:
        return 'stud'
    elif salary >= 7000:
        return 'mid_high'
    elif salary >= 5000:
        return 'mid'
    elif salary >= 4000:
        return 'value'
    else:
        return 'punt'

def load_ownership_calibration():
    """Load learned calibration factors from database."""
    calibration = {}
    try:
        conn = sqlite3.connect("dfs_nba.db")
        cursor = conn.cursor()
        cursor.execute("""
            SELECT salary_tier, scale_factor, bias_offset, sample_count
            FROM ownership_calibration
            WHERE sample_count >= 3
        """)
        for row in cursor.fetchall():
            tier, scale, bias, count = row
            calibration[tier] = {
                'scale': scale,
                'bias': bias,
                'samples': count
            }
        conn.close()
    except Exception:
        pass
    return calibration

def apply_calibration(ownership_df, calibration):
    """Apply learned calibration factors (scale only) to raw Monte Carlo ownership."""
    if not calibration:
        return ownership_df

    df = ownership_df.copy()
    df['salary_tier'] = df['salary'].apply(get_salary_tier)
    df['raw_pown'] = df['pown_pct']

    adjusted_count = 0
    for idx, row in df.iterrows():
        tier = row['salary_tier']
        if tier in calibration:
            cal = calibration[tier]
            raw = row['pown_pct']
            adjusted = raw * cal['scale']
            adjusted = max(0, min(100, adjusted))
            df.loc[idx, 'pown_pct'] = round(adjusted, 1)
            adjusted_count += 1

    if adjusted_count > 0:
        print(f"  Applied ownership calibration to {adjusted_count} players across {len(calibration)} tiers")
        for tier, cal in sorted(calibration.items()):
            print(f"    {tier}: scale={cal['scale']:.3f} ({cal['samples']} samples)")

    return df

def save_ownership_snapshot(ownership_df):
    """Save today's ownership estimates for future calibration training."""
    today = get_eastern_date_str()
    try:
        conn = sqlite3.connect("dfs_nba.db")
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ownership_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_date TEXT,
                player_name TEXT,
                salary INTEGER,
                salary_tier TEXT,
                mc_pown_pct REAL,
                calibrated_pown_pct REAL,
                fta_pown_pct REAL,
                created_at TEXT
            )
        """)

        cursor.execute("DELETE FROM ownership_snapshots WHERE game_date = ?", (today,))

        now = get_eastern_now().isoformat()

        fta_data = {}
        try:
            fta_df = pd.read_sql_query(
                "SELECT player_name, ownership_pct FROM fta_ownership WHERE platform='FanDuel' AND game_date=?",
                conn, params=[today]
            )
            for _, r in fta_df.iterrows():
                key = _normalize_name(r['player_name']).lower()
                fta_data[key] = r['ownership_pct']
        except Exception:
            pass

        df = ownership_df.copy()
        if 'salary_tier' not in df.columns:
            df['salary_tier'] = df['salary'].apply(get_salary_tier)
        raw_col = 'raw_pown' if 'raw_pown' in df.columns else 'pown_pct'

        rows = []
        for _, row in df.iterrows():
            key = _normalize_name(row['player_name']).lower()
            fta_val = fta_data.get(key)
            rows.append((
                today,
                row['player_name'],
                int(row['salary']),
                row['salary_tier'],
                round(row.get(raw_col, row['pown_pct']), 1),
                round(row['pown_pct'], 1),
                fta_val,
                now
            ))

        cursor.executemany("""
            INSERT INTO ownership_snapshots
            (game_date, player_name, salary, salary_tier, mc_pown_pct, calibrated_pown_pct, fta_pown_pct, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()

        matched = sum(1 for r in rows if r[6] is not None)
        print(f"  Saved {len(rows)} ownership snapshots ({matched} matched to FTA data)")
        conn.close()
    except Exception as e:
        print(f"  Warning: Could not save ownership snapshot: {e}")

def update_calibration_factors():
    """Learn calibration factors from historical FTA vs MC comparisons."""
    try:
        conn = sqlite3.connect("dfs_nba.db")
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ownership_calibration (
                salary_tier TEXT PRIMARY KEY,
                scale_factor REAL DEFAULT 1.0,
                bias_offset REAL DEFAULT 0.0,
                mean_error REAL DEFAULT 0.0,
                sample_count INTEGER DEFAULT 0,
                updated_at TEXT
            )
        """)

        df = pd.read_sql_query("""
            SELECT salary_tier, mc_pown_pct, fta_pown_pct
            FROM ownership_snapshots
            WHERE fta_pown_pct IS NOT NULL
            AND mc_pown_pct IS NOT NULL
        """, conn)

        if len(df) < 5:
            print(f"  Ownership calibration: only {len(df)} matched samples, need 5+ to train")
            conn.close()
            return

        now = get_eastern_now().isoformat()

        for tier, group in df.groupby('salary_tier'):
            if len(group) < 3:
                continue

            mc_vals = group['mc_pown_pct'].values
            fta_vals = group['fta_pown_pct'].values

            mc_sum_sq = np.sum(mc_vals ** 2)
            if mc_sum_sq > 0.01:
                scale = np.sum(mc_vals * fta_vals) / mc_sum_sq
                scale = np.clip(scale, 0.3, 3.0)
            else:
                scale = 1.0

            mean_error = np.mean(fta_vals - mc_vals)

            cursor.execute("""
                INSERT INTO ownership_calibration (salary_tier, scale_factor, bias_offset, mean_error, sample_count, updated_at)
                VALUES (?, ?, 0.0, ?, ?, ?)
                ON CONFLICT(salary_tier) DO UPDATE SET
                    scale_factor = ?,
                    bias_offset = 0.0,
                    mean_error = ?,
                    sample_count = ?,
                    updated_at = ?
            """, (tier, round(scale, 4), round(mean_error, 2), len(group), now,
                  round(scale, 4), round(mean_error, 2), len(group), now))

        conn.commit()
        conn.close()

        tiers_trained = df['salary_tier'].nunique()
        print(f"  Ownership calibration: trained {tiers_trained} tier factors from {len(df)} total samples")

    except Exception as e:
        print(f"  Warning: Calibration update failed: {e}")

def estimate_ownership(csv_path='dfs_players.csv', num_iterations=100, min_minutes=15):
    """
    Estimate ownership by running optimizer many times with variations.
    
    Variations include:
    - Different noise levels on projections
    - Different salary caps ($58k-$60k)
    - Different minute thresholds
    
    After raw Monte Carlo, applies learned calibration from FTA data.
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
    
    calibration = load_ownership_calibration()
    if calibration:
        ownership_df = apply_calibration(ownership_df, calibration)
    else:
        print("  No calibration data yet - using raw Monte Carlo estimates")
    
    save_ownership_snapshot(ownership_df)
    
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
    parser.add_argument('--update-calibration', action='store_true', help='Update calibration factors from historical data')
    args = parser.parse_args()
    
    if args.update_calibration:
        print("Updating ownership calibration factors from FTA data...")
        update_calibration_factors()
    
    ownership_df = estimate_ownership(
        num_iterations=args.iterations,
        min_minutes=args.min_minutes
    )
    
    report_df = print_ownership_report(ownership_df)
    
    report_df.to_csv(args.output, index=False)
    print(f"\nSaved ownership projections to {args.output}")
