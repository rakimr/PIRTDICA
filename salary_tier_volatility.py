"""
Salary-Tier Volatility Model (Model 5)

Uses empirical salary-tier data to regularize player variance distributions.
Salary informs the SHAPE of the distribution (sigma), not the mean projection.

Key principles:
- Individual player fp_sd is blended with tier-expected fp_sd
- Players with many games keep their own sigma; low-sample players shrink toward tier
- Unrealistic tails are capped based on empirical tier percentiles
- Coefficient of variation (CV = sigma/mean) enforces tier-appropriate ranges
"""

import sqlite3
import pandas as pd
import numpy as np


TIER_DEFAULTS = {
    'stud':     {'cv': 0.30, 'median_sd': 14.0, 'p95_cap': 70.0, 'p5_floor': 20.0, 'cv_max': 0.45},
    'mid_high': {'cv': 0.30, 'median_sd': 10.5, 'p95_cap': 55.0, 'p5_floor': 18.0, 'cv_max': 0.50},
    'mid':      {'cv': 0.37, 'median_sd': 9.6,  'p95_cap': 45.0, 'p5_floor': 10.0, 'cv_max': 0.55},
    'value':    {'cv': 0.46, 'median_sd': 9.1,  'p95_cap': 38.0, 'p5_floor': 4.0,  'cv_max': 0.65},
    'punt':     {'cv': 0.70, 'median_sd': 7.0,  'p95_cap': 28.0, 'p5_floor': 0.0,  'cv_max': 0.90},
}


def get_salary_tier(salary):
    if salary >= 9000: return 'stud'
    if salary >= 7000: return 'mid_high'
    if salary >= 5000: return 'mid'
    if salary >= 4000: return 'value'
    return 'punt'


def compute_empirical_tier_profiles(db_path='dfs_nba.db'):
    conn = sqlite3.connect(db_path)
    
    try:
        gl = pd.read_sql('SELECT player_name, fp, min FROM player_game_logs WHERE min > 0', conn)
        sal = pd.read_sql('SELECT player_name, salary FROM player_salaries', conn)
    except Exception:
        conn.close()
        return TIER_DEFAULTS
    
    conn.close()
    
    if len(gl) == 0 or len(sal) == 0:
        return TIER_DEFAULTS
    
    merged = gl.merge(sal, on='player_name', how='inner')
    if len(merged) < 50:
        return TIER_DEFAULTS
    
    merged['tier'] = merged['salary'].apply(get_salary_tier)
    
    profiles = {}
    for tier_name in ['stud', 'mid_high', 'mid', 'value', 'punt']:
        sub = merged[merged['tier'] == tier_name]
        if len(sub) < 20:
            profiles[tier_name] = TIER_DEFAULTS[tier_name]
            continue
        
        player_stats = sub.groupby('player_name').agg(
            mean_fp=('fp', 'mean'),
            sd_fp=('fp', 'std'),
            games=('fp', 'count')
        ).dropna()
        
        player_stats = player_stats[player_stats['games'] >= 5]
        if len(player_stats) < 3:
            profiles[tier_name] = TIER_DEFAULTS[tier_name]
            continue
        
        cvs = (player_stats['sd_fp'] / player_stats['mean_fp']).clip(0.1, 2.0)
        
        profiles[tier_name] = {
            'cv': round(cvs.median(), 3),
            'median_sd': round(player_stats['sd_fp'].median(), 1),
            'p95_cap': round(np.percentile(sub['fp'], 95), 1),
            'p5_floor': round(np.percentile(sub['fp'], 5), 1),
            'cv_max': round(cvs.quantile(0.85), 3),
        }
    
    return profiles


def regularize_fp_sd(df, profiles=None):
    if profiles is None:
        profiles = compute_empirical_tier_profiles()
    
    df = df.copy()
    
    if 'salary' not in df.columns or 'fp_sd' not in df.columns or 'proj_fp' not in df.columns:
        return df
    
    df['tier'] = df['salary'].apply(get_salary_tier)
    df['raw_fp_sd'] = df['fp_sd'].copy()
    
    games_col = None
    for col in ['games_pct', 'games_played']:
        if col in df.columns:
            games_col = col
            break
    
    adjusted_count = 0
    
    for idx, row in df.iterrows():
        tier = row['tier']
        profile = profiles.get(tier, TIER_DEFAULTS.get(tier, TIER_DEFAULTS['mid']))
        
        player_sd = row['fp_sd']
        proj = row['proj_fp']
        
        if pd.isna(player_sd) or pd.isna(proj) or proj <= 0:
            continue
        
        tier_expected_sd = profile['cv'] * proj
        
        if games_col and pd.notna(row.get(games_col)):
            if games_col == 'games_pct':
                confidence = min(row[games_col] / 100.0, 1.0)
            else:
                confidence = min(row[games_col] / 30.0, 1.0)
        else:
            confidence = 0.5
        
        confidence = max(0.3, confidence)
        
        blended_sd = (player_sd * confidence) + (tier_expected_sd * (1 - confidence))
        
        player_cv = blended_sd / proj
        if player_cv > profile['cv_max']:
            blended_sd = profile['cv_max'] * proj
        
        min_sd = max(3.0, profile['median_sd'] * 0.4)
        blended_sd = max(blended_sd, min_sd)
        
        df.at[idx, 'fp_sd'] = round(blended_sd, 2)
        df.at[idx, 'tier_cv'] = round(blended_sd / proj, 3) if proj > 0 else 0
        df.at[idx, 'tier_expected_sd'] = round(tier_expected_sd, 2)
        
        if abs(blended_sd - player_sd) > 0.5:
            adjusted_count += 1
    
    return df, adjusted_count, profiles


def cap_tails(df, profiles=None):
    if profiles is None:
        profiles = compute_empirical_tier_profiles()
    
    df = df.copy()
    capped_count = 0
    
    if 'tier' not in df.columns:
        df['tier'] = df['salary'].apply(get_salary_tier)
    
    for idx, row in df.iterrows():
        tier = row['tier']
        profile = profiles.get(tier, TIER_DEFAULTS.get(tier))
        if not profile:
            continue
        
        ceiling = row.get('ceiling', None)
        floor_val = row.get('floor', None)
        
        if pd.notna(ceiling) and ceiling > profile['p95_cap']:
            df.at[idx, 'ceiling'] = profile['p95_cap']
            capped_count += 1
        
        if pd.notna(floor_val) and floor_val < profile['p5_floor']:
            df.at[idx, 'floor'] = profile['p5_floor']
    
    if 'ceiling' in df.columns and 'floor' in df.columns:
        df['fp_range'] = df['ceiling'] - df['floor']
    
    if 'ceiling' in df.columns and 'proj_fp' in df.columns:
        mask = df['proj_fp'] > 0
        df.loc[mask, 'upside_ratio'] = ((df.loc[mask, 'ceiling'] - df.loc[mask, 'proj_fp']) / df.loc[mask, 'proj_fp']).round(3)
    
    return df, capped_count


def compute_value_score(df):
    df = df.copy()
    
    if 'proj_fp' not in df.columns or 'salary' not in df.columns:
        return df
    
    if 'tier' not in df.columns:
        df['tier'] = df['salary'].apply(get_salary_tier)
    
    df['value_ratio'] = (df['proj_fp'] / (df['salary'] / 1000)).round(2)
    
    tier_median_value = df.groupby('tier')['value_ratio'].transform('median')
    df['value_vs_tier'] = ((df['value_ratio'] / tier_median_value) - 1).round(3)
    
    return df


if __name__ == '__main__':
    profiles = compute_empirical_tier_profiles()
    print("=== Empirical Salary-Tier Volatility Profiles ===\n")
    for tier, data in profiles.items():
        print(f"{tier:10s}: CV={data['cv']:.3f}  median_sd={data['median_sd']:.1f}  "
              f"P5={data['p5_floor']:.1f}  P95={data['p95_cap']:.1f}  CV_max={data['cv_max']:.3f}")
    
    print("\n=== Testing on current player pool ===\n")
    dfs = pd.read_csv('dfs_players.csv')
    print(f"Loaded {len(dfs)} players")
    
    dfs, adj_count, _ = regularize_fp_sd(dfs, profiles)
    print(f"Regularized fp_sd for {adj_count} players")
    
    dfs["ceiling"] = (dfs["proj_fp"] + 1.5 * dfs["fp_sd"]).round(1)
    dfs["floor"] = (dfs["proj_fp"] - 1.0 * dfs["fp_sd"]).clip(lower=0).round(1)
    
    dfs, cap_count = cap_tails(dfs, profiles)
    print(f"Capped {cap_count} unrealistic tails")
    
    dfs = compute_value_score(dfs)
    
    print("\n=== Before vs After by Tier ===")
    for tier in ['stud', 'mid_high', 'mid', 'value', 'punt']:
        sub = dfs[dfs['tier'] == tier]
        if len(sub) == 0:
            continue
        print(f"\n{tier} (n={len(sub)}):")
        print(f"  raw_sd:   {sub['raw_fp_sd'].mean():.1f} avg")
        print(f"  reg_sd:   {sub['fp_sd'].mean():.1f} avg")
        print(f"  CV:       {sub['tier_cv'].mean():.3f} avg")
        print(f"  ceiling:  {sub['ceiling'].mean():.1f} avg")
        print(f"  floor:    {sub['floor'].mean():.1f} avg")
        print(f"  upside:   {sub['upside_ratio'].mean():.3f} avg")
        print(f"  value:    {sub['value_vs_tier'].mean():.3f} avg")
