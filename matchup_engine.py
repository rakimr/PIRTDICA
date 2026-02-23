"""
Matchup Interaction Layer Engine

Combines physical measurements, historical game logs, and archetype data
to produce matchup-adjusted projection modifiers.

Components:
1. Matchup Familiarity Score (player-vs-team, archetype-vs-archetype)
2. Size Advantage Metric (height + weight + wingspan differential)
3. Durability Modifier (opponent minutes stability)
4. Projection Adjustment Layer (blended modifier output)

Named internally after the "Clingan vs Williams Law" observation.
"""

import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime


def load_data(db_path='dfs_nba.db'):
    """Load all required data tables from SQLite."""
    conn = sqlite3.connect(db_path)
    
    data = {}
    
    try:
        data['game_logs'] = pd.read_sql("SELECT * FROM player_game_logs", conn)
        print(f"  Game logs: {len(data['game_logs'])} records")
    except Exception as e:
        print(f"  WARNING: No game logs: {e}")
        data['game_logs'] = pd.DataFrame()
    
    try:
        data['measurements'] = pd.read_sql("SELECT * FROM player_measurements", conn)
        print(f"  Measurements: {len(data['measurements'])} players")
    except Exception as e:
        print(f"  WARNING: No measurements: {e}")
        data['measurements'] = pd.DataFrame()
    
    try:
        data['archetypes'] = pd.read_sql("SELECT player_name, team, archetype FROM player_archetypes", conn)
        print(f"  Archetypes: {len(data['archetypes'])} players")
    except Exception as e:
        print(f"  WARNING: No archetypes: {e}")
        data['archetypes'] = pd.DataFrame()
    
    try:
        data['historical'] = pd.read_sql("SELECT * FROM historical_player_seasons", conn)
        print(f"  Historical seasons: {len(data['historical'])} records")
    except Exception as e:
        print(f"  WARNING: No historical data: {e}")
        data['historical'] = pd.DataFrame()
    
    try:
        data['volatility'] = pd.read_sql("SELECT * FROM player_volatility", conn)
        print(f"  Volatility: {len(data['volatility'])} players")
    except Exception as e:
        data['volatility'] = pd.DataFrame()
    
    conn.close()
    return data


def parse_opponent_team(matchup_str):
    """Extract opponent team abbreviation from matchup string.
    e.g. 'TOR vs. IND' -> 'IND', 'TOR @ SAC' -> 'SAC'"""
    if pd.isna(matchup_str):
        return None
    matchup_str = str(matchup_str)
    if 'vs.' in matchup_str:
        parts = matchup_str.split('vs.')
        return parts[1].strip() if len(parts) > 1 else None
    elif '@' in matchup_str:
        parts = matchup_str.split('@')
        return parts[1].strip() if len(parts) > 1 else None
    return None


def parse_player_team(matchup_str):
    """Extract player's team from matchup string."""
    if pd.isna(matchup_str):
        return None
    matchup_str = str(matchup_str).strip()
    return matchup_str.split(' ')[0] if ' ' in matchup_str else None


ARCHETYPE_POSITION_MAP = {
    'Traditional Big': 'C',
    'Versatile Big': 'PF/C',
    'Stretch 5': 'C',
    'Point Center': 'C',
    'Stretch 4': 'PF',
    'Point Forward': 'SF/PF',
    'Scoring Wing': 'SF/SG',
    '3-and-D Wing': 'SF/SG',
    'Combo Guard': 'PG/SG',
    'Playmaker': 'PG',
}

ARCHETYPE_MATCHUP_GROUPS = {
    'bigs': ['Traditional Big', 'Versatile Big', 'Stretch 5', 'Point Center'],
    'forwards': ['Stretch 4', 'Point Forward', 'Scoring Wing'],
    'wings': ['3-and-D Wing', 'Scoring Wing'],
    'guards': ['Combo Guard', 'Playmaker'],
}


def get_matchup_group(archetype):
    """Get which group an archetype primarily matches up against."""
    for group, archetypes in ARCHETYPE_MATCHUP_GROUPS.items():
        if archetype in archetypes:
            return group
    return 'wings'


def archetypes_can_match(arch1, arch2):
    """Check if two archetypes typically guard each other."""
    g1 = get_matchup_group(arch1)
    g2 = get_matchup_group(arch2)
    if g1 == g2:
        return True
    adjacent = {
        ('bigs', 'forwards'): True,
        ('forwards', 'bigs'): True,
        ('forwards', 'wings'): True,
        ('wings', 'forwards'): True,
        ('wings', 'guards'): True,
        ('guards', 'wings'): True,
    }
    return adjacent.get((g1, g2), False)


def build_matchup_familiarity(data):
    """Build player-vs-team performance differentials from current season game logs.
    
    For each player, calculates:
    - How many times they've played each opponent
    - Their FP/min vs each opponent vs their season average
    - Shrunk by exposure count (Bayesian)
    """
    game_logs = data['game_logs']
    if len(game_logs) == 0:
        return pd.DataFrame()
    
    gl = game_logs.copy()
    gl['opponent'] = gl['matchup'].apply(parse_opponent_team)
    gl['player_team'] = gl['matchup'].apply(parse_player_team)
    gl = gl[gl['opponent'].notna() & (gl['min'] > 5)]
    gl['fppm'] = gl['fp'] / gl['min']
    
    season_avg = gl.groupby('player_name').agg(
        season_fppm=('fppm', 'mean'),
        season_games=('fppm', 'count'),
        season_fp_avg=('fp', 'mean'),
    ).reset_index()
    
    vs_team = gl.groupby(['player_name', 'opponent']).agg(
        vs_fppm=('fppm', 'mean'),
        vs_fp_avg=('fp', 'mean'),
        games_vs=('fppm', 'count'),
    ).reset_index()
    
    familiarity = vs_team.merge(season_avg, on='player_name', how='left')
    
    familiarity['fppm_diff'] = familiarity['vs_fppm'] - familiarity['season_fppm']
    familiarity['fp_diff'] = familiarity['vs_fp_avg'] - familiarity['season_fp_avg']
    
    familiarity['exposure'] = familiarity['games_vs'] / max(familiarity['games_vs'].max(), 10)
    familiarity['shrinkage_weight'] = np.log1p(familiarity['games_vs']) / np.log1p(max(familiarity['games_vs'].max(), 10))
    
    familiarity['matchup_score'] = familiarity['fppm_diff'] * familiarity['shrinkage_weight']
    
    return familiarity


def build_archetype_matchup_profiles(data):
    """Build archetype-vs-archetype performance profiles.
    
    For each game log, match the player's archetype against the opponent team's
    archetype roster, and calculate average performance differentials.
    """
    game_logs = data['game_logs']
    archetypes = data['archetypes']
    
    if len(game_logs) == 0 or len(archetypes) == 0:
        return pd.DataFrame()
    
    gl = game_logs.copy()
    gl['opponent'] = gl['matchup'].apply(parse_opponent_team)
    gl = gl[gl['opponent'].notna() & (gl['min'] > 5)]
    gl['fppm'] = gl['fp'] / gl['min']
    
    gl = gl.merge(archetypes[['player_name', 'archetype']], on='player_name', how='left')
    gl = gl[gl['archetype'].notna()]
    
    opp_archetypes = archetypes.rename(columns={'archetype': 'opp_archetype', 'team': 'opp_team'})
    
    team_archetype_map = {}
    for _, row in opp_archetypes.iterrows():
        team = row['opp_team']
        if team not in team_archetype_map:
            team_archetype_map[team] = []
        team_archetype_map[team].append(row['opp_archetype'])
    
    records = []
    for _, game in gl.iterrows():
        opp_team = game['opponent']
        player_arch = game['archetype']
        
        if opp_team in team_archetype_map:
            for opp_arch in set(team_archetype_map[opp_team]):
                if archetypes_can_match(player_arch, opp_arch):
                    records.append({
                        'player_archetype': player_arch,
                        'opp_archetype': opp_arch,
                        'fppm': game['fppm'],
                        'fp': game['fp'],
                    })
    
    if not records:
        return pd.DataFrame()
    
    matchup_df = pd.DataFrame(records)
    
    season_arch_avg = gl.groupby('archetype').agg(
        arch_avg_fppm=('fppm', 'mean'),
        arch_avg_fp=('fp', 'mean'),
    ).reset_index()
    
    arch_vs_arch = matchup_df.groupby(['player_archetype', 'opp_archetype']).agg(
        matchup_fppm=('fppm', 'mean'),
        matchup_fp=('fp', 'mean'),
        sample_size=('fppm', 'count'),
    ).reset_index()
    
    arch_vs_arch = arch_vs_arch.merge(
        season_arch_avg.rename(columns={'archetype': 'player_archetype'}),
        on='player_archetype', how='left'
    )
    
    arch_vs_arch['fppm_diff'] = arch_vs_arch['matchup_fppm'] - arch_vs_arch['arch_avg_fppm']
    arch_vs_arch['fp_diff'] = arch_vs_arch['matchup_fp'] - arch_vs_arch['arch_avg_fp']
    
    arch_vs_arch['confidence'] = np.clip(arch_vs_arch['sample_size'] / 100, 0, 1)
    
    return arch_vs_arch


def compute_size_advantage(player_name, opponent_team, data):
    """Compute the Size Advantage Metric for a player vs opponent.
    
    S_ij = (Height_i - Height_j) + 0.5*(Weight_i - Weight_j) + 0.3*(Wingspan_i - Wingspan_j)
    Weighted by whether the player is an interior archetype.
    """
    measurements = data['measurements']
    archetypes = data['archetypes']
    
    if len(measurements) == 0 or len(archetypes) == 0:
        return 0.0, {}
    
    player_meas = measurements[measurements['player_name'] == player_name]
    if len(player_meas) == 0:
        return 0.0, {}
    player_meas = player_meas.iloc[0]
    
    player_arch_row = archetypes[archetypes['player_name'] == player_name]
    player_arch = player_arch_row.iloc[0]['archetype'] if len(player_arch_row) > 0 else None
    
    opp_players = archetypes[archetypes['team'] == opponent_team]
    if len(opp_players) == 0:
        return 0.0, {}
    
    matchable_opps = []
    for _, opp in opp_players.iterrows():
        if player_arch and archetypes_can_match(player_arch, opp['archetype']):
            opp_meas = measurements[measurements['player_name'] == opp['player_name']]
            if len(opp_meas) > 0:
                matchable_opps.append((opp['player_name'], opp['archetype'], opp_meas.iloc[0]))
    
    if not matchable_opps:
        return 0.0, {}
    
    p_h = player_meas.get('height_inches', 0) or 0
    p_w = player_meas.get('weight_lbs', 0) or 0
    p_ws = player_meas.get('wingspan_inches', 0) or 0
    
    size_advantages = {}
    for opp_name, opp_arch, opp_m in matchable_opps:
        o_h = opp_m.get('height_inches', 0) or 0
        o_w = opp_m.get('weight_lbs', 0) or 0
        o_ws = opp_m.get('wingspan_inches', 0) or 0
        
        raw = (p_h - o_h) + 0.5 * (p_w - o_w) + 0.3 * (p_ws - o_ws)
        size_advantages[opp_name] = {
            'raw_size_diff': round(raw, 1),
            'height_diff': round(p_h - o_h, 1),
            'weight_diff': round(p_w - o_w, 1),
            'wingspan_diff': round(p_ws - o_ws, 1),
            'opp_archetype': opp_arch,
        }
    
    interior_archetypes = {'Traditional Big', 'Versatile Big', 'Stretch 5', 'Point Center', 'Stretch 4'}
    is_interior = player_arch in interior_archetypes
    
    interior_weight = 1.0 if is_interior else 0.3
    
    primary_opp = max(size_advantages.items(), key=lambda x: abs(x[1]['raw_size_diff']))
    primary_advantage = primary_opp[1]['raw_size_diff'] * interior_weight
    
    return round(primary_advantage, 2), {
        'primary_matchup': primary_opp[0],
        'details': size_advantages,
        'interior_weight': interior_weight,
        'player_archetype': player_arch,
    }


def compute_durability_modifier(opponent_team, data):
    """Compute durability modifier for opponent team's players.
    
    Low durability = more volatile opponent = potential advantage for opposing player.
    Based on minutes SD from player_volatility table.
    """
    volatility = data.get('volatility', pd.DataFrame())
    archetypes = data.get('archetypes', pd.DataFrame())
    
    if len(volatility) == 0 or len(archetypes) == 0:
        return {}
    
    opp_players = archetypes[archetypes['team'] == opponent_team]
    if len(opp_players) == 0:
        return {}
    
    result = {}
    for _, opp in opp_players.iterrows():
        vol = volatility[volatility['player_name'] == opp['player_name']]
        if len(vol) > 0:
            v = vol.iloc[0]
            min_sd = v.get('min_sd', 5.0) or 5.0
            games = v.get('games_played', 0) or 0
            avg_min = v.get('avg_min', 20) or 20
            
            stability = max(0, 1.0 - (min_sd / 10.0))
            
            if games < 20:
                stability *= 0.7
            
            result[opp['player_name']] = {
                'durability': round(stability, 3),
                'min_sd': round(min_sd, 1),
                'games_played': games,
                'avg_min': round(avg_min, 1),
                'archetype': opp['archetype'],
            }
    
    return result


def compute_matchup_adjustment(player_name, opponent_team, data, familiarity_df=None, arch_profiles=None):
    """Compute the full matchup adjustment for a player vs opponent team.
    
    Combines:
    - Familiarity score (player vs team history)
    - Archetype matchup profile
    - Size advantage
    - Durability modifier
    
    Returns adjustment in FP/min and supporting details.
    """
    alpha_familiarity = 0.35
    alpha_archetype = 0.25
    alpha_size = 0.25
    alpha_durability = 0.15
    
    adjustment = 0.0
    details = {}
    
    if familiarity_df is not None and len(familiarity_df) > 0:
        player_vs = familiarity_df[
            (familiarity_df['player_name'] == player_name) &
            (familiarity_df['opponent'] == opponent_team)
        ]
        if len(player_vs) > 0:
            fam = player_vs.iloc[0]
            fam_score = fam['matchup_score']
            adjustment += alpha_familiarity * fam_score
            details['familiarity'] = {
                'score': round(fam_score, 4),
                'games_vs': int(fam['games_vs']),
                'fppm_diff': round(fam['fppm_diff'], 4),
                'shrinkage_weight': round(fam['shrinkage_weight'], 3),
            }
    
    archetypes = data.get('archetypes', pd.DataFrame())
    player_arch_row = archetypes[archetypes['player_name'] == player_name]
    player_arch = player_arch_row.iloc[0]['archetype'] if len(player_arch_row) > 0 else None
    
    if arch_profiles is not None and len(arch_profiles) > 0 and player_arch:
        opp_players = archetypes[archetypes['team'] == opponent_team]
        best_arch_match = None
        best_arch_diff = 0
        
        for _, opp in opp_players.iterrows():
            if archetypes_can_match(player_arch, opp['archetype']):
                match = arch_profiles[
                    (arch_profiles['player_archetype'] == player_arch) &
                    (arch_profiles['opp_archetype'] == opp['archetype'])
                ]
                if len(match) > 0:
                    m = match.iloc[0]
                    diff = m['fppm_diff'] * m['confidence']
                    if abs(diff) > abs(best_arch_diff):
                        best_arch_diff = diff
                        best_arch_match = {
                            'opp_archetype': opp['archetype'],
                            'fppm_diff': round(m['fppm_diff'], 4),
                            'confidence': round(m['confidence'], 3),
                            'sample_size': int(m['sample_size']),
                        }
        
        if best_arch_match:
            adjustment += alpha_archetype * best_arch_diff
            details['archetype_matchup'] = best_arch_match
    
    size_adv, size_details = compute_size_advantage(player_name, opponent_team, data)
    if size_adv != 0:
        normalized_size = np.clip(size_adv / 30.0, -1.0, 1.0)
        adjustment += alpha_size * normalized_size * 0.15
        details['size_advantage'] = {
            'raw_advantage': size_adv,
            'normalized': round(normalized_size, 3),
            **size_details,
        }
    
    durability = compute_durability_modifier(opponent_team, data)
    if durability and player_arch:
        matchable_opp_dur = {}
        for opp_name, opp_info in durability.items():
            if archetypes_can_match(player_arch, opp_info['archetype']):
                matchable_opp_dur[opp_name] = opp_info
        
        if matchable_opp_dur:
            lowest_dur = min(matchable_opp_dur.items(), key=lambda x: x[1]['durability'])
            dur_factor = 1.0 - lowest_dur[1]['durability']
            adjustment += alpha_durability * dur_factor * 0.05
            details['durability'] = {
                'weakest_opponent': lowest_dur[0],
                'durability_score': lowest_dur[1]['durability'],
                'factor': round(dur_factor, 3),
            }
    
    MAX_FP_ADJUSTMENT = 3.0
    fp_adj = round(adjustment * 30, 1)
    fp_adj = max(-MAX_FP_ADJUSTMENT, min(MAX_FP_ADJUSTMENT, fp_adj))
    
    return {
        'player_name': player_name,
        'opponent_team': opponent_team,
        'player_archetype': player_arch,
        'fppm_adjustment': round(fp_adj / 30, 5),
        'fp_adjustment_est': fp_adj,
        'details': details,
    }


def build_all_matchup_data(db_path='dfs_nba.db'):
    """Build all matchup data structures and save to database."""
    print("=== Building Matchup Interaction Layer ===\n")
    
    print("Loading data...")
    data = load_data(db_path)
    
    print("\nBuilding familiarity scores...")
    familiarity = build_matchup_familiarity(data)
    if len(familiarity) > 0:
        print(f"  Built {len(familiarity)} player-vs-team matchup records")
    
    print("\nBuilding archetype matchup profiles...")
    arch_profiles = build_archetype_matchup_profiles(data)
    if len(arch_profiles) > 0:
        print(f"  Built {len(arch_profiles)} archetype-vs-archetype matchup records")
    
    conn = sqlite3.connect(db_path)
    
    if len(familiarity) > 0:
        save_cols = ['player_name', 'opponent', 'games_vs', 'vs_fppm', 'vs_fp_avg',
                     'season_fppm', 'season_fp_avg', 'fppm_diff', 'fp_diff',
                     'exposure', 'shrinkage_weight', 'matchup_score']
        available_cols = [c for c in save_cols if c in familiarity.columns]
        familiarity[available_cols].to_sql('matchup_history', conn, if_exists='replace', index=False)
        print(f"\nSaved matchup_history table: {len(familiarity)} records")
    
    if len(arch_profiles) > 0:
        arch_profiles.to_sql('archetype_matchup_profiles', conn, if_exists='replace', index=False)
        print(f"Saved archetype_matchup_profiles table: {len(arch_profiles)} records")
    
    conn.close()
    
    return data, familiarity, arch_profiles


if __name__ == "__main__":
    data, familiarity, arch_profiles = build_all_matchup_data()
    
    print("\n" + "="*60)
    print("=== MATCHUP INTERACTION LAYER TEST CASES ===")
    print("="*60)
    
    test_cases = [
        ("Donovan Clingan", "CHA"),
        ("Mark Williams", "POR"),
        ("LaMelo Ball", "WAS"),
        ("Jalen Green", "POR"),
    ]
    
    for player, opp in test_cases:
        result = compute_matchup_adjustment(player, opp, data, familiarity, arch_profiles)
        print(f"\n--- {player} vs {opp} ---")
        print(f"  Archetype: {result['player_archetype']}")
        print(f"  FPPM Adjustment: {result['fppm_adjustment']:+.5f}")
        print(f"  Est. FP Adjustment (30 min): {result['fp_adjustment_est']:+.1f}")
        
        for key, val in result['details'].items():
            if isinstance(val, dict):
                print(f"  {key}: {val}")
    
    if len(arch_profiles) > 0:
        print("\n\n=== Top Archetype Matchup Advantages ===")
        top = arch_profiles.nlargest(10, 'fppm_diff')
        for _, row in top.iterrows():
            print(f"  {row['player_archetype']} vs {row['opp_archetype']}: "
                  f"+{row['fppm_diff']:.4f} FPPM ({int(row['sample_size'])} games, "
                  f"conf: {row['confidence']:.2f})")
        
        print("\n=== Worst Archetype Matchup Disadvantages ===")
        bottom = arch_profiles.nsmallest(10, 'fppm_diff')
        for _, row in bottom.iterrows():
            print(f"  {row['player_archetype']} vs {row['opp_archetype']}: "
                  f"{row['fppm_diff']:.4f} FPPM ({int(row['sample_size'])} games, "
                  f"conf: {row['confidence']:.2f})")
