"""
Context Engine v2 — Interaction-Based Projection Adjustment Layer

Evolves from roster-average matchup effects to interaction-probability-weighted
adjustments. Each component (size, archetype, familiarity, durability) is weighted
by P(i<->j), the probability that player i meaningfully interacts with opponent j.

Architecture:
  Layer 0: Baseline projection (external, matchup-agnostic)
  Layer 1: Interaction Probability Matrix W_ij
  Layer 2: Interaction-weighted Size Impact
  Layer 3: Interaction-weighted Archetype Matchup
  Layer 4: Interaction-weighted Familiarity (Bayesian-shrunk)
  Layer 5: Interaction-weighted Durability Modifier

Named after the "Clingan vs Williams Law" — the observation that roster-average
size effects masked a clear physical mismatch between two centers.
"""

import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime


TEAM_ABBR_CANONICAL = {
    'GSW': 'GS', 'NYK': 'NY', 'PHX': 'PHO', 'NOP': 'NO', 'SAS': 'SA',
    'GS': 'GS', 'NY': 'NY', 'PHO': 'PHO', 'NO': 'NO', 'SA': 'SA',
    'BKN': 'BKN', 'BK': 'BKN',
}

def normalize_team_abbr(team):
    """Normalize team abbreviation to canonical form (FanDuel standard).

    Handles NBA.com (GSW, NYK, PHX) -> FanDuel (GS, NY, PHO) mapping.
    Returns input unchanged if not a known variant.
    """
    if not team or not isinstance(team, str):
        return team
    return TEAM_ABBR_CANONICAL.get(team.strip(), team.strip())


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

INTERIOR_ARCHETYPES = {'Traditional Big', 'Versatile Big', 'Stretch 5', 'Point Center', 'Stretch 4'}

ARCHETYPE_DISTANCE = {}

def _build_archetype_distance():
    """Pre-compute pairwise archetype distances for position overlap weights.
    
    Distance is based on positional proximity:
      0.0 = same archetype (weight = 1.0)
      1.0 = adjacent positions (weight ~0.37)
      2.0 = two steps apart (weight ~0.14)
      3.0+ = far apart (weight ~0.05 or less)
    """
    archetypes = list(ARCHETYPE_POSITION_MAP.keys())
    
    position_groups = {
        'C': ['Traditional Big', 'Stretch 5', 'Point Center'],
        'PF': ['Versatile Big', 'Stretch 4'],
        'SF': ['Point Forward', 'Scoring Wing', '3-and-D Wing'],
        'SG': ['Scoring Wing', '3-and-D Wing', 'Combo Guard'],
        'PG': ['Combo Guard', 'Playmaker'],
    }
    
    pos_order = ['C', 'PF', 'SF', 'SG', 'PG']
    
    def get_positions(arch):
        return [p for p, members in position_groups.items() if arch in members]
    
    for a1 in archetypes:
        for a2 in archetypes:
            if a1 == a2:
                ARCHETYPE_DISTANCE[(a1, a2)] = 0.0
                continue
            
            pos1 = get_positions(a1)
            pos2 = get_positions(a2)
            
            min_dist = 5.0
            for p1 in pos1:
                for p2 in pos2:
                    d = abs(pos_order.index(p1) - pos_order.index(p2))
                    min_dist = min(min_dist, d)
            
            ARCHETYPE_DISTANCE[(a1, a2)] = min_dist

_build_archetype_distance()


def _normalize_name(name):
    """Normalize player name for matching (strip diacritics, standardize)."""
    import unicodedata
    if not name or not isinstance(name, str):
        return ''
    normalized = unicodedata.normalize('NFKD', name)
    ascii_name = normalized.encode('ascii', 'ignore').decode('ascii')
    return ascii_name.strip()


def load_data(db_path='dfs_nba.db', dfs_csv_path='dfs_players.csv'):
    """Load all required data tables from SQLite."""
    conn = sqlite3.connect(db_path)
    data = {}
    
    tables = {
        'game_logs': ("SELECT * FROM player_game_logs", "Game logs"),
        'measurements': ("SELECT * FROM player_measurements", "Measurements"),
        'archetypes': ("SELECT player_name, team, archetype FROM player_archetypes", "Archetypes"),
        'historical': ("SELECT * FROM historical_player_seasons", "Historical seasons"),
        'volatility': ("SELECT * FROM player_volatility", "Volatility"),
        'shot_zones': ("SELECT player_name, total_fga, ra_fga, paint_fga, mid_fga, three_fga, rim_paint_pct FROM player_shot_zones", "Shot zones"),
    }
    
    for key, (query, label) in tables.items():
        try:
            data[key] = pd.read_sql(query, conn)
            print(f"  {label}: {len(data[key])} records")
        except Exception as e:
            if key not in ('shot_zones', 'historical'):
                print(f"  WARNING: No {label.lower()}: {e}")
            data[key] = pd.DataFrame()
    
    conn.close()
    
    try:
        dfs_df = pd.read_csv(dfs_csv_path)
        if 'archetype' in dfs_df.columns and 'player_name' in dfs_df.columns:
            csv_archetypes = dfs_df[['player_name', 'team', 'archetype']].dropna(subset=['archetype'])
            db_archetypes = data['archetypes']
            
            db_names_normalized = {_normalize_name(n): n for n in db_archetypes['player_name'].values}
            
            new_rows = []
            for _, row in csv_archetypes.iterrows():
                norm_name = _normalize_name(row['player_name'])
                if norm_name not in db_names_normalized and row['player_name'] not in db_archetypes['player_name'].values:
                    new_rows.append({
                        'player_name': row['player_name'],
                        'team': row.get('team', ''),
                        'archetype': row['archetype'],
                    })
            
            if new_rows:
                data['archetypes'] = pd.concat([db_archetypes, pd.DataFrame(new_rows)], ignore_index=True)
                print(f"  Archetypes enriched: +{len(new_rows)} from DFS CSV (total: {len(data['archetypes'])})")
    except Exception as e:
        pass
    
    for df_key in ['archetypes', 'measurements', 'shot_zones', 'volatility']:
        df = data.get(df_key, pd.DataFrame())
        if 'team' in df.columns:
            data[df_key]['team'] = df['team'].apply(normalize_team_abbr)

    data['_name_map'] = {}
    data['_name_variants'] = {}
    for df_key in ['measurements', 'volatility', 'shot_zones', 'archetypes']:
        df = data.get(df_key, pd.DataFrame())
        if 'player_name' in df.columns:
            for name in df['player_name'].unique():
                norm = _normalize_name(name)
                if norm:
                    if norm not in data['_name_variants']:
                        data['_name_variants'][norm] = set()
                    data['_name_variants'][norm].add(name)
                    if norm not in data['_name_map']:
                        data['_name_map'][norm] = name
    
    return data


def _resolve_name(player_name, data):
    """Resolve a player name to its canonical form in the database."""
    name_map = data.get('_name_map', {})
    norm = _normalize_name(player_name)
    return name_map.get(norm, player_name)


def _find_player_in_df(player_name, df, data, col='player_name'):
    """Find a player in a dataframe, trying both original and normalized names."""
    result = df[df[col] == player_name]
    if len(result) > 0:
        return result
    
    variants = data.get('_name_variants', {})
    norm = _normalize_name(player_name)
    for variant in variants.get(norm, []):
        if variant != player_name:
            result = df[df[col] == variant]
            if len(result) > 0:
                return result
    return df[df[col] == player_name]


def parse_opponent_team(matchup_str):
    """Extract opponent team abbreviation from matchup string, normalized."""
    if pd.isna(matchup_str):
        return None
    matchup_str = str(matchup_str)
    if 'vs.' in matchup_str:
        parts = matchup_str.split('vs.')
        raw = parts[1].strip() if len(parts) > 1 else None
    elif '@' in matchup_str:
        parts = matchup_str.split('@')
        raw = parts[1].strip() if len(parts) > 1 else None
    else:
        return None
    return normalize_team_abbr(raw) if raw else None


def parse_player_team(matchup_str):
    """Extract player's team from matchup string, normalized."""
    if pd.isna(matchup_str):
        return None
    matchup_str = str(matchup_str).strip()
    raw = matchup_str.split(' ')[0] if ' ' in matchup_str else None
    return normalize_team_abbr(raw) if raw else None


def compute_interaction_weights(player_name, opponent_team, data):
    """Layer 1: Compute interaction probability matrix W_ij.
    
    W_ij = (PositionWeight * MinutesOverlap * RoleInteraction) / sum(all opponents)
    
    Returns dict mapping opponent_name -> weight (sums to 1.0).
    """
    archetypes = data.get('archetypes', pd.DataFrame())
    volatility = data.get('volatility', pd.DataFrame())
    shot_zones = data.get('shot_zones', pd.DataFrame())
    
    player_arch_row = _find_player_in_df(player_name, archetypes, data)
    if len(player_arch_row) == 0:
        return {}
    player_arch = player_arch_row.iloc[0]['archetype']
    
    player_vol = _find_player_in_df(player_name, volatility, data)
    player_min = player_vol.iloc[0]['avg_min'] if len(player_vol) > 0 else 20.0
    
    player_sz = _find_player_in_df(player_name, shot_zones, data)
    if len(player_sz) > 0:
        sz = player_sz.iloc[0]
        total = max(sz.get('total_fga', 1), 1)
        player_interior_usage = (sz.get('ra_fga', 0) + sz.get('paint_fga', 0)) / total
        player_perimeter_usage = (sz.get('three_fga', 0) + sz.get('mid_fga', 0)) / total
    else:
        player_interior_usage = 0.7 if player_arch in INTERIOR_ARCHETYPES else 0.3
        player_perimeter_usage = 1.0 - player_interior_usage
    
    opp_players = archetypes[archetypes['team'] == opponent_team]
    if len(opp_players) == 0:
        return {}
    
    raw_weights = {}
    
    for _, opp in opp_players.iterrows():
        opp_name = opp['player_name']
        opp_arch = opp['archetype']
        
        dist = ARCHETYPE_DISTANCE.get((player_arch, opp_arch), 4.0)
        position_weight = np.exp(-dist)
        
        opp_vol = volatility[volatility['player_name'] == opp_name]
        opp_min = opp_vol.iloc[0]['avg_min'] if len(opp_vol) > 0 else 15.0
        minutes_overlap = (player_min * opp_min) / (48.0 * 48.0)
        
        opp_sz = shot_zones[shot_zones['player_name'] == opp_name]
        if len(opp_sz) > 0:
            sz_o = opp_sz.iloc[0]
            total_o = max(sz_o.get('total_fga', 1), 1)
            opp_interior = (sz_o.get('ra_fga', 0) + sz_o.get('paint_fga', 0)) / total_o
            opp_perimeter = (sz_o.get('three_fga', 0) + sz_o.get('mid_fga', 0)) / total_o
        else:
            opp_interior = 0.7 if opp_arch in INTERIOR_ARCHETYPES else 0.3
            opp_perimeter = 1.0 - opp_interior
        
        role_interaction = (player_interior_usage * opp_interior +
                          player_perimeter_usage * opp_perimeter)
        
        raw_w = position_weight * minutes_overlap * role_interaction
        if raw_w > 1e-6:
            raw_weights[opp_name] = {
                'raw_weight': raw_w,
                'position_weight': round(position_weight, 4),
                'minutes_overlap': round(minutes_overlap, 4),
                'role_interaction': round(role_interaction, 4),
                'archetype': opp_arch,
                'avg_min': opp_min,
            }
    
    total_w = sum(v['raw_weight'] for v in raw_weights.values())
    if total_w == 0:
        return {}
    
    weights = {}
    for opp_name, info in raw_weights.items():
        weights[opp_name] = {
            'weight': round(info['raw_weight'] / total_w, 4),
            'position_weight': info['position_weight'],
            'minutes_overlap': info['minutes_overlap'],
            'role_interaction': info['role_interaction'],
            'archetype': info['archetype'],
            'avg_min': info['avg_min'],
        }
    
    return weights


def compute_size_impact(player_name, weights, data):
    """Layer 2: Interaction-weighted size impact.
    
    SizeImpact_i = Σ_j W_ij * SizeDiff_ij * InteriorGate
    
    Size differentials are z-scored league-wide.
    """
    measurements = data.get('measurements', pd.DataFrame())
    archetypes = data.get('archetypes', pd.DataFrame())
    shot_zones = data.get('shot_zones', pd.DataFrame())
    
    if len(measurements) == 0 or not weights:
        return 0.0, {}
    
    player_meas = _find_player_in_df(player_name, measurements, data)
    if len(player_meas) == 0:
        return 0.0, {}
    pm = player_meas.iloc[0]
    
    p_h = pm.get('height_inches', 0) or 0
    p_w = pm.get('weight_lbs', 0) or 0
    p_ws = pm.get('wingspan_inches', 0) or 0
    
    if p_h == 0 and p_w == 0:
        return 0.0, {}
    
    player_arch_row = _find_player_in_df(player_name, archetypes, data)
    player_arch = player_arch_row.iloc[0]['archetype'] if len(player_arch_row) > 0 else None
    
    player_sz = _find_player_in_df(player_name, shot_zones, data)
    if len(player_sz) > 0:
        sz = player_sz.iloc[0]
        total = max(sz.get('total_fga', 1), 1)
        interior_usage = (sz.get('ra_fga', 0) + sz.get('paint_fga', 0)) / total
    else:
        interior_usage = 0.7 if player_arch in INTERIOR_ARCHETYPES else 0.25
    
    total_impact = 0.0
    details = {}
    
    for opp_name, w_info in weights.items():
        w = w_info['weight']
        opp_meas = measurements[measurements['player_name'] == opp_name]
        if len(opp_meas) == 0:
            continue
        om = opp_meas.iloc[0]
        
        o_h = om.get('height_inches', 0) or 0
        o_w = om.get('weight_lbs', 0) or 0
        o_ws = om.get('wingspan_inches', 0) or 0
        
        height_diff = p_h - o_h
        weight_diff = p_w - o_w
        wingspan_diff = p_ws - o_ws
        
        raw_size = height_diff + 0.5 * weight_diff + 0.3 * wingspan_diff
        
        normalized = np.clip(raw_size / 30.0, -1.0, 1.0)
        
        size_gated = normalized * interior_usage
        
        weighted_impact = w * size_gated
        total_impact += weighted_impact
        
        if abs(w) > 0.05:
            details[opp_name] = {
                'weight': w,
                'raw_size_diff': round(raw_size, 1),
                'height_diff': round(height_diff, 1),
                'weight_diff': round(weight_diff, 1),
                'wingspan_diff': round(wingspan_diff, 1),
                'normalized': round(normalized, 3),
                'gated_impact': round(size_gated, 3),
                'weighted_impact': round(weighted_impact, 4),
                'opp_archetype': w_info['archetype'],
            }
    
    return round(total_impact, 4), {
        'total_impact': round(total_impact, 4),
        'interior_usage': round(interior_usage, 3),
        'player_archetype': player_arch,
        'opponent_details': details,
    }


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


def compute_familiarity_impact(player_name, opponent_team, weights, data, familiarity_df):
    """Layer 4: Interaction-weighted familiarity effect.
    
    Two components:
    1. Player-vs-team familiarity (team-level, from game logs)
    2. Archetype-level familiarity (from archetype matchup profiles)
    
    Both Bayesian-shrunk to prevent 1-game overfitting.
    """
    if familiarity_df is None or len(familiarity_df) == 0:
        return 0.0, {}
    
    player_vs = familiarity_df[
        (familiarity_df['player_name'] == player_name) &
        (familiarity_df['opponent'] == opponent_team)
    ]
    
    if len(player_vs) == 0:
        return 0.0, {}
    
    fam = player_vs.iloc[0]
    fam_score = fam['matchup_score']
    
    return round(fam_score, 4), {
        'score': round(fam_score, 4),
        'games_vs': int(fam['games_vs']),
        'fppm_diff': round(fam['fppm_diff'], 4),
        'shrinkage_weight': round(fam['shrinkage_weight'], 3),
    }


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
    
    team_archetype_map = {}
    for _, row in archetypes.iterrows():
        team = row['team']
        if team not in team_archetype_map:
            team_archetype_map[team] = []
        team_archetype_map[team].append(row['archetype'])
    
    records = []
    for _, game in gl.iterrows():
        opp_team = game['opponent']
        player_arch = game['archetype']
        
        if opp_team in team_archetype_map:
            for opp_arch in set(team_archetype_map[opp_team]):
                dist = ARCHETYPE_DISTANCE.get((player_arch, opp_arch), 4.0)
                if dist <= 2.0:
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


def compute_archetype_impact(player_name, weights, data, arch_profiles):
    """Layer 3: Interaction-weighted archetype matchup effect.
    
    ArchetypeImpact_i = Σ_j W_ij * AM(A_i, A_j)
    where AM is the archetype-vs-archetype performance differential.
    """
    archetypes = data.get('archetypes', pd.DataFrame())
    
    if arch_profiles is None or len(arch_profiles) == 0 or not weights:
        return 0.0, {}
    
    player_arch_row = _find_player_in_df(player_name, archetypes, data)
    if len(player_arch_row) == 0:
        return 0.0, {}
    player_arch = player_arch_row.iloc[0]['archetype']
    
    total_impact = 0.0
    details = {}
    
    for opp_name, w_info in weights.items():
        w = w_info['weight']
        opp_arch = w_info['archetype']
        
        match = arch_profiles[
            (arch_profiles['player_archetype'] == player_arch) &
            (arch_profiles['opp_archetype'] == opp_arch)
        ]
        
        if len(match) > 0:
            m = match.iloc[0]
            diff = m['fppm_diff'] * m['confidence']
            weighted = w * diff
            total_impact += weighted
            
            if abs(w) > 0.05:
                details[opp_name] = {
                    'weight': w,
                    'opp_archetype': opp_arch,
                    'fppm_diff': round(float(m['fppm_diff']), 4),
                    'confidence': round(float(m['confidence']), 3),
                    'weighted_impact': round(weighted, 4),
                }
    
    return round(total_impact, 4), {
        'total_impact': round(total_impact, 4),
        'player_archetype': player_arch,
        'opponent_details': details,
    }


def compute_durability_impact(player_name, weights, data):
    """Layer 5: Interaction-weighted durability modifier.
    
    Bidirectional:
    - Fragile opponents (high min_sd, low games) -> positive (opportunity)
    - Stable opponents (low min_sd, high games) -> negative (tougher matchup)
    
    Centered around league-average stability so the effect is zero-mean.
    """
    volatility = data.get('volatility', pd.DataFrame())
    
    if len(volatility) == 0 or not weights:
        return 0.0, {}
    
    league_avg_stability = 0.5
    if len(volatility) > 10:
        all_min_sd = volatility['min_sd'].dropna()
        all_stability = np.clip(1.0 - (all_min_sd / 10.0), 0, 1)
        league_avg_stability = all_stability.mean()
    
    total_impact = 0.0
    details = {}
    
    for opp_name, w_info in weights.items():
        w = w_info['weight']
        
        vol = volatility[volatility['player_name'] == opp_name]
        if len(vol) == 0:
            continue
        
        v = vol.iloc[0]
        min_sd = v.get('min_sd', 5.0) or 5.0
        games = v.get('games_played', 0) or 0
        
        stability = max(0, 1.0 - (min_sd / 10.0))
        
        if games < 20:
            stability *= 0.7
        
        durability_effect = league_avg_stability - stability
        
        weighted = w * durability_effect
        total_impact += weighted
        
        if abs(w) > 0.05:
            details[opp_name] = {
                'weight': w,
                'stability': round(stability, 3),
                'vs_league_avg': round(durability_effect, 3),
                'min_sd': round(min_sd, 1),
                'games': games,
            }
    
    return round(total_impact, 4), {
        'total_impact': round(total_impact, 4),
        'league_avg_stability': round(league_avg_stability, 3),
        'opponent_details': details,
    }


def compute_matchup_adjustment(player_name, opponent_team, data, familiarity_df=None, arch_profiles=None):
    """Compute the full Context Engine v2 matchup adjustment.
    
    Final formula:
    P_final = P_base + α1*SizeImpact + α2*ArchetypeImpact + α3*Familiarity + α4*Durability
    
    All components are interaction-weighted via W_ij.
    """
    alpha_size = 0.30
    alpha_archetype = 0.25
    alpha_familiarity = 0.30
    alpha_durability = 0.15
    
    opponent_team = normalize_team_abbr(opponent_team)
    resolved_name = _resolve_name(player_name, data)
    weights = compute_interaction_weights(resolved_name, opponent_team, data)
    
    if not weights:
        weights = compute_interaction_weights(player_name, opponent_team, data)
    
    if not weights:
        return {
            'player_name': player_name,
            'opponent_team': opponent_team,
            'player_archetype': None,
            'fppm_adjustment': 0.0,
            'fp_adjustment_est': 0.0,
            'details': {},
        }
    
    archetypes = data.get('archetypes', pd.DataFrame())
    player_arch_row = _find_player_in_df(player_name, archetypes, data)
    player_arch = player_arch_row.iloc[0]['archetype'] if len(player_arch_row) > 0 else None
    
    lookup_name = resolved_name if resolved_name != player_name else player_name
    size_impact, size_details = compute_size_impact(lookup_name, weights, data)
    if size_impact == 0.0 and lookup_name != player_name:
        size_impact, size_details = compute_size_impact(player_name, weights, data)
    arch_impact, arch_details = compute_archetype_impact(lookup_name, weights, data, arch_profiles)
    if arch_impact == 0.0 and lookup_name != player_name:
        arch_impact, arch_details = compute_archetype_impact(player_name, weights, data, arch_profiles)
    fam_impact, fam_details = compute_familiarity_impact(player_name, opponent_team, weights, data, familiarity_df)
    dur_impact, dur_details = compute_durability_impact(lookup_name, weights, data)
    
    raw_adjustment = (
        alpha_size * size_impact +
        alpha_archetype * arch_impact +
        alpha_familiarity * fam_impact +
        alpha_durability * dur_impact
    )
    
    MAX_FP_ADJUSTMENT = 3.0
    fp_adj = round(raw_adjustment * 30, 1)
    fp_adj = max(-MAX_FP_ADJUSTMENT, min(MAX_FP_ADJUSTMENT, fp_adj))
    
    details = {}
    top_weights = sorted(weights.items(), key=lambda x: x[1]['weight'], reverse=True)[:5]
    details['interaction_weights'] = {
        name: {'weight': info['weight'], 'archetype': info['archetype'], 'avg_min': info['avg_min']}
        for name, info in top_weights
    }
    
    if size_details:
        details['size_impact'] = size_details
    if arch_details:
        details['archetype_impact'] = arch_details
    if fam_details:
        details['familiarity'] = fam_details
    if dur_details:
        details['durability'] = dur_details
    
    details['component_scores'] = {
        'size': round(alpha_size * size_impact, 4),
        'archetype': round(alpha_archetype * arch_impact, 4),
        'familiarity': round(alpha_familiarity * fam_impact, 4),
        'durability': round(alpha_durability * dur_impact, 4),
        'raw_total': round(raw_adjustment, 4),
    }
    
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
    print("=== Building Context Engine v2 (Matchup Interaction Layer) ===\n")
    
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
    print("=== CONTEXT ENGINE v2 — VALIDATION ===")
    print("="*60)
    
    print("\n--- THE CLINGAN VS WILLIAMS LAW TEST ---")
    print("Clingan should get a boost, Williams should get a penalty vs each other's team.\n")
    
    r1 = compute_matchup_adjustment("Donovan Clingan", "CHA", data, familiarity, arch_profiles)
    r2 = compute_matchup_adjustment("Mark Williams", "POR", data, familiarity, arch_profiles)
    
    print(f"Clingan vs CHA: {r1['fp_adjustment_est']:+.1f} FP")
    print(f"  Components: {r1['details'].get('component_scores', {})}")
    if 'interaction_weights' in r1['details']:
        print(f"  Top matchups: ", end="")
        for name, info in list(r1['details']['interaction_weights'].items())[:3]:
            print(f"{name} ({info['weight']:.0%}), ", end="")
        print()
    
    print(f"\nWilliams vs POR: {r2['fp_adjustment_est']:+.1f} FP")
    print(f"  Components: {r2['details'].get('component_scores', {})}")
    if 'interaction_weights' in r2['details']:
        print(f"  Top matchups: ", end="")
        for name, info in list(r2['details']['interaction_weights'].items())[:3]:
            print(f"{name} ({info['weight']:.0%}), ", end="")
        print()
    
    size_clingan = r1['details'].get('component_scores', {}).get('size', 0)
    size_williams = r2['details'].get('component_scores', {}).get('size', 0)
    print(f"\nSize component comparison:")
    print(f"  Clingan size score: {size_clingan:+.4f}")
    print(f"  Williams size score: {size_williams:+.4f}")
    if size_clingan > 0 and size_williams < 0:
        print(f"  PASS: Size advantage is directionally correct (opposite signs)")
    elif size_clingan > 0 and size_williams >= 0:
        print(f"  PARTIAL: Clingan boosted but Williams not penalized enough")
    else:
        print(f"  CHECK: Review size component behavior")
    
    print("\n--- ADDITIONAL TEST CASES ---")
    test_cases = [
        ("LaMelo Ball", "WAS"),
        ("Jalen Green", "POR"),
        ("Nikola Jokic", "GSW"),
        ("Stephen Curry", "DEN"),
    ]
    
    for player, opp in test_cases:
        result = compute_matchup_adjustment(player, opp, data, familiarity, arch_profiles)
        print(f"\n{player} vs {opp}: {result['fp_adjustment_est']:+.1f} FP ({result['player_archetype']})")
        cs = result['details'].get('component_scores', {})
        print(f"  Size: {cs.get('size', 0):+.4f}, Arch: {cs.get('archetype', 0):+.4f}, "
              f"Fam: {cs.get('familiarity', 0):+.4f}, Dur: {cs.get('durability', 0):+.4f}")
    
    print("\n\n--- DISTRIBUTION CHECK ---")
    import pandas as pd_check
    dfs = pd_check.read_csv('dfs_players.csv')
    
    pos_count = 0
    neg_count = 0
    zero_count = 0
    all_adj = []
    
    for _, row in dfs.iterrows():
        player = row.get('player_name', '')
        opponent = row.get('opponent', '')
        if player and opponent:
            r = compute_matchup_adjustment(player, opponent, data, familiarity, arch_profiles)
            adj = r['fp_adjustment_est']
            if adj > 0:
                pos_count += 1
            elif adj < 0:
                neg_count += 1
            else:
                zero_count += 1
            all_adj.append((player, opponent, adj))
    
    print(f"Positive adjustments: {pos_count}")
    print(f"Negative adjustments: {neg_count}")
    print(f"Zero adjustments: {zero_count}")
    
    if all_adj:
        adjs = [a[2] for a in all_adj]
        print(f"Mean adjustment: {np.mean(adjs):+.2f}")
        print(f"Median adjustment: {np.median(adjs):+.2f}")
        print(f"Std adjustment: {np.std(adjs):.2f}")
        print(f"Range: [{min(adjs):+.1f}, {max(adjs):+.1f}]")
    
    if all_adj:
        sorted_adj = sorted(all_adj, key=lambda x: x[2])
        print(f"\nTop 5 Losers:")
        for p, o, a in sorted_adj[:5]:
            print(f"  {a:+.1f} FP: {p} vs {o}")
        print(f"\nTop 5 Gainers:")
        for p, o, a in sorted_adj[-5:]:
            print(f"  {a:+.1f} FP: {p} vs {o}")
