"""
Player Value Analysis and Prop Insights
Calculates value metrics and identifies high-value prop opportunities using DVP data.
"""
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import pandas as pd
import sqlite3
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
from utils.timezone import get_eastern_date_str

STAT_CATEGORIES = ['pts', 'reb', 'ast', 'stl', 'blk', '3pm']

def load_data():
    """Load player projections, DVP data, and DVA data."""
    players_df = pd.read_csv("dfs_players.csv")
    
    conn = sqlite3.connect("dfs_nba.db")
    dvp_df = pd.read_sql_query("SELECT * FROM dvp_blended", conn)
    stats_df = pd.read_sql_query("""
        SELECT player_name, team, pts_pg, reb_pg, ast_pg, stl_pg, blk_pg 
        FROM player_stats
    """, conn)
    try:
        dva_df = pd.read_sql_query("SELECT * FROM dva_stats", conn)
    except Exception:
        dva_df = pd.DataFrame()
    conn.close()
    
    return players_df, dvp_df, stats_df, dva_df

def calculate_value_metrics(players_df):
    """Calculate player value metrics."""
    df = players_df.copy()
    
    df = df[df['salary'] > 0].copy()
    
    df['value'] = df['proj_fp'] / (df['salary'] / 1000)
    
    df['ceiling_value'] = df['ceiling'] / (df['salary'] / 1000)
    
    df['floor_value'] = df['floor'] / (df['salary'] / 1000)
    
    df['upside_per_k'] = (df['ceiling'] - df['proj_fp']) / (df['salary'] / 1000)
    
    df['value_rank'] = df['value'].rank(ascending=False)
    df['salary_tier'] = pd.cut(df['salary'], 
                               bins=[0, 4500, 6000, 8000, 20000],
                               labels=['Punt', 'Mid', 'High', 'Star'])
    
    return df

def normalize_team(team):
    """Normalize team abbreviations."""
    mappings = {
        'SA': 'SAS', 'SAS': 'SAS',
        'NY': 'NYK', 'NYK': 'NYK', 
        'GS': 'GSW', 'GSW': 'GSW',
        'NO': 'NOP', 'NOP': 'NOP',
        'PHO': 'PHX', 'PHX': 'PHX'
    }
    return mappings.get(team, team)

def get_dvp_advantages(players_df, dvp_df, stats_df):
    """Find DVP advantages for each player based on opponent and position."""
    
    players_df_norm = players_df[players_df['salary'] > 0].copy()
    stats_df_norm = stats_df.copy()
    players_df_norm['team_norm'] = players_df_norm['team'].apply(normalize_team)
    stats_df_norm['team_norm'] = stats_df_norm['team'].apply(normalize_team)
    
    players_with_stats = players_df_norm.merge(
        stats_df_norm, 
        left_on=['player_name', 'team_norm'],
        right_on=['player_name', 'team_norm'],
        how='left',
        suffixes=('', '_stats')
    )
    
    results = []
    
    for _, player in players_with_stats.iterrows():
        opponent = player.get('opponent', '')
        position = player.get('true_position', '')
        
        if not opponent or pd.isna(opponent):
            continue
        if not position or pd.isna(position):
            continue
            
        opp_dvp = dvp_df[(dvp_df['team'] == opponent) & (dvp_df['position'] == position)]
        if len(opp_dvp) == 0:
            opp_dvp = dvp_df[dvp_df['team'] == opponent]
            if len(opp_dvp) == 0:
                continue
            
        opp_dvp = opp_dvp.iloc[0]
        
        advantages = []
        
        stat_mapping = {
            'pts': ('pts_pg', 'pts'),
            'reb': ('reb_pg', 'reb'),
            'ast': ('ast_pg', 'ast'),
            'stl': ('stl_pg', 'stl'),
            'blk': ('blk_pg', 'blk')
        }
        
        avg_dvp = opp_dvp.get('dvp_score', 50)
        
        for stat_key, (player_col, dvp_col) in stat_mapping.items():
            player_avg = player.get(player_col, 0)
            opp_allows = opp_dvp.get(dvp_col, 0)
            
            if pd.notna(player_avg) and player_avg > 0 and pd.notna(avg_dvp):
                if avg_dvp >= 50:
                    edge = (avg_dvp - 48) / 10
                    boosted_avg = player_avg * (1 + edge * 0.08)
                    advantages.append({
                        'stat': stat_key,
                        'dvp_score': avg_dvp,
                        'opp_allows': opp_allows,
                        'player_avg': player_avg,
                        'boosted_avg': boosted_avg,
                        'edge_pct': edge * 8
                    })
        
        if advantages:
            results.append({
                'player_name': player['player_name'],
                'team': player['team'],
                'opponent': opponent,
                'position': position,
                'salary': player['salary'],
                'value': player.get('value', 0),
                'proj_fp': player.get('proj_fp', 0),
                'advantages': advantages
            })
    
    return results

def apply_chart_style(ax, title, xlabel, ylabel):
    """Apply consistent black border styling to charts."""
    ax.set_xlabel(xlabel, fontsize=12, color='black', fontweight='bold')
    ax.set_ylabel(ylabel, fontsize=12, color='black', fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold', color='black')
    ax.tick_params(colors='black')
    for spine in ax.spines.values():
        spine.set_color('black')
        spine.set_linewidth(2)
    ax.grid(True, alpha=0.3, color='gray')

def generate_value_chart(players_df, output_path='static/images/value_chart.png'):
    """Generate usage vs projected FP scatter plot with value-based sizing."""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    df = players_df.copy()
    df = df[df['usg_pct'].notna() & df['proj_fp'].notna() & (df['usg_pct'] > 10)]
    
    fig, ax = plt.subplots(figsize=(12, 8))
    fig.patch.set_facecolor('white')
    ax.set_facecolor('white')
    
    colors = {'Punt': '#888888', 'Mid': '#555555', 'High': '#333333', 'Star': '#000000'}
    
    for tier in ['Punt', 'Mid', 'High', 'Star']:
        tier_df = df[df['salary_tier'] == tier]
        if len(tier_df) == 0:
            continue
        sizes = tier_df['value'].clip(3, 10) * 15
        ax.scatter(tier_df['usg_pct'], tier_df['proj_fp'], 
                  c=colors[tier], label=tier, alpha=0.7, s=sizes, 
                  edgecolors='black', linewidths=0.5)
    
    top_value = df.nlargest(5, 'value')
    for _, player in top_value.iterrows():
        ax.annotate(player['player_name'], 
                   (player['usg_pct'], player['proj_fp']),
                   xytext=(5, 5), textcoords='offset points',
                   fontsize=9, fontweight='bold', color='black')
    
    top_fp = df.nlargest(3, 'proj_fp')
    for _, player in top_fp.iterrows():
        if player['player_name'] not in top_value['player_name'].values:
            ax.annotate(player['player_name'], 
                       (player['usg_pct'], player['proj_fp']),
                       xytext=(5, -8), textcoords='offset points',
                       fontsize=8, color='#444')
    
    apply_chart_style(ax, 'Usage % vs Projected FP (size = value)', 'Usage Rate (%)', 'Projected Fantasy Points')
    legend = ax.legend(title='Salary Tier', frameon=True, edgecolor='black', loc='lower right')
    legend.get_frame().set_linewidth(2)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, facecolor='white', edgecolor='black')
    plt.close()
    
    return output_path

def generate_upside_chart(players_df, output_path='static/images/upside_chart.png'):
    """Generate μ vs σ scatter plot - the core risk-reward frontier chart."""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    df = players_df.copy()
    df = df[df['proj_fp'].notna() & df['fp_sd'].notna() & (df['proj_fp'] > 5)]
    
    position_colors = {
        'PG': '#1a1a1a',
        'SG': '#4a4a4a',
        'SF': '#7a7a7a',
        'PF': '#9a9a9a',
        'C': '#bababa'
    }
    
    fig, ax = plt.subplots(figsize=(12, 8))
    fig.patch.set_facecolor('white')
    ax.set_facecolor('white')
    
    for pos in ['PG', 'SG', 'SF', 'PF', 'C']:
        pos_df = df[df['true_position'] == pos]
        if len(pos_df) == 0:
            continue
        
        sizes = (pos_df['salary'] / 1000) * 8
        ax.scatter(pos_df['proj_fp'], pos_df['fp_sd'], 
                  c=position_colors.get(pos, '#333'),
                  s=sizes, alpha=0.7, edgecolors='black', linewidths=0.5,
                  label=pos)
    
    cash_plays = df[(df['proj_fp'] > df['proj_fp'].quantile(0.7)) & (df['fp_sd'] < df['fp_sd'].quantile(0.3))]
    for _, player in cash_plays.head(3).iterrows():
        ax.annotate(player['player_name'],
                   (player['proj_fp'], player['fp_sd']),
                   xytext=(5, -8), textcoords='offset points',
                   fontsize=8, fontweight='bold', color='black')
    
    gpp_darts = df[(df['proj_fp'] < df['proj_fp'].quantile(0.4)) & (df['fp_sd'] > df['fp_sd'].quantile(0.7))]
    for _, player in gpp_darts.head(3).iterrows():
        ax.annotate(player['player_name'],
                   (player['proj_fp'], player['fp_sd']),
                   xytext=(5, 5), textcoords='offset points',
                   fontsize=8, color='#666')
    
    slate_breakers = df[(df['proj_fp'] > df['proj_fp'].quantile(0.8)) & (df['fp_sd'] > df['fp_sd'].quantile(0.7))]
    for _, player in slate_breakers.head(3).iterrows():
        ax.annotate(player['player_name'],
                   (player['proj_fp'], player['fp_sd']),
                   xytext=(5, 5), textcoords='offset points',
                   fontsize=8, fontweight='bold', color='black')
    
    mu_median = df['proj_fp'].median()
    sigma_median = df['fp_sd'].median()
    ax.axvline(x=mu_median, color='black', linestyle=':', linewidth=1, alpha=0.3)
    ax.axhline(y=sigma_median, color='black', linestyle=':', linewidth=1, alpha=0.3)
    
    ax.text(ax.get_xlim()[1] * 0.95, ax.get_ylim()[0] + 0.5, 'CASH', ha='right', fontsize=7, color='#666', alpha=0.7)
    ax.text(ax.get_xlim()[0] + 1, ax.get_ylim()[1] * 0.95, 'GPP DARTS', ha='left', fontsize=7, color='#666', alpha=0.7)
    ax.text(ax.get_xlim()[1] * 0.95, ax.get_ylim()[1] * 0.95, 'SLATE BREAKERS', ha='right', fontsize=7, color='#666', alpha=0.7)
    
    apply_chart_style(ax, 'Risk-Reward Frontier (μ vs σ)', 'Projected FP (μ)', 'Volatility / Std Dev (σ)')
    
    legend = ax.legend(loc='lower right', frameon=True, edgecolor='black', facecolor='white', title='Position')
    legend.get_title().set_fontweight('bold')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, facecolor='white', edgecolor='black')
    plt.close()
    
    return output_path

def generate_ref_foul_chart(output_path='static/images/ref_foul_chart.png'):
    """Generate referee foul analysis chart - crew fouls/game vs home/away foul bias."""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    import sqlite3
    conn = sqlite3.connect("dfs_nba.db")
    
    today = get_eastern_date_str()
    
    assignments = pd.read_sql_query(
        "SELECT home_team, away_team, crew_chief, referee, umpire FROM referee_assignments WHERE game_date = ?",
        conn, params=[today]
    )
    assignments = assignments.drop_duplicates(subset=['home_team', 'away_team'])
    
    todays_games = pd.read_sql_query(
        "SELECT DISTINCT home_team, away_team FROM game_odds", conn
    )
    
    ref_stats = pd.read_sql_query(
        "SELECT referee, fouls_pg, foul_diff, foul_pct_home, foul_pct_road, games_officiated FROM referee_stats",
        conn
    )
    conn.close()
    
    if len(todays_games) > 0 and len(assignments) > 0:
        valid_pairs = set()
        for _, g in todays_games.iterrows():
            valid_pairs.add((g['home_team'], g['away_team']))
            valid_pairs.add((g['away_team'], g['home_team']))
        assignments = assignments[
            assignments.apply(lambda r: (r['home_team'], r['away_team']) in valid_pairs, axis=1)
        ]
    
    if len(assignments) == 0 or len(ref_stats) == 0:
        print("No referee data available for chart")
        if os.path.exists(output_path):
            os.remove(output_path)
            print(f"Removed stale chart: {output_path}")
        return None
    
    ref_stats_deduped = ref_stats.sort_values('games_officiated', ascending=False).drop_duplicates(subset='referee', keep='first')
    ref_lookup = {}
    for _, row in ref_stats_deduped.iterrows():
        ref_lookup[row['referee'].strip().lower()] = {
            'fouls_pg': row['fouls_pg'],
            'foul_diff': row['foul_diff'],
            'foul_pct_home': row['foul_pct_home'],
            'foul_pct_road': row['foul_pct_road']
        }
    
    game_data = []
    for _, game in assignments.iterrows():
        crew = [game['crew_chief'], game['referee'], game['umpire']]
        crew = [r for r in crew if r and pd.notna(r)]
        
        crew_fouls = []
        crew_diffs = []
        crew_home_pct = []
        
        for ref_name in crew:
            key = ref_name.strip().lower()
            if key in ref_lookup:
                stats = ref_lookup[key]
                crew_fouls.append(stats['fouls_pg'])
                crew_diffs.append(stats['foul_diff'])
                crew_home_pct.append(stats['foul_pct_home'])
        
        if len(crew_fouls) >= 2:
            avg_fouls = sum(crew_fouls) / len(crew_fouls)
            avg_diff = sum(crew_diffs) / len(crew_diffs)
            avg_home_pct = sum(crew_home_pct) / len(crew_home_pct)
            
            home = game['home_team'] or '?'
            away = game['away_team'] or '?'
            
            game_data.append({
                'matchup': f"{away} @ {home}",
                'home_team': home,
                'away_team': away,
                'crew_avg_fouls': avg_fouls,
                'crew_avg_diff': avg_diff,
                'crew_home_pct': avg_home_pct,
                'refs_matched': len(crew_fouls),
                'crew_names': crew
            })
    
    if len(game_data) == 0:
        print("Could not match any refs to stats")
        if os.path.exists(output_path):
            os.remove(output_path)
            print(f"Removed stale chart: {output_path}")
        return None
    
    gdf = pd.DataFrame(game_data)
    
    fig, ax = plt.subplots(figsize=(12, 8))
    fig.patch.set_facecolor('white')
    ax.set_facecolor('white')
    
    league_avg_fouls = ref_stats_deduped['fouls_pg'].mean()
    league_avg_diff = ref_stats_deduped['foul_diff'].mean()
    
    for _, g in gdf.iterrows():
        shade = 0.15 + 0.7 * ((g['crew_avg_fouls'] - gdf['crew_avg_fouls'].min()) / 
                               max(gdf['crew_avg_fouls'].max() - gdf['crew_avg_fouls'].min(), 1))
        color = str(max(0.1, min(0.85, 1.0 - shade)))
        
        marker = '^' if g['crew_avg_diff'] > 0 else 'v'
        
        ax.scatter(g['crew_avg_fouls'], g['crew_avg_diff'],
                  c=color, s=220, marker=marker,
                  edgecolors='black', linewidths=1.5, zorder=5)
        
        y_offset = 8 if g['crew_avg_diff'] >= 0 else -12
        ax.annotate(g['matchup'],
                   (g['crew_avg_fouls'], g['crew_avg_diff']),
                   xytext=(6, y_offset), textcoords='offset points',
                   fontsize=9, fontweight='bold', color='black',
                   ha='left')
    
    ax.axhline(y=0, color='black', linewidth=1.5, alpha=0.6)
    ax.axvline(x=league_avg_fouls, color='gray', linestyle='--', linewidth=1, alpha=0.5)
    
    x_min, x_max = ax.get_xlim()
    y_min, y_max = ax.get_ylim()
    
    pad = 0.3
    y_max = max(y_max, abs(y_min)) + pad
    y_min = -y_max
    ax.set_ylim(y_min, y_max)
    
    ax.fill_between([x_min, x_max], 0, y_max, color='#e8e8e8', alpha=0.3, zorder=0)
    ax.fill_between([x_min, x_max], y_min, 0, color='#d0d0d0', alpha=0.3, zorder=0)
    
    ax.text(x_max - (x_max - x_min) * 0.02, y_max * 0.88,
            'HOME ADVANTAGE', ha='right', fontsize=8, color='#555',
            fontweight='bold', alpha=0.7, style='italic')
    ax.text(x_max - (x_max - x_min) * 0.02, y_min * 0.88,
            'ROAD ADVANTAGE', ha='right', fontsize=8, color='#555',
            fontweight='bold', alpha=0.7, style='italic')
    ax.text(x_min + (x_max - x_min) * 0.02, y_max * 0.88,
            'FEWER FOULS', ha='left', fontsize=7, color='#888', alpha=0.6)
    ax.text(x_max - (x_max - x_min) * 0.02, y_max * 0.78,
            'MORE FOULS', ha='right', fontsize=7, color='#888', alpha=0.6)
    
    from matplotlib.lines import Line2D
    legend_elements = [
        Line2D([0], [0], marker='^', color='w', markerfacecolor='#555', 
               markeredgecolor='black', markersize=10, label='Home-favored crew'),
        Line2D([0], [0], marker='v', color='w', markerfacecolor='#999',
               markeredgecolor='black', markersize=10, label='Road-favored crew'),
        Line2D([0], [0], color='gray', linestyle='--', linewidth=1, label=f'League avg ({league_avg_fouls:.1f} fouls/g)')
    ]
    legend = ax.legend(handles=legend_elements, loc='upper center',
                       bbox_to_anchor=(0.5, -0.12), ncol=3, frameon=True,
                       edgecolor='black', facecolor='white')
    legend.get_frame().set_linewidth(2)
    
    apply_chart_style(ax, "Tonight's Referee Crews — Foul Volume vs Home/Away Bias",
                      'Crew Avg Fouls Per Game', 'Foul Differential (+ = more road fouls)')
    
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.18)
    plt.savefig(output_path, dpi=150, facecolor='white', edgecolor='black', bbox_inches='tight')
    plt.close()
    
    print(f"Referee foul chart saved to {output_path}")
    for _, g in gdf.iterrows():
        bias = "HOME" if g['crew_avg_diff'] > 0 else "ROAD"
        print(f"  {g['matchup']}: {g['crew_avg_fouls']:.1f} fouls/g, diff {g['crew_avg_diff']:+.1f} ({bias} advantage)")
    
    return output_path

def generate_dvp_heatmap(dvp_df, output_path='static/images/dvp_heatmap.png'):
    """Generate DVP heatmap by team and position."""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    pivot = dvp_df.pivot_table(
        index='team',
        columns='position',
        values='dvp_score',
        aggfunc='first'
    )
    
    if len(pivot) == 0:
        return None
    
    fig, ax = plt.subplots(figsize=(10, 14))
    fig.patch.set_facecolor('white')
    ax.set_facecolor('white')
    
    im = ax.imshow(pivot.values, cmap='RdYlGn', aspect='auto', vmin=40, vmax=60)
    
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=45, ha='right', fontweight='bold', color='black')
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index, fontweight='bold', color='black')
    
    for i in range(len(pivot.index)):
        for j in range(len(pivot.columns)):
            val = pivot.iloc[i, j]
            if pd.notna(val):
                ax.text(j, i, f'{val:.1f}', ha='center', va='center', 
                       fontsize=8, fontweight='bold', color='black' if 45 < val < 55 else 'white')
    
    for spine in ax.spines.values():
        spine.set_color('black')
        spine.set_linewidth(2)
    
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label('DVP Score (Higher = Easier Matchup)', fontweight='bold', color='black')
    cbar.outline.set_color('black')
    cbar.outline.set_linewidth(2)
    
    ax.set_title('Defense vs Position - Matchup Heatmap', fontsize=14, fontweight='bold', color='black')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, facecolor='white', edgecolor='black')
    plt.close()
    
    return output_path

def _normalize_prop_name(name):
    """Normalize player name for matching prop lines."""
    import unicodedata, re
    if pd.isna(name):
        return ""
    name = str(name).strip().lower()
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
    name = re.sub(r'\.', '', name)
    name = re.sub(r'-', ' ', name)
    name = re.sub(r'\s+(jr|sr|ii|iii|iv|v)\.?$', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def _load_book_props():
    """Load player prop lines from The Odds API data."""
    try:
        conn = sqlite3.connect("dfs_nba.db")
        today = get_eastern_date_str()
        df = pd.read_sql_query(
            "SELECT player_name, stat, line, over_odds, under_odds, bookmaker FROM player_props WHERE game_date = ?",
            conn, params=[today]
        )
        conn.close()
        if len(df) == 0:
            return {}

        lookup = {}
        for _, row in df.iterrows():
            key = (_normalize_prop_name(row['player_name']), row['stat'])
            lookup[key] = {
                'line': row['line'],
                'over_odds': row['over_odds'],
                'under_odds': row['under_odds'],
                'bookmaker': row['bookmaker']
            }
        return lookup
    except Exception as e:
        print(f"Could not load book props: {e}")
        return {}

def _classify_move_pattern(per_snap_diffs, total_drift):
    """Classify the *shape* of intra-day line movement from per-snapshot diffs.

    Precedence (most specific signal wins):
      1. 'flat' — no per-snapshot diffs or every tick is zero
      2. 'sudden_swing' — single tick is >=0.5 absolute, accounts for >=70%
         of the day's cumulative absolute movement, and is aligned with net
         direction. Captures "flat-then-jump" *and* "big jump with a small
         pullback" (e.g. +1.5 then -0.2 still reads as sudden_swing because
         the +1.5 tick dominates the path). Strongest sharp-action tell.
      3. 'reversal' — the line moved meaningfully in both directions during
         the day (a positive tick >=0.25 *and* a negative tick >=0.25 are
         present) without any single tick dominating per rule 2. This
         catches genuine two-way chop including zero-net days like +1.0/-1.0.
      4. 'flat' (net-flat fallback) — total drift < 0.5 with no dominant
         single tick and no meaningful two-way action (just noise).
      5. 'gradual_drift' — non-flat movement that doesn't fit anything above
         (steady creep across multiple snapshots in roughly one direction).

    sudden_swing is checked before reversal so that one big aligned move
    plus a small counter-pullback still surfaces as the dominant signal it
    is, rather than being downgraded to a generic "choppy" reversal.
    """
    if not per_snap_diffs:
        return 'flat'

    abs_diffs = [abs(d) for d in per_snap_diffs]
    sum_abs = sum(abs_diffs)
    if sum_abs == 0:
        return 'flat'

    max_abs = max(abs_diffs)
    max_idx = abs_diffs.index(max_abs)
    max_diff = per_snap_diffs[max_idx]
    if total_drift != 0:
        aligned_with_net = (max_diff > 0) == (total_drift > 0)
        if max_abs >= 0.5 and (max_abs / sum_abs) >= 0.70 and aligned_with_net:
            return 'sudden_swing'

    pos_max = max((d for d in per_snap_diffs if d > 0), default=0.0)
    neg_min = min((d for d in per_snap_diffs if d < 0), default=0.0)
    if pos_max >= 0.25 and abs(neg_min) >= 0.25:
        return 'reversal'

    if abs(total_drift) < 0.5:
        return 'flat'

    return 'gradual_drift'


def _load_line_movement():
    """Load opening vs current book-line snapshots from player_props_history.

    Returns a dict keyed by (normalized_player, stat_label) with opening_line,
    current_line, line_drift (current - opening), line_drift_pct, snapshot_count,
    hours_tracked, recent-window drift fields (last_hour_drift, last_hour_from,
    last_hour_minutes — anchor must be 60 min to 4 h before the latest snapshot)
    used to flag late sharp money moves separately from total overnight drift,
    and a `move_pattern` field describing the *shape* of the intra-day path
    ('flat' / 'gradual_drift' / 'sudden_swing' / 'reversal') so the article
    generator can call out sudden swings as a distinct sharp-action signal.

    Gracefully returns an empty dict when the history table doesn't exist yet
    (first deploy after schema change) or when no snapshots exist for today.
    """
    try:
        conn = sqlite3.connect("dfs_nba.db")
        today = get_eastern_date_str()
        try:
            df = pd.read_sql_query(
                "SELECT player_name, stat, line, scraped_at "
                "FROM player_props_history WHERE game_date = ? ORDER BY scraped_at",
                conn, params=[today]
            )
        except Exception:
            conn.close()
            return {}
        conn.close()

        if len(df) == 0:
            return {}

        df['scraped_at_dt'] = pd.to_datetime(df['scraped_at'], errors='coerce', utc=True)
        df = df.dropna(subset=['line', 'scraped_at_dt'])
        if len(df) == 0:
            return {}

        lookup = {}
        for (player_name, stat), grp in df.groupby(['player_name', 'stat']):
            grp_sorted = grp.sort_values('scraped_at_dt')
            # Collapse duplicate-timestamp rows so per-snapshot diffs reflect
            # genuine intra-day ticks rather than scraper retries within a run.
            grp_unique = grp_sorted.drop_duplicates('scraped_at_dt', keep='last')
            opening = grp_unique.iloc[0]
            current = grp_unique.iloc[-1]
            opening_line = float(opening['line'])
            current_line = float(current['line'])
            snapshots = int(len(grp_unique))

            if snapshots < 2 or opening['scraped_at_dt'] == current['scraped_at_dt']:
                drift = 0.0
                hours_tracked = 0.0
            else:
                drift = round(current_line - opening_line, 2)
                hours_tracked = round(
                    (current['scraped_at_dt'] - opening['scraped_at_dt']).total_seconds() / 3600.0, 2
                )

            drift_pct = round((drift / opening_line) * 100.0, 2) if opening_line > 0 else 0.0

            # Per-snapshot diffs power move-pattern classification: we want
            # "flat then 1.5-pt jump in one tick" to look different from
            # "0.3 + 0.4 + 0.3 + 0.5 steady creep" even though both end at the
            # same total drift. Ratio is computed against summed absolute
            # movement (path length), not net drift, so reversals don't get
            # misread as sudden swings.
            line_series = grp_unique['line'].astype(float).tolist()
            per_snap_diffs = [
                round(line_series[i] - line_series[i - 1], 2)
                for i in range(1, len(line_series))
            ]
            move_pattern = _classify_move_pattern(per_snap_diffs, drift)
            largest_swing = 0.0
            largest_swing_share = 0.0
            if per_snap_diffs:
                abs_diffs = [abs(d) for d in per_snap_diffs]
                sum_abs = sum(abs_diffs)
                max_abs = max(abs_diffs)
                signed_max = per_snap_diffs[abs_diffs.index(max_abs)]
                largest_swing = round(signed_max, 2)
                largest_swing_share = round((max_abs / sum_abs) if sum_abs > 0 else 0.0, 2)

            # Recent-window drift: pick the latest snapshot taken at least 60
            # minutes before "current" and measure drift since then. With 4-5
            # daily snapshots this isolates late same-day movement (often injury
            # news or sharp action) from overnight drift off the open. Anchor
            # is bounded to the prior 4 hours so the signal is genuinely
            # "recent" and degrades gracefully on dark slates with sparse
            # snapshots — outside that window we report None and the article
            # generator falls back to the open-vs-current narrative only.
            last_hour_drift = 0.0
            last_hour_from = None
            last_hour_minutes = 0.0
            if snapshots >= 2:
                upper_cutoff = current['scraped_at_dt'] - pd.Timedelta(minutes=60)
                lower_cutoff = current['scraped_at_dt'] - pd.Timedelta(hours=4)
                window = grp_unique[
                    (grp_unique['scraped_at_dt'] <= upper_cutoff)
                    & (grp_unique['scraped_at_dt'] >= lower_cutoff)
                ]
                if len(window) > 0:
                    anchor = window.iloc[-1]
                    anchor_line = float(anchor['line'])
                    last_hour_drift = round(current_line - anchor_line, 2)
                    last_hour_from = anchor_line
                    last_hour_minutes = round(
                        (current['scraped_at_dt'] - anchor['scraped_at_dt']).total_seconds() / 60.0, 1
                    )

            key = (_normalize_prop_name(player_name), stat)
            lookup[key] = {
                'opening_line': opening_line,
                'current_line': current_line,
                'line_drift': drift,
                'line_drift_pct': drift_pct,
                'snapshot_count': snapshots,
                'hours_tracked': hours_tracked,
                'last_hour_drift': last_hour_drift,
                'last_hour_from': last_hour_from,
                'last_hour_minutes': last_hour_minutes,
                'move_pattern': move_pattern,
                'largest_swing': largest_swing,
                'largest_swing_share': largest_swing_share,
            }
        return lookup
    except Exception as e:
        print(f"Could not load line movement: {e}")
        return {}

def _american_to_implied_prob(odds):
    """Convert American odds to implied probability."""
    if odds is None or pd.isna(odds):
        return None
    odds = float(odds)
    if odds > 0:
        return 100.0 / (odds + 100.0)
    else:
        return abs(odds) / (abs(odds) + 100.0)

def _get_season_pct():
    """Calculate how far into the NBA season we are (0.0 to 1.0)."""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    now = datetime.now(ZoneInfo("America/New_York"))
    season_start = datetime(now.year if now.month >= 10 else now.year - 1, 10, 22, tzinfo=ZoneInfo("America/New_York"))
    season_end = datetime(now.year if now.month <= 6 else now.year + 1, 4, 13, tzinfo=ZoneInfo("America/New_York"))
    elapsed = (now - season_start).days
    total = (season_end - season_start).days
    return max(0.0, min(1.0, elapsed / total))

def _load_game_logs_for_confidence():
    """Load player game logs from SQLite for prop confidence filtering."""
    db_path = os.path.join(os.path.dirname(__file__), '..', 'dfs_nba.db')
    try:
        conn = sqlite3.connect(db_path)
        logs = pd.read_sql("SELECT player_name, game_date, pts, reb, ast, stl, blk FROM player_game_logs ORDER BY game_date DESC", conn)
        conn.close()
        return logs
    except Exception:
        return pd.DataFrame()

def _evaluate_prop_confidence(player_name, stat_key, book_line, player_avg, game_logs_df, recommendation='OVER',
                              dva_diff=0, dvp_diff=0, usage_boost=0, opportunity_spike=False,
                              implied_team_total=None):
    """Evaluate confidence for a prop pick based on hit rate, variance, trend, sample size,
    recent form vs line, matchup alignment, implied team total, and signal consistency.

    Returns dict with: hit_rate, cv, last5_avg, confidence ('HIGH'/'LOW'),
    confidence_reasons (list — populated for BOTH HIGH and LOW picks),
    gate_failures (dict — tracks which specific gates failed for diagnostics)
    """
    stat_col_map = {'pts': 'pts', 'reb': 'reb', 'ast': 'ast', 'stl': 'stl', 'blk': 'blk'}
    col = stat_col_map.get(stat_key)
    if col is None or len(game_logs_df) == 0:
        return {'hit_rate': None, 'cv': None, 'last5_avg': None, 'confidence': 'LOW',
                'confidence_reasons': ['No game log data'], 'gate_failures': {'no_data': True}}

    player_logs = game_logs_df[game_logs_df['player_name'] == player_name]
    if len(player_logs) == 0 or col not in player_logs.columns:
        return {'hit_rate': None, 'cv': None, 'last5_avg': None, 'confidence': 'LOW',
                'confidence_reasons': ['No game log data'], 'gate_failures': {'no_data': True}}

    stat_values = player_logs[col].dropna()
    fail_reasons = []
    support_reasons = []
    gate_failures = {}

    if len(stat_values) < 15:
        fail_reasons.append(f'Small sample ({len(stat_values)} games < 15)')
        return {'hit_rate': None, 'cv': None, 'last5_avg': None, 'confidence': 'LOW',
                'confidence_reasons': fail_reasons, 'gate_failures': {'small_sample': True}}

    line = book_line if book_line and not pd.isna(book_line) and book_line > 0 else round(player_avg) - 0.5
    if recommendation == 'UNDER':
        hit_rate = round((stat_values < line).mean() * 100, 1)
    else:
        hit_rate = round((stat_values > line).mean() * 100, 1)

    avg = stat_values.mean()
    std = stat_values.std()
    cv = round(std / avg, 2) if avg > 0 else 99.0

    last5 = stat_values.head(5)
    last5_avg = round(last5.mean(), 1)

    if hit_rate < 58:
        fail_reasons.append(f'Hit rate {hit_rate}% < 58%')
        gate_failures['hit_rate'] = True
    else:
        support_reasons.append(f'Hit rate {hit_rate}%')

    if cv > 0.30:
        fail_reasons.append(f'High variance (CV {cv} > 0.30)')
        gate_failures['cv'] = True
    else:
        support_reasons.append(f'CV {cv}')

    if recommendation == 'OVER' and avg > 0 and last5_avg < avg * 0.80:
        fail_reasons.append(f'Trending down (last 5: {last5_avg} vs avg {round(avg, 1)})')
        gate_failures['trend_down'] = True
    elif recommendation == 'UNDER' and avg > 0 and last5_avg > avg * 1.20:
        fail_reasons.append(f'Trending up (last 5: {last5_avg} vs avg {round(avg, 1)}) — bad for UNDER')
        gate_failures['trend_up'] = True

    if recommendation == 'OVER' and last5_avg < line:
        fail_reasons.append(f'Last-5 avg {last5_avg} below line {line} for OVER')
        gate_failures['last5_vs_line'] = True
    elif recommendation == 'UNDER' and last5_avg > line:
        fail_reasons.append(f'Last-5 avg {last5_avg} above line {line} for UNDER')
        gate_failures['last5_vs_line'] = True
    else:
        margin = abs(last5_avg - line)
        support_reasons.append(f'Last-5 {"clears" if recommendation == "OVER" else "under"} line by {margin:.1f}')

    dva_val = dva_diff if dva_diff and not pd.isna(dva_diff) else 0
    dvp_val = dvp_diff if dvp_diff and not pd.isna(dvp_diff) else 0

    if recommendation == 'OVER':
        dva_strong_support = dva_val >= 0.5
        dvp_strong_support = dvp_val >= 0.5
        dva_strongly_against = dva_val < -0.5
        dvp_strongly_against = dvp_val < -0.5
    else:
        dva_strong_support = dva_val <= -0.5
        dvp_strong_support = dvp_val <= -0.5
        dva_strongly_against = dva_val > 0.5
        dvp_strongly_against = dvp_val > 0.5

    has_strong_support = dva_strong_support or dvp_strong_support

    if dva_strongly_against and dvp_strongly_against:
        fail_reasons.append(f'DVA ({dva_val:+.1f}) and DVP ({dvp_val:+.1f}) both oppose {recommendation}')
        gate_failures['matchup_both_oppose'] = True
    elif not has_strong_support:
        fail_reasons.append(f'Weak matchup: DVA {dva_val:+.1f}, DVP {dvp_val:+.1f} — need ≥0.5 directional support for {recommendation}')
        gate_failures['matchup_weak'] = True
    else:
        matchup_parts = []
        if dva_strong_support:
            matchup_parts.append(f'DVA {dva_val:+.1f}')
        if dvp_strong_support:
            matchup_parts.append(f'DVP {dvp_val:+.1f}')
        if matchup_parts:
            support_reasons.append(f'Matchup {", ".join(matchup_parts)} supports {recommendation}')

    itt = implied_team_total if implied_team_total and not pd.isna(implied_team_total) else None
    if itt is not None and stat_key in ('pts', 'ast'):
        if recommendation == 'OVER' and itt < 105:
            fail_reasons.append(f'Low implied team total ({itt:.1f}) suppresses {recommendation}')
            gate_failures['low_team_total'] = True
        elif recommendation == 'UNDER' and itt > 115:
            fail_reasons.append(f'High implied team total ({itt:.1f}) opposes {recommendation}')
            gate_failures['high_team_total'] = True
        elif recommendation == 'OVER' and itt >= 110:
            support_reasons.append(f'Implied total {itt:.1f} supports {recommendation}')

    usage_val = usage_boost if usage_boost and not pd.isna(usage_boost) else 0
    if recommendation == 'UNDER' and usage_val > 3.0:
        fail_reasons.append(f'Contradictory: UNDER call with +{usage_val:.1f} usage boost')
        gate_failures['usage_contradiction'] = True

    opp_spike = opportunity_spike if opportunity_spike else False
    if opp_spike and recommendation == 'OVER':
        support_reasons.append('Opportunity Spike: usage explosion + positive matchup')

    confidence = 'HIGH' if len(fail_reasons) == 0 else 'LOW'

    if confidence == 'HIGH' and not support_reasons:
        support_reasons.append('All gates passed')
    reasons_out = support_reasons if confidence == 'HIGH' else fail_reasons

    return {'hit_rate': hit_rate, 'cv': cv, 'last5_avg': last5_avg, 'confidence': confidence,
            'confidence_reasons': reasons_out, 'gate_failures': gate_failures,
            'gate_fail_count': len(gate_failures)}


def _build_projection_cache():
    """Load ALL data tables once for stat-specific projection models."""
    db_path = os.path.join(os.path.dirname(__file__), '..', 'dfs_nba.db')
    conn = sqlite3.connect(db_path)
    cache = {}

    try:
        df = pd.read_sql("SELECT player_name, height_inches, weight_lbs, wingspan_inches FROM player_measurements", conn)
        cache['measurements'] = {r['player_name']: r.to_dict() for _, r in df.iterrows()}
    except Exception:
        cache['measurements'] = {}

    try:
        df = pd.read_sql("SELECT player_name, ra_pct, paint_pct, mid_pct, three_pct, rim_paint_pct, total_fga, ra_fga, ra_fgm, paint_fga, paint_fgm, mid_fga, mid_fgm, three_fga, three_fgm FROM player_shot_zones", conn)
        cache['shot_zones'] = {r['player_name']: r.to_dict() for _, r in df.iterrows()}
    except Exception:
        cache['shot_zones'] = {}

    try:
        df = pd.read_sql("SELECT player_name, cs_pct, pu_pct, paint_pct, cs_3_share, pu_3_share FROM player_shot_creation", conn)
        cache['shot_creation'] = {r['player_name']: r.to_dict() for _, r in df.iterrows()}
    except Exception:
        cache['shot_creation'] = {}

    try:
        df = pd.read_sql("SELECT player_name, deflections_per48, contested_per48, contested_2pt, loose_per48, box_outs_per48, screen_ast_per48 FROM player_hustle_stats", conn)
        cache['hustle'] = {r['player_name']: r.to_dict() for _, r in df.iterrows()}
    except Exception:
        cache['hustle'] = {}

    try:
        df = pd.read_sql("SELECT player_name, archetype, creation_idx, playmaking_idx, interior_idx, perimeter_idx, offball_idx, rebound_idx, defense_idx, size_idx FROM player_archetypes", conn)
        cache['archetypes'] = {r['player_name']: r.to_dict() for _, r in df.iterrows()}
    except Exception:
        cache['archetypes'] = {}

    try:
        df = pd.read_sql("SELECT team, ra_freq, paint_freq, mid_freq, corner3_freq, atb3_freq, ra_fg_pct, paint_fg_pct, mid_fg_pct, corner3_fg_pct, atb3_fg_pct, total_fga, ra_fga, ra_fgm, paint_fga, paint_fgm, mid_fga, mid_fgm, corner3_fga, corner3_fgm, atb3_fga, atb3_fgm FROM team_defense_shot_zones", conn)
        cache['team_def_zones'] = {r['team']: r.to_dict() for _, r in df.iterrows()}
    except Exception:
        cache['team_def_zones'] = {}

    try:
        df = pd.read_sql("SELECT team, play_type, type_grouping, poss_pct, ppp, tov_poss_pct, percentile FROM team_play_types", conn)
        cache['play_types'] = {}
        for _, r in df.iterrows():
            key = (r['team'], r['type_grouping'], r['play_type'])
            cache['play_types'][key] = r.to_dict()
        cache['play_types_df'] = df
    except Exception:
        cache['play_types'] = {}
        cache['play_types_df'] = pd.DataFrame()

    try:
        df = pd.read_sql("SELECT team, pace FROM team_pace", conn)
        cache['pace'] = {r['team']: r['pace'] for _, r in df.iterrows()}
        cache['league_avg_pace'] = df['pace'].mean() if len(df) > 0 else 98.0
    except Exception:
        cache['pace'] = {}
        cache['league_avg_pace'] = 98.0

    try:
        df = pd.read_sql("SELECT away_team, home_team, spread, total FROM game_odds", conn)
        cache['odds'] = {}
        for _, r in df.iterrows():
            cache['odds'][r['away_team']] = {'total': r['total'], 'spread': r['spread'], 'is_home': False, 'opponent': r['home_team']}
            cache['odds'][r['home_team']] = {'total': r['total'], 'spread': -r['spread'], 'is_home': True, 'opponent': r['away_team']}
    except Exception:
        cache['odds'] = {}

    try:
        df = pd.read_sql("SELECT player_name, status FROM injury_alerts WHERE status='OUT'", conn)
        cache['injuries_out'] = set(df['player_name'].tolist())
    except Exception:
        cache['injuries_out'] = set()

    try:
        df = pd.read_sql("SELECT player_name, team, position_slot, baseline_min FROM depth_charts", conn)
        cache['depth_charts'] = {}
        for _, r in df.iterrows():
            team = r['team']
            if team not in cache['depth_charts']:
                cache['depth_charts'][team] = []
            cache['depth_charts'][team].append(r.to_dict())
    except Exception:
        cache['depth_charts'] = {}

    try:
        df = pd.read_sql("SELECT player_name, opponent, matchup_score, vs_fp_avg, vs_fppm, fppm_diff FROM matchup_history", conn)
        cache['matchup_hist'] = {(r['player_name'], r['opponent']): r.to_dict() for _, r in df.iterrows()}
    except Exception:
        cache['matchup_hist'] = {}

    try:
        df = pd.read_sql("SELECT player_name, team, pts_pg, reb_pg, ast_pg, stl_pg, blk_pg, mpg, usg_pct, games_played FROM player_stats", conn)
        cache['player_stats'] = {r['player_name']: r.to_dict() for _, r in df.iterrows()}
    except Exception:
        cache['player_stats'] = {}

    try:
        df = pd.read_sql("SELECT player_name, avg_min, min_sd, games_played FROM player_volatility", conn)
        cache['player_volatility'] = {r['player_name']: r.to_dict() for _, r in df.iterrows()}
    except Exception:
        cache['player_volatility'] = {}

    try:
        df = pd.read_sql("SELECT player_name, game_date, pts, reb, ast, stl, blk, min FROM player_game_logs ORDER BY game_date DESC", conn)
        cache['game_logs'] = df
        cache['game_logs_by_player'] = {name: grp for name, grp in df.groupby('player_name')}
        cache['minutes_percentiles'] = {}
        for name, grp in df.groupby('player_name'):
            mins = grp['min'].dropna()
            mins = mins[mins > 0]
            if len(mins) >= 10:
                cache['minutes_percentiles'][name] = {
                    'p25': float(mins.quantile(0.25)),
                    'p50': float(mins.quantile(0.50)),
                    'p75': float(mins.quantile(0.75)),
                    'p90': float(mins.quantile(0.90)),
                    'mean': float(mins.mean()),
                    'std': float(mins.std()),
                    'count': len(mins),
                }
    except Exception:
        cache['game_logs'] = pd.DataFrame()
        cache['game_logs_by_player'] = {}
        cache['minutes_percentiles'] = {}

    try:
        df = pd.read_sql("SELECT position, team, pts, reb, ast, stl, blk, fg_pct FROM dvp_blended", conn)
        cache['dvp_avgs'] = {}
        for pos in df['position'].unique():
            pos_df = df[df['position'] == pos]
            cache['dvp_avgs'][pos] = {col: pos_df[col].mean() for col in ['pts', 'reb', 'ast', 'stl', 'blk']}
    except Exception:
        cache['dvp_avgs'] = {}

    totals = [v['total'] for v in cache['odds'].values() if v.get('total')]
    cache['league_avg_total'] = np.mean(totals) if totals else 224.0

    try:
        df = pd.read_sql("SELECT player_name, team, reb_per100 FROM player_per100", conn)
        cache['per100'] = {r['player_name']: r.to_dict() for _, r in df.iterrows()}
    except Exception:
        cache['per100'] = {}

    try:
        df = pd.read_sql("SELECT team, SUM(reb_pg) as team_reb FROM player_stats WHERE games_played > 10 AND team NOT LIKE '%TM' GROUP BY team", conn)
        cache['team_reb_totals'] = {r['team']: float(r['team_reb']) for _, r in df.iterrows()}
    except Exception:
        cache['team_reb_totals'] = {}

    if cache['team_def_zones']:
        fg_pcts = []
        three_rates = []
        for t, zones in cache['team_def_zones'].items():
            tfga = zones.get('total_fga', 0)
            if tfga and tfga > 0:
                tfgm = sum([
                    zones.get('ra_fgm', 0) or 0, zones.get('paint_fgm', 0) or 0,
                    zones.get('mid_fgm', 0) or 0, zones.get('corner3_fgm', 0) or 0,
                    zones.get('atb3_fgm', 0) or 0,
                ])
                fg_pcts.append(tfgm / tfga)
                three_fga = (zones.get('corner3_fga', 0) or 0) + (zones.get('atb3_fga', 0) or 0)
                three_rates.append(three_fga / tfga * 100.0)
        cache['league_avg_fg_pct'] = np.mean(fg_pcts) if fg_pcts else 0.46
        cache['league_avg_3pa_rate'] = np.mean(three_rates) if three_rates else 42.0
    else:
        cache['league_avg_fg_pct'] = 0.46
        cache['league_avg_3pa_rate'] = 42.0

    conn.close()
    return cache


def _build_game_environment(player, data_cache):
    """Build game context for a specific player."""
    team = player['team']
    opponent = player.get('opponent', '')
    env = {
        'game_total': None, 'spread': 0, 'is_home': False,
        'pace_factor': 1.0, 'total_factor': 1.0, 'blowout_minutes_cap': 1.0,
        'projected_min': player.get('projected_min', player.get('proj_min', 30)),
        'teammate_out_usage': 0.0, 'teammate_out_reb': 0.0, 'teammate_out_ast': 0.0,
    }

    odds = data_cache['odds'].get(team)
    if odds:
        env['game_total'] = odds['total']
        env['spread'] = odds['spread']
        env['is_home'] = odds['is_home']
        if odds['total'] and odds['spread'] is not None:
            env['implied_team_total'] = (odds['total'] - odds['spread']) / 2.0

    team_pace = data_cache['pace'].get(team, data_cache['league_avg_pace'])
    opp_pace = data_cache['pace'].get(opponent, data_cache['league_avg_pace'])
    game_pace = (team_pace + opp_pace) / 2.0
    env['pace_factor'] = game_pace / data_cache['league_avg_pace'] if data_cache['league_avg_pace'] > 0 else 1.0

    if env['game_total']:
        env['total_factor'] = env['game_total'] / data_cache['league_avg_total'] if data_cache['league_avg_total'] > 0 else 1.0

    player_name = player.get('player_name', '')
    proj_min = env['projected_min']
    if pd.isna(proj_min) or proj_min <= 0:
        proj_min = 30

    mp = data_cache.get('minutes_percentiles', {}).get(player_name)
    vol = data_cache.get('player_volatility', {}).get(player_name)

    if mp and mp['count'] >= 15:
        p90_cap = min(mp['p90'], 38)
        proj_min = min(proj_min, p90_cap)
        min_sd = mp['std']
        variance_discount = min_sd * 0.12
        proj_min = max(proj_min - variance_discount, mp['p25'])
        env['minutes_model'] = 'percentile'
    elif vol and pd.notna(vol.get('min_sd')) and pd.notna(vol.get('avg_min')):
        min_sd = float(vol['min_sd'])
        avg_min = float(vol['avg_min'])
        hard_cap = min(avg_min + min_sd, 38)
        proj_min = min(proj_min, hard_cap)
        variance_discount = min_sd * 0.12
        proj_min = max(proj_min - variance_discount, avg_min - min_sd)
        env['minutes_model'] = 'volatility'
    else:
        proj_min = min(proj_min, 36)
        env['minutes_model'] = 'default'

    env['pre_escalation_min'] = proj_min
    env['projected_min'] = proj_min

    depth = data_cache['depth_charts'].get(team, [])
    out_players = data_cache['injuries_out']
    seen_out = set()
    out_details = []

    for dc_entry in depth:
        pname = dc_entry['player_name']
        if pname in out_players and pname != player_name and pname not in seen_out:
            seen_out.add(pname)
            pstats_out = data_cache['player_stats'].get(pname)
            if pstats_out is not None:
                usg = pstats_out.get('usg_pct', 0)
                mpg = pstats_out.get('mpg', 0)
                if pd.notna(usg) and pd.notna(mpg) and float(mpg) > 10:
                    arch_out = data_cache['archetypes'].get(pname, {})
                    out_details.append({
                        'name': pname,
                        'usg': float(usg),
                        'mpg': float(mpg),
                        'rpg': float(pstats_out.get('reb_pg', 0) if pd.notna(pstats_out.get('reb_pg', 0)) else 0),
                        'apg': float(pstats_out.get('ast_pg', 0) if pd.notna(pstats_out.get('ast_pg', 0)) else 0),
                        'archetype': arch_out.get('archetype', 'Unknown'),
                        'creation_idx': float(arch_out.get('creation_idx', 0) if pd.notna(arch_out.get('creation_idx', 0)) else 0),
                        'playmaking_idx': float(arch_out.get('playmaking_idx', 0) if pd.notna(arch_out.get('playmaking_idx', 0)) else 0),
                        'perimeter_idx': float(arch_out.get('perimeter_idx', 0) if pd.notna(arch_out.get('perimeter_idx', 0)) else 0),
                        'interior_idx': float(arch_out.get('interior_idx', 0) if pd.notna(arch_out.get('interior_idx', 0)) else 0),
                    })

    total_vacated_usage = sum(d['usg'] for d in out_details)
    total_vacated_minutes = sum(d['mpg'] for d in out_details)

    if total_vacated_usage < 15:
        cascade_tier = 0
    elif total_vacated_usage < 35:
        cascade_tier = 1
    else:
        cascade_tier = 2

    env['out_details'] = out_details
    env['vacated_usage'] = total_vacated_usage
    env['vacated_minutes'] = total_vacated_minutes
    env['cascade_tier'] = cascade_tier

    player_usg = 0
    player_mpg_val = 0
    player_arch = data_cache['archetypes'].get(player_name, {})
    own_stats = data_cache['player_stats'].get(player_name)
    if own_stats:
        player_usg = float(own_stats.get('usg_pct', 15) if pd.notna(own_stats.get('usg_pct', 15)) else 15)
        player_mpg_val = float(own_stats.get('mpg', 25) if pd.notna(own_stats.get('mpg', 25)) else 25)

    game_pace_factor = env.get('pace_factor', 1.0)

    if cascade_tier == 0 and total_vacated_usage > 0 and out_details:
        env['teammate_out_usage'] = total_vacated_usage * 0.15
        env['teammate_out_reb'] = sum(d['rpg'] for d in out_details) * 0.10
        env['teammate_out_ast'] = sum(d['apg'] for d in out_details) * 0.08
        env['absorbed_usage'] = env['teammate_out_usage']
        env['min_escalation'] = 0
        env['opportunity_index'] = 1.0
        env['opportunity_spike'] = False
        env['opportunity_spike_eligible'] = False

    elif total_vacated_usage > 0 and out_details:
        team_remaining_depth = data_cache['depth_charts'].get(team, [])
        remaining_players = {}
        for dc_e in team_remaining_depth:
            rp_name = dc_e['player_name']
            if rp_name not in seen_out and rp_name not in remaining_players:
                rp_stats = data_cache['player_stats'].get(rp_name)
                if rp_stats and pd.notna(rp_stats.get('usg_pct')) and pd.notna(rp_stats.get('mpg')) and float(rp_stats.get('mpg', 0)) > 10:
                    remaining_players[rp_name] = float(rp_stats['usg_pct'])

        total_remaining_usg = sum(remaining_players.values())

        if total_remaining_usg > 0 and player_name in remaining_players:
            base_share = remaining_players[player_name] / total_remaining_usg

            arch_sim = _archetype_similarity(player_arch, out_details)
            weighted_share = base_share * (1.0 + arch_sim * 0.5)

            top_remaining_player = max(remaining_players, key=remaining_players.get)
            is_top_usage_player = (player_name == top_remaining_player)

            if cascade_tier == 2:
                cascade_mult = 1.0 + ((total_vacated_usage / 100.0) ** 1.15)
                absorption_cap = 0.45
                absorption_floor = 0.35 if is_top_usage_player else 0.0
            else:
                cascade_mult = 1.0 + (total_vacated_usage / 100.0) * 0.5
                absorption_cap = 0.35
                absorption_floor = 0.0

            absorbed_usage = total_vacated_usage * weighted_share * cascade_mult
            absorbed_usage = min(absorbed_usage, total_vacated_usage * absorption_cap)
            if absorption_floor > 0:
                absorbed_usage = max(absorbed_usage, total_vacated_usage * absorption_floor)
            if cascade_tier == 2 and not is_top_usage_player:
                top_player_share = remaining_players[top_remaining_player] / total_remaining_usg
                top_arch_sim = _archetype_similarity(
                    data_cache['archetypes'].get(top_remaining_player, {}), out_details)
                top_weighted = top_player_share * (1.0 + top_arch_sim * 0.5)
                top_absorbed = total_vacated_usage * top_weighted * cascade_mult
                top_absorbed = max(top_absorbed, total_vacated_usage * 0.35)
                top_absorbed = min(top_absorbed, total_vacated_usage * 0.45, 18.0)
                absorbed_usage = min(absorbed_usage, top_absorbed * 0.95)
            absorbed_usage = min(absorbed_usage, 18.0)

            is_starter = player_mpg_val >= 25
            starter_weight = 1.4 if is_starter else 0.7

            total_starter_weight = 0
            for rp_name, rp_usg in remaining_players.items():
                rp_stats = data_cache['player_stats'].get(rp_name)
                rp_mpg = float(rp_stats.get('mpg', 20)) if rp_stats and pd.notna(rp_stats.get('mpg')) else 20
                rp_is_starter = rp_mpg >= 25
                total_starter_weight += (1.4 if rp_is_starter else 0.7)

            min_escalation = 0
            if total_vacated_minutes > 0 and total_starter_weight > 0:
                role_share = starter_weight / total_starter_weight
                min_escalation = total_vacated_minutes * role_share
                min_cap = 38 - player_mpg_val
                min_escalation = min(min_escalation, max(min_cap, 0))

            env['teammate_out_usage'] = absorbed_usage
            env['teammate_out_reb'] = sum(d['rpg'] for d in out_details) * weighted_share * 0.6
            env['teammate_out_ast'] = sum(d['apg'] for d in out_details) * weighted_share * 0.5

            env['absorbed_usage'] = absorbed_usage
            env['min_escalation'] = min_escalation
            env['arch_similarity'] = arch_sim
            env['usage_share'] = weighted_share
            env['out_player_details'] = out_details
            env['total_vacated_usage'] = total_vacated_usage
            env['total_vacated_minutes'] = total_vacated_minutes

            baseline_opp = player_mpg_val * player_usg * game_pace_factor
            new_min = player_mpg_val + min_escalation
            new_usg = player_usg + absorbed_usage
            new_opp = new_min * new_usg * game_pace_factor
            opp_ratio = new_opp / baseline_opp if baseline_opp > 0 else 1.0
            env['opportunity_index'] = opp_ratio

            env['opportunity_spike_eligible'] = (opp_ratio >= 1.3)
            env['opportunity_spike'] = False

            escalated_min = env['pre_escalation_min'] + min_escalation
            spread = env['spread']
            if spread < -10:
                blowout_severity = min((abs(spread) - 10) / 12.0, 1.0)
                env['blowout_minutes_cap'] = 1.0 - (blowout_severity * 0.08)
                escalated_min *= env['blowout_minutes_cap']
            env['projected_min'] = min(escalated_min, 38)
        else:
            env['teammate_out_usage'] = total_vacated_usage * 0.15
            env['teammate_out_reb'] = sum(d['rpg'] for d in out_details) * 0.10
            env['teammate_out_ast'] = sum(d['apg'] for d in out_details) * 0.08
            env['absorbed_usage'] = env['teammate_out_usage']
            env['min_escalation'] = 0
            env['opportunity_index'] = 1.0
            env['opportunity_spike'] = False
            env['opportunity_spike_eligible'] = False

    else:
        env['absorbed_usage'] = 0
        env['min_escalation'] = 0
        env['opportunity_index'] = 1.0
        env['opportunity_spike'] = False
        env['opportunity_spike_eligible'] = False

    spread = env['spread']
    if spread < -10 and env['blowout_minutes_cap'] >= 1.0:
        blowout_severity = min((abs(spread) - 10) / 12.0, 1.0)
        env['blowout_minutes_cap'] = 1.0 - (blowout_severity * 0.08)
        env['projected_min'] *= env['blowout_minutes_cap']

    return env


def _archetype_similarity(player_arch, out_details):
    """Calculate how similar the remaining player's archetype is to the OUT players.

    Returns a score from 0.0 (very different) to 1.0 (very similar).
    Higher similarity = player absorbs more of the vacated usage.
    Uses cosine similarity on 4 archetype indices, weighted by OUT player usage.
    """
    if not player_arch or not out_details:
        return 0.25

    indices = ['creation_idx', 'playmaking_idx', 'perimeter_idx', 'interior_idx']
    player_vec = []
    for idx in indices:
        val = player_arch.get(idx, 0)
        player_vec.append(float(val) if val is not None and pd.notna(val) else 0.0)

    if all(v == 0 for v in player_vec):
        return 0.25

    total_weight = sum(d['usg'] for d in out_details)
    if total_weight == 0:
        return 0.25

    weighted_out_vec = [0.0] * len(indices)
    for d in out_details:
        w = d['usg'] / total_weight
        for i, idx in enumerate(indices):
            weighted_out_vec[i] += d.get(idx, 0) * w

    if all(v == 0 for v in weighted_out_vec):
        return 0.25

    dot = sum(a * b for a, b in zip(player_vec, weighted_out_vec))
    mag_a = max(sum(x**2 for x in player_vec) ** 0.5, 0.01)
    mag_b = max(sum(x**2 for x in weighted_out_vec) ** 0.5, 0.01)
    cosine_sim = dot / (mag_a * mag_b)

    return max(min((cosine_sim + 1.0) / 2.0, 1.0), 0.0)


def _detect_role_change(player_name, data_cache):
    """Detect bench-to-starter or minutes regime shifts.
    
    Compares last-5 game minutes against season average.
    Returns (is_role_change, recent_min_avg, season_min_avg, direction).
    """
    logs = data_cache['game_logs_by_player'].get(player_name)
    if logs is None or len(logs) == 0 or 'min' not in logs.columns:
        return False, 0, 0, None

    min_vals = logs['min'].dropna()
    min_vals = min_vals[min_vals > 1]
    if len(min_vals) < 5:
        return False, 0, 0, None

    last5_min = min_vals.head(5).mean()

    pstats = data_cache['player_stats'].get(player_name)
    season_mpg = float(pstats['mpg']) if pstats and pd.notna(pstats.get('mpg')) and float(pstats.get('mpg', 0)) > 0 else min_vals.mean()

    if season_mpg < 5:
        return False, last5_min, season_mpg, None

    min_ratio = last5_min / season_mpg

    if min_ratio >= 1.40 and last5_min - season_mpg >= 6:
        return True, round(last5_min, 1), round(season_mpg, 1), 'up'
    elif min_ratio <= 0.65 and season_mpg - last5_min >= 6:
        return True, round(last5_min, 1), round(season_mpg, 1), 'down'

    return False, round(last5_min, 1), round(season_mpg, 1), None


def _recency_weighted_avg(player_name, stat_col, data_cache, season_avg):
    """Calculate recency-weighted average with role change detection.
    
    Normal: 60% last-10, 40% season.
    Role change up: 80% last-5, 20% season (recent production in new role matters more).
    Role change down: 80% last-5, 20% season (captures reduced role).
    """
    logs = data_cache['game_logs_by_player'].get(player_name)
    if logs is None or len(logs) == 0 or stat_col not in logs.columns:
        return season_avg

    vals = logs[stat_col].dropna()
    if len(vals) < 5:
        return season_avg

    role_changed, recent_min, season_min, direction = _detect_role_change(player_name, data_cache)

    if role_changed:
        last5 = vals.head(5).mean()
        return (last5 * 0.80) + (season_avg * 0.20)

    last10 = vals.head(10).mean()
    return (last10 * 0.60) + (season_avg * 0.40)


def _shot_zone_efficiency_adjustment(player_name, opponent, data_cache):
    """Calculate expected points adjustment from shot zone alignment vs opponent defense.
    
    Compares where the player shoots from against how well the opponent defends each zone.
    Returns a points-per-game adjustment.
    """
    pzones = data_cache['shot_zones'].get(player_name)
    opp_def = data_cache['team_def_zones'].get(opponent)
    if pzones is None or opp_def is None:
        return 0.0, {}

    league_ra_fg = 65.0
    league_paint_fg = 42.0
    league_mid_fg = 41.0
    league_3_fg = 36.0

    total_fga = pzones.get('total_fga', 0)
    if not total_fga or total_fga == 0:
        return 0.0, {}

    adj = 0.0
    details = {}

    ra_share = (pzones.get('ra_pct', 0) or 0) / 100.0
    opp_ra_fg = opp_def.get('ra_fg_pct', league_ra_fg) or league_ra_fg
    ra_diff = (opp_ra_fg - league_ra_fg) / 100.0
    est_fga_pg = min(total_fga / 50.0, 20.0)
    ra_adj = ra_share * ra_diff * 2.0 * est_fga_pg
    if abs(ra_adj) > 0.01:
        details['rim'] = round(ra_adj, 2)
    adj += ra_adj

    paint_share = (pzones.get('paint_pct', 0) or 0) / 100.0
    opp_paint_fg = opp_def.get('paint_fg_pct', league_paint_fg) or league_paint_fg
    paint_diff = (opp_paint_fg - league_paint_fg) / 100.0
    paint_adj = paint_share * paint_diff * 2.0 * est_fga_pg
    if abs(paint_adj) > 0.01:
        details['paint'] = round(paint_adj, 2)
    adj += paint_adj

    mid_share = (pzones.get('mid_pct', 0) or 0) / 100.0
    opp_mid_fg = opp_def.get('mid_fg_pct', league_mid_fg) or league_mid_fg
    mid_diff = (opp_mid_fg - league_mid_fg) / 100.0
    mid_adj = mid_share * mid_diff * 2.0 * est_fga_pg
    if abs(mid_adj) > 0.01:
        details['mid'] = round(mid_adj, 2)
    adj += mid_adj

    three_share = (pzones.get('three_pct', 0) or 0) / 100.0
    opp_c3_fg = opp_def.get('corner3_fg_pct', league_3_fg) or league_3_fg
    opp_atb_fg = opp_def.get('atb3_fg_pct', league_3_fg) or league_3_fg
    opp_3_fg = (opp_c3_fg + opp_atb_fg) / 2.0
    three_diff = (opp_3_fg - league_3_fg) / 100.0
    three_adj = three_share * three_diff * 3.0 * est_fga_pg
    if abs(three_adj) > 0.01:
        details['three'] = round(three_adj, 2)
    adj += three_adj

    return round(adj, 2), details


def _physical_mismatch_score(player_name, opponent, position, data_cache):
    """Calculate physical mismatch advantage for rebounds/blocks.
    
    Compares player's height/weight/wingspan against opponent positional averages.
    Returns a normalized score (-1.0 to +1.0).
    """
    meas = data_cache['measurements'].get(player_name)
    if meas is None:
        return 0.0, {}

    h = meas.get('height_inches')
    w = meas.get('weight_lbs')
    ws = meas.get('wingspan_inches')
    if pd.isna(h) or pd.isna(w) or pd.isna(ws):
        return 0.0, {}

    pos_heights = {'PG': 75, 'SG': 77, 'SF': 79, 'PF': 81, 'C': 83}
    pos_weights = {'PG': 190, 'SG': 200, 'SF': 215, 'PF': 230, 'C': 250}
    pos_wings = {'PG': 78, 'SG': 80, 'SF': 82, 'PF': 84, 'C': 86}

    avg_h = pos_heights.get(position, 79)
    avg_w = pos_weights.get(position, 215)
    avg_ws = pos_wings.get(position, 82)

    h_diff = (h - avg_h) / 4.0
    w_diff = (w - avg_w) / 30.0
    ws_diff = (ws - avg_ws) / 4.0

    score = (h_diff * 0.35) + (w_diff * 0.30) + (ws_diff * 0.35)
    score = max(-1.5, min(1.5, score))

    details = {}
    if abs(h - avg_h) >= 2:
        details['height'] = f"{'+' if h > avg_h else ''}{h - avg_h:.0f}in"
    if abs(w - avg_w) >= 15:
        details['weight'] = f"{'+' if w > avg_w else ''}{w - avg_w:.0f}lbs"
    if abs(ws - avg_ws) >= 2:
        details['wingspan'] = f"{'+' if ws > avg_ws else ''}{ws - avg_ws:.0f}in"

    return round(score, 3), details


def _rebound_environment_score(team, opponent, game_env, data_cache):
    """Stage 1 of opportunity-based rebound model: predict game rebound environment.

    Returns (RES_multiplier, estimated_total_game_rebounds).
    RES is a normalized score where 1.0 = average game.
    """
    opp_def = data_cache['team_def_zones'].get(opponent)
    team_def = data_cache['team_def_zones'].get(team)

    league_avg_fg = data_cache.get('league_avg_fg_pct', 0.46)
    league_avg_3rate = data_cache.get('league_avg_3pa_rate', 42.0)
    league_avg_pace_val = data_cache.get('league_avg_pace', 99.0)

    miss_rate_score = 0.0
    three_rate_score = 0.0
    shot_distance_score = 0.0
    valid_sides = 0

    for def_zones in [opp_def, team_def]:
        if def_zones is None:
            continue
        tfga = def_zones.get('total_fga', 0)
        if not tfga or tfga == 0:
            continue
        valid_sides += 1

        tfgm = sum([
            def_zones.get('ra_fgm', 0) or 0, def_zones.get('paint_fgm', 0) or 0,
            def_zones.get('mid_fgm', 0) or 0, def_zones.get('corner3_fgm', 0) or 0,
            def_zones.get('atb3_fgm', 0) or 0,
        ])
        fg_pct = tfgm / tfga
        miss_rate_score += (1.0 - fg_pct) - (1.0 - league_avg_fg)

        three_fga = (def_zones.get('corner3_fga', 0) or 0) + (def_zones.get('atb3_fga', 0) or 0)
        three_rate = three_fga / tfga * 100.0
        three_rate_score += (three_rate - league_avg_3rate) / league_avg_3rate

        paint_fga = (def_zones.get('ra_fga', 0) or 0) + (def_zones.get('paint_fga', 0) or 0)
        paint_miss_pct = 1.0 - ((def_zones.get('ra_fgm', 0) or 0) + (def_zones.get('paint_fgm', 0) or 0)) / max(paint_fga, 1)
        three_miss_pct = 1.0 - ((def_zones.get('corner3_fgm', 0) or 0) + (def_zones.get('atb3_fgm', 0) or 0)) / max(three_fga, 1)
        shot_distance_score += (paint_miss_pct * (paint_fga / tfga) + three_miss_pct * (three_fga / tfga)) - 0.50

    divisor = max(valid_sides, 1)
    miss_rate_score /= divisor
    three_rate_score /= divisor
    shot_distance_score /= divisor

    pace_score = (game_env['pace_factor'] - 1.0)

    spread = abs(game_env.get('spread', 0))
    if spread <= 5:
        spread_score = 0.05
    elif spread <= 10:
        spread_score = 0.0
    elif spread <= 15:
        spread_score = -0.05
    else:
        spread_score = -0.10

    RES = 1.0 + (
        0.35 * miss_rate_score * 5.0 +
        0.20 * three_rate_score * 2.0 +
        0.15 * pace_score * 3.0 +
        0.15 * shot_distance_score * 3.0 +
        0.15 * spread_score
    )
    RES = max(0.80, min(1.30, RES))

    REALISTIC_TEAM_REB = 43.5
    est_game_rebounds = REALISTIC_TEAM_REB * 2.0 * RES

    return round(RES, 3), round(est_game_rebounds, 1)


def _player_rebound_share(player_name, position, team, opponent, game_env, data_cache, dva_diff, dvp_diff, dvp_blend, dva_blend):
    """Stage 2 of opportunity-based rebound model: predict player's share of available rebounds.

    Uses TRB% proxy, frontcourt redistribution, size advantage, rebound type fit,
    contested rebound rate, and DVA/DVP matchup.
    Returns (share, factors_list).
    """
    factors = []

    pstats = data_cache['player_stats'].get(player_name)
    per100 = data_cache.get('per100', {}).get(player_name)

    avg_total_game_reb = 87.0

    reb_pg = float(pstats['reb_pg']) if pstats and pd.notna(pstats.get('reb_pg')) else 4.0
    mpg = float(pstats['mpg']) if pstats and pd.notna(pstats.get('mpg')) and float(pstats.get('mpg', 0)) > 0 else 28.0

    base_trb_pct = reb_pg / (avg_total_game_reb * (mpg / 48.0))
    base_trb_pct = max(0.01, min(0.25, base_trb_pct))

    recent_reb = _recency_weighted_avg(player_name, 'reb', data_cache, reb_pg)
    recent_trb_pct = recent_reb / (avg_total_game_reb * (mpg / 48.0))
    recent_trb_pct = max(0.01, min(0.25, recent_trb_pct))

    share = (base_trb_pct * 0.40) + (recent_trb_pct * 0.60)
    if abs(recent_reb - reb_pg) > 0.5:
        factors.append(f"Recent {'↑' if recent_reb > reb_pg else '↓'}{abs(recent_reb - reb_pg):.1f}")

    team_reb_total = data_cache.get('team_reb_totals', {}).get(team, 43.0)
    out_players = data_cache['injuries_out']
    depth = data_cache['depth_charts'].get(team, [])
    reb_share_lost = 0.0
    for dc_entry in depth:
        pname = dc_entry['player_name']
        if pname in out_players and pname != player_name:
            out_stats = data_cache['player_stats'].get(pname)
            if out_stats and pd.notna(out_stats.get('reb_pg')) and pd.notna(out_stats.get('mpg')) and float(out_stats.get('mpg', 0)) > 15:
                out_reb = float(out_stats['reb_pg'])
                reb_share_lost += out_reb / max(team_reb_total, 30.0)
    if reb_share_lost > 0:
        redistribution = min(reb_share_lost * 0.35, 0.20)
        share *= (1.0 + redistribution)
        factors.append(f"Redistrib +{redistribution*100:.0f}%")

    meas = data_cache['measurements'].get(player_name)
    if meas is not None:
        h = meas.get('height_inches')
        w = meas.get('weight_lbs')
        ws = meas.get('wingspan_inches')
        pos_heights = {'PG': 75, 'SG': 77, 'SF': 79, 'PF': 81, 'C': 83}
        pos_weights = {'PG': 190, 'SG': 200, 'SF': 215, 'PF': 230, 'C': 250}
        pos_wings = {'PG': 78, 'SG': 80, 'SF': 82, 'PF': 84, 'C': 86}
        if pd.notna(h) and pd.notna(w) and pd.notna(ws):
            avg_h = pos_heights.get(position, 79)
            avg_w = pos_weights.get(position, 215)
            avg_ws = pos_wings.get(position, 82)
            sas = 0.4 * (h - avg_h) / 4.0 + 0.4 * (ws - avg_ws) / 4.0 + 0.2 * (w - avg_w) / 30.0
            sas = max(-2.0, min(2.0, sas))
            share *= (1.0 + 0.05 * sas)
            if abs(sas) > 0.3:
                details = []
                if abs(h - avg_h) >= 2:
                    details.append(f"{'+'if h>avg_h else ''}{h-avg_h:.0f}in")
                if abs(ws - avg_ws) >= 2:
                    details.append(f"ws{'+'if ws>avg_ws else ''}{ws-avg_ws:.0f}")
                factors.append(f"Size {' '.join(details)}" if details else f"SAS {sas:+.2f}")

    opp_def = data_cache['team_def_zones'].get(opponent)
    if opp_def is not None:
        tfga = opp_def.get('total_fga', 0)
        if tfga and tfga > 0:
            three_fga = (opp_def.get('corner3_fga', 0) or 0) + (opp_def.get('atb3_fga', 0) or 0)
            three_rate = three_fga / tfga
            paint_fga = (opp_def.get('ra_fga', 0) or 0) + (opp_def.get('paint_fga', 0) or 0)
            paint_rate = paint_fga / tfga

            arch = data_cache['archetypes'].get(player_name)
            reb_idx = float(arch.get('rebound_idx', 0)) if arch is not None and pd.notna(arch.get('rebound_idx')) else 0.0
            interior_idx = float(arch.get('interior_idx', 0)) if arch is not None and pd.notna(arch.get('interior_idx')) else 0.0

            if position in ('C', 'PF') or interior_idx > 1.0:
                short_reb_fit = paint_rate * max(1.0 + interior_idx * 0.03, 1.0)
            else:
                short_reb_fit = paint_rate * 0.95

            if reb_idx > 0.5:
                long_reb_fit = three_rate * (1.0 + reb_idx * 0.04)
            else:
                long_reb_fit = three_rate * 0.98

            reb_fit = short_reb_fit + long_reb_fit
            league_avg_fit = 0.50 + 0.42
            fit_adj = (reb_fit - league_avg_fit) / league_avg_fit
            share *= (1.0 + fit_adj * 0.15)
            if abs(fit_adj) > 0.05:
                factors.append(f"RebFit {fit_adj:+.0%}")

    hustle = data_cache['hustle'].get(player_name)
    if hustle is not None:
        contested = hustle.get('contested_per48', 0)
        box_outs = hustle.get('box_outs_per48', 0)
        if pd.notna(contested) and contested > 6.0:
            share *= (1.0 + (contested - 6.0) * 0.008)
        if pd.notna(box_outs) and box_outs > 2.0:
            share *= (1.0 + (box_outs - 2.0) * 0.015)
            factors.append(f"BoxOuts {box_outs:.1f}/48")

    if dva_diff != 0 or dvp_diff != 0:
        blended_matchup = (dvp_diff * dvp_blend) + (dva_diff * dva_blend)
        if pstats and pd.notna(pstats.get('reb_pg')) and float(pstats['reb_pg']) > 0:
            matchup_pct = blended_matchup / float(pstats['reb_pg'])
            share *= (1.0 + matchup_pct * 0.5)
        if abs(dva_diff) > 0.2:
            factors.append(f"DVA {dva_diff:+.1f}")

    share = max(0.02, min(0.28, share))

    return round(share, 5), factors


def _opp_play_type_vulnerability(opponent, play_type, data_cache):
    """Check if opponent is weak against a specific play type defensively."""
    key = (opponent, 'Defensive', play_type)
    pt = data_cache['play_types'].get(key)
    if pt is None:
        return 0.0, None
    ppp = pt.get('ppp', 0)
    pctile = pt.get('percentile', 0.5)
    tov_pct = pt.get('tov_poss_pct', 0)
    vulnerability = (1.0 - pctile) * 0.5 + max(0, ppp - 1.0) * 0.3
    return round(vulnerability, 3), {'ppp': round(ppp, 3), 'pctile': round(pctile, 2), 'tov': round(tov_pct, 3)}


def _project_points(player_name, player_avg, opponent, position, game_env, data_cache, dva_diff, dvp_diff, dvp_blend, dva_blend):
    """Stat-specific projection for POINTS using all available data."""
    factors = []

    role_changed, recent_min, season_min, direction = _detect_role_change(player_name, data_cache)
    base = _recency_weighted_avg(player_name, 'pts', data_cache, player_avg)
    if role_changed:
        factors.append(f"ROLE CHANGE {'↑' if direction == 'up' else '↓'} ({season_min:.0f}→{recent_min:.0f} min)")
    elif abs(base - player_avg) > 0.5:
        factors.append(f"Recent trend {'↑' if base > player_avg else '↓'}{abs(base - player_avg):.1f}")

    proj = base

    pace_adj = proj * (game_env['pace_factor'] - 1.0)
    proj += pace_adj
    if abs(game_env['pace_factor'] - 1.0) > 0.01:
        factors.append(f"Pace {game_env['pace_factor']:.2f}x")

    total_adj = proj * (game_env['total_factor'] - 1.0) * 0.6
    proj += total_adj
    if abs(game_env['total_factor'] - 1.0) > 0.02:
        factors.append(f"Total {game_env['game_total']:.0f}")

    if dva_diff != 0 or dvp_diff != 0:
        blended = (dvp_diff * dvp_blend) + (dva_diff * dva_blend)
        proj += blended
        if abs(dva_diff) > 0.3:
            factors.append(f"DVA {dva_diff:+.1f}")
        if abs(dvp_diff) > 0.3:
            factors.append(f"DVP {dvp_diff:+.1f}")

    sz_adj, sz_details = _shot_zone_efficiency_adjustment(player_name, opponent, data_cache)
    proj += sz_adj
    if abs(sz_adj) > 0.3:
        factors.append(f"ShotZone {sz_adj:+.1f}")

    opp_idx = game_env.get('opportunity_index', 1.0)
    if opp_idx > 1.0:
        pre_opp = proj
        proj *= opp_idx
        opp_boost = proj - pre_opp
        cascade_tier = game_env.get('cascade_tier', 0)
        if opp_boost > 0.3:
            tier_label = f" T{cascade_tier}" if cascade_tier > 0 else ""
            factors.append(f"Opp Ratio ×{opp_idx:.2f} (+{opp_boost:.1f}){tier_label}")
        if game_env.get('opportunity_spike'):
            factors.append("★ OPP SPIKE")
    elif game_env.get('teammate_out_usage', 0) > 0:
        usage_boost = player_avg * game_env['teammate_out_usage'] / 100.0
        proj += usage_boost
        if usage_boost > 0.3:
            factors.append(f"Usage boost +{usage_boost:.1f}")

    if game_env['blowout_minutes_cap'] < 1.0:
        cap_reduction = (1.0 - game_env['blowout_minutes_cap'])
        proj *= (1.0 - cap_reduction * 0.5)
        factors.append(f"Blowout cap {game_env['blowout_minutes_cap']:.2f}")

    pstats = data_cache['player_stats'].get(player_name)
    if opp_idx <= 1.0 and pstats is not None:
        mpg_val = pstats.get('mpg', 0)
        if pd.notna(mpg_val) and float(mpg_val) > 0:
            min_ratio = game_env['projected_min'] / float(mpg_val)
            if abs(min_ratio - 1.0) > 0.08:
                proj *= (1.0 + (min_ratio - 1.0) * 0.7)
                factors.append(f"Min proj {game_env['projected_min']:.0f}")

    mh = data_cache['matchup_hist'].get((player_name, opponent))
    if mh is not None and pd.notna(mh.get('fppm_diff')):
        mh_adj = float(mh['fppm_diff']) * game_env['projected_min'] * 0.15
        if abs(mh_adj) > 0.3:
            proj += mh_adj * 0.3
            factors.append(f"H2H {mh_adj:+.1f}")

    max_proj = base * 1.20
    if proj > max_proj:
        factors.append(f"Inflation cap {proj:.1f}→{max_proj:.1f}")
        proj = max_proj

    return round(max(proj, 0), 1), factors


def _project_rebounds(player_name, player_avg, opponent, position, game_env, data_cache, dva_diff, dvp_diff, dvp_blend, dva_blend):
    """Opportunity-based rebound projection model.

    Stage 1: Rebound Environment Score (RES) — how many rebounds exist in this game
    Stage 2: Player Rebound Share — what fraction this player captures
    Final:   Projected Rebounds = Game Rebounds × Player Share × Minutes Ratio
    """
    team = None
    pstats = data_cache['player_stats'].get(player_name)
    if pstats:
        team = pstats.get('team')
    if not team:
        for t, entries in data_cache['depth_charts'].items():
            if any(e['player_name'] == player_name for e in entries):
                team = t
                break
    if not team:
        team = opponent

    res, est_game_reb = _rebound_environment_score(team, opponent, game_env, data_cache)
    share, share_factors = _player_rebound_share(player_name, position, team, opponent, game_env, data_cache, dva_diff, dvp_diff, dvp_blend, dva_blend)

    role_changed, recent_min, season_min, direction = _detect_role_change(player_name, data_cache)
    factors = []
    if role_changed:
        factors.append(f"ROLE CHANGE {'↑' if direction == 'up' else '↓'} ({season_min:.0f}→{recent_min:.0f} min)")
    factors.append(f"RES {res:.2f}")
    factors.extend(share_factors)

    proj_min = game_env.get('projected_min', 30)
    if pstats and pd.notna(pstats.get('mpg')) and float(pstats.get('mpg', 0)) > 0:
        actual_mpg = float(pstats['mpg'])
        capped_min = min(proj_min, actual_mpg * 1.25)
        minutes_ratio = capped_min / 48.0
        if proj_min > actual_mpg * 1.25:
            factors.append(f"Min cap {capped_min:.0f}/{proj_min:.0f}")
    else:
        minutes_ratio = proj_min / 48.0

    opp_model_proj = est_game_reb * share * minutes_ratio

    if game_env['blowout_minutes_cap'] < 1.0:
        opp_model_proj *= game_env['blowout_minutes_cap']
        factors.append(f"Blowout cap {game_env['blowout_minutes_cap']:.2f}")

    if role_changed:
        recent_reb_avg = _recency_weighted_avg(player_name, 'reb', data_cache, player_avg)
        proj = (opp_model_proj * 0.45) + (recent_reb_avg * 0.55)
    else:
        proj = (opp_model_proj * 0.55) + (player_avg * 0.45)

    teammate_out_reb = game_env.get('teammate_out_reb', 0)
    if teammate_out_reb > 0:
        proj += teammate_out_reb
        if teammate_out_reb > 0.2:
            factors.append(f"Teammate OUT +{teammate_out_reb:.1f} reb")
        if game_env.get('opportunity_spike'):
            factors.append("★ OPP SPIKE")

    max_proj = player_avg * 1.20
    if proj > max_proj:
        factors.append(f"Inflation cap {proj:.1f}→{max_proj:.1f}")
        proj = max_proj

    factors.append(f"Share {share*100:.1f}%")
    factors.append(f"GameReb {est_game_reb:.0f}")

    return round(max(proj, 0), 1), factors


def _project_assists(player_name, player_avg, opponent, position, game_env, data_cache, dva_diff, dvp_diff, dvp_blend, dva_blend):
    """Stat-specific projection for ASSISTS using playmaking and defensive vulnerability data."""
    factors = []

    role_changed, recent_min, season_min, direction = _detect_role_change(player_name, data_cache)
    base = _recency_weighted_avg(player_name, 'ast', data_cache, player_avg)
    if role_changed:
        factors.append(f"ROLE CHANGE {'↑' if direction == 'up' else '↓'} ({season_min:.0f}→{recent_min:.0f} min)")
    elif abs(base - player_avg) > 0.3:
        factors.append(f"Recent {'↑' if base > player_avg else '↓'}{abs(base - player_avg):.1f}")

    proj = base

    pace_adj = proj * (game_env['pace_factor'] - 1.0) * 0.7
    proj += pace_adj
    if abs(game_env['pace_factor'] - 1.0) > 0.01:
        factors.append(f"Pace {game_env['pace_factor']:.2f}x")

    total_adj = proj * (game_env['total_factor'] - 1.0) * 0.5
    proj += total_adj
    if abs(game_env['total_factor'] - 1.0) > 0.02:
        factors.append(f"Total {game_env['game_total']:.0f}")

    if dva_diff != 0 or dvp_diff != 0:
        blended = (dvp_diff * dvp_blend) + (dva_diff * dva_blend)
        proj += blended
        if abs(dva_diff) > 0.2:
            factors.append(f"DVA {dva_diff:+.1f}")

    arch = data_cache['archetypes'].get(player_name)
    if arch is not None:
        pm_idx = arch.get('playmaking_idx', 0)
        if pd.notna(pm_idx) and pm_idx > 3.0:
            pm_boost = (pm_idx - 3.0) * 0.2
            proj += pm_boost
            factors.append(f"Playmaking idx {pm_idx:.1f}")

    pnr_vuln, pnr_details = _opp_play_type_vulnerability(opponent, 'PRBallHandler', data_cache)
    if pnr_vuln > 0.15:
        creation = data_cache['shot_creation'].get(player_name)
        if creation is not None and pd.notna(creation.get('pu_pct')) and float(creation['pu_pct']) > 20:
            pnr_adj = pnr_vuln * 0.8
            proj += pnr_adj
            if pnr_details:
                factors.append(f"P&R vuln ppp:{pnr_details['ppp']}")

    iso_vuln, iso_details = _opp_play_type_vulnerability(opponent, 'Isolation', data_cache)
    if iso_vuln > 0.15:
        if arch is not None and pd.notna(arch.get('creation_idx')) and arch['creation_idx'] > 3.0:
            iso_adj = iso_vuln * 0.4
            proj += iso_adj

    opp_idx = game_env.get('opportunity_index', 1.0)
    if opp_idx > 1.0:
        pre_opp = proj
        proj *= opp_idx
        ast_opp_boost = proj - pre_opp
        if ast_opp_boost > 0.2:
            factors.append(f"Opp Ratio ×{opp_idx:.2f} (+{ast_opp_boost:.1f} ast)")
        if game_env.get('opportunity_spike'):
            factors.append("★ OPP SPIKE")
    elif game_env.get('teammate_out_ast', 0) > 0:
        proj += game_env['teammate_out_ast']
        factors.append(f"Teammate OUT +{game_env['teammate_out_ast']:.1f} ast")

    if game_env['blowout_minutes_cap'] < 1.0:
        proj *= (1.0 - (1.0 - game_env['blowout_minutes_cap']) * 0.5)

    pstats = data_cache['player_stats'].get(player_name)
    if opp_idx <= 1.0 and pstats is not None and pd.notna(pstats.get('mpg', 0)) and float(pstats.get('mpg', 0)) > 0:
        min_ratio = game_env['projected_min'] / float(pstats['mpg'])
        if abs(min_ratio - 1.0) > 0.08:
            proj *= (1.0 + (min_ratio - 1.0) * 0.7)
            factors.append(f"Min proj {game_env['projected_min']:.0f}")

    max_proj = base * 1.20
    if proj > max_proj:
        factors.append(f"Inflation cap {proj:.1f}→{max_proj:.1f}")
        proj = max_proj

    return round(max(proj, 0), 1), factors


def _project_steals(player_name, player_avg, opponent, position, game_env, data_cache, dva_diff, dvp_diff, dvp_blend, dva_blend):
    """Stat-specific projection for STEALS using hustle and turnover data."""
    factors = []

    role_changed, recent_min, season_min, direction = _detect_role_change(player_name, data_cache)
    base = _recency_weighted_avg(player_name, 'stl', data_cache, player_avg)
    if role_changed:
        factors.append(f"ROLE CHANGE {'↑' if direction == 'up' else '↓'} ({season_min:.0f}→{recent_min:.0f} min)")

    proj = base

    proj *= game_env['pace_factor']

    if dva_diff != 0 or dvp_diff != 0:
        blended = (dvp_diff * dvp_blend) + (dva_diff * dva_blend)
        proj += blended
        if abs(dva_diff) > 0.1:
            factors.append(f"DVA {dva_diff:+.2f}")

    hustle = data_cache['hustle'].get(player_name)
    if hustle is not None:
        defl = hustle.get('deflections_per48', 0)
        if pd.notna(defl) and defl > 3.0:
            defl_boost = (defl - 3.0) * 0.08
            proj += defl_boost
            factors.append(f"Deflections {defl:.1f}/48")

    arch = data_cache['archetypes'].get(player_name)
    if arch is not None:
        def_idx = arch.get('defense_idx', 0)
        if pd.notna(def_idx) and def_idx > 1.0:
            proj += (def_idx - 1.0) * 0.04
            factors.append(f"Def idx {def_idx:.1f}")

    dvp_data = data_cache.get('dvp_avgs', {}).get(position, {})
    opp_tov = dvp_data.get('stl', 0)
    if opp_tov > 0:
        league_avg_stl = sum(v.get('stl', 0) for v in data_cache.get('dvp_avgs', {}).values()) / max(len(data_cache.get('dvp_avgs', {})), 1)
        if league_avg_stl > 0:
            tov_factor = opp_tov / league_avg_stl
            if abs(tov_factor - 1.0) > 0.05:
                proj *= (1.0 + (tov_factor - 1.0) * 0.3)

    opp_idx = game_env.get('opportunity_index', 1.0)
    if opp_idx > 1.0:
        pre_opp = proj
        proj *= opp_idx
        stl_opp_boost = proj - pre_opp
        if stl_opp_boost > 0.1:
            factors.append(f"Opp Ratio ×{opp_idx:.2f} (+{stl_opp_boost:.1f} stl)")
        if game_env.get('opportunity_spike'):
            factors.append("★ OPP SPIKE")
    else:
        pstats = data_cache['player_stats'].get(player_name)
        if pstats is not None and pd.notna(pstats.get('mpg', 0)) and float(pstats.get('mpg', 0)) > 0:
            min_ratio = game_env['projected_min'] / float(pstats['mpg'])
            if abs(min_ratio - 1.0) > 0.08:
                proj *= (1.0 + (min_ratio - 1.0) * 0.6)

    max_proj = base * 1.20
    if proj > max_proj:
        factors.append(f"Inflation cap {proj:.1f}→{max_proj:.1f}")
        proj = max_proj

    return round(max(proj, 0), 1), factors


def _project_blocks(player_name, player_avg, opponent, position, game_env, data_cache, dva_diff, dvp_diff, dvp_blend, dva_blend):
    """Stat-specific projection for BLOCKS using physical and opponent rim-attack data."""
    factors = []

    role_changed, recent_min, season_min, direction = _detect_role_change(player_name, data_cache)
    base = _recency_weighted_avg(player_name, 'blk', data_cache, player_avg)
    if role_changed:
        factors.append(f"ROLE CHANGE {'↑' if direction == 'up' else '↓'} ({season_min:.0f}→{recent_min:.0f} min)")

    proj = base

    proj *= game_env['pace_factor']

    if dva_diff != 0 or dvp_diff != 0:
        blended = (dvp_diff * dvp_blend) + (dva_diff * dva_blend)
        proj += blended
        if abs(dva_diff) > 0.1:
            factors.append(f"DVA {dva_diff:+.2f}")

    phys_score, phys_details = _physical_mismatch_score(player_name, opponent, position, data_cache)
    if phys_score > 0.2:
        phys_adj = phys_score * 0.3
        proj += phys_adj
        if phys_details:
            factors.append(f"Size advantage")

    opp_def = data_cache['team_def_zones'].get(opponent)
    if opp_def is not None:
        ra_freq = float(opp_def.get('ra_freq', 30) or 30)
        league_avg_ra = 28.0
        if ra_freq > league_avg_ra:
            rim_boost = (ra_freq - league_avg_ra) / 100.0 * 1.5
            proj += rim_boost
            factors.append(f"Rim attack {ra_freq:.0f}%")

    hustle = data_cache['hustle'].get(player_name)
    if hustle is not None:
        contested = hustle.get('contested_2pt', 0)
        if pd.notna(contested) and contested > 50:
            c_boost = (contested - 50) / 200.0
            proj += c_boost
            factors.append(f"Contested 2pt {contested:.0f}")

    arch = data_cache['archetypes'].get(player_name)
    if arch is not None:
        int_idx = arch.get('interior_idx', 0)
        if pd.notna(int_idx) and int_idx > 1.0:
            proj += (int_idx - 1.0) * 0.05
            factors.append(f"Interior idx {int_idx:.1f}")

    opp_idx = game_env.get('opportunity_index', 1.0)
    if opp_idx > 1.0:
        pre_opp = proj
        proj *= opp_idx
        blk_opp_boost = proj - pre_opp
        if blk_opp_boost > 0.1:
            factors.append(f"Opp Ratio ×{opp_idx:.2f} (+{blk_opp_boost:.1f} blk)")
        if game_env.get('opportunity_spike'):
            factors.append("★ OPP SPIKE")
    else:
        pstats = data_cache['player_stats'].get(player_name)
        if pstats is not None and pd.notna(pstats.get('mpg', 0)) and float(pstats.get('mpg', 0)) > 0:
            min_ratio = game_env['projected_min'] / float(pstats['mpg'])
            if abs(min_ratio - 1.0) > 0.08:
                proj *= (1.0 + (min_ratio - 1.0) * 0.6)

    max_proj = base * 1.20
    if proj > max_proj:
        factors.append(f"Inflation cap {proj:.1f}→{max_proj:.1f}")
        proj = max_proj

    return round(max(proj, 0), 1), factors


def _calculate_composite_score(projected, player_avg, book_line, dva_diff, dvp_diff, game_env, confidence, hit_rate, cv, factors_list, recommendation='OVER', line_drift=None, last_hour_drift=None, move_pattern=None, snapshot_count=0, is_b2b=False):
    """Calculate a composite score (0-100) that ranks prop quality across all dimensions.

    Weights: Edge size (30%), Matchup alignment DVA+DVP (20%), game environment (20%),
    consistency (15%), trend alignment (15%), plus a directional line-movement
    modifier capped at +/-5 points (split between total drift, last-hour drift,
    and a small move-pattern bonus).

    Matchup scoring is DIRECTIONAL: DVA/DVP values are scored based on whether they
    support the pick direction, not by raw absolute value. Line drift scoring is also
    directional: line moving toward our pick = bonus (sharp money confirmation),
    line moving against our pick = malus (yellow flag).

    B2B fatigue: when `is_b2b` is True and the pick is a HIGH-confidence OVER, deduct
    a small penalty (-3) UNLESS the matchup is already very poor (dva_diff <= -1.0),
    in which case the composite is already suppressed and an additional B2B penalty
    would double-count. Gated to HIGH only because lower-confidence OVERs are already
    discounted by their failing gates and an extra penalty would over-suppress them.

    Line-movement calibration (Task #24, 2026-04-25):
        After Task #23 added the 11/14/16 ET intra-day scrapes, we now have the
        raw material for empirical calibration via `analysis/calibrate_drift_bonus.py`,
        which buckets graded picks by directed total drift and last-hour drift.
        The first run (60-day window, 36 graded picks matched to history) found
        ZERO picks with multi-snapshot drift data — the intra-day scrapes had
        only just started shipping multi-snapshot rows, so the empirical lift
        per bucket cannot be measured yet. Until that backtest accumulates a
        useful sample (>=15 multi-snapshot decided picks), the cap and slope
        below are *prior-knowledge* defaults rather than fully data-driven.
        The priors are designed to:
          - Reduce the total-drift cap from +/-3 (open-vs-current placeholder)
            to +/-2 because, with intra-day snapshots, the more recent move is
            the sharper signal and total drift would otherwise double-count it.
          - Add a separate last-hour-drift modifier (slope 2.0, cap +/-2.5):
            higher slope per unit because late moves are usually injury or
            sharp action, not noise. Threshold lowered to 0.25 so a single
            half-point intra-day tick still scores.
          - Add a small move-pattern bonus: a `sudden_swing` aligned with the
            pick gets +0.5 (sharp confirmation), misaligned gets -0.5, and a
            `reversal` gets -0.25 regardless of direction (book uncertainty).
          - Combined cap of +/-5 keeps the modifier from ever overwhelming the
            edge/matchup/form anchor of the composite.
          - `snapshot_count` gates the richer signals: total drift requires
            >=2 snapshots; last-hour drift and the move-pattern bonus both
            require >=3 so we don't infer "intra-day shape" from data we
            don't have.
        Re-run the backtest tool once the multi-snapshot sample is large
        enough and tighten these numbers from the empirical lift table.
    """
    edge_score = 0
    if book_line and not pd.isna(book_line) and book_line > 0:
        edge_pct = abs(projected - book_line) / book_line
        edge_score = min(edge_pct * 200, 30)
    elif player_avg > 0:
        edge_pct = abs(projected - player_avg) / player_avg
        edge_score = min(edge_pct * 150, 20)

    matchup_score = 0
    dva_contrib = 0
    dvp_contrib = 0
    dva_val = dva_diff if dva_diff and not pd.isna(dva_diff) else 0
    dvp_val = dvp_diff if dvp_diff and not pd.isna(dvp_diff) else 0

    if recommendation == 'OVER':
        directed_dva = dva_val
        directed_dvp = dvp_val
    else:
        directed_dva = -dva_val
        directed_dvp = -dvp_val

    if directed_dva > 0:
        dva_contrib = min(directed_dva * 4, 12)
    else:
        dva_contrib = max(directed_dva * 2, -6)

    if directed_dvp > 0:
        dvp_contrib = min(directed_dvp * 3, 8)
    else:
        dvp_contrib = max(directed_dvp * 1.5, -4)

    matchup_score = max(min(dva_contrib + dvp_contrib, 20), 0)

    env_score = 0
    tf = game_env.get('total_factor', 1.0)
    pf = game_env.get('pace_factor', 1.0)
    env_score += min((tf - 0.95) * 50, 10) if tf > 0.95 else 0
    env_score += min((pf - 0.95) * 50, 5) if pf > 0.95 else 0
    if game_env.get('teammate_out_usage', 0) > 0:
        env_score += min(game_env['teammate_out_usage'] * 1.5, 5)
    if game_env.get('opportunity_spike') and recommendation == 'OVER':
        env_score += 5
    env_score = min(env_score, 20)

    consistency_score = 0
    if cv is not None and not pd.isna(cv):
        if cv <= 0.20:
            consistency_score = 15
        elif cv <= 0.25:
            consistency_score = 12
        elif cv <= 0.30:
            consistency_score = 8
        elif cv <= 0.40:
            consistency_score = 5
        else:
            consistency_score = 2

    trend_score = 0
    if hit_rate is not None and not pd.isna(hit_rate):
        if hit_rate >= 65:
            trend_score = 15
        elif hit_rate >= 58:
            trend_score = 11
        elif hit_rate >= 50:
            trend_score = 7
        else:
            trend_score = 3

    # Line-movement modifier — see calibration notes in the docstring above.
    # Three components, each capped, with a final combined cap of +/-5.
    # `snapshot_count` gates the richer signals: total drift only requires
    # 2 snapshots (open vs current) to be meaningful, but last-hour drift
    # and the move pattern need >=3 to actually distinguish "late move" from
    # "single overnight tick" — without that we'd be inferring intra-day
    # shape from data we don't have.
    line_score = 0.0
    snaps = snapshot_count or 0

    if (
        snaps >= 2
        and line_drift is not None
        and not pd.isna(line_drift)
        and abs(line_drift) >= 0.5
    ):
        directed_drift = line_drift if recommendation == 'OVER' else -line_drift
        line_score += max(min(directed_drift * 1.0, 2.0), -2.0)

    if (
        snaps >= 3
        and last_hour_drift is not None
        and not pd.isna(last_hour_drift)
        and abs(last_hour_drift) >= 0.25
    ):
        directed_lh = last_hour_drift if recommendation == 'OVER' else -last_hour_drift
        line_score += max(min(directed_lh * 2.0, 2.5), -2.5)

    if snaps >= 3:
        if move_pattern == 'sudden_swing' and line_drift is not None and not pd.isna(line_drift) and line_drift != 0:
            aligned = (line_drift > 0) == (recommendation == 'OVER')
            line_score += 0.5 if aligned else -0.5
        elif move_pattern == 'reversal':
            line_score -= 0.25

    line_score = max(min(line_score, 5.0), -5.0)

    b2b_score = 0
    if is_b2b and recommendation == 'OVER' and str(confidence).upper() == 'HIGH':
        dva_for_check = dva_diff if dva_diff and not pd.isna(dva_diff) else 0
        if dva_for_check > -1.0:
            b2b_score = -3
            if isinstance(factors_list, list):
                factors_list.append("B2B fatigue penalty -3")

    total = edge_score + matchup_score + env_score + consistency_score + trend_score + line_score + b2b_score
    return round(max(min(total, 100), 0), 1)


STAT_PROJECTION_FN = {
    'pts': _project_points,
    'reb': _project_rebounds,
    'ast': _project_assists,
    'stl': _project_steals,
    'blk': _project_blocks,
}


def get_prop_recommendations(players_df, dvp_df, per100_df, dva_df=None, min_value=4.0, top_n=50):
    """Generate prop bet recommendations using stat-specific projection models.
    
    Each stat (PTS, REB, AST, STL, BLK) uses its own projection model that incorporates:
    - Pace and game total environment
    - Shot zone alignment vs opponent defensive zones
    - Physical mismatch scoring (height/weight/wingspan)
    - Hustle stats (deflections, box-outs, contested shots)
    - Opponent play type vulnerabilities
    - Injury-driven usage redistribution
    - Recency-weighted averages (60% last-10, 40% season)
    - DVA/DVP blended matchup edges
    - Minutes projection with blowout risk capping
    - Head-to-head matchup history
    """
    import unicodedata, re

    valued_df = calculate_value_metrics(players_df)
    high_value = valued_df[valued_df['value'] >= min_value].nlargest(top_n, 'value')

    data_cache = _build_projection_cache()
    game_logs_df = data_cache['game_logs']

    stats_norm = per100_df.copy()
    stats_norm['team_norm'] = stats_norm['team'].apply(normalize_team)

    per100_cols = ['pts_per100', 'reb_per100', 'ast_per100', 'stl_per100', 'blk_per100']
    for col in per100_cols:
        if col in stats_norm.columns and 'mpg' in stats_norm.columns:
            pg_col = col.replace('_per100', '_pg')
            stats_norm[pg_col] = stats_norm[col] * stats_norm['mpg'] / 48.0

    stat_config = {
        'pts': ('pts_pg', 'PTS', 1.0, 8.0, 18.0),
        'reb': ('reb_pg', 'REB', 1.2, 3.0, 8.0),
        'ast': ('ast_pg', 'AST', 1.5, 2.0, 6.0),
        'stl': ('stl_pg', 'STL', 3.0, 0.5, 1.5),
        'blk': ('blk_pg', 'BLK', 3.0, 0.5, 1.2)
    }

    dva_stat_map = {
        'pts': 'pts_pm_diff',
        'reb': 'reb_pm_diff',
        'ast': 'ast_pm_diff',
        'stl': 'stl_pm_diff',
        'blk': 'blk_pm_diff',
    }

    has_dva = dva_df is not None and len(dva_df) > 0
    season_pct = _get_season_pct()
    dvp_blend = 0.70 - (0.40 * season_pct)
    dva_blend = 1.0 - dvp_blend
    blend_label = f"{int(dvp_blend*100)}/{int(dva_blend*100)} DVP/DVA"

    book_props = _load_book_props()
    has_book = len(book_props) > 0
    line_movement = _load_line_movement()
    if line_movement:
        snapshots_seen = sum(1 for v in line_movement.values() if v['snapshot_count'] > 1)
        late_movers = sum(1 for v in line_movement.values()
                          if v.get('last_hour_drift') is not None
                          and abs(v['last_hour_drift']) >= 0.5)
        sudden_swings = sum(1 for v in line_movement.values()
                            if v.get('move_pattern') == 'sudden_swing')
        reversals = sum(1 for v in line_movement.values()
                        if v.get('move_pattern') == 'reversal')
        print(f"  Line movement: tracking {len(line_movement)} props ({snapshots_seen} with multi-snapshot drift data, {late_movers} with last-hour drift >=0.5, {sudden_swings} sudden swings, {reversals} reversals)")

    props = []
    gate_failure_log = []
    dva_hits = 0
    role_change_players = []

    for _, player in high_value.iterrows():
        player_name = player['player_name']
        opponent = player.get('opponent')
        position = player.get('true_position')
        team = player['team']
        archetype = player.get('archetype')

        if pd.isna(opponent) or pd.isna(position):
            continue

        team_norm = normalize_team(team)
        player_stats_row = stats_norm[(stats_norm['player_name'] == player_name) & (stats_norm['team_norm'] == team_norm)]
        if len(player_stats_row) == 0:
            continue
        player_stats_row = player_stats_row.iloc[0]

        opp_dvp = dvp_df[(dvp_df['team'] == opponent) & (dvp_df['position'] == position)]
        if len(opp_dvp) == 0:
            continue
        opp_dvp = opp_dvp.iloc[0]

        opp_dva = None
        if has_dva and archetype and not pd.isna(archetype):
            dva_match = dva_df[(dva_df['opp_team'] == opponent) & (dva_df['archetype'] == archetype)]
            if len(dva_match) > 0:
                opp_dva = dva_match.iloc[0]

        game_env = _build_game_environment(player, data_cache)

        if game_env.get('opportunity_spike_eligible'):
            max_dvp = 0.0
            max_dva = 0.0
            for sk in ['pts', 'reb', 'ast', 'stl', 'blk']:
                opp_allows_sk = opp_dvp.get(sk, 0) if opp_dvp is not None else 0
                pos_dvp_all = dvp_df[dvp_df['position'] == position]
                if len(pos_dvp_all) > 0 and pd.notna(opp_allows_sk):
                    league_avg_sk = pos_dvp_all[sk].mean()
                    dvp_sk = opp_allows_sk - league_avg_sk
                    if dvp_sk > max_dvp:
                        max_dvp = dvp_sk

                if opp_dva is not None:
                    dva_col = dva_stat_map.get(sk)
                    if dva_col and dva_col in opp_dva.index:
                        raw_dva = opp_dva[dva_col]
                        if pd.notna(raw_dva):
                            dva_sk = raw_dva * game_env.get('projected_min', 30)
                            if dva_sk > max_dva:
                                max_dva = dva_sk

            game_env['opportunity_spike'] = (max_dvp > 0.5 or max_dva > 0.5)

        player_norm = _normalize_prop_name(player_name)

        rc, rc_recent, rc_season, rc_dir = _detect_role_change(player_name, data_cache)
        if rc and player_name not in [p[0] for p in role_change_players]:
            role_change_players.append((player_name, rc_dir, rc_season, rc_recent))

        for stat_key, (col, label, fp_mult, min_under, min_over) in stat_config.items():
            player_avg = player_stats_row.get(col, 0)
            if pd.isna(player_avg):
                continue

            opp_allows = opp_dvp.get(stat_key, 0)
            all_pos_dvp = dvp_df[dvp_df['position'] == position]
            if len(all_pos_dvp) == 0:
                continue

            league_avg = all_pos_dvp[stat_key].mean()
            dvp_diff = opp_allows - league_avg

            dva_diff = 0.0
            dva_source = None
            if opp_dva is not None:
                dva_col = dva_stat_map.get(stat_key)
                if dva_col and dva_col in opp_dva.index:
                    raw_pm_diff = opp_dva[dva_col]
                    if pd.notna(raw_pm_diff):
                        proj_min = game_env['projected_min']
                        if pd.isna(proj_min) or proj_min <= 0:
                            proj_min = 30
                        dva_diff = raw_pm_diff * proj_min
                        dva_source = archetype

            if opp_dva is not None and dva_source:
                dva_hits += 1

            proj_fn = STAT_PROJECTION_FN.get(stat_key)
            if proj_fn:
                projected_value, proj_factors = proj_fn(
                    player_name, player_avg, opponent, position,
                    game_env, data_cache, dva_diff, dvp_diff, dvp_blend, dva_blend
                )
            else:
                blended = (dvp_diff * dvp_blend) + (dva_diff * dva_blend) if dva_source else dvp_diff
                projected_value = round(player_avg + blended, 1)
                proj_factors = []

            book_key = (player_norm, label)
            book = book_props.get(book_key)
            book_line = None
            book_over = None
            book_under = None
            edge = None

            if book:
                book_line = book['line']
                book_over = book['over_odds']
                book_under = book['under_odds']
                if book_line and book_line > 0:
                    edge = round(((projected_value - book_line) / book_line) * 100, 1)

            should_include = False
            diff_from_line = projected_value - (book_line if book_line else player_avg)

            if diff_from_line > 0 and player_avg >= min_over * 0.7:
                recommendation = 'OVER'
                if abs(diff_from_line) >= 0.5 or (book_line and abs(edge or 0) >= 3):
                    should_include = True
            elif diff_from_line < 0 and player_avg <= min_under * 1.3:
                recommendation = 'UNDER'
                if abs(diff_from_line) >= 0.5 or (book_line and abs(edge or 0) >= 3):
                    should_include = True
            elif book and book_line:
                gap = projected_value - book_line
                if abs(gap) >= 1.0:
                    recommendation = 'OVER' if gap > 0 else 'UNDER'
                    should_include = True

            if not should_include and abs(dva_diff) > 1.0 and player_avg >= min_over * 0.5:
                recommendation = 'OVER' if dva_diff > 0 else 'UNDER'
                should_include = True

            if should_include:
                usage_boost_val = game_env.get('teammate_out_usage', 0)
                opp_spike = game_env.get('opportunity_spike', False)
                itt = game_env.get('implied_team_total')
                conf = _evaluate_prop_confidence(player_name, stat_key, book_line, player_avg, game_logs_df, recommendation,
                                                dva_diff=dva_diff, dvp_diff=dvp_diff, usage_boost=usage_boost_val,
                                                opportunity_spike=opp_spike, implied_team_total=itt)

                blended_diff = (dvp_diff * dvp_blend) + (dva_diff * dva_blend) if dva_source else dvp_diff

                line_drift_data = line_movement.get(book_key) if line_movement else None
                opening_line_val = line_drift_data['opening_line'] if line_drift_data else None
                current_line_val = line_drift_data['current_line'] if line_drift_data else None
                line_drift_val = line_drift_data['line_drift'] if line_drift_data else None
                line_drift_pct_val = line_drift_data['line_drift_pct'] if line_drift_data else None
                line_snapshots_val = line_drift_data['snapshot_count'] if line_drift_data else 0
                last_hour_drift_val = line_drift_data['last_hour_drift'] if line_drift_data else None
                last_hour_from_val = line_drift_data['last_hour_from'] if line_drift_data else None
                last_hour_minutes_val = line_drift_data['last_hour_minutes'] if line_drift_data else None
                move_pattern_val = line_drift_data.get('move_pattern') if line_drift_data else None
                largest_swing_val = line_drift_data.get('largest_swing') if line_drift_data else None
                largest_swing_share_val = line_drift_data.get('largest_swing_share') if line_drift_data else None

                team_is_b2b_val = bool(player.get('team_is_b2b', False)) if hasattr(player, 'get') else False
                composite = _calculate_composite_score(
                    projected_value, player_avg, book_line,
                    dva_diff, dvp_diff, game_env, conf['confidence'],
                    conf['hit_rate'], conf['cv'], proj_factors,
                    recommendation=recommendation,
                    line_drift=line_drift_val,
                    last_hour_drift=last_hour_drift_val,
                    move_pattern=move_pattern_val,
                    snapshot_count=line_snapshots_val or 0,
                    is_b2b=team_is_b2b_val,
                )

                physical_edge = None
                if stat_key in ('reb', 'blk'):
                    pe, _ = _physical_mismatch_score(player_name, opponent, position, data_cache)
                    physical_edge = round(pe, 2) if abs(pe) > 0.05 else None

                prop_entry = {
                    'player': player_name,
                    'team': team,
                    'opponent': opponent,
                    'salary': player['salary'],
                    'value': round(player['value'], 2),
                    'stat': label,
                    'player_avg': round(player_avg, 1),
                    'projected_value': projected_value,
                    'adjusted_avg': round(player_avg + blended_diff, 1),
                    'extra_fp': round(blended_diff * fp_mult, 1),
                    'edge_pct': round((blended_diff / league_avg * 100) if league_avg > 0 else 0, 1),
                    'recommendation': recommendation,
                    'book_line': book_line,
                    'book_over': book_over,
                    'book_under': book_under,
                    'vs_book_edge': edge,
                    'archetype': dva_source if dva_source else '',
                    'dva_edge': round(dva_diff, 2) if dva_source else None,
                    'dvp_edge': round(dvp_diff, 2),
                    'blend': blend_label if dva_source else 'DVP only',
                    'hit_rate': conf['hit_rate'],
                    'cv': conf['cv'],
                    'last5_avg': conf['last5_avg'],
                    'confidence': conf['confidence'],
                    'confidence_reasons': '; '.join(conf['confidence_reasons']) if conf['confidence_reasons'] else '',
                    'gate_fail_count': conf.get('gate_fail_count', 0),
                    'composite_score': composite,
                    'pace_factor': round(game_env['pace_factor'], 3),
                    'total_factor': round(game_env['total_factor'], 3),
                    'physical_edge': physical_edge,
                    'usage_boost': round(game_env['teammate_out_usage'], 1) if game_env.get('teammate_out_usage', 0) > 0 else None,
                    'opportunity_index': round(game_env.get('opportunity_index', 1.0), 2) if game_env.get('opportunity_index', 1.0) > 1.0 else None,
                    'cascade_tier': game_env.get('cascade_tier', 0) if game_env.get('cascade_tier', 0) > 0 else None,
                    'opportunity_spike': opp_spike if opp_spike else None,
                    'out_player_details': game_env.get('out_player_details'),
                    'total_vacated_usage': game_env.get('total_vacated_usage', 0),
                    'total_vacated_minutes': game_env.get('total_vacated_minutes', 0),
                    'projected_min': round(game_env['projected_min'], 1),
                    'implied_team_total': round(game_env.get('implied_team_total', 0), 1) if game_env.get('implied_team_total') else None,
                    'blowout_cap': round(game_env['blowout_minutes_cap'], 3) if game_env['blowout_minutes_cap'] < 1.0 else None,
                    'projection_factors': '; '.join(proj_factors) if proj_factors else '',
                    'opening_line': opening_line_val,
                    'current_line': current_line_val,
                    'line_drift': line_drift_val,
                    'line_drift_pct': line_drift_pct_val,
                    'line_snapshots': line_snapshots_val if line_snapshots_val and line_snapshots_val > 1 else None,
                    'last_hour_drift': last_hour_drift_val,
                    'last_hour_from': last_hour_from_val,
                    'last_hour_minutes': last_hour_minutes_val,
                    'move_pattern': move_pattern_val,
                    'largest_swing': largest_swing_val,
                    'largest_swing_share': largest_swing_share_val,
                }
                props.append(prop_entry)
                if conf.get('gate_failures'):
                    gate_failure_log.append(conf['gate_failures'])

    if role_change_players:
        print(f"  Role Change Detection: {len(role_change_players)} players with regime shifts (80/20 recency weighting applied)")
        for pname, pdir, pold, pnew in role_change_players:
            arrow = '↑' if pdir == 'up' else '↓'
            print(f"    {arrow} {pname}: {pold:.0f} → {pnew:.0f} min/game")

    if has_dva:
        print(f"  DVA Integration: {dva_hits} prop edges enhanced with archetype matchups ({blend_label}, season {season_pct*100:.0f}%)")

    injury_impact_teams = {}
    for p in props:
        ct = p.get('cascade_tier')
        if ct and ct > 0:
            team_key = p['team']
            if team_key not in injury_impact_teams:
                injury_impact_teams[team_key] = {
                    'cascade_tier': ct,
                    'players': [],
                    'out_details': None,
                    'total_vacated_usage': 0,
                    'total_vacated_minutes': 0,
                }
            opp_i = p.get('opportunity_index')
            if opp_i:
                injury_impact_teams[team_key]['players'].append({
                    'name': p['player'],
                    'opp_index': opp_i,
                    'spike': p.get('opportunity_spike', False),
                    'absorbed_usage': p.get('usage_boost', 0),
                })
    for p in props:
        ct = p.get('cascade_tier')
        if ct and ct > 0:
            team_key = p['team']
            if team_key in injury_impact_teams:
                out_dets = p.get('out_player_details')
                if out_dets and not injury_impact_teams[team_key]['out_details']:
                    injury_impact_teams[team_key]['out_details'] = out_dets
                    injury_impact_teams[team_key]['total_vacated_usage'] = p.get('total_vacated_usage', 0)
                    injury_impact_teams[team_key]['total_vacated_minutes'] = p.get('total_vacated_minutes', 0)

    if injury_impact_teams:
        print(f"  Injury Impact Redistribution v2:")
        for team_k, info in injury_impact_teams.items():
            vac_usg = info.get('total_vacated_usage', 0)
            vac_min = info.get('total_vacated_minutes', 0)
            print(f"    {team_k} (Cascade Tier {info['cascade_tier']}, vacated {vac_usg:.1f} usg / {vac_min:.1f} min):")
            out_dets = info.get('out_details')
            if out_dets:
                print(f"      OUT players:")
                for od in out_dets:
                    arch_name = od.get('archetype', 'Unknown')
                    print(f"        - {od['name']}: {od['usg']:.1f}% usg, {od['mpg']:.1f} mpg, arch={arch_name}")
            print(f"      Top absorbed recipients:")
            seen_p = set()
            top_3 = sorted(info['players'], key=lambda x: -x['opp_index'])[:3]
            for rank, pi in enumerate(top_3, 1):
                if pi['name'] not in seen_p:
                    seen_p.add(pi['name'])
                    spike_flag = " ★ OPPORTUNITY SPIKE" if pi.get('spike') else ""
                    absorbed = pi.get('absorbed_usage', 0)
                    absorbed_str = f", absorbed {absorbed:.1f} usg" if absorbed else ""
                    print(f"        #{rank} {pi['name']}: Opp Index {pi['opp_index']:.2f}x{absorbed_str}{spike_flag}")

    props_df = pd.DataFrame(props)
    if len(props_df) > 0:
        props_df = props_df.sort_values('composite_score', ascending=False)

        high_count = (props_df['confidence'] == 'HIGH').sum()
        low_count = (props_df['confidence'] == 'LOW').sum()
        top_score = props_df['composite_score'].max()
        avg_score = props_df['composite_score'].mean()
        print(f"  Prop Confidence Filter: {high_count} HIGH / {low_count} LOW confidence plays")
        print(f"  Composite Scores: top={top_score:.1f}, avg={avg_score:.1f}, {len(props_df)} total plays")

        gate_counts = {}
        for gf in gate_failure_log:
            for gate_name in gf:
                gate_counts[gate_name] = gate_counts.get(gate_name, 0) + 1
        if gate_counts:
            print(f"  Confidence Gate Failures (picks filtered to LOW):")
            for gate, cnt in sorted(gate_counts.items(), key=lambda x: -x[1]):
                gate_labels = {
                    'hit_rate': f'Hit rate < 58%',
                    'cv': f'CV > 0.30',
                    'last5_vs_line': f'Last-5 avg wrong side of line',
                    'matchup_both_oppose': f'DVA + DVP both oppose pick',
                    'matchup_no_support': f'No matchup support',
                    'matchup_weak': f'Weak matchup (need ≥0.5 directional support)',
                    'matchup_neutral': f'Neutral matchup (DVA/DVP both < 0.5)',
                    'low_team_total': f'Low implied team total (< 105)',
                    'high_team_total': f'High implied team total (> 115)',
                    'usage_contradiction': f'UNDER with large usage boost',
                    'trend_down': f'Trending down (last-5 < 80% of avg)',
                    'trend_up': f'Trending up (last-5 > 120% of avg) — bad for UNDER',
                    'no_data': f'No game log data',
                    'small_sample': f'Small sample size',
                }
                print(f"    {gate_labels.get(gate, gate)}: {cnt} picks")

    return props_df

def get_stat_matchups(dvp_df, players_df, stats_df):
    """Find best stat matchups: which teams give up most of each stat by position."""
    
    stat_cols = ['pts', 'reb', 'ast', 'stl', 'blk', 'three_pm']
    stat_names = {'pts': 'Points', 'reb': 'Rebounds', 'ast': 'Assists', 
                  'stl': 'Steals', 'blk': 'Blocks', 'three_pm': '3-Pointers'}
    
    matchups = []
    
    for position in ['PG', 'SG', 'SF', 'PF', 'C']:
        pos_dvp = dvp_df[dvp_df['position'] == position].copy()
        if len(pos_dvp) == 0:
            continue
            
        for stat in stat_cols:
            if stat not in pos_dvp.columns:
                continue
            top_givers = pos_dvp.nlargest(5, stat)
            
            for _, row in top_givers.iterrows():
                matchups.append({
                    'position': position,
                    'stat': stat_names.get(stat, stat),
                    'stat_key': stat,
                    'opponent': row['team'],
                    'allowed': round(row[stat], 1),
                    'dvp_score': round(row.get('dvp_score', 50), 1)
                })
    
    matchups_df = pd.DataFrame(matchups)
    return matchups_df

def get_targeted_plays(players_df, stats_df, dvp_df):
    """Link high-usage players to favorable stat matchups."""
    
    players_norm = players_df.copy()
    stats_norm = stats_df.copy()
    players_norm['team_norm'] = players_norm['team'].apply(normalize_team)
    stats_norm['team_norm'] = stats_norm['team'].apply(normalize_team)
    
    merged = players_norm.merge(
        stats_norm,
        left_on=['player_name', 'team_norm'],
        right_on=['player_name', 'team_norm'],
        how='left',
        suffixes=('', '_stats')
    )
    
    stat_mapping = {
        'pts': ('pts_pg', 'Points', 1.0, 10.0),
        'reb': ('reb_pg', 'Rebounds', 1.2, 4.0),
        'ast': ('ast_pg', 'Assists', 1.5, 3.0),
        'stl': ('stl_pg', 'Steals', 3.0, 1.0),
        'blk': ('blk_pg', 'Blocks', 3.0, 0.8)
    }
    
    targeted = []
    
    for _, player in merged.iterrows():
        opponent = player.get('opponent')
        position = player.get('true_position')
        
        if pd.isna(opponent) or pd.isna(position):
            continue
        
        opp_dvp = dvp_df[(dvp_df['team'] == opponent) & (dvp_df['position'] == position)]
        if len(opp_dvp) == 0:
            continue
        opp_dvp = opp_dvp.iloc[0]
        
        for stat_key, (player_col, stat_name, fp_mult, min_avg) in stat_mapping.items():
            player_avg = player.get(player_col, 0)
            
            if pd.isna(player_avg) or player_avg < min_avg:
                continue
            
            opp_allows = opp_dvp.get(stat_key, 0)
            all_pos_dvp = dvp_df[dvp_df['position'] == position]
            if len(all_pos_dvp) == 0:
                continue
            
            league_avg = all_pos_dvp[stat_key].mean()
            extra_stat = opp_allows - league_avg
            extra_fp = extra_stat * fp_mult
            pct_above_avg = ((opp_allows - league_avg) / league_avg * 100) if league_avg > 0 else 0
            
            if extra_fp >= 0.5:
                targeted.append({
                    'player_name': player['player_name'],
                    'team': player['team'],
                    'opponent': opponent,
                    'position': position,
                    'salary': player['salary'],
                    'stat': stat_name,
                    'player_avg': round(player_avg, 1),
                    'opp_allows': round(opp_allows, 1),
                    'league_avg': round(league_avg, 1),
                    'extra_fp': round(extra_fp, 1),
                    'edge_pct': round(pct_above_avg, 1),
                    'recommendation': f"{opponent} gives up +{round(extra_fp, 1)} FP in {stat_name} to {position}s"
                })
    
    targeted_df = pd.DataFrame(targeted)
    if len(targeted_df) > 0:
        targeted_df = targeted_df.sort_values('extra_fp', ascending=False)
    
    return targeted_df

def run_analysis():
    """Run full analysis and generate all outputs."""
    print("Loading data...")
    players_df, dvp_df, per100_df, dva_df = load_data()
    
    print(f"Loaded {len(players_df)} players")
    if len(dva_df) > 0:
        print(f"Loaded {len(dva_df)} DVA matchup records ({dva_df['archetype'].nunique()} archetypes)")
    
    print("Calculating value metrics...")
    valued_df = calculate_value_metrics(players_df)
    
    valued_df.to_csv("dfs_players_valued.csv", index=False)
    print("Saved valued players to dfs_players_valued.csv")
    
    print("\n=== TOP 10 VALUE PLAYS ===")
    top_value = valued_df.nlargest(10, 'value')[['player_name', 'team', 'salary', 'proj_fp', 'value', 'salary_tier']]
    print(top_value.to_string(index=False))
    
    print("\n=== TOP 5 UPSIDE PLAYS ===")
    top_upside = valued_df.nlargest(5, 'upside_ratio')[['player_name', 'salary', 'proj_fp', 'ceiling', 'upside_ratio']]
    print(top_upside.to_string(index=False))
    
    print("\nGenerating prop recommendations (DVP + DVA blend)...")
    props_df = get_prop_recommendations(valued_df, dvp_df, per100_df, dva_df=dva_df)
    
    if len(props_df) > 0:
        pre_filter = len(props_df)
        props_df = props_df[props_df['book_line'].notna() & (props_df['book_line'] > 0)].reset_index(drop=True)
        filtered_out = pre_filter - len(props_df)
        if filtered_out > 0:
            print(f"  Filtered {filtered_out} props without book lines (Odds API coverage)")
        print("\n=== PROP RECOMMENDATIONS (DVP + DVA BLENDED) ===")
        print(props_df.head(15).to_string(index=False))
        props_df.to_csv("prop_recommendations.csv", index=False)
        print("\nSaved prop recommendations to prop_recommendations.csv")
    else:
        print("No prop recommendations available (need opponent data)")
    
    print("\nGenerating targeted stat plays...")
    targeted_df = get_targeted_plays(valued_df, per100_df, dvp_df)
    
    if len(targeted_df) > 0:
        print("\n=== TARGETED STAT PLAYS ===")
        print(targeted_df.head(15)[['player_name', 'position', 'stat', 'player_avg', 'edge_pct', 'recommendation']].to_string(index=False))
        targeted_df.to_csv("targeted_plays.csv", index=False)
        print("\nSaved targeted plays to targeted_plays.csv")
    else:
        print("No targeted plays available")
    
    print("\nGenerating charts...")
    try:
        value_chart = generate_value_chart(valued_df)
        print(f"Value chart: {value_chart}")
        
        upside_chart = generate_upside_chart(valued_df)
        print(f"Upside chart: {upside_chart}")
        
        dvp_heatmap = generate_dvp_heatmap(dvp_df)
        if dvp_heatmap:
            print(f"DVP heatmap: {dvp_heatmap}")
        
        ref_chart = generate_ref_foul_chart()
        if ref_chart:
            print(f"Referee foul chart: {ref_chart}")
    except Exception as e:
        print(f"Chart generation error: {e}")
    
    print("\nAnalysis complete!")
    return valued_df, props_df

if __name__ == "__main__":
    run_analysis()
