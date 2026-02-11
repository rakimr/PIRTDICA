"""
Player Value Analysis and Prop Insights
Calculates value metrics and identifies high-value prop opportunities using DVP data.
"""
import os
import pandas as pd
import sqlite3
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

STAT_CATEGORIES = ['pts', 'reb', 'ast', 'stl', 'blk', '3pm']

def load_data():
    """Load player projections and DVP data."""
    players_df = pd.read_csv("dfs_players.csv")
    
    conn = sqlite3.connect("dfs_nba.db")
    dvp_df = pd.read_sql_query("SELECT * FROM dvp_blended", conn)
    stats_df = pd.read_sql_query("""
        SELECT player_name, team, pts_pg, reb_pg, ast_pg, stl_pg, blk_pg 
        FROM player_stats
    """, conn)
    conn.close()
    
    return players_df, dvp_df, stats_df

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
    
    from datetime import datetime
    from zoneinfo import ZoneInfo
    today = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    
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
        from datetime import datetime
        from zoneinfo import ZoneInfo
        today = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
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

def _american_to_implied_prob(odds):
    """Convert American odds to implied probability."""
    if odds is None or pd.isna(odds):
        return None
    odds = float(odds)
    if odds > 0:
        return 100.0 / (odds + 100.0)
    else:
        return abs(odds) / (abs(odds) + 100.0)

def get_prop_recommendations(players_df, dvp_df, per100_df, min_value=4.0, top_n=50):
    """Generate prop bet recommendations based on player averages + DVP matchups + book lines."""
    import unicodedata, re

    valued_df = calculate_value_metrics(players_df)
    high_value = valued_df[valued_df['value'] >= min_value].nlargest(top_n, 'value')
    
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

    book_props = _load_book_props()
    has_book = len(book_props) > 0
    
    props = []
    
    for _, player in high_value.iterrows():
        player_name = player['player_name']
        opponent = player.get('opponent')
        position = player.get('true_position')
        team = player['team']
        
        if pd.isna(opponent) or pd.isna(position):
            continue
        
        team_norm = normalize_team(team)
        player_stats = stats_norm[(stats_norm['player_name'] == player_name) & (stats_norm['team_norm'] == team_norm)]
        if len(player_stats) == 0:
            continue
        player_stats = player_stats.iloc[0]
        
        opp_dvp = dvp_df[(dvp_df['team'] == opponent) & (dvp_df['position'] == position)]
        if len(opp_dvp) == 0:
            continue
        opp_dvp = opp_dvp.iloc[0]

        player_norm = _normalize_prop_name(player_name)
        
        for stat_key, (col, label, fp_mult, min_under, min_over) in stat_config.items():
            player_avg = player_stats.get(col, 0)
            if pd.isna(player_avg):
                continue
            
            opp_allows = opp_dvp.get(stat_key, 0)
            all_pos_dvp = dvp_df[dvp_df['position'] == position]
            if len(all_pos_dvp) == 0:
                continue
            
            league_avg = all_pos_dvp[stat_key].mean()
            diff_stat = opp_allows - league_avg
            diff_fp = diff_stat * fp_mult

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
                adjusted_avg = player_avg + diff_stat
                if book_line and book_line > 0:
                    edge = round(((adjusted_avg - book_line) / book_line) * 100, 1)

            should_include = False
            if diff_stat > 0 and player_avg >= min_over and diff_fp >= 0.3:
                recommendation = 'OVER'
                should_include = True
            elif diff_stat < 0 and player_avg <= min_under and diff_fp <= -0.3:
                recommendation = 'UNDER'
                should_include = True
            elif book and book_line:
                adjusted_avg = player_avg + diff_stat
                gap = adjusted_avg - book_line
                if abs(gap) >= 1.5:
                    recommendation = 'OVER' if gap > 0 else 'UNDER'
                    should_include = True

            if should_include:
                props.append({
                    'player': player_name,
                    'team': team,
                    'opponent': opponent,
                    'salary': player['salary'],
                    'value': round(player['value'], 2),
                    'stat': label,
                    'player_avg': round(player_avg, 1),
                    'adjusted_avg': round(player_avg + diff_stat, 1),
                    'extra_fp': round(diff_fp, 1),
                    'edge_pct': round((diff_stat / league_avg * 100) if league_avg > 0 else 0, 1),
                    'recommendation': recommendation,
                    'book_line': book_line,
                    'book_over': book_over,
                    'book_under': book_under,
                    'vs_book_edge': edge,
                })
    
    props_df = pd.DataFrame(props)
    if len(props_df) > 0:
        if has_book and 'vs_book_edge' in props_df.columns:
            props_df['_sort_key'] = props_df['vs_book_edge'].apply(lambda x: abs(x) if x is not None and pd.notna(x) else 0)
            props_df = props_df.sort_values('_sort_key', ascending=False).drop(columns=['_sort_key'])
        else:
            props_df = props_df.sort_values('extra_fp', key=abs, ascending=False)
    
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
    players_df, dvp_df, per100_df = load_data()
    
    print(f"Loaded {len(players_df)} players")
    
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
    
    print("\nGenerating prop recommendations...")
    props_df = get_prop_recommendations(valued_df, dvp_df, per100_df)
    
    if len(props_df) > 0:
        print("\n=== PROP RECOMMENDATIONS (DVP EDGE) ===")
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
