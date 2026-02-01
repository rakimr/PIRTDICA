"""
Player Value Analysis and Prop Insights
Calculates value metrics and identifies high-value prop opportunities using DVP data.
"""
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
    dvp_df = pd.read_sql_query("SELECT * FROM dvp_stats", conn)
    stats_df = pd.read_sql_query("""
        SELECT player_name, team, pts_pg, reb_pg, ast_pg, stl_pg, blk_pg 
        FROM player_stats
    """, conn)
    conn.close()
    
    return players_df, dvp_df, stats_df

def calculate_value_metrics(players_df):
    """Calculate player value metrics."""
    df = players_df.copy()
    
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
    
    players_df_norm = players_df.copy()
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

def generate_value_chart(players_df, output_path='static/images/value_chart.png'):
    """Generate a value vs salary scatter plot."""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    df = players_df.copy()
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    colors = {'Punt': '#888888', 'Mid': '#555555', 'High': '#333333', 'Star': '#000000'}
    
    for tier in ['Punt', 'Mid', 'High', 'Star']:
        tier_df = df[df['salary_tier'] == tier]
        ax.scatter(tier_df['salary'], tier_df['value'], 
                  c=colors[tier], label=tier, alpha=0.7, s=80)
    
    top_value = df.nlargest(5, 'value')
    for _, player in top_value.iterrows():
        ax.annotate(player['player_name'], 
                   (player['salary'], player['value']),
                   xytext=(5, 5), textcoords='offset points',
                   fontsize=9, fontweight='bold')
    
    ax.set_xlabel('Salary ($)', fontsize=12)
    ax.set_ylabel('Value (Proj FP per $1K)', fontsize=12)
    ax.set_title('Player Value Analysis', fontsize=14, fontweight='bold')
    ax.legend(title='Salary Tier')
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, facecolor='white')
    plt.close()
    
    return output_path

def generate_upside_chart(players_df, output_path='static/images/upside_chart.png'):
    """Generate ceiling vs floor chart showing upside potential."""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    df = players_df.copy()
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    ax.scatter(df['floor'], df['ceiling'], c='#333333', alpha=0.6, s=80)
    
    min_val = min(df['floor'].min(), 0)
    max_val = df['ceiling'].max()
    ax.plot([min_val, max_val], [min_val, max_val], 'k--', alpha=0.3, label='Equal Line')
    
    high_upside = df.nlargest(5, 'upside_ratio')
    for _, player in high_upside.iterrows():
        ax.annotate(player['player_name'],
                   (player['floor'], player['ceiling']),
                   xytext=(5, 5), textcoords='offset points',
                   fontsize=9, fontweight='bold', color='#000000')
    
    ax.set_xlabel('Floor (FP)', fontsize=12)
    ax.set_ylabel('Ceiling (FP)', fontsize=12)
    ax.set_title('Player Upside Analysis (Ceiling vs Floor)', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, facecolor='white')
    plt.close()
    
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
    
    im = ax.imshow(pivot.values, cmap='RdYlGn', aspect='auto', vmin=40, vmax=60)
    
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=45, ha='right')
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    
    for i in range(len(pivot.index)):
        for j in range(len(pivot.columns)):
            val = pivot.iloc[i, j]
            if pd.notna(val):
                ax.text(j, i, f'{val:.1f}', ha='center', va='center', 
                       fontsize=8, color='black' if 45 < val < 55 else 'white')
    
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label('DVP Score (Higher = Easier Matchup)')
    
    ax.set_title('Defense vs Position - Stat Category Heatmap', fontsize=14, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, facecolor='white')
    plt.close()
    
    return output_path

def get_prop_recommendations(players_df, dvp_df, per100_df, min_value=5.0, top_n=10):
    """Generate prop bet recommendations for high-value players."""
    
    valued_df = calculate_value_metrics(players_df)
    high_value = valued_df[valued_df['value'] >= min_value].nlargest(top_n, 'value')
    
    advantages = get_dvp_advantages(high_value, dvp_df, per100_df)
    
    props = []
    for player_data in advantages:
        for adv in player_data['advantages']:
            props.append({
                'player': player_data['player_name'],
                'team': player_data['team'],
                'opponent': player_data['opponent'],
                'salary': player_data['salary'],
                'value': player_data['value'],
                'stat': adv['stat'].upper(),
                'player_avg': round(adv['player_avg'], 1),
                'boosted_avg': round(adv['boosted_avg'], 1),
                'dvp_score': round(adv['dvp_score'], 1),
                'edge_pct': round(adv['edge_pct'], 1),
                'recommendation': 'OVER'
            })
    
    props_df = pd.DataFrame(props)
    if len(props_df) > 0:
        props_df = props_df.sort_values('edge_pct', ascending=False)
    
    return props_df

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
    
    print("\nGenerating charts...")
    try:
        value_chart = generate_value_chart(valued_df)
        print(f"Value chart: {value_chart}")
        
        upside_chart = generate_upside_chart(valued_df)
        print(f"Upside chart: {upside_chart}")
        
        dvp_heatmap = generate_dvp_heatmap(dvp_df)
        if dvp_heatmap:
            print(f"DVP heatmap: {dvp_heatmap}")
    except Exception as e:
        print(f"Chart generation error: {e}")
    
    print("\nAnalysis complete!")
    return valued_df, props_df

if __name__ == "__main__":
    run_analysis()
