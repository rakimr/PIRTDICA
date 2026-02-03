import requests
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
from team_map import TEAM_MAP

conn = sqlite3.connect("dfs_nba.db")
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS dvp_stats")
cursor.execute("""
CREATE TABLE IF NOT EXISTS dvp_stats (
    position TEXT,
    team TEXT,
    pts REAL,
    fg_pct REAL,
    ft_pct REAL,
    three_pm REAL,
    reb REAL,
    ast REAL,
    stl REAL,
    blk REAL,
    tov REAL,
    dvp_score REAL,
    timeframe TEXT,
    scraped_at TEXT
)
""")

cursor.execute("DROP TABLE IF EXISTS dvp_blended")
cursor.execute("""
CREATE TABLE IF NOT EXISTS dvp_blended (
    position TEXT,
    team TEXT,
    pts REAL,
    fg_pct REAL,
    ft_pct REAL,
    three_pm REAL,
    reb REAL,
    ast REAL,
    stl REAL,
    blk REAL,
    tov REAL,
    dvp_score REAL,
    weight_30d REAL,
    weight_season REAL,
    scraped_at TEXT
)
""")
conn.commit()

TIMEFRAMES = {
    'season': 'https://hashtagbasketball.com/nba-defense-vs-position',
    '30d': 'https://hashtagbasketball.com/nba-defense-vs-position'
}

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

def calculate_dvp_score(row):
    """
    Calculate DVP score using FanDuel scoring weights.
    Higher score = weaker defense = better fantasy matchup.
    """
    score = (
        row["pts"] * 1.0 +
        row["reb"] * 1.2 +
        row["ast"] * 1.5 +
        row["stl"] * 3.0 +
        row["blk"] * 3.0 -
        row["tov"] * 1.0
    )
    return round(score, 2)

def scrape_dvp(url, timeframe):
    """Scrape DVP data from Hashtag Basketball for a given timeframe."""
    print(f"Fetching {timeframe} DvP data from HashtagBasketball...")
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code != 200:
            print(f"Error fetching {timeframe} page: {response.status_code}")
            return pd.DataFrame()
    except Exception as e:
        print(f"Error fetching {timeframe}: {e}")
        return pd.DataFrame()
    
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", id="ContentPlaceHolder1_GridView1")
    
    if not table:
        print(f"Could not find DvP table for {timeframe}.")
        return pd.DataFrame()
    
    rows = []
    trs = table.find_all("tr")
    
    for tr in trs:
        tds = tr.find_all("td")
        if len(tds) != 11:
            continue
        
        try:
            position = tds[0].get_text(strip=True)
            team_elem = tds[1].find("span")
            if not team_elem:
                continue
            team = team_elem.get_text(strip=True)
            team = TEAM_MAP.get(team, team)
            
            def get_value(td):
                span = td.find("span")
                if span:
                    text = span.get_text(strip=True)
                    num = ''.join(c for c in text if c.isdigit() or c == '.' or c == '-')
                    return float(num) if num else 0.0
                return 0.0
            
            pts = get_value(tds[2])
            fg_pct = get_value(tds[3])
            ft_pct = get_value(tds[4])
            three_pm = get_value(tds[5])
            reb = get_value(tds[6])
            ast = get_value(tds[7])
            stl = get_value(tds[8])
            blk = get_value(tds[9])
            tov = get_value(tds[10])
            
            rows.append({
                "position": position,
                "team": team,
                "pts": pts,
                "fg_pct": fg_pct,
                "ft_pct": ft_pct,
                "three_pm": three_pm,
                "reb": reb,
                "ast": ast,
                "stl": stl,
                "blk": blk,
                "tov": tov,
                "timeframe": timeframe
            })
        except Exception as e:
            continue
    
    df = pd.DataFrame(rows)
    if not df.empty:
        df["dvp_score"] = df.apply(calculate_dvp_score, axis=1)
        df["scraped_at"] = datetime.utcnow().isoformat()
    
    return df

def calculate_adaptive_weights(season_df, days30_df):
    """
    Calculate adaptive weights based on:
    1. Sample size (games in 30-day window) - estimated from data presence
    2. Volatility (delta between 30d and season)
    """
    blended_rows = []
    
    season_by_key = season_df.set_index(['position', 'team']).to_dict('index') if not season_df.empty else {}
    days30_by_key = days30_df.set_index(['position', 'team']).to_dict('index') if not days30_df.empty else {}
    
    all_keys = set(season_by_key.keys()) | set(days30_by_key.keys())
    
    stats_cols = ['pts', 'fg_pct', 'ft_pct', 'three_pm', 'reb', 'ast', 'stl', 'blk', 'tov']
    
    days_in_season = 120
    sample_size_30d = 12
    estimated_season_games = int(days_in_season / 2.5)
    sample_ratio = min(1.0, sample_size_30d / 15)
    
    for key in all_keys:
        position, team = key
        season_row = season_by_key.get(key, {})
        days30_row = days30_by_key.get(key, {})
        
        if not season_row and not days30_row:
            continue
        
        base_weight_30d = min(0.7, sample_ratio * 0.8)
        base_weight_season = 1 - base_weight_30d
        
        if season_row and days30_row:
            season_score = season_row.get('dvp_score', 0)
            days30_score = days30_row.get('dvp_score', 0)
            
            if season_score > 0:
                delta = abs(days30_score - season_score) / season_score
                
                if delta > 0.15:
                    base_weight_30d = min(0.80, base_weight_30d + delta * 0.3)
                    base_weight_season = 1 - base_weight_30d
                elif delta < 0.05:
                    base_weight_30d = max(0.50, base_weight_30d - 0.1)
                    base_weight_season = 1 - base_weight_30d
        
        blended = {
            'position': position,
            'team': team,
            'weight_30d': round(base_weight_30d, 2),
            'weight_season': round(base_weight_season, 2),
            'scraped_at': datetime.utcnow().isoformat()
        }
        
        for col in stats_cols:
            season_val = season_row.get(col, 0) if season_row else 0
            days30_val = days30_row.get(col, 0) if days30_row else 0
            
            if days30_row and season_row:
                blended[col] = round(base_weight_30d * days30_val + base_weight_season * season_val, 2)
            elif days30_row:
                blended[col] = days30_val
            else:
                blended[col] = season_val
        
        blended['dvp_score'] = calculate_dvp_score(blended)
        blended_rows.append(blended)
    
    return pd.DataFrame(blended_rows)

print("=" * 50)
print("Scraping DVP Data (Blended Approach)")
print("=" * 50)

season_df = scrape_dvp(TIMEFRAMES['season'], 'season')
days30_df = scrape_dvp(TIMEFRAMES['30d'], '30d')

if not season_df.empty and not days30_df.empty:
    season_avg = season_df['dvp_score'].mean()
    days30_avg = days30_df['dvp_score'].mean()
    diff_pct = abs(season_avg - days30_avg) / season_avg * 100 if season_avg > 0 else 0
    
    if diff_pct < 0.1:
        print("\nWARNING: 30-day and season data appear identical - URL may need verification")
    else:
        print(f"\nData validation passed: Season avg={season_avg:.1f}, 30d avg={days30_avg:.1f} ({diff_pct:.1f}% diff)")

all_data = pd.concat([season_df, days30_df], ignore_index=True)
if not all_data.empty:
    all_data.to_sql("dvp_stats", conn, if_exists="append", index=False)
    print(f"Raw DvP stats saved: {len(season_df)} season rows, {len(days30_df)} 30-day rows")

blended_df = calculate_adaptive_weights(season_df, days30_df)
if not blended_df.empty:
    blended_df.to_sql("dvp_blended", conn, if_exists="append", index=False)
    print(f"Blended DvP stats saved: {len(blended_df)} rows")

print("\n=== Blended DVP Scores by Position (Top 5 easiest matchups) ===")
if not blended_df.empty:
    for pos in blended_df["position"].unique():
        pos_df = blended_df[blended_df["position"] == pos].nlargest(5, "dvp_score")
        print(f"\n{pos}:")
        print(pos_df[["team", "dvp_score", "weight_30d"]].to_string(index=False))

print("\n=== Sample Weight Adjustments (High Volatility Teams) ===")
if not blended_df.empty:
    high_vol = blended_df[blended_df['weight_30d'] > 0.65].head(10)
    if not high_vol.empty:
        print("Teams with boosted 30-day weight (recent performance differs from season):")
        print(high_vol[['position', 'team', 'weight_30d', 'dvp_score']].to_string(index=False))
    else:
        print("No significant deviations detected - using standard 60/40 blend")

conn.close()
print("\nDVP scraping complete!")
