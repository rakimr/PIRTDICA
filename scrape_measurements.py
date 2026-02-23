"""
Scrape physical measurements (height, weight, wingspan) for all active NBA players.
Sources:
  - Basketball Reference team roster pages: height, weight for all active players
  - Wingspan estimated from height where draft combine data unavailable
Stores in player_measurements table in SQLite.
"""

import requests
import pandas as pd
import sqlite3
import time
import re
from io import StringIO
from datetime import datetime
from bs4 import BeautifulSoup

BR_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

NBA_TEAMS = [
    'ATL', 'BOS', 'BRK', 'CHO', 'CHI', 'CLE', 'DAL', 'DEN',
    'DET', 'GSW', 'HOU', 'IND', 'LAC', 'LAL', 'MEM', 'MIA',
    'MIL', 'MIN', 'NOP', 'NYK', 'OKC', 'ORL', 'PHI', 'PHO',
    'POR', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS'
]

BR_TO_NBA_TEAM = {
    'ATL': 'ATL', 'BOS': 'BOS', 'BRK': 'BKN', 'CHO': 'CHA', 'CHI': 'CHI',
    'CLE': 'CLE', 'DAL': 'DAL', 'DEN': 'DEN', 'DET': 'DET', 'GSW': 'GSW',
    'HOU': 'HOU', 'IND': 'IND', 'LAC': 'LAC', 'LAL': 'LAL', 'MEM': 'MEM',
    'MIA': 'MIA', 'MIL': 'MIL', 'MIN': 'MIN', 'NOP': 'NOP', 'NYK': 'NYK',
    'OKC': 'OKC', 'ORL': 'ORL', 'PHI': 'PHI', 'PHO': 'PHX', 'POR': 'POR',
    'SAC': 'SAC', 'SAS': 'SAS', 'TOR': 'TOR', 'UTA': 'UTA', 'WAS': 'WAS'
}

KNOWN_WINGSPANS = {
    'Victor Wembanyama': 96.0, 'Donovan Clingan': 90.5, 'Rudy Gobert': 90.5,
    'Chet Holmgren': 88.5, 'Anthony Davis': 87.0, 'Giannis Antetokounmpo': 87.5,
    'Joel Embiid': 87.0, 'Jaren Jackson Jr.': 86.5, 'Evan Mobley': 87.0,
    'Bam Adebayo': 85.5, 'Karl-Anthony Towns': 86.5, 'Jarrett Allen': 87.0,
    'Brook Lopez': 85.5, 'Myles Turner': 87.0, 'Clint Capela': 86.5,
    'DeAndre Ayton': 87.5, 'Nikola Jokic': 83.5, 'Domantas Sabonis': 83.0,
    'Alperen Sengun': 85.0, 'Mark Williams': 87.0, 'Walker Kessler': 88.0,
    'Ivica Zubac': 86.0, 'Jonas Valanciunas': 85.5, 'Jusuf Nurkic': 85.5,
    'Mitchell Robinson': 88.0, 'Robert Williams III': 87.5,
    'Paolo Banchero': 84.5, 'Scottie Barnes': 85.5, 'Zion Williamson': 82.0,
    'LeBron James': 84.0, 'Kawhi Leonard': 84.5, 'Jimmy Butler': 82.5,
    'OG Anunoby': 84.5, 'Mikal Bridges': 83.5, 'Jalen Williams': 84.5,
    'Franz Wagner': 83.0, 'Brandon Ingram': 84.0, 'Pascal Siakam': 84.5,
    'Jabari Smith Jr.': 84.0, 'Lauri Markkanen': 83.0, 'Julius Randle': 83.5,
    'Draymond Green': 82.0, 'John Collins': 83.0,
    'Luka Doncic': 82.5, 'Shai Gilgeous-Alexander': 82.0, 'Ja Morant': 79.0,
    'James Harden': 79.5, 'Stephen Curry': 78.5, 'Jalen Brunson': 77.5,
    'Donovan Mitchell': 80.5, 'Tyrese Haliburton': 81.5, 'LaMelo Ball': 82.0,
    'Trae Young': 74.5, 'Damian Lillard': 79.5, 'Devin Booker': 80.5,
    'Jayson Tatum': 83.0, 'Jaylen Brown': 82.5, 'Anthony Edwards': 81.5,
    'Jalen Green': 79.5, 'Desmond Bane': 80.0, 'Tyler Herro': 79.0,
    'De\'Aaron Fox': 80.5, 'Derrick White': 81.5,
}


def height_to_inches(height_str):
    if pd.isna(height_str) or not height_str:
        return None
    height_str = str(height_str).strip()
    m = re.match(r"(\d+)['\-](\d+)", height_str)
    if m:
        return int(m.group(1)) * 12 + int(m.group(2))
    try:
        val = float(height_str)
        if val > 60:
            return val
    except (ValueError, TypeError):
        pass
    return None


def estimate_wingspan(height_inches, position=None):
    """Estimate wingspan from height using NBA average ratios.
    NBA average wingspan-to-height ratio is ~1.06 (106%).
    Bigs tend higher (~1.07), guards lower (~1.04)."""
    if height_inches is None:
        return None
    ratio = 1.06
    if position:
        pos = str(position).upper()
        if pos in ('C', 'PF'):
            ratio = 1.07
        elif pos in ('PG', 'SG'):
            ratio = 1.04
    return round(height_inches * ratio, 1)


def scrape_team_roster(team_abbr, season=2026):
    """Scrape roster from a single Basketball Reference team page."""
    url = f"https://www.basketball-reference.com/teams/{team_abbr}/{season}.html"
    try:
        resp = requests.get(url, headers=BR_HEADERS, timeout=30)
        if resp.status_code != 200:
            print(f"  {team_abbr}: HTTP {resp.status_code}")
            return pd.DataFrame()
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        roster_table = soup.find('table', {'id': 'roster'})
        if roster_table is None:
            print(f"  {team_abbr}: No roster table found")
            return pd.DataFrame()
        
        df = pd.read_html(StringIO(str(roster_table)))[0]
        
        if 'Player' not in df.columns:
            print(f"  {team_abbr}: No Player column. Cols: {list(df.columns)}")
            return pd.DataFrame()
        
        result = pd.DataFrame()
        result['player_name'] = df['Player'].str.replace(r'\s*\(TW\)$', '', regex=True).str.strip()
        result['team'] = BR_TO_NBA_TEAM.get(team_abbr, team_abbr)
        result['position'] = df.get('Pos', '')
        
        if 'Ht' in df.columns:
            result['height_inches'] = df['Ht'].apply(height_to_inches)
        else:
            result['height_inches'] = None
        
        if 'Wt' in df.columns:
            result['weight_lbs'] = pd.to_numeric(df['Wt'], errors='coerce')
        else:
            result['weight_lbs'] = None
        
        result = result[result['player_name'].notna() & (result['player_name'] != '')]
        return result
    
    except Exception as e:
        print(f"  {team_abbr}: Error - {e}")
        return pd.DataFrame()


def scrape_all_measurements():
    """Scrape height/weight from all 30 team roster pages, add wingspan data."""
    print("=== Scraping Player Physical Measurements ===")
    print(f"Fetching rosters from {len(NBA_TEAMS)} teams...\n")
    
    all_rosters = []
    for i, team in enumerate(NBA_TEAMS):
        roster = scrape_team_roster(team)
        if len(roster) > 0:
            all_rosters.append(roster)
            print(f"  {team}: {len(roster)} players")
        
        if i < len(NBA_TEAMS) - 1:
            time.sleep(3.5)
    
    if not all_rosters:
        print("ERROR: No roster data retrieved")
        return None
    
    measurements = pd.concat(all_rosters, ignore_index=True)
    measurements = measurements.drop_duplicates(subset=['player_name'], keep='first')
    
    measurements['wingspan_inches'] = measurements['player_name'].map(KNOWN_WINGSPANS)
    
    no_wingspan = measurements['wingspan_inches'].isna()
    measurements.loc[no_wingspan, 'wingspan_inches'] = measurements.loc[no_wingspan].apply(
        lambda row: estimate_wingspan(row['height_inches'], row.get('position')), axis=1
    )
    measurements['wingspan_source'] = measurements['player_name'].apply(
        lambda n: 'measured' if n in KNOWN_WINGSPANS else 'estimated'
    )
    
    measurements['scraped_at'] = datetime.now().isoformat()
    
    conn = sqlite3.connect('dfs_nba.db')
    measurements.to_sql('player_measurements', conn, if_exists='replace', index=False)
    conn.close()
    
    has_height = measurements['height_inches'].notna().sum()
    has_weight = measurements['weight_lbs'].notna().sum()
    has_wingspan = measurements['wingspan_inches'].notna().sum()
    measured_ws = (measurements['wingspan_source'] == 'measured').sum()
    
    print(f"\n=== Player Measurements Summary ===")
    print(f"Total players: {len(measurements)}")
    print(f"With height: {has_height}")
    print(f"With weight: {has_weight}")
    print(f"With wingspan: {has_wingspan} ({measured_ws} measured, {has_wingspan - measured_ws} estimated)")
    print(f"Saved to player_measurements table in dfs_nba.db")
    
    return measurements


if __name__ == "__main__":
    df = scrape_all_measurements()
    if df is not None:
        print("\n=== Tallest Players ===")
        tall = df[df['height_inches'].notna()].nlargest(10, 'height_inches')
        for _, row in tall.iterrows():
            h = row['height_inches']
            ft = int(h // 12)
            inches = int(h % 12)
            src = f" [{row.get('wingspan_source', '?')}]" if pd.notna(row.get('wingspan_inches')) else ""
            ws = f", wingspan: {row['wingspan_inches']:.1f}\"{src}" if pd.notna(row.get('wingspan_inches')) else ""
            wt = int(row['weight_lbs']) if pd.notna(row.get('weight_lbs')) else 0
            print(f"  {row['player_name']}: {ft}'{inches}\" ({wt} lbs{ws})")
        
        print("\n=== Heaviest Players ===")
        heavy = df[df['weight_lbs'].notna()].nlargest(10, 'weight_lbs')
        for _, row in heavy.iterrows():
            h = row['height_inches']
            ft = int(h // 12) if pd.notna(h) else 0
            inches = int(h % 12) if pd.notna(h) else 0
            print(f"  {row['player_name']}: {int(row['weight_lbs'])} lbs ({ft}'{inches}\")")
        
        clingan = df[df['player_name'].str.contains('Clingan', case=False, na=False)]
        williams = df[df['player_name'].str.contains('Mark Williams', case=False, na=False)]
        if len(clingan) > 0 and len(williams) > 0:
            c = clingan.iloc[0]
            w = williams.iloc[0]
            print(f"\n=== Clingan vs Williams Law Test ===")
            print(f"  Clingan: {c['height_inches']}in, {c['weight_lbs']}lbs, ws:{c.get('wingspan_inches', 'N/A')}")
            print(f"  Williams: {w['height_inches']}in, {w['weight_lbs']}lbs, ws:{w.get('wingspan_inches', 'N/A')}")
            if pd.notna(c['height_inches']) and pd.notna(w['height_inches']):
                size_diff = (c['height_inches'] - w['height_inches']) + 0.5*(c['weight_lbs'] - w['weight_lbs'])
                ws_c = c.get('wingspan_inches', 0) or 0
                ws_w = w.get('wingspan_inches', 0) or 0
                if ws_c and ws_w:
                    size_diff += 0.3 * (ws_c - ws_w)
                print(f"  Raw size advantage (Clingan): {size_diff:.1f}")
