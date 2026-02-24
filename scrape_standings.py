"""
Scrape NBA standings and calculate Team Incentive Score.

Standings affect decisions (minutes, rotation discipline, blowout risk), not fantasy points directly.

Incentive buckets:
- Must-win (playoff race): +1 → lower variance, predictable minutes
- Comfortable (seed locked): 0 → neutral
- Tank/development: -1 → higher variance, experimental lineups
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from utils.timezone import get_eastern_now

def scrape_standings():
    """Scrape current NBA standings from ESPN."""
    
    url = "https://www.espn.com/nba/standings"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
    except Exception as e:
        print(f"Error fetching standings: {e}")
        return None
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    standings = []
    
    tables = soup.find_all('table', class_='Table')
    
    for table in tables:
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 3:
                team_cell = row.find('span', class_='hide-mobile')
                if team_cell:
                    team_name = team_cell.text.strip()
                    
                    try:
                        wins = int(cells[0].text.strip()) if cells[0].text.strip().isdigit() else 0
                        losses = int(cells[1].text.strip()) if cells[1].text.strip().isdigit() else 0
                        
                        gb_text = cells[3].text.strip() if len(cells) > 3 else '0'
                        if gb_text == '-':
                            games_behind = 0.0
                        else:
                            games_behind = float(gb_text) if gb_text.replace('.', '').replace('-', '').isdigit() else 0.0
                        
                        standings.append({
                            'team_name': team_name,
                            'wins': wins,
                            'losses': losses,
                            'games_behind': games_behind,
                            'win_pct': wins / (wins + losses) if (wins + losses) > 0 else 0.5
                        })
                    except (ValueError, IndexError):
                        continue
    
    if not standings:
        print("Could not parse standings from ESPN, using fallback data")
        standings = get_fallback_standings()
    
    return pd.DataFrame(standings)

def get_fallback_standings():
    """Fallback standings data if scraping fails."""
    return [
        {'team_name': 'Cavaliers', 'wins': 40, 'losses': 9, 'games_behind': 0, 'win_pct': 0.816},
        {'team_name': 'Celtics', 'wins': 35, 'losses': 15, 'games_behind': 5.5, 'win_pct': 0.700},
        {'team_name': 'Knicks', 'wins': 32, 'losses': 18, 'games_behind': 8.5, 'win_pct': 0.640},
        {'team_name': 'Bucks', 'wins': 28, 'losses': 21, 'games_behind': 12, 'win_pct': 0.571},
        {'team_name': 'Pacers', 'wins': 28, 'losses': 22, 'games_behind': 12.5, 'win_pct': 0.560},
        {'team_name': 'Hawks', 'wins': 26, 'losses': 24, 'games_behind': 14.5, 'win_pct': 0.520},
        {'team_name': 'Heat', 'wins': 24, 'losses': 25, 'games_behind': 16, 'win_pct': 0.490},
        {'team_name': 'Magic', 'wins': 24, 'losses': 26, 'games_behind': 16.5, 'win_pct': 0.480},
        {'team_name': 'Pistons', 'wins': 23, 'losses': 27, 'games_behind': 17.5, 'win_pct': 0.460},
        {'team_name': '76ers', 'wins': 20, 'losses': 29, 'games_behind': 20, 'win_pct': 0.408},
        {'team_name': 'Bulls', 'wins': 19, 'losses': 31, 'games_behind': 21.5, 'win_pct': 0.380},
        {'team_name': 'Nets', 'wins': 17, 'losses': 33, 'games_behind': 23.5, 'win_pct': 0.340},
        {'team_name': 'Raptors', 'wins': 14, 'losses': 37, 'games_behind': 27, 'win_pct': 0.275},
        {'team_name': 'Hornets', 'wins': 12, 'losses': 36, 'games_behind': 27.5, 'win_pct': 0.250},
        {'team_name': 'Wizards', 'wins': 8, 'losses': 41, 'games_behind': 32, 'win_pct': 0.163},
        {'team_name': 'Thunder', 'wins': 38, 'losses': 10, 'games_behind': 0, 'win_pct': 0.792},
        {'team_name': 'Rockets', 'wins': 32, 'losses': 16, 'games_behind': 6, 'win_pct': 0.667},
        {'team_name': 'Grizzlies', 'wins': 31, 'losses': 18, 'games_behind': 7.5, 'win_pct': 0.633},
        {'team_name': 'Lakers', 'wins': 29, 'losses': 18, 'games_behind': 8.5, 'win_pct': 0.617},
        {'team_name': 'Nuggets', 'wins': 30, 'losses': 19, 'games_behind': 8.5, 'win_pct': 0.612},
        {'team_name': 'Clippers', 'wins': 27, 'losses': 22, 'games_behind': 11.5, 'win_pct': 0.551},
        {'team_name': 'Timberwolves', 'wins': 27, 'losses': 22, 'games_behind': 11.5, 'win_pct': 0.551},
        {'team_name': 'Warriors', 'wins': 24, 'losses': 23, 'games_behind': 13.5, 'win_pct': 0.511},
        {'team_name': 'Spurs', 'wins': 24, 'losses': 25, 'games_behind': 14.5, 'win_pct': 0.490},
        {'team_name': 'Kings', 'wins': 23, 'losses': 26, 'games_behind': 15.5, 'win_pct': 0.469},
        {'team_name': 'Mavericks', 'wins': 23, 'losses': 27, 'games_behind': 16, 'win_pct': 0.460},
        {'team_name': 'Suns', 'wins': 22, 'losses': 27, 'games_behind': 16.5, 'win_pct': 0.449},
        {'team_name': 'Trail Blazers', 'wins': 20, 'losses': 29, 'games_behind': 18.5, 'win_pct': 0.408},
        {'team_name': 'Pelicans', 'wins': 16, 'losses': 34, 'games_behind': 23, 'win_pct': 0.320},
        {'team_name': 'Jazz', 'wins': 13, 'losses': 36, 'games_behind': 25.5, 'win_pct': 0.265},
    ]

TEAM_ABBREV = {
    'Cavaliers': 'CLE', 'Celtics': 'BOS', 'Knicks': 'NYK', 'Bucks': 'MIL',
    'Pacers': 'IND', 'Hawks': 'ATL', 'Heat': 'MIA', 'Magic': 'ORL',
    'Pistons': 'DET', '76ers': 'PHI', 'Bulls': 'CHI', 'Nets': 'BKN',
    'Raptors': 'TOR', 'Hornets': 'CHA', 'Wizards': 'WAS',
    'Thunder': 'OKC', 'Rockets': 'HOU', 'Grizzlies': 'MEM', 'Lakers': 'LAL',
    'Nuggets': 'DEN', 'Clippers': 'LAC', 'Timberwolves': 'MIN', 'Warriors': 'GS',
    'Spurs': 'SAS', 'Kings': 'SAC', 'Mavericks': 'DAL', 'Suns': 'PHO',
    'Trail Blazers': 'POR', 'Pelicans': 'NOP', 'Jazz': 'UTA'
}

def calculate_incentive_score(row, playoff_cutoff_gb=6.0):
    """
    Calculate Team Incentive Score [-1, +1].
    
    +1 = Must-win (playoff race) → lower variance, predictable minutes
     0 = Neutral (comfortable position)
    -1 = Tank/development → higher variance, experimental lineups
    
    Formula based on:
    - Games behind playoff cutoff
    - Win percentage
    - Season phase (late season matters more)
    """
    gb = row['games_behind']
    win_pct = row['win_pct']
    
    if win_pct >= 0.600:
        if gb <= 3:
            return 0.8
        else:
            return 0.3
    
    elif win_pct >= 0.450:
        if gb <= playoff_cutoff_gb:
            return 1.0
        elif gb <= playoff_cutoff_gb + 3:
            return 0.5
        else:
            return 0.0
    
    elif win_pct >= 0.350:
        if gb <= playoff_cutoff_gb:
            return 0.6
        else:
            return -0.5
    
    else:
        return -1.0

def calculate_variance_multiplier(incentive_score):
    """
    Higher incentive → lower variance (more predictable rotations).
    Lower incentive → higher variance (experimental lineups).
    
    Returns multiplier for fp_sd.
    """
    return 1.0 - (0.15 * incentive_score)

def save_standings(df):
    """Save standings to database."""
    conn = sqlite3.connect('dfs_nba.db')
    
    conn.execute("DROP TABLE IF EXISTS team_standings")
    conn.execute("""
        CREATE TABLE team_standings (
            team TEXT PRIMARY KEY,
            team_name TEXT,
            wins INTEGER,
            losses INTEGER,
            games_behind REAL,
            win_pct REAL,
            incentive_score REAL,
            variance_multiplier REAL,
            updated_at TEXT
        )
    """)
    
    for _, row in df.iterrows():
        team_abbrev = TEAM_ABBREV.get(row['team_name'], row['team_name'][:3].upper())
        incentive = calculate_incentive_score(row)
        var_mult = calculate_variance_multiplier(incentive)
        
        conn.execute("""
            INSERT OR REPLACE INTO team_standings 
            (team, team_name, wins, losses, games_behind, win_pct, incentive_score, variance_multiplier, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            team_abbrev,
            row['team_name'],
            row['wins'],
            row['losses'],
            row['games_behind'],
            row['win_pct'],
            incentive,
            var_mult,
            get_eastern_now().isoformat()
        ))
    
    conn.commit()
    conn.close()
    print(f"Saved {len(df)} team standings with incentive scores")

def main():
    print("Scraping NBA standings...")
    df = scrape_standings()
    
    if df is not None and len(df) > 0:
        save_standings(df)
        
        print("\n=== TEAM INCENTIVE SCORES ===")
        print("(+1 = must-win, 0 = neutral, -1 = tank)")
        
        conn = sqlite3.connect('dfs_nba.db')
        result = pd.read_sql("SELECT team, wins, losses, incentive_score, variance_multiplier FROM team_standings ORDER BY incentive_score DESC", conn)
        conn.close()
        
        print(result.to_string(index=False))
    else:
        print("Failed to get standings data")

if __name__ == "__main__":
    main()
