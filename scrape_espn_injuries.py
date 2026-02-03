"""
Scrape injury data from ESPN as a backup to RotoGrinders.

ESPN shows injury status more reliably and in real-time.
This catches players that RotoGrinders might miss or delay reporting.
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import re

TEAM_URLS = [
    ("ATL", "atlanta-hawks"),
    ("BOS", "boston-celtics"),
    ("BKN", "brooklyn-nets"),
    ("CHA", "charlotte-hornets"),
    ("CHI", "chicago-bulls"),
    ("CLE", "cleveland-cavaliers"),
    ("DAL", "dallas-mavericks"),
    ("DEN", "denver-nuggets"),
    ("DET", "detroit-pistons"),
    ("GS", "golden-state-warriors"),
    ("HOU", "houston-rockets"),
    ("IND", "indiana-pacers"),
    ("LAC", "la-clippers"),
    ("LAL", "los-angeles-lakers"),
    ("MEM", "memphis-grizzlies"),
    ("MIA", "miami-heat"),
    ("MIL", "milwaukee-bucks"),
    ("MIN", "minnesota-timberwolves"),
    ("NOP", "new-orleans-pelicans"),
    ("NY", "new-york-knicks"),
    ("OKC", "oklahoma-city-thunder"),
    ("ORL", "orlando-magic"),
    ("PHI", "philadelphia-76ers"),
    ("PHO", "phoenix-suns"),
    ("POR", "portland-trail-blazers"),
    ("SAC", "sacramento-kings"),
    ("SAS", "san-antonio-spurs"),
    ("TOR", "toronto-raptors"),
    ("UTA", "utah-jazz"),
    ("WAS", "washington-wizards"),
]

def scrape_espn_injuries():
    """Scrape injury report from ESPN."""
    
    url = "https://www.espn.com/nba/injuries"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    injuries = []
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        tables = soup.find_all('table')
        
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 3:
                    name_cell = cells[0]
                    status_cell = cells[1] if len(cells) > 1 else None
                    
                    player_link = name_cell.find('a')
                    if player_link:
                        player_name = player_link.text.strip()
                        
                        status_text = status_cell.text.strip() if status_cell else ""
                        
                        if status_text.upper() in ['OUT', 'O', 'DOUBTFUL', 'D']:
                            injuries.append({
                                'player_name': player_name,
                                'status': 'OUT' if status_text.upper() in ['OUT', 'O'] else 'DOUBTFUL',
                                'source': 'ESPN'
                            })
    except Exception as e:
        print(f"ESPN main injuries page failed: {e}")
    
    for team_abbr, team_slug in TEAM_URLS:
        try:
            team_url = f"https://www.espn.com/nba/team/injuries/_/name/{team_abbr.lower()}/{team_slug}"
            response = requests.get(team_url, headers=headers, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                injury_rows = soup.find_all('tr', class_='Table__TR')
                for row in injury_rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        name_el = row.find('a', class_='AnchorLink')
                        if name_el:
                            player_name = name_el.text.strip()
                            status_el = row.find('span', class_='TextStatus')
                            if status_el:
                                status = status_el.text.strip().upper()
                                if status in ['OUT', 'O', 'DOUBTFUL', 'D']:
                                    injuries.append({
                                        'player_name': player_name,
                                        'status': 'OUT' if status in ['OUT', 'O'] else 'DOUBTFUL',
                                        'source': 'ESPN',
                                        'team': team_abbr
                                    })
        except Exception as e:
            pass
    
    return injuries

def scrape_depth_chart_injuries():
    """Check ESPN depth chart for injury markers."""
    
    url = "https://www.espn.com/nba/depth/_/type/full"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
    }
    
    injuries = []
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        for link in soup.find_all('a'):
            text = link.get_text(strip=True)
            parent_text = link.parent.get_text(strip=True) if link.parent else ""
            
            if any(marker in parent_text.upper() for marker in ['(OUT)', '(O)', '(DNP)', '(INJ)']):
                href = link.get('href', '')
                if '/player/' in href or '/id/' in href:
                    try:
                        slug = href.rstrip('/').split('/')[-1]
                        player_name = slug.replace('-', ' ').title()
                        injuries.append({
                            'player_name': player_name,
                            'status': 'OUT',
                            'source': 'ESPN_DEPTH'
                        })
                    except:
                        pass
    except Exception as e:
        print(f"Depth chart injury scan failed: {e}")
    
    return injuries

def save_espn_injuries(injuries):
    """Save ESPN injuries to database, merging with existing RotoGrinders data."""
    
    conn = sqlite3.connect('dfs_nba.db')
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS espn_injuries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_name TEXT,
            status TEXT,
            source TEXT,
            team TEXT,
            scraped_at TEXT,
            UNIQUE(player_name)
        )
    """)
    
    cursor.execute("DELETE FROM espn_injuries")
    
    now = datetime.utcnow().isoformat()
    
    for inj in injuries:
        cursor.execute("""
            INSERT OR REPLACE INTO espn_injuries (player_name, status, source, team, scraped_at)
            VALUES (?, ?, ?, ?, ?)
        """, (
            inj['player_name'],
            inj.get('status', 'OUT'),
            inj.get('source', 'ESPN'),
            inj.get('team', ''),
            now
        ))
    
    cursor.execute("""
        INSERT OR IGNORE INTO injury_alerts (player_name, status, reason, alert_title, scraped_at)
        SELECT player_name, status, 'ESPN injury report', 'ESPN: ' || player_name || ' ' || status, scraped_at
        FROM espn_injuries
        WHERE status = 'OUT'
    """)
    
    conn.commit()
    conn.close()
    
    return len(injuries)

def main():
    print("Scraping ESPN injuries as backup source...")
    
    injuries = []
    
    espn_injuries = scrape_espn_injuries()
    injuries.extend(espn_injuries)
    print(f"  Found {len(espn_injuries)} from ESPN injury pages")
    
    depth_injuries = scrape_depth_chart_injuries()
    injuries.extend(depth_injuries)
    print(f"  Found {len(depth_injuries)} from depth chart markers")
    
    seen = set()
    unique_injuries = []
    for inj in injuries:
        name = inj['player_name'].lower()
        if name not in seen:
            seen.add(name)
            unique_injuries.append(inj)
    
    count = save_espn_injuries(unique_injuries)
    print(f"Saved {count} ESPN injuries (merged into injury_alerts)")
    
    if unique_injuries:
        print("\n=== ESPN Injuries Detected ===")
        for inj in unique_injuries[:20]:
            print(f"  {inj['player_name']}: {inj['status']} ({inj.get('source', 'ESPN')})")

if __name__ == "__main__":
    main()
