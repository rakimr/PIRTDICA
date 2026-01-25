import requests
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
from team_map import TEAM_MAP

# ============================
# 1. CONNECT TO DATABASE
# ============================

conn = sqlite3.connect("dfs_nba.db")
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS player_salaries")
cursor.execute("""
CREATE TABLE IF NOT EXISTS player_salaries (
    player_name TEXT,
    team TEXT,
    position TEXT,
    salary INTEGER,
    status TEXT,
    game TEXT,
    scraped_at TEXT
)
""")
conn.commit()

# ============================
# 2. SCRAPE ROTOGRINDERS
# ============================

URL = "https://rotogrinders.com/lineups/nba?site=fanduel"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

print(f"Fetching FanDuel lineups from RotoGrinders...")
response = requests.get(URL, headers=headers, timeout=30)

if response.status_code != 200:
    print(f"Error fetching page: {response.status_code}")
    conn.close()
    exit(1)

html = response.text
soup = BeautifulSoup(html, "html.parser")

rows = []

game_cards = soup.find_all("div", class_="game-card")
print(f"Found {len(game_cards)} games")

for game_card in game_cards:
    teams_div = game_card.find("div", class_="game-card-teams")
    team_abbrs = []
    if teams_div:
        nameplates = teams_div.find_all("div", class_="team-nameplate")
        for np in nameplates:
            title = np.find("span", class_="team-nameplate-title")
            if title and title.get("data-abbr"):
                team_abbrs.append(title.get("data-abbr"))
    
    if len(team_abbrs) >= 2:
        game_title = f"{team_abbrs[0]} @ {team_abbrs[1]}"
        away_team = team_abbrs[0]
        home_team = team_abbrs[1]
    else:
        game_title = "Unknown"
        away_team = None
        home_team = None
    
    lineup_cards = game_card.find_all("div", class_="lineup-card")
    if not lineup_cards:
        continue
    
    for idx, lineup_card in enumerate(lineup_cards):
        current_team = away_team if idx == 0 else home_team
        players_div = lineup_card.find("div", class_="lineup-card-players")
        if not players_div:
            continue
        
        current_status = None
        for child in players_div.children:
            if not hasattr(child, 'name') or not child.name:
                continue
            
            if child.name == 'span':
                text = child.get_text(strip=True).lower()
                if 'starter' in text:
                    current_status = 'Starter'
                elif 'bench' in text:
                    current_status = 'Bench'
            
            elif child.name == 'ul':
                players = child.find_all("li", class_="lineup-card-player")
                
                for player in players:
                    nameplate = player.find("span", class_="player-nameplate")
                    if not nameplate:
                        continue
                    
                    name_elem = nameplate.find("a", class_="player-nameplate-name")
                    if not name_elem:
                        continue
                        
                    player_name = name_elem.get_text(strip=True)
                    position = nameplate.get("data-position")
                    
                    salary = nameplate.get("data-salary")
                    if salary:
                        try:
                            salary = int(salary)
                        except:
                            salary = None
                    
                    team = TEAM_MAP.get(current_team, current_team) if current_team else None
                    
                    rows.append({
                        "player_name": player_name,
                        "team": team,
                        "position": position,
                        "salary": salary,
                        "status": current_status,
                        "game": game_title,
                        "scraped_at": datetime.utcnow().isoformat()
                    })

# ============================
# 3. SAVE TO DATABASE
# ============================

df = pd.DataFrame(rows)

if not df.empty:
    df = df.drop_duplicates(subset=["player_name"], keep="first")
    df.to_sql("player_salaries", conn, if_exists="append", index=False)
    print(f"FanDuel salaries scraped successfully. {len(df)} players found.")
    print(df.head(10))
else:
    print("No player salary data found.")

conn.close()