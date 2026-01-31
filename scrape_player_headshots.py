import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import re

conn = sqlite3.connect("dfs_nba.db")
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS player_headshots")
cursor.execute("""
    CREATE TABLE player_headshots (
        player_name TEXT PRIMARY KEY,
        headshot_url TEXT
    )
""")
conn.commit()

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

print("Fetching player list from RotoGrinders FanDuel page...")

url = "https://rotogrinders.com/lineups/nba?site=fanduel"
resp = requests.get(url, headers=headers, timeout=30)
soup = BeautifulSoup(resp.text, "html.parser")

players_found = []
seen = set()

for img in soup.find_all("img"):
    src = img.get("src", "") or img.get("data-src", "")
    
    if "headshots" in src.lower() or "player" in src.lower():
        parent = img.find_parent(class_=lambda x: x and "player" in str(x).lower())
        if parent:
            name_el = parent.find(class_=lambda x: x and "name" in str(x).lower())
            if name_el:
                player_name = name_el.get_text(strip=True)
                if player_name and player_name not in seen:
                    seen.add(player_name)
                    players_found.append((player_name, src))

if len(players_found) < 10:
    print("RotoGrinders parsing changed, trying alternative approach...")
    
    for div in soup.find_all(["div", "span", "a"], class_=lambda x: x and "player" in str(x).lower()):
        text = div.get_text(strip=True)
        img = div.find("img")
        if img and text and len(text) > 3 and len(text) < 40:
            src = img.get("src", "") or img.get("data-src", "")
            if src and text not in seen:
                seen.add(text)
                players_found.append((text, src))

print(f"Found {len(players_found)} players with embedded headshots")

if len(players_found) < 50:
    print("\nFalling back to Basketball Reference headshots...")
    
    br_url = "https://www.basketball-reference.com/leagues/NBA_2025_per_game.html"
    try:
        resp = requests.get(br_url, headers=headers, timeout=30)
        soup = BeautifulSoup(resp.text, "html.parser")
        
        links = soup.find_all("a", href=lambda x: x and "/players/" in str(x) and str(x).endswith(".html"))
        for link in links:
            href = link.get("href", "")
            player_name = link.text.strip()
            
            if not player_name or player_name == "Players" or len(player_name) < 3:
                continue
            
            if player_name in seen:
                continue
            seen.add(player_name)
            
            match = re.search(r"/players/\w/(\w+)\.html", href)
            if match:
                bbref_id = match.group(1)
                headshot_url = f"https://www.basketball-reference.com/req/202106291/images/headshots/{bbref_id}.jpg"
                players_found.append((player_name, headshot_url))
        
        print(f"Added Basketball Reference players, total now: {len(players_found)}")
    except Exception as e:
        print(f"Basketball Reference failed: {e}")

cursor.executemany("""
    INSERT OR REPLACE INTO player_headshots (player_name, headshot_url)
    VALUES (?, ?)
""", players_found)
conn.commit()

print(f"\nSaved {len(players_found)} player headshots to database")
for name, url in players_found[:5]:
    print(f"  {name}")

conn.close()
print("Done!")
