import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import re

conn = sqlite3.connect("dfs_nba.db")
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS player_headshots (
        player_name TEXT PRIMARY KEY,
        bbref_id TEXT,
        headshot_url TEXT
    )
""")
conn.commit()

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

print("Scraping Basketball Reference for player IDs...")
url = "https://www.basketball-reference.com/leagues/NBA_2026_per_game.html"
resp = requests.get(url, headers=headers)
soup = BeautifulSoup(resp.text, "html.parser")

players = []
seen = set()

links = soup.find_all("a", href=lambda x: x and "/players/" in x and x.endswith(".html"))
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
        players.append((player_name, bbref_id, headshot_url))

print(f"Found {len(players)} unique players with headshots")

cursor.executemany("""
    INSERT OR REPLACE INTO player_headshots (player_name, bbref_id, headshot_url)
    VALUES (?, ?, ?)
""", players)
conn.commit()

for name, bbref_id, url in players[:5]:
    print(f"  {name}: {bbref_id}")

conn.close()
print("Done!")
