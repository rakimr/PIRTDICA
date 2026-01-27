import requests
from bs4 import BeautifulSoup
import sqlite3
import re
from datetime import datetime, date

conn = sqlite3.connect("dfs_nba.db")
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS injury_alerts")
cursor.execute("""
CREATE TABLE IF NOT EXISTS injury_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_name TEXT,
    status TEXT,
    reason TEXT,
    alert_title TEXT,
    scraped_at TEXT,
    UNIQUE(player_name, alert_title)
)
""")
conn.commit()

today = date.today().strftime("%Y-%m-%d")

URL = "https://rotogrinders.com/news/nba"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

print(f"Fetching injury alerts from RotoGrinders for {today}...")
response = requests.get(URL, headers=headers, timeout=30)

if response.status_code != 200:
    print(f"Error fetching page: {response.status_code}")
    conn.close()
    exit(1)

soup = BeautifulSoup(response.text, "html.parser")

alert_titles = soup.find_all("h3", class_="alert-title")
print(f"Found {len(alert_titles)} alerts")

out_players = []
new_alerts = 0

for alert in alert_titles:
    title = alert.get_text(strip=True)
    title_lower = title.lower()
    
    if " out " in title_lower or title_lower.endswith(" out") or title_lower.endswith(" out."):
        match = re.match(r'^([A-Za-z\'\-\. ]+?)\s*\(', title)
        if match:
            player_name = match.group(1).strip()
        else:
            parts = title.split()
            player_name = " ".join(parts[:2]) if len(parts) >= 2 else parts[0]
        
        reason_match = re.search(r'\(([^)]+)\)', title)
        reason = reason_match.group(1) if reason_match else "unknown"
        
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO injury_alerts (player_name, status, reason, alert_title, scraped_at)
                VALUES (?, ?, ?, ?, ?)
            """, (player_name, "OUT", reason, title, datetime.utcnow().isoformat()))
            
            if cursor.rowcount > 0:
                new_alerts += 1
                print(f"  NEW: {player_name} - OUT ({reason})")
            
            out_players.append({
                "player_name": player_name,
                "status": "OUT",
                "reason": reason,
                "title": title
            })
        except Exception as e:
            print(f"Error inserting alert: {e}")

conn.commit()

print(f"\nInjury alerts scraped. {len(out_players)} players marked OUT, {new_alerts} new alerts.")

if out_players:
    print("\n=== Players OUT Today ===")
    for p in out_players:
        print(f"  {p['player_name']}: {p['reason']}")

conn.close()
