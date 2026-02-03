import requests
from bs4 import BeautifulSoup
import sqlite3
import re
from datetime import datetime, date
from utils.timezone import get_eastern_date_str

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

today = get_eastern_date_str()

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

injury_players = []
new_alerts = 0

for alert in alert_titles:
    title = alert.get_text(strip=True)
    title_lower = title.lower()
    
    status = None
    if " out " in title_lower or title_lower.endswith(" out") or title_lower.endswith(" out."):
        status = "OUT"
    elif "questionable" in title_lower or "gtd" in title_lower or "game-time" in title_lower:
        status = "QUESTIONABLE"
    elif "doubtful" in title_lower:
        status = "DOUBTFUL"
    
    if status:
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
            """, (player_name, status, reason, title, datetime.utcnow().isoformat()))
            
            if cursor.rowcount > 0:
                new_alerts += 1
                print(f"  NEW: {player_name} - {status} ({reason})")
            
            injury_players.append({
                "player_name": player_name,
                "status": status,
                "reason": reason,
                "title": title
            })
        except Exception as e:
            print(f"Error inserting alert: {e}")

conn.commit()

out_count = sum(1 for p in injury_players if p['status'] == 'OUT')
questionable_count = sum(1 for p in injury_players if p['status'] == 'QUESTIONABLE')
doubtful_count = sum(1 for p in injury_players if p['status'] == 'DOUBTFUL')

print(f"\nInjury alerts scraped: {out_count} OUT, {questionable_count} QUESTIONABLE, {doubtful_count} DOUBTFUL ({new_alerts} new)")

if injury_players:
    for status_type in ['OUT', 'DOUBTFUL', 'QUESTIONABLE']:
        status_players = [p for p in injury_players if p['status'] == status_type]
        if status_players:
            print(f"\n=== Players {status_type} Today ===")
            for p in status_players:
                print(f"  {p['player_name']}: {p['reason']}")

conn.close()
