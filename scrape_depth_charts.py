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

cursor.execute("DROP TABLE IF EXISTS depth_charts")
cursor.execute("""
CREATE TABLE IF NOT EXISTS depth_charts (
    team TEXT,
    position_slot TEXT,
    player_name TEXT,
    scraped_at TEXT
)
""")
conn.commit()

# ============================
# 2. SCRAPE ESPN DEPTH CHARTS
# ============================

URL = "https://www.espn.com/nba/depth/_/type/full"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

print("Fetching ESPN depth charts...")
response = requests.get(URL, headers=headers, timeout=30)

if response.status_code != 200:
    print(f"Error fetching page: {response.status_code}")
    conn.close()
    exit(1)

soup = BeautifulSoup(response.text, "html.parser")

rows = []

# ESPN uses many nested tables; each team depth chart is a <table class="tablehead">
team_tables = soup.find_all("table", class_="tablehead")

for table in team_tables:
    # Team name is in a <tr class="colhead"><td>TeamName</td></tr>
    header = table.find("tr", class_="colhead")
    if not header:
        continue

    team_name = header.text.strip()
    team_abbr = TEAM_MAP.get(team_name, team_name)

    # All depth rows follow the format:
    # PG1 - Player Name
    # SG3 - Player Name
    depth_rows = table.find_all("tr")[1:]  # skip header row

    for row in depth_rows:
        td = row.find("td")
        if not td:
            continue

        text = td.get_text(" ", strip=True)

        if " - " not in text:
            continue

        position_slot, _ = text.split(" - ", 1)

        # Extract full name from hyperlink
        link = td.find("a")
        if link:
            href = link.get("href", "")
            try:
                # Extract slug: /id/xxxxxxx/first-last
                slug = href.rstrip("/").split("/")[-1]
                full_name = slug.replace("-", " ").title()
            except:
                full_name = link.get_text(strip=True)
        else:
            # Fallback: use the text after the dash
            full_name = text.split(" - ")[-1].strip()

        rows.append({
            "team": team_abbr,
            "position_slot": position_slot,
            "player_name": full_name,
            "scraped_at": datetime.utcnow().isoformat()
        })

# ============================
# 3. SAVE TO DATABASE
# ============================

df = pd.DataFrame(rows)

if not df.empty:
    df.to_sql("depth_charts", conn, if_exists="append", index=False)
    print(f"Depth charts scraped successfully. {len(df)} rows saved.")
    print(df.head(10))
else:
    print("No depth chart data found.")

conn.close()