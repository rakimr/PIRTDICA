import requests
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
import re
import time
import random
from datetime import datetime
from team_map import TEAM_MAP
from baseline_minutes import get_baseline_minutes

# ============================
# 1. CONNECT TO DATABASE
# ============================

conn = sqlite3.connect("dfs_nba.db")
cursor = conn.cursor()

# Ensure table exists but DO NOT drop it yet // we only clear rows after a
# successful fetch+parse so a transient ESPN failure (e.g. 202 bot-throttle)
# leaves the previous depth chart in place as a fallback.
cursor.execute("""
CREATE TABLE IF NOT EXISTS depth_charts (
    team TEXT,
    position_slot TEXT,
    player_name TEXT,
    baseline_min REAL,
    injury_indicator TEXT,
    scraped_at TEXT
)
""")
conn.commit()

# ============================
# 2. SCRAPE ESPN DEPTH CHARTS (with retries + browser-like headers)
# ============================

URL = "https://www.espn.com/nba/depth/_/type/full"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

def fetch_espn_depth(max_attempts: int = 4):
    last_status = None
    for attempt in range(1, max_attempts + 1):
        ua = random.choice(USER_AGENTS)
        headers = {
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": "https://www.espn.com/nba/",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Upgrade-Insecure-Requests": "1",
        }
        try:
            resp = requests.get(URL, headers=headers, timeout=30)
            last_status = resp.status_code
            if resp.status_code == 200 and "tablehead" in resp.text:
                return resp
            print(f"  attempt {attempt}/{max_attempts}: status={resp.status_code} len={len(resp.text)}")
        except Exception as e:
            print(f"  attempt {attempt}/{max_attempts}: error={e}")
        # Exponential backoff with jitter
        time.sleep(min(2 ** attempt, 8) + random.uniform(0, 1.5))
    print(f"All {max_attempts} attempts failed (last status={last_status})")
    return None

print("Fetching ESPN depth charts...")
response = fetch_espn_depth()

if response is None:
    # Preserve existing depth_charts rows so downstream rotation_minutes
    # can still build off the most recent successful scrape.
    cursor.execute("SELECT COUNT(*) FROM depth_charts")
    existing = cursor.fetchone()[0]
    print(f"Depth chart fetch failed // preserving {existing} existing rows as fallback")
    conn.close()
    exit(0 if existing > 0 else 1)

soup = BeautifulSoup(response.text, "html.parser")

rows = []

team_tables = soup.find_all("table", class_="tablehead")

if len(team_tables) > 1:
    team_tables = team_tables[1:]

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

        injury_indicator = None
        marker_match = re.search(r'\(([A-Z]+)\)', text)
        if marker_match:
            marker = marker_match.group(1)
            if marker in ('IL', 'INJ', 'OUT', 'O', 'DNP', 'SSPD', 'SUS'):
                injury_indicator = 'OUT'
            elif marker in ('D', 'DTD'):
                injury_indicator = 'DOUBTFUL'
            elif marker in ('Q', 'GTD'):
                injury_indicator = 'QUESTIONABLE'

        link = td.find("a")
        if link:
            href = link.get("href", "")
            try:
                slug = href.rstrip("/").split("/")[-1]
                full_name = slug.replace("-", " ").title()
            except:
                full_name = link.get_text(strip=True)
        else:
            full_name = text.split(" - ")[-1].strip()
            full_name = re.sub(r'\s*\([A-Z]+\)\s*', '', full_name).strip()

        rows.append({
            "team": team_abbr,
            "position_slot": position_slot,
            "player_name": full_name,
            "baseline_min": get_baseline_minutes(position_slot),
            "injury_indicator": injury_indicator,
            "scraped_at": datetime.utcnow().isoformat()
        })

# ============================
# 3. SAVE TO DATABASE
# ============================

df = pd.DataFrame(rows)

if not df.empty:
    # Atomic replace: clear old rows then insert fresh ones in one transaction.
    cursor.execute("DELETE FROM depth_charts")
    df.to_sql("depth_charts", conn, if_exists="append", index=False)
    conn.commit()
    print(f"Depth charts scraped successfully. {len(df)} rows saved.")
    print(df.head(10))

    il_players = df[df['injury_indicator'].notna()]
    if len(il_players) > 0:
        print(f"\n=== Depth Chart Injury Indicators: {len(il_players)} players ===")
        for _, p in il_players.iterrows():
            print(f"  {p['player_name']:25s} ({p['team']}) {p['position_slot']:5s} -> {p['injury_indicator']}")
else:
    # Fetch succeeded but parse yielded zero rows (HTML format change?).
    # Preserve existing rows rather than wipe them.
    cursor.execute("SELECT COUNT(*) FROM depth_charts")
    existing = cursor.fetchone()[0]
    print(f"No depth chart rows parsed from response // preserving {existing} existing rows")

conn.close()