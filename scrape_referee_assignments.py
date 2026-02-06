import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3
import re
from team_map import to_abbrev
from utils.timezone import get_eastern_date_str, get_eastern_now

# ============================
# 1. CONNECT TO DATABASE
# ============================

conn = sqlite3.connect("dfs_nba.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS referee_assignments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game TEXT,
    home_team TEXT,
    away_team TEXT,
    crew_chief TEXT,
    referee TEXT,
    umpire TEXT,
    alternate TEXT,
    game_date TEXT,
    scraped_at TEXT
)
""")

conn.commit()

# ============================
# 2. SCRAPE TODAY'S ASSIGNMENTS
# ============================

url = "https://official.nba.com/referee-assignments/"
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
html = requests.get(url, headers=headers, timeout=30).text
soup = BeautifulSoup(html, "html.parser")

table = soup.find("table", class_="table")
tbody = table.find("tbody")

rows = []

# Clean names like "Scott Foster (#48)" → "Scott Foster"
def clean_name(text):
    return re.sub(r"\(#\d+\)", "", text).strip()

def extract(td):
    a = td.find("a")
    if not a:
        return None
    name = a.text.strip()
    return clean_name(name)

for tr in tbody.find_all("tr"):
    tds = tr.find_all("td")
    if len(tds) < 4:
        continue

    game = tds[0].text.strip()

    # Split "Houston @ Detroit"
    if "@" in game:
        away_team, home_team = [x.strip() for x in game.split("@")]
    else:
        away_team = home_team = None

    crew_chief = extract(tds[1])
    referee = extract(tds[2])
    umpire = extract(tds[3])
    alternate = extract(tds[4]) if len(tds) > 4 else None

    rows.append([
        game, home_team, away_team,
        crew_chief, referee, umpire, alternate
    ])

# ============================
# 3. BUILD DATAFRAME
# ============================

df = pd.DataFrame(rows, columns=[
    "game", "home_team", "away_team",
    "crew_chief", "referee", "umpire", "alternate"
])

df["home_team"] = df["home_team"].apply(to_abbrev)
df["away_team"] = df["away_team"].apply(to_abbrev)

today = get_eastern_date_str()
df["game_date"] = today
df["scraped_at"] = get_eastern_now().isoformat()

# ============================
# 4. WRITE TO DATABASE
# ============================

cursor.execute("DELETE FROM referee_assignments WHERE game_date = ?", (today,))
conn.commit()

df.to_sql("referee_assignments", conn, if_exists="append", index=False)
conn.close()

print("Today's referee assignments scraped successfully.")
print(df.head())