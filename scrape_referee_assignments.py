import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3
import re

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
html = requests.get(url).text
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

df["game_date"] = datetime.utcnow().date().isoformat()
df["scraped_at"] = datetime.utcnow().isoformat()

# ============================
# 4. WRITE TO DATABASE
# ============================

df.to_sql("referee_assignments", conn, if_exists="append", index=False)
conn.close()

print("Today's referee assignments scraped successfully.")
print(df.head())