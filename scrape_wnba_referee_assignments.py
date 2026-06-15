"""
WNBA referee assignments scraper for PIRTDICA SPORTS CO.

The WNBA shares its officiating-assignments page with the NBA:
  https://official.nba.com/referee-assignments/
That page renders TWO tables, each preceded by an <h1 class="entry-title">:
  "NBA Referee Assignments"  and  "WNBA Referee Assignments".
The existing NBA scraper grabs the first table; this one grabs the WNBA table.

Output (full-refresh per game_date): SQLite table wnba_referee_assignments
  (game, home_team, away_team, crew_chief, referee, umpire, alternate,
   game_date, scraped_at).

Team names on this page are CITY names (e.g. "Las Vegas @ Dallas"); we map them
to the WNBA abbreviations used elsewhere in the platform.
"""
import re
import sqlite3

import pandas as pd
import requests
from bs4 import BeautifulSoup

from utils.timezone import get_eastern_date_str, get_eastern_now

DB = "dfs_nba.db"
URL = "https://official.nba.com/referee-assignments/"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"}

# WNBA city -> abbreviation (matches the abbrevs used across the WNBA tables).
WNBA_CITY_ABBR = {
    "atlanta": "ATL",
    "chicago": "CHI",
    "connecticut": "CON",
    "dallas": "DAL",
    "golden state": "GS",
    "indiana": "IND",
    "los angeles": "LA",
    "las vegas": "LV",
    "minnesota": "MIN",
    "new york": "NY",
    "phoenix": "PHX",
    "portland": "POR",
    "seattle": "SEA",
    "toronto": "TOR",
    "washington": "WSH",
}


def _abbr(city):
    if not city:
        return None
    return WNBA_CITY_ABBR.get(city.strip().lower(), city.strip())


def _clean_name(text):
    return re.sub(r"\(#\d+\)", "", text or "").strip()


def _extract(td):
    a = td.find("a")
    text = a.text if a else td.get_text()
    name = _clean_name(text)
    return name if name else None


def _wnba_table(soup):
    """Return the assignment table that follows the 'WNBA' entry-title."""
    for h in soup.find_all("h1", class_="entry-title"):
        if "wnba" in h.get_text(strip=True).lower():
            return h.find_next("table", class_="table")
    return None


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS wnba_referee_assignments (
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
    )""")
    conn.commit()

    html = requests.get(URL, headers=UA, timeout=30).text
    soup = BeautifulSoup(html, "html.parser")
    table = _wnba_table(soup)

    rows = []
    if table is not None and table.find("tbody") is not None:
        for tr in table.find("tbody").find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 4:
                continue
            game = tds[0].get_text(strip=True)
            if "@" in game:
                away_city, home_city = [x.strip() for x in game.split("@")]
            else:
                away_city = home_city = None
            rows.append([
                game,
                _abbr(home_city),
                _abbr(away_city),
                _extract(tds[1]),
                _extract(tds[2]),
                _extract(tds[3]),
                _extract(tds[4]) if len(tds) > 4 else None,
            ])

    today = get_eastern_date_str()
    cur.execute("DELETE FROM wnba_referee_assignments WHERE game_date = ?", (today,))
    conn.commit()

    if rows:
        df = pd.DataFrame(rows, columns=[
            "game", "home_team", "away_team",
            "crew_chief", "referee", "umpire", "alternate"])
        df["game_date"] = today
        df["scraped_at"] = get_eastern_now().isoformat()
        df.to_sql("wnba_referee_assignments", conn, if_exists="append", index=False)
        print(f"WNBA referee assignments scraped: {len(df)} game(s) for {today}")
        print(df[["game", "crew_chief", "referee", "umpire"]].to_string(index=False))
    else:
        print(f"No WNBA referee assignments posted for {today} yet.")

    conn.close()


if __name__ == "__main__":
    main()
