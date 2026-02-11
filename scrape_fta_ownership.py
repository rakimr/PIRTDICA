import requests
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

def scrape_fta_ownership():
    today = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

    conn = sqlite3.connect("dfs_nba.db")
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fta_ownership (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_name TEXT,
        team TEXT,
        ownership_pct REAL,
        platform TEXT,
        game_date TEXT,
        scraped_at TEXT
    )
    """)
    conn.commit()

    url = "https://fantasyteamadvice.com/ownerships?sport=nba"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    print(f"Fetching FTA ownership data for {today}...")
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    tables = soup.find_all("table")
    headings = soup.find_all("h2")

    fanduel_table = None
    draftkings_table = None

    for h in headings:
        text = h.get_text(strip=True).lower()
        if "fanduel" in text and "nba" in text:
            for table in tables:
                if table.find_previous("h2") == h:
                    fanduel_table = table
                    break
        elif "draftkings" in text and "nba" in text:
            for table in tables:
                if table.find_previous("h2") == h:
                    draftkings_table = table
                    break

    if not fanduel_table and not draftkings_table and len(tables) >= 2:
        print("  Warning: heading-based matching failed, falling back to table order")
        draftkings_table = tables[0]
        fanduel_table = tables[1]

    now = datetime.now(ZoneInfo("America/New_York")).isoformat()
    total_saved = 0

    for platform, table in [("FanDuel", fanduel_table), ("DraftKings", draftkings_table)]:
        if table is None:
            print(f"  {platform}: table not found, skipping")
            continue

        rows = []
        tbody = table.find("tbody")
        if not tbody:
            trs = table.find_all("tr")[1:]
        else:
            trs = tbody.find_all("tr")

        for tr in trs:
            tds = tr.find_all("td")
            if len(tds) < 3:
                continue
            player = tds[0].get_text(strip=True)
            team = tds[1].get_text(strip=True)
            try:
                own_pct = float(tds[2].get_text(strip=True))
            except (ValueError, TypeError):
                continue
            rows.append((player, team, own_pct, platform, today, now))

        if rows:
            cursor.execute(
                "DELETE FROM fta_ownership WHERE game_date = ? AND platform = ?",
                (today, platform)
            )
            cursor.executemany(
                "INSERT INTO fta_ownership (player_name, team, ownership_pct, platform, game_date, scraped_at) VALUES (?, ?, ?, ?, ?, ?)",
                rows
            )
            conn.commit()
            total_saved += len(rows)
            print(f"  {platform}: {len(rows)} players saved")

    conn.close()
    print(f"Total: {total_saved} ownership entries saved for {today}")
    return total_saved


if __name__ == "__main__":
    scrape_fta_ownership()
