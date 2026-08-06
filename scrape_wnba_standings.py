"""
Scrape current WNBA standings from the ESPN WNBA API.

Parallel to scrape_standings.py (NBA), but writes to the isolated `wnba_standings`
table so the NBA pipeline is never touched. ESPN groups WNBA into Eastern and
Western conferences and provides team logos directly, so the same two-conference
home page layout renders cleanly.
"""

import sqlite3
import requests

from utils.espn_fetch import espn_get_json
from utils.timezone import get_eastern_now

ESPN_STANDINGS_URL = "https://site.api.espn.com/apis/v2/sports/basketball/wnba/standings"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def _stat(entry, type_name):
    for s in entry.get("stats", []):
        if s.get("type") == type_name:
            return s
    return {}


def scrape_wnba_standings():
    """Return a list of standings dicts from the ESPN WNBA API."""
    data = espn_get_json(ESPN_STANDINGS_URL, timeout=30)
    if not data:
        raise RuntimeError("ESPN WNBA standings fetch failed (requests + curl fallback)")

    standings = []
    for conf in data.get("children", []):
        conf_name = conf.get("name", "")
        conference = "East" if "East" in conf_name else "West"
        for entry in conf.get("standings", {}).get("entries", []):
            team = entry.get("team", {})
            wins = _stat(entry, "wins").get("value")
            losses = _stat(entry, "losses").get("value")
            if wins is None or losses is None:
                continue
            wins = int(wins)
            losses = int(losses)
            win_pct = _stat(entry, "winpercent").get("value")
            if win_pct is None:
                win_pct = wins / (wins + losses) if (wins + losses) > 0 else 0.5
            gb = _stat(entry, "gamesbehind").get("value")
            try:
                gb = float(gb) if gb is not None else 0.0
            except (TypeError, ValueError):
                gb = 0.0
            seed = _stat(entry, "playoffseed").get("value")
            try:
                seed = int(seed) if seed is not None else 0
            except (TypeError, ValueError):
                seed = 0
            logo = ""
            logos = team.get("logos") or []
            if logos:
                logo = logos[0].get("href", "")
            standings.append({
                "team": team.get("abbreviation", "")[:5].upper(),
                "team_name": team.get("name", team.get("displayName", "")),
                "conference": conference,
                "wins": wins,
                "losses": losses,
                "games_behind": round(gb, 1),
                "win_pct": round(float(win_pct), 4),
                "playoff_seed": seed,
                "logo": logo,
            })
    return standings


def save_wnba_standings(rows):
    """Full-refresh the wnba_standings table."""
    conn = sqlite3.connect("dfs_nba.db")
    conn.execute("DROP TABLE IF EXISTS wnba_standings")
    conn.execute("""
        CREATE TABLE wnba_standings (
            team TEXT PRIMARY KEY,
            team_name TEXT,
            conference TEXT,
            wins INTEGER,
            losses INTEGER,
            games_behind REAL,
            win_pct REAL,
            playoff_seed INTEGER,
            logo TEXT,
            updated_at TEXT
        )
    """)
    now = get_eastern_now().isoformat()
    for r in rows:
        conn.execute("""
            INSERT OR REPLACE INTO wnba_standings
            (team, team_name, conference, wins, losses, games_behind, win_pct, playoff_seed, logo, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            r["team"], r["team_name"], r["conference"], r["wins"], r["losses"],
            r["games_behind"], r["win_pct"], r["playoff_seed"], r["logo"], now,
        ))
    conn.commit()
    conn.close()
    print(f"Saved {len(rows)} WNBA team standings")


def main():
    print("Scraping WNBA standings from ESPN...")
    try:
        rows = scrape_wnba_standings()
    except Exception as e:
        print(f"WNBA standings scrape failed: {e}")
        return
    if not rows:
        print("No WNBA standings parsed (offseason or source change) // leaving table as-is")
        return
    save_wnba_standings(rows)
    for r in sorted(rows, key=lambda x: (-x["win_pct"])):
        print(f"  {r['conference']:4} {r['team']:4} {r['team_name']:14} {r['wins']:2}-{r['losses']:<2} ({r['win_pct']:.3f})")


if __name__ == "__main__":
    main()
