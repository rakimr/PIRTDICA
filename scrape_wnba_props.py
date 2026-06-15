"""Fetch live WNBA games and FanDuel player props from The Odds API.

Self-contained WNBA ingestion that writes to parallel tables in dfs_nba.db
(`wnba_games`, `wnba_props`) so it never touches the NBA pipeline data. The
web tier reads these via the *_live mirror tables created by sync_to_postgres.

Verified source: The Odds API sport key `basketball_wnba` (FanDuel + DraftKings
player props: points / rebounds / assists / threes).
"""
import os
import sys
import sqlite3
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

from utils.timezone import get_eastern_date_str, get_eastern_now

API_KEY = os.environ.get('THE_ODDS_API_KEY', '')
BASE_URL = 'https://api.the-odds-api.com/v4'
SPORT = 'basketball_wnba'
EASTERN = ZoneInfo("America/New_York")

MARKETS = ['player_points', 'player_rebounds', 'player_assists', 'player_threes']
MARKET_TO_STAT = {
    'player_points': 'PTS',
    'player_rebounds': 'REB',
    'player_assists': 'AST',
    'player_threes': '3PM',
}
PREFERRED_BOOKS = ['fanduel', 'draftkings']


def _utc_to_et_date(commence_time):
    try:
        dt = datetime.fromisoformat(commence_time.replace('Z', '+00:00'))
        return dt.astimezone(EASTERN).date().isoformat()
    except Exception:
        return get_eastern_date_str()


def _ensure_tables(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS wnba_games (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT,
        home_team TEXT,
        away_team TEXT,
        commence_time TEXT,
        game_date TEXT,
        scraped_at TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS wnba_props (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_name TEXT,
        stat TEXT,
        line REAL,
        over_odds INTEGER,
        under_odds INTEGER,
        bookmaker TEXT,
        home_team TEXT,
        away_team TEXT,
        game_date TEXT,
        scraped_at TEXT
    )
    """)


def main():
    if not API_KEY:
        print("ERROR: THE_ODDS_API_KEY not set")
        return 1

    force = '--force' in sys.argv
    conn = sqlite3.connect("dfs_nba.db")
    cur = conn.cursor()
    _ensure_tables(cur)
    conn.commit()

    today = get_eastern_date_str()
    fresh = cur.execute(
        "SELECT COUNT(*) FROM wnba_props WHERE substr(scraped_at, 1, 10) = ?", (today,)
    ).fetchone()[0]
    if fresh > 0 and not force:
        print(f"WNBA props already scraped today ({fresh} lines). Use --force to re-fetch.")
        conn.close()
        return 0

    print("Fetching WNBA events...")
    try:
        ev_resp = requests.get(
            f"{BASE_URL}/sports/{SPORT}/events",
            params={"apiKey": API_KEY}, timeout=25,
        )
        ev_resp.raise_for_status()
        events = ev_resp.json()
    except Exception as e:
        print(f"ERROR fetching WNBA events: {e}")
        conn.close()
        return 1

    print(f"Found {len(events)} WNBA events.")
    scraped_at = get_eastern_now().isoformat()

    # Full refresh each run so the slate stays current without duplicating.
    cur.execute("DELETE FROM wnba_games")
    cur.execute("DELETE FROM wnba_props")

    games_saved = 0
    props_saved = 0

    for ev in events:
        event_id = ev.get('id')
        home = ev.get('home_team', '')
        away = ev.get('away_team', '')
        commence = ev.get('commence_time', '')
        game_date = _utc_to_et_date(commence)

        cur.execute(
            "INSERT INTO wnba_games (event_id, home_team, away_team, commence_time, game_date, scraped_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (event_id, home, away, commence, game_date, scraped_at),
        )
        games_saved += 1

        try:
            od = requests.get(
                f"{BASE_URL}/sports/{SPORT}/events/{event_id}/odds",
                params={
                    "apiKey": API_KEY,
                    "regions": "us",
                    "markets": ",".join(MARKETS),
                    "bookmakers": ",".join(PREFERRED_BOOKS),
                    "oddsFormat": "american",
                },
                timeout=25,
            )
            od.raise_for_status()
            data = od.json()
        except Exception as e:
            print(f"  WARN: odds fetch failed for {away} @ {home}: {e}")
            continue

        # Pick the first preferred book that has markets for this game.
        books = {bm['key']: bm for bm in data.get('bookmakers', [])}
        chosen_key = next((b for b in PREFERRED_BOOKS if b in books), None)
        if not chosen_key:
            continue
        book = books[chosen_key]

        for mk in book.get('markets', []):
            stat = MARKET_TO_STAT.get(mk.get('key'))
            if not stat:
                continue
            # Pivot Over/Under outcomes per player into one row.
            per_player = {}
            for o in mk.get('outcomes', []):
                player = o.get('description')
                side = (o.get('name') or '').lower()
                if not player:
                    continue
                rec = per_player.setdefault(player, {'line': o.get('point')})
                if side == 'over':
                    rec['over_odds'] = o.get('price')
                    rec['line'] = o.get('point')
                elif side == 'under':
                    rec['under_odds'] = o.get('price')
                    rec['line'] = o.get('point')
            for player, rec in per_player.items():
                cur.execute(
                    "INSERT INTO wnba_props (player_name, stat, line, over_odds, under_odds, "
                    "bookmaker, home_team, away_team, game_date, scraped_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (player, stat, rec.get('line'), rec.get('over_odds'),
                     rec.get('under_odds'), chosen_key, home, away, game_date, scraped_at),
                )
                props_saved += 1

    conn.commit()
    conn.close()
    print(f"Saved {games_saved} WNBA games and {props_saved} prop lines for {today}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
