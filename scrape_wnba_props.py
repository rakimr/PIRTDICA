"""Fetch live WNBA games and FanDuel player props from The Odds API.

Self-contained WNBA ingestion that writes to parallel tables in dfs_nba.db
(`wnba_games`, `wnba_props`) so it never touches the NBA pipeline data. The
web tier reads these via the *_live mirror tables created by sync_to_postgres.

Verified source: The Odds API sport key `basketball_wnba` (FanDuel + DraftKings
player props: points / rebounds / assists / threes / steals / blocks).
"""
import os
import sys
import sqlite3
import re
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

from utils.timezone import get_eastern_date_str, get_eastern_now
from utils.espn_fetch import espn_get_json

API_KEY = os.environ.get('THE_ODDS_API_KEY', '')
BASE_URL = 'https://api.the-odds-api.com/v4'
SPORT = 'basketball_wnba'
EASTERN = ZoneInfo("America/New_York")
ESPN_SCOREBOARD_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/scoreboard"
)
SEASON_TYPE_NAMES = {"2": "REGULAR", "3": "PLAYOFF"}

MARKETS = [
    'player_points', 'player_rebounds', 'player_assists', 'player_threes',
    'player_steals', 'player_blocks',
]
MARKET_TO_STAT = {
    'player_points': 'PTS',
    'player_rebounds': 'REB',
    'player_assists': 'AST',
    'player_threes': '3PM',
    'player_steals': 'STL',
    'player_blocks': 'BLK',
}
PREFERRED_BOOKS = ['fanduel', 'draftkings']


def _utc_to_et_date(commence_time):
    try:
        dt = datetime.fromisoformat(commence_time.replace('Z', '+00:00'))
        return dt.astimezone(EASTERN).date().isoformat()
    except Exception:
        # A guessed date can associate a game with the wrong ESPN phase.
        return ""


def _normalized_team_name(name):
    """Return a comparison form shared by Odds API and ESPN team names."""
    return re.sub(r"[^a-z0-9]", "", (name or "").casefold())


def _espn_season_type(event):
    """Translate ESPN's numeric event season type to our persisted value."""
    season = event.get("season")
    if not isinstance(season, dict):
        return "UNKNOWN"
    season_type = season.get("type") or {}
    value = season_type.get("id") if isinstance(season_type, dict) else season_type
    return SEASON_TYPE_NAMES.get(str(value), "UNKNOWN")


def _fetch_espn_season_types(odds_events, fetch_json=None):
    """Build (ET date, home, away) -> phase from dated ESPN scoreboards.

    Matching deliberately uses the teams and Eastern calendar date rather than
    either provider's event ID.  A missing feed, malformed event, or unknown
    ESPN phase remains UNKNOWN; calendar dates are never used to infer phase.
    """
    dates = {_utc_to_et_date(ev.get("commence_time", "")) for ev in odds_events}
    dates.discard("")
    fetch_json = fetch_json or espn_get_json
    result = {}
    for game_date in dates:
        try:
            data = fetch_json(
                f"{ESPN_SCOREBOARD_URL}?dates={game_date.replace('-', '')}"
            )
        except Exception:
            data = None
        if not isinstance(data, dict):
            continue
        for event in data.get("events", []):
            if not isinstance(event, dict):
                continue
            event_date = _utc_to_et_date(event.get("date", ""))
            if event_date != game_date:
                continue
            competitions = event.get("competitions") or []
            if not isinstance(competitions, list) or not competitions:
                continue
            competition = competitions[0]
            if not isinstance(competition, dict):
                continue
            competitors = competition.get("competitors", [])
            if not isinstance(competitors, list):
                continue
            teams = {}
            for competitor in competitors:
                if not isinstance(competitor, dict):
                    continue
                team = competitor.get("team") or {}
                if not isinstance(team, dict):
                    continue
                name = team.get("displayName") or team.get("name") or ""
                if competitor.get("homeAway") in ("home", "away"):
                    teams[competitor["homeAway"]] = name
            if "home" not in teams or "away" not in teams:
                continue
            key = (
                game_date,
                _normalized_team_name(teams["home"]),
                _normalized_team_name(teams["away"]),
            )
            result[key] = _espn_season_type(event)
    return result


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
    # Game odds columns (spread from the HOME team's perspective, game total).
    # Added later than the base table, so patch older DBs in place.
    existing = {r[1] for r in cur.execute("PRAGMA table_info(wnba_games)")}
    for col in ("home_spread REAL", "game_total REAL"):
        if col.split()[0] not in existing:
            cur.execute(f"ALTER TABLE wnba_games ADD COLUMN {col}")
    # Idempotent schema migration for databases created before phase metadata.
    if "season_type" not in existing:
        cur.execute(
            "ALTER TABLE wnba_games ADD COLUMN season_type TEXT DEFAULT 'UNKNOWN'"
        )
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
    season_types = _fetch_espn_season_types(events)

    # Game odds (spread + total) in ONE bulk request for the whole slate.
    # Costs a single API call and gives the models the game environment
    # (tight game vs blowout risk) that player props alone cannot show.
    game_odds = {}
    try:
        go_resp = requests.get(
            f"{BASE_URL}/sports/{SPORT}/odds",
            params={
                "apiKey": API_KEY,
                "regions": "us",
                "markets": "spreads,totals",
                "bookmakers": ",".join(PREFERRED_BOOKS),
                "oddsFormat": "american",
            },
            timeout=25,
        )
        go_resp.raise_for_status()
        for g in go_resp.json():
            spread, total = None, None
            books = {bm['key']: bm for bm in g.get('bookmakers', [])}
            chosen = next((books[b] for b in PREFERRED_BOOKS if b in books), None)
            if not chosen:
                continue
            for mk in chosen.get('markets', []):
                if mk.get('key') == 'spreads':
                    for o in mk.get('outcomes', []):
                        if o.get('name') == g.get('home_team'):
                            spread = o.get('point')
                elif mk.get('key') == 'totals':
                    for o in mk.get('outcomes', []):
                        if (o.get('name') or '').lower() == 'over':
                            total = o.get('point')
            game_odds[g.get('id')] = (spread, total)
        print(f"Game odds fetched for {len(game_odds)} events (spread/total).")
    except Exception as e:
        print(f"WARN: game odds fetch failed ({e}) // continuing without spreads/totals.")

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
        season_type = season_types.get(
            (game_date, _normalized_team_name(home), _normalized_team_name(away)),
            "UNKNOWN",
        )

        spread, total = game_odds.get(event_id, (None, None))
        cur.execute(
            "INSERT INTO wnba_games (event_id, home_team, away_team, commence_time, game_date, "
            "scraped_at, home_spread, game_total, season_type) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (event_id, home, away, commence, game_date, scraped_at, spread, total,
             season_type),
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
