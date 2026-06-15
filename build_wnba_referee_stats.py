"""
WNBA referee foul-tendency builder for PIRTDICA SPORTS CO.

There is no free WNBA equivalent of the nbastuffer referee table the NBA uses,
and refmetrics paywalls its per-ref foul history. We therefore compute the same
numbers ourselves from ESPN (the same source as the WNBA game logs):

  ESPN summary endpoint per game gives BOTH:
    - gameInfo.officials      -> the crew that worked the game
    - boxscore team "Fouls"   -> home fouls vs away fouls

Aggregating across the season yields, per referee, the exact home/away foul
differential and fouls-per-game the NBA chart plots.

Tables (SQLite dfs_nba.db):
  wnba_game_officials  CACHE of per-game officials + fouls (keyed on game_id),
                       so daily runs only fetch newly-completed games.
  wnba_referee_stats   aggregated per-referee tendencies consumed by the chart:
                       referee, games_officiated, fouls_pg, foul_diff,
                       foul_pct_home, foul_pct_road.

Convention (matches the NBA chart): foul_diff = away(road) fouls - home fouls,
so a POSITIVE value means more fouls called on the road team (home advantage).
"""
import sqlite3
import time
from datetime import datetime

import requests

DB = "dfs_nba.db"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 Chrome/120 Safari/537.36"}

SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/scoreboard?dates={d}"
SUMMARY = "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/summary?event={e}"


def _get(url, tries=3):
    for attempt in range(tries):
        try:
            r = requests.get(url, headers=UA, timeout=20)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            if attempt == tries - 1:
                print(f"  [WARN] GET failed {url}: {e}")
        time.sleep(0.5 * (attempt + 1))
    return None


def _ensure_tables(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS wnba_game_officials (
        game_id TEXT PRIMARY KEY,
        game_date TEXT,
        home_team TEXT,
        away_team TEXT,
        home_fouls REAL,
        away_fouls REAL,
        officials TEXT,
        scraped_at TEXT
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS wnba_referee_stats (
        referee TEXT PRIMARY KEY,
        games_officiated INTEGER,
        fouls_pg REAL,
        foul_diff REAL,
        foul_pct_home REAL,
        foul_pct_road REAL,
        updated_at TEXT
    )""")


def _team_fouls(team):
    """Total personal fouls for a team.

    The boxscore exposes several foul stats (Technical Fouls, Flagrant Fouls,
    Fouls). We want exactly the "fouls" / personal-fouls line, NOT the first
    stat that merely contains the word "foul".
    """
    stats = team.get("statistics", [])
    for st in stats:
        if str(st.get("name", "")).lower() in ("fouls", "personalfouls", "totalfouls"):
            try:
                return float(st.get("displayValue"))
            except (TypeError, ValueError):
                return None
    for st in stats:
        if str(st.get("label", "")).strip().lower() == "fouls":
            try:
                return float(st.get("displayValue"))
            except (TypeError, ValueError):
                return None
    return None


def _game_dates(cur):
    """Distinct WNBA game days we already have logs for (cheap date list)."""
    rows = cur.execute(
        "SELECT DISTINCT game_date FROM wnba_player_game_logs "
        "WHERE game_date IS NOT NULL ORDER BY game_date").fetchall()
    return [r[0] for r in rows if r[0]]


def _completed_game_ids(date_str):
    """Return regular/post-season completed ESPN game ids for a YYYY-MM-DD date."""
    d = date_str.replace("-", "")
    data = _get(SCOREBOARD.format(d=d))
    if not data:
        return []
    ids = []
    for ev in data.get("events", []):
        status = ev.get("status", {}).get("type", {})
        if not status.get("completed"):
            continue
        # season type: 1 = preseason (skip), 2 = regular, 3 = postseason
        stype = ev.get("season", {}).get("type")
        if stype == 1:
            continue
        ids.append(ev["id"])
    return ids


def _fetch_game(game_id):
    """Return dict of officials + home/away fouls for a game, or None."""
    s = _get(SUMMARY.format(e=game_id))
    if not s:
        return None
    officials = [o.get("fullName") for o in s.get("gameInfo", {}).get("officials", [])
                 if o.get("fullName")]
    teams = s.get("boxscore", {}).get("teams", [])
    if len(teams) != 2 or not officials:
        return None
    home_fouls = away_fouls = None
    home_team = away_team = None
    for t in teams:
        side = t.get("homeAway")
        abbr = t.get("team", {}).get("abbreviation")
        fouls = _team_fouls(t)
        if side == "home":
            home_team, home_fouls = abbr, fouls
        elif side == "away":
            away_team, away_fouls = abbr, fouls
    if home_fouls is None or away_fouls is None:
        return None
    return {
        "home_team": home_team, "away_team": away_team,
        "home_fouls": home_fouls, "away_fouls": away_fouls,
        "officials": officials,
    }


def refresh_cache(conn):
    """Fetch officials + fouls for any completed game not already cached."""
    cur = conn.cursor()
    cached = {r[0] for r in cur.execute("SELECT game_id FROM wnba_game_officials").fetchall()}
    added = 0
    for date_str in _game_dates(cur):
        for gid in _completed_game_ids(date_str):
            if gid in cached:
                continue
            g = _fetch_game(gid)
            if not g:
                continue
            cur.execute(
                "INSERT OR REPLACE INTO wnba_game_officials "
                "(game_id, game_date, home_team, away_team, home_fouls, away_fouls, "
                "officials, scraped_at) VALUES (?,?,?,?,?,?,?,?)",
                (gid, date_str, g["home_team"], g["away_team"],
                 g["home_fouls"], g["away_fouls"], "|".join(g["officials"]),
                 datetime.utcnow().isoformat()))
            cached.add(gid)
            added += 1
            time.sleep(0.25)
    conn.commit()
    print(f"  cached {added} new game(s); {len(cached)} total")


def aggregate(conn):
    """Aggregate cached games into per-referee tendencies."""
    cur = conn.cursor()
    games = cur.execute(
        "SELECT home_fouls, away_fouls, officials FROM wnba_game_officials").fetchall()
    # per ref accumulators
    acc = {}  # name -> [games, sum_home, sum_away]
    for home_f, away_f, officials in games:
        if home_f is None or away_f is None or not officials:
            continue
        for name in officials.split("|"):
            name = name.strip()
            if not name:
                continue
            a = acc.setdefault(name, [0, 0.0, 0.0])
            a[0] += 1
            a[1] += home_f
            a[2] += away_f

    cur.execute("DELETE FROM wnba_referee_stats")
    now = datetime.utcnow().isoformat()
    out = []
    for name, (g, sh, sa) in acc.items():
        if g == 0:
            continue
        avg_home = sh / g
        avg_away = sa / g
        fouls_pg = avg_home + avg_away
        foul_diff = avg_away - avg_home  # + => more road fouls => home advantage
        total = avg_home + avg_away
        foul_pct_home = (avg_home / total) if total else 0.0
        foul_pct_road = (avg_away / total) if total else 0.0
        out.append((name, g, fouls_pg, foul_diff, foul_pct_home, foul_pct_road, now))
    cur.executemany(
        "INSERT OR REPLACE INTO wnba_referee_stats "
        "(referee, games_officiated, fouls_pg, foul_diff, foul_pct_home, "
        "foul_pct_road, updated_at) VALUES (?,?,?,?,?,?,?)", out)
    conn.commit()
    print(f"  aggregated {len(out)} referees")
    out.sort(key=lambda r: -r[1])
    for name, g, fpg, fd, _, _, _ in out[:8]:
        print(f"    {name:22s} g={g:3d} fouls/g={fpg:5.1f} diff={fd:+.2f}")


def main():
    conn = sqlite3.connect(DB)
    _ensure_tables(conn.cursor())
    conn.commit()
    print("WNBA referee stats: refreshing game cache from ESPN...")
    refresh_cache(conn)
    print("WNBA referee stats: aggregating per-referee tendencies...")
    aggregate(conn)
    conn.close()


if __name__ == "__main__":
    main()
