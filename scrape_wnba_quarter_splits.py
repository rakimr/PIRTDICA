"""
WNBA quarter / clutch splits builder for PIRTDICA SPORTS CO.

Derives per-player quarter-by-quarter scoring and late-game usage from the SAME
ESPN play-by-play source already used by the defensive shot-zone builder:

  summary?event=<id> -> plays[] with period.number, clock, scores, participants

Per game, every scoring play and field-goal attempt is attributed to the
shooter (participants[0].athlete.id) and bucketed by quarter. "Clutch" is the
standard definition: 4th quarter or overtime, game within 5 points, under 5:00
on the clock.

Tables (SQLite dfs_nba.db):
  wnba_game_quarter_splits    CACHE of per-game per-player quarter counts keyed
                              on game_id, so daily runs only fetch new games.
  wnba_player_quarter_splits  season aggregate per player: avg points by
                              quarter, share of team 4th-quarter FGA, and
                              clutch scoring volume.
"""
import json
import re
import sqlite3
import time
from datetime import datetime

import requests

from utils.espn_fetch import espn_get_json

DB = "dfs_nba.db"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 Chrome/120 Safari/537.36"}
SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/scoreboard?dates={d}"
SUMMARY = "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/summary?event={e}"


def _get(url, tries=3):
    return espn_get_json(url, tries=tries, timeout=20)


def _ensure_tables(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS wnba_game_quarter_splits (
        game_id TEXT PRIMARY KEY,
        game_date TEXT,
        splits_json TEXT,
        scraped_at TEXT
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS wnba_player_quarter_splits (
        espn_id TEXT PRIMARY KEY,
        player_name TEXT,
        team TEXT,
        games INTEGER,
        q1_pts REAL,
        q2_pts REAL,
        q3_pts REAL,
        q4_pts REAL,
        q4_pts_share REAL,
        q4_team_fga_share REAL,
        clutch_pts_pg REAL,
        clutch_fga_pg REAL,
        updated_at TEXT
    )""")


def _game_dates(cur):
    rows = cur.execute(
        "SELECT DISTINCT game_date FROM wnba_player_game_logs "
        "WHERE game_date IS NOT NULL ORDER BY game_date").fetchall()
    return [r[0] for r in rows if r[0]]


def _completed_game_ids(date_str):
    d = date_str.replace("-", "")
    data = _get(SCOREBOARD.format(d=d))
    if not data:
        return []
    ids = []
    for ev in data.get("events", []):
        status = ev.get("status", {}).get("type", {})
        if not status.get("completed"):
            continue
        if ev.get("season", {}).get("type") == 1:  # preseason
            continue
        ids.append(ev["id"])
    return ids


def _clock_seconds(disp):
    """'4:37' -> 277; '37.4' -> 37; None on parse failure."""
    if not disp:
        return None
    m = re.match(r"^(\d+):(\d+)", disp)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    m = re.match(r"^(\d+(?:\.\d+)?)$", disp)
    if m:
        return int(float(m.group(1)))
    return None


def _fetch_game_splits(game_id):
    """Return per-game splits or None.

    {
      "players": {espn_id: {"team": tid, "q_pts": {q: n}, "q_fga": {q: n},
                            "clutch_pts": n, "clutch_fga": n}},
      "team_q4_fga": {team_id: n}
    }
    """
    s = _get(SUMMARY.format(e=game_id))
    if not s:
        return None
    plays = s.get("plays", [])
    if not plays:
        return None

    players = {}
    team_q4_fga = {}

    for p in plays:
        period = (p.get("period") or {}).get("number")
        if not period:
            continue
        athletes = p.get("participants") or []
        aid = None
        if athletes:
            aid = str(((athletes[0] or {}).get("athlete") or {}).get("id") or "") or None

        scoring = bool(p.get("scoringPlay"))
        score_val = int(p.get("scoreValue") or 0)
        shooting = bool(p.get("shootingPlay"))
        txt = (p.get("text") or "").lower()
        is_ft = "free throw" in txt
        is_fga = shooting and not is_ft

        if not aid or (not scoring and not is_fga):
            continue

        q = min(period, 4)  # fold OT into "Q4/late-game"
        d = players.setdefault(aid, {
            "team": str((p.get("team") or {}).get("id") or ""),
            "q_pts": {}, "q_fga": {},
            "clutch_pts": 0, "clutch_fga": 0,
        })
        if scoring and score_val:
            d["q_pts"][q] = d["q_pts"].get(q, 0) + score_val
        if is_fga:
            d["q_fga"][q] = d["q_fga"].get(q, 0) + 1
            if q == 4:
                tid = d["team"]
                if tid:
                    team_q4_fga[tid] = team_q4_fga.get(tid, 0) + 1

        # Clutch: Q4/OT, under 5:00, margin within 5 at the time of the play.
        if period >= 4:
            secs = _clock_seconds((p.get("clock") or {}).get("displayValue"))
            home, away = p.get("homeScore"), p.get("awayScore")
            if (secs is not None and secs <= 300
                    and home is not None and away is not None
                    and abs(int(home) - int(away)) <= 5):
                if scoring and score_val:
                    d["clutch_pts"] += score_val
                if is_fga:
                    d["clutch_fga"] += 1

    if not players:
        return None
    return {"players": players, "team_q4_fga": team_q4_fga}


def refresh_cache(conn):
    cur = conn.cursor()
    cached = {r[0] for r in cur.execute(
        "SELECT game_id FROM wnba_game_quarter_splits").fetchall()}
    added = 0
    now = datetime.utcnow().isoformat()
    for date_str in _game_dates(cur):
        for gid in _completed_game_ids(date_str):
            if gid in cached:
                continue
            splits = _fetch_game_splits(gid)
            if not splits:
                continue
            cur.execute(
                "INSERT OR REPLACE INTO wnba_game_quarter_splits "
                "(game_id, game_date, splits_json, scraped_at) VALUES (?,?,?,?)",
                (gid, date_str, json.dumps(splits), now))
            cached.add(gid)
            added += 1
            time.sleep(0.25)
    conn.commit()
    print(f"  cached {added} new game(s); {len(cached)} total")


def aggregate(conn):
    cur = conn.cursor()
    # espn_id -> (name, team) from the game-log scraper, our identity source.
    id_map = {}
    for name, eid, team in cur.execute(
        "SELECT player_name, espn_id, team FROM wnba_player_stats"):
        if eid:
            id_map[str(eid)] = (name, team)

    # True games played per player (game logs, minutes > 0). Using only games
    # where the player generated a scoring/FGA event would inflate per-game
    # averages for low-usage players, so this is the honest denominator.
    games_played = {}
    for eid, n in cur.execute(
        "SELECT espn_id, COUNT(*) FROM wnba_player_game_logs "
        "WHERE espn_id IS NOT NULL AND min IS NOT NULL AND min > 0 "
        "GROUP BY espn_id"):
        games_played[str(eid)] = n

    games = cur.execute(
        "SELECT splits_json FROM wnba_game_quarter_splits").fetchall()
    acc = {}  # espn_id -> accumulators
    for (sj,) in games:
        try:
            data = json.loads(sj)
        except Exception:
            continue
        team_q4 = data.get("team_q4_fga", {})
        for aid, d in data.get("players", {}).items():
            a = acc.setdefault(aid, {
                "games": 0, "q_pts": [0, 0, 0, 0],
                "q4_fga": 0, "team_q4_fga": 0,
                "clutch_pts": 0, "clutch_fga": 0,
            })
            a["games"] += 1
            for q in range(1, 5):
                a["q_pts"][q - 1] += int(d.get("q_pts", {}).get(str(q), d.get("q_pts", {}).get(q, 0)) or 0)
            q4f = int(d.get("q_fga", {}).get("4", d.get("q_fga", {}).get(4, 0)) or 0)
            a["q4_fga"] += q4f
            a["team_q4_fga"] += int(team_q4.get(d.get("team", ""), 0) or 0)
            a["clutch_pts"] += int(d.get("clutch_pts", 0) or 0)
            a["clutch_fga"] += int(d.get("clutch_fga", 0) or 0)

    _ensure_tables(cur)
    cur.execute("DELETE FROM wnba_player_quarter_splits")
    now = datetime.utcnow().isoformat()
    out = []
    for aid, a in acc.items():
        name, team = id_map.get(aid, (None, None))
        if not name:
            continue
        g = max(games_played.get(aid, 0), a["games"])
        if g < 3:
            continue
        qp = [round(v / g, 1) for v in a["q_pts"]]
        total = sum(a["q_pts"])
        q4_share = round(100.0 * a["q_pts"][3] / total, 1) if total else None
        fga_share = (round(100.0 * a["q4_fga"] / a["team_q4_fga"], 1)
                     if a["team_q4_fga"] else None)
        out.append((aid, name, team, g, qp[0], qp[1], qp[2], qp[3],
                    q4_share, fga_share,
                    round(a["clutch_pts"] / g, 2), round(a["clutch_fga"] / g, 2),
                    now))
    cur.executemany(
        "INSERT INTO wnba_player_quarter_splits "
        "(espn_id, player_name, team, games, q1_pts, q2_pts, q3_pts, q4_pts, "
        "q4_pts_share, q4_team_fga_share, clutch_pts_pg, clutch_fga_pg, updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", out)
    conn.commit()
    print(f"  aggregated {len(out)} players")
    top = sorted(out, key=lambda r: -(r[10] or 0))[:8]
    print(f"  {'Player':<26}{'G':>4}{'Q1':>6}{'Q2':>6}{'Q3':>6}{'Q4':>6}{'Q4FGA%':>8}{'ClPts':>7}")
    for r in top:
        print(f"  {r[1]:<26}{r[3]:>4}{r[4]:>6}{r[5]:>6}{r[6]:>6}{r[7]:>6}"
              f"{(r[9] if r[9] is not None else 0):>7.1f}%{r[10]:>7.2f}")


def main():
    conn = sqlite3.connect(DB)
    _ensure_tables(conn.cursor())
    conn.commit()
    print("WNBA quarter splits: refreshing per-game cache from ESPN...")
    refresh_cache(conn)
    print("WNBA quarter splits: aggregating per-player quarter/clutch profile...")
    aggregate(conn)
    conn.close()


if __name__ == "__main__":
    main()
