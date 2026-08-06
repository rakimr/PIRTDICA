"""
WNBA defensive shot-zone builder for PIRTDICA SPORTS CO.

Mirrors the NBA "Defensive Shot Charts" (team_defense_shot_zones) for the WNBA.
NBA pulls opponent shooting-by-zone straight from NBA.com; stats.wnba.com is
blocked from this host, so we reconstruct the SAME numbers from ESPN WNBA
play-by-play (the source already used for WNBA game logs + referee stats):

  summary?event=<id> -> plays[] with shootingPlay + coordinate{x,y}

Every made/missed field-goal attempt is geo-classified into the five chart zones
(restricted area / paint / mid-range / corner 3 / above-the-break 3) from its
(x,y) coordinate and attributed to the DEFENDING team (the shooter's opponent),
yielding per-team opponent FGA/FGM by zone.

Coordinate calibration (fit empirically against the foot-distances ESPN prints
in each play's text): basket at raw (25, 0); 1 raw unit ~= 0.985 feet; the WNBA
three-point arc sits at ~22 ft and the restricted area at 4 ft.

Tables (SQLite dfs_nba.db):
  wnba_game_shot_zones        CACHE of per-game opponent zone counts keyed on
                              game_id, so daily runs only fetch new games.
  wnba_team_defense_shot_zones aggregated per-team opponent zone distribution,
                              mirroring the NBA team_defense_shot_zones schema so
                              /api/team-defense-shot-chart works unchanged.
"""
import json
import math
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

BASKET_X = 25.0
BASKET_Y = 0.0
FT_PER_UNIT = 0.985

ZONE_KEYS = ["ra", "paint", "mid", "corner3", "atb3"]


def _get(url, tries=3):
    return espn_get_json(url, tries=tries, timeout=20)


def _classify(x, y, is_three):
    """Return one of ra/paint/mid/corner3/atb3 from ESPN raw (x,y)."""
    dxf = (x - BASKET_X) * FT_PER_UNIT
    dyf = (y - BASKET_Y) * FT_PER_UNIT
    dist = math.hypot(dxf, dyf)
    if is_three:
        # Corner 3: out by the sideline and low toward the baseline (below the
        # break where the arc straightens into the corner).
        if abs(dxf) >= 18 and dyf <= 14:
            return "corner3"
        return "atb3"
    if dist <= 4:
        return "ra"
    if abs(dxf) <= 8 and dyf <= 15:
        return "paint"
    return "mid"


def _ensure_tables(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS wnba_game_shot_zones (
        game_id TEXT PRIMARY KEY,
        game_date TEXT,
        zones_json TEXT,
        scraped_at TEXT
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS wnba_team_defense_shot_zones (
        team TEXT,
        team_name TEXT,
        team_id INTEGER,
        total_fga INTEGER,
        ra_fga INTEGER,
        ra_fgm INTEGER,
        paint_fga INTEGER,
        paint_fgm INTEGER,
        mid_fga INTEGER,
        mid_fgm INTEGER,
        corner3_fga INTEGER,
        corner3_fgm INTEGER,
        atb3_fga INTEGER,
        atb3_fgm INTEGER,
        ra_freq REAL,
        paint_freq REAL,
        mid_freq REAL,
        corner3_freq REAL,
        atb3_freq REAL,
        ra_fg_pct REAL,
        paint_fg_pct REAL,
        mid_fg_pct REAL,
        corner3_fg_pct REAL,
        atb3_fg_pct REAL,
        scraped_at TEXT
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


def _blank():
    z = {}
    for k in ZONE_KEYS:
        z[k + "_fga"] = 0
        z[k + "_fgm"] = 0
    return z


def _fetch_game_zones(game_id):
    """Return {def_team_abbr: {name, <zone>_fga/fgm...}} for one game, or None."""
    s = _get(SUMMARY.format(e=game_id))
    if not s:
        return None
    comps = s.get("header", {}).get("competitions", [])
    if not comps:
        return None
    competitors = comps[0].get("competitors", [])
    if len(competitors) != 2:
        return None
    by_id = {}
    for c in competitors:
        tid = str(c.get("id"))
        team = c.get("team", {})
        by_id[tid] = {
            "abbr": team.get("abbreviation"),
            "name": team.get("displayName"),
        }
    team_ids = list(by_id.keys())

    out = {}  # def_abbr -> counts
    names = {}
    for p in s.get("plays", []):
        if not p.get("shootingPlay"):
            continue
        txt = (p.get("text") or "").lower()
        if "free throw" in txt:
            continue
        coord = p.get("coordinate")
        if not coord:
            continue
        x, y = coord.get("x"), coord.get("y")
        if x is None or y is None or abs(x) > 900 or abs(y) > 900:
            continue
        shooter = p.get("team", {})
        sid = str(shooter.get("id")) if shooter else None
        if sid not in by_id:
            continue
        def_id = team_ids[0] if team_ids[1] == sid else team_ids[1]
        def_abbr = by_id[def_id]["abbr"]
        if not def_abbr:
            continue
        names[def_abbr] = by_id[def_id]["name"]

        is_three = p.get("scoreValue") == 3 or "three point" in txt
        zone = _classify(x, y, is_three)
        made = bool(p.get("scoringPlay"))

        z = out.setdefault(def_abbr, _blank())
        z[zone + "_fga"] += 1
        if made:
            z[zone + "_fgm"] += 1

    if not out:
        return None
    for abbr in out:
        out[abbr]["name"] = names.get(abbr, abbr)
    return out


def refresh_cache(conn):
    cur = conn.cursor()
    cached = {r[0] for r in cur.execute("SELECT game_id FROM wnba_game_shot_zones").fetchall()}
    added = 0
    now = datetime.utcnow().isoformat()
    for date_str in _game_dates(cur):
        for gid in _completed_game_ids(date_str):
            if gid in cached:
                continue
            zones = _fetch_game_zones(gid)
            if not zones:
                continue
            cur.execute(
                "INSERT OR REPLACE INTO wnba_game_shot_zones "
                "(game_id, game_date, zones_json, scraped_at) VALUES (?,?,?,?)",
                (gid, date_str, json.dumps(zones), now))
            cached.add(gid)
            added += 1
            time.sleep(0.25)
    conn.commit()
    print(f"  cached {added} new game(s); {len(cached)} total")


def aggregate(conn):
    cur = conn.cursor()
    games = cur.execute("SELECT zones_json FROM wnba_game_shot_zones").fetchall()
    acc = {}    # abbr -> counts
    names = {}  # abbr -> display name
    for (zj,) in games:
        try:
            data = json.loads(zj)
        except Exception:
            continue
        for abbr, z in data.items():
            names[abbr] = z.get("name", abbr)
            a = acc.setdefault(abbr, _blank())
            for k in ZONE_KEYS:
                a[k + "_fga"] += int(z.get(k + "_fga", 0) or 0)
                a[k + "_fgm"] += int(z.get(k + "_fgm", 0) or 0)

    _ensure_tables(cur)
    cur.execute("DELETE FROM wnba_team_defense_shot_zones")
    now = datetime.utcnow().isoformat()
    out = []
    for abbr, z in acc.items():
        total = sum(z[k + "_fga"] for k in ZONE_KEYS)
        if total == 0:
            continue

        def freq(k):
            return round(z[k + "_fga"] / total * 100, 1) if total else 0.0

        def pct(k):
            fga = z[k + "_fga"]
            return round(z[k + "_fgm"] / fga * 100, 1) if fga else 0.0

        out.append((
            abbr, names.get(abbr, abbr), 0, total,
            z["ra_fga"], z["ra_fgm"], z["paint_fga"], z["paint_fgm"],
            z["mid_fga"], z["mid_fgm"], z["corner3_fga"], z["corner3_fgm"],
            z["atb3_fga"], z["atb3_fgm"],
            freq("ra"), freq("paint"), freq("mid"), freq("corner3"), freq("atb3"),
            pct("ra"), pct("paint"), pct("mid"), pct("corner3"), pct("atb3"),
            now,
        ))
    cur.executemany(
        "INSERT INTO wnba_team_defense_shot_zones "
        "(team, team_name, team_id, total_fga, ra_fga, ra_fgm, paint_fga, paint_fgm, "
        "mid_fga, mid_fgm, corner3_fga, corner3_fgm, atb3_fga, atb3_fgm, "
        "ra_freq, paint_freq, mid_freq, corner3_freq, atb3_freq, "
        "ra_fg_pct, paint_fg_pct, mid_fg_pct, corner3_fg_pct, atb3_fg_pct, scraped_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", out)
    conn.commit()
    print(f"  aggregated {len(out)} teams")
    out.sort(key=lambda r: r[14])  # by ra_freq
    print(f"  {'Team':<5}{'FGA':>7}{'RA%':>7}{'Paint%':>8}{'Mid%':>7}{'C3%':>7}{'ATB3%':>7}")
    for r in out:
        print(f"  {r[0]:<5}{r[3]:>7}{r[14]:>6.1f}%{r[15]:>7.1f}%{r[16]:>6.1f}%{r[17]:>6.1f}%{r[18]:>6.1f}%")


def main():
    conn = sqlite3.connect(DB)
    _ensure_tables(conn.cursor())
    conn.commit()
    print("WNBA defensive shot zones: refreshing per-game cache from ESPN...")
    refresh_cache(conn)
    print("WNBA defensive shot zones: aggregating per-team opponent distribution...")
    aggregate(conn)
    conn.close()


if __name__ == "__main__":
    main()
