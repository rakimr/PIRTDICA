"""
WNBA player game-log + season-stat ingestion for PIRTDICA SPORTS CO.

Source: ESPN WNBA public APIs (same family as the standings scraper).
  teams   -> site.api.espn.com/.../wnba/teams
  roster  -> site.api.espn.com/.../wnba/teams/{id}/roster
  gamelog -> site.web.api.espn.com/.../wnba/athletes/{id}/gamelog

This is the WNBA mirror of the NBA game-log ingestion. It writes three
full-refresh SQLite tables that the WNBA projection engine consumes:
  - wnba_teams            (espn_id, abbr, name, logo)
  - wnba_player_game_logs (per game box line + fantasy points)
  - wnba_player_stats     (season aggregates: averages, std devs, last-5)

WNBA props/standings are league-specific and live in parallel wnba_* tables,
NOT a league column. This script keeps that convention.
"""
import sqlite3
import time
from datetime import datetime, timezone

import requests

from utils.espn_fetch import espn_get_json

try:
    import zoneinfo
    EASTERN = zoneinfo.ZoneInfo("US/Eastern")
except ImportError:  # pragma: no cover
    import pytz
    EASTERN = pytz.timezone("US/Eastern")

DB = "dfs_nba.db"
UA = {"User-Agent": "Mozilla/5.0"}


def _espn_gamedate_to_et(raw):
    """Convert ESPN's UTC gameDate timestamp to the Eastern calendar date.

    ESPN returns gameDate as a UTC instant (e.g. '2026-06-26T02:00:00.000+00:00').
    Late tip-offs (>= 8 PM ET) fall on the NEXT UTC day, so naively slicing the
    first 10 chars logs them under the wrong date. The slate/props side keys on
    the Eastern date (scrape_wnba_props._utc_to_et_date), so the game logs must
    match or the grader can never join late games to their picks.
    """
    if not raw:
        return ""
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(EASTERN).date().isoformat()
    except Exception:
        return raw[:10]

TEAMS_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/teams"
ROSTER_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/teams/{tid}/roster"
GAMELOG_URL = "https://site.web.api.espn.com/apis/common/v3/sports/basketball/wnba/athletes/{aid}/gamelog"

# Index of each stat inside the gamelog `stats` array (aligned to `labels`):
# ['MIN','PTS','REB','AST','STL','BLK','TO','FG','FG%','3PT','3P%','FT','FT%','PF']
IDX = {"min": 0, "pts": 1, "reb": 2, "ast": 3, "stl": 4, "blk": 5, "tov": 6, "fg3": 9}


def _get(url, tries=3):
    return espn_get_json(url, tries=tries, timeout=20)


def _num(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _made(val):
    """Parse a 'made-attempted' string like '2-3' -> 2.0."""
    try:
        return float(str(val).split("-")[0])
    except (TypeError, ValueError, IndexError):
        return 0.0


def fanduel_fp(pts, reb, ast, stl, blk, tov):
    """FanDuel-style fantasy points (used for the value/upside charts)."""
    return pts * 1.0 + reb * 1.2 + ast * 1.5 + stl * 3.0 + blk * 3.0 - tov * 1.0


def _mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def _sd(xs):
    if len(xs) < 2:
        return 0.0
    m = _mean(xs)
    return (sum((x - m) ** 2 for x in xs) / (len(xs) - 1)) ** 0.5


def fetch_teams():
    data = _get(TEAMS_URL)
    teams = []
    if data:
        try:
            for t in data["sports"][0]["leagues"][0]["teams"]:
                team = t["team"]
                teams.append({
                    "espn_id": team["id"],
                    "abbr": team.get("abbreviation", ""),
                    "name": team.get("displayName", ""),
                    "logo": (team.get("logos") or [{}])[0].get("href", ""),
                })
        except (KeyError, IndexError, TypeError) as e:
            print(f"  [WARN] ESPN teams response shape changed: {e}")
            teams = []
    if not teams:
        # ESPN fetch failed — reuse the last good franchise list so one
        # blocked endpoint can't abort the whole ingestion (and with it
        # pick grading). Mid-season the league's teams never change.
        try:
            con = sqlite3.connect(DB)
            rows = con.execute(
                "SELECT espn_id, abbr, name, logo FROM wnba_teams").fetchall()
            con.close()
            teams = [{"espn_id": r[0], "abbr": r[1], "name": r[2],
                      "logo": r[3]} for r in rows]
            if teams:
                print(f"  [WARN] ESPN teams fetch failed — using "
                      f"{len(teams)} cached teams from wnba_teams")
        except sqlite3.Error as e:
            print(f"  [WARN] cached-teams fallback failed: {e}")
    return teams


def fetch_roster(tid):
    data = _get(ROSTER_URL.format(tid=tid))
    out = []
    if not data:
        return out
    for a in data.get("athletes", []):
        out.append({
            "id": a.get("id"),
            "name": a.get("displayName", ""),
            "pos": (a.get("position") or {}).get("abbreviation", ""),
        })
    return out


def _season_type_name(block):
    """Return our stable season label for an ESPN season/type block."""
    text = " ".join(str(block.get(k, "")) for k in
                    ("displayName", "name", "abbreviation", "slug")).lower()
    # ESPN calls the play-in a postseason type in some responses.  It belongs
    # with playoff data for consumers which weight meaningful postseason games.
    if any(word in text for word in ("playoff", "postseason", "play-in",
                                     "play in", "elimination")) or \
            text.strip() in ("post", "po"):
        return "PLAYOFF"
    if "regular" in text or text.strip() in ("reg", "rs"):
        return "REGULAR"
    return None


def _season_blocks(data):
    """Yield ESPN gamelog season-type blocks, across response variants.

    The common API has historically returned either ``seasonTypes`` directly
    or nested it below a season object.  Do not assume the first block is the
    current season: retaining every labelled block prevents a new season
    response from deleting last year's playoff games.
    """
    found = set()

    def walk(value):
        if isinstance(value, dict):
            blocks = value.get("seasonTypes")
            if isinstance(blocks, list):
                for block in blocks:
                    if isinstance(block, dict):
                        key = id(block)
                        if key not in found and block.get("categories"):
                            found.add(key)
                            yield block
            for child in value.values():
                yield from walk(child)
        elif isinstance(value, list):
            for child in value:
                yield from walk(child)

    yield from walk(data)


def fetch_gamelog(aid):
    """Return all labelled regular-season and playoff games ESPN provides."""
    data = _get(GAMELOG_URL.format(aid=aid))
    if not data:
        return []
    events_meta = data.get("events", {})
    seasons = list(_season_blocks(data))
    if not seasons:
        return []
    rows = []
    seen = set()
    for season in seasons:
        season_type = _season_type_name(season)
        for cat in season.get("categories", []):
            # Some older responses put the year on the season block and the
            # actual "Regular Season"/"Playoffs" label on each category.
            category_type = season_type or _season_type_name(cat)
            if category_type is None:
                continue
            for ev in cat.get("events", []):
                event_id = ev.get("eventId") or ev.get("id")
                # A few ESPN payloads repeat an event in a totals and splits
                # category.  Keep one box line, not one line per category.
                key = (category_type, event_id) if event_id else (
                    category_type, ev.get("gameDate"), str(ev.get("stats")))
                if key in seen:
                    continue
                seen.add(key)
                stats = ev.get("stats") or []
                if len(stats) <= IDX["fg3"]:
                    continue
                meta = events_meta.get(str(event_id), events_meta.get(event_id, {}))
                opp = (meta.get("opponent") or {}).get("abbreviation", "")
                team = (meta.get("team") or {}).get("abbreviation", "")
                gdate = _espn_gamedate_to_et(meta.get("gameDate") or ev.get("gameDate"))
                is_home = 1 if meta.get("atVs", "") == "vs" else 0
                mins = _num(stats[IDX["min"]])
                if mins <= 0:
                    continue  # DNP
                pts = _num(stats[IDX["pts"]])
                reb = _num(stats[IDX["reb"]])
                ast = _num(stats[IDX["ast"]])
                stl = _num(stats[IDX["stl"]])
                blk = _num(stats[IDX["blk"]])
                tov = _num(stats[IDX["tov"]])
                fg3 = _made(stats[IDX["fg3"]])
                rows.append({
                    "game_date": gdate, "season_type": category_type,
                    "team": team, "opp": opp, "is_home": is_home,
                    "min": mins, "pts": pts, "reb": reb, "ast": ast, "stl": stl,
                    "blk": blk, "tov": tov, "fg3m": fg3,
                    "fp": round(fanduel_fp(pts, reb, ast, stl, blk, tov), 2),
                })
    return rows


def _create_tables(cur):
    cur.execute("DROP TABLE IF EXISTS wnba_teams")
    cur.execute("""CREATE TABLE wnba_teams (
        espn_id TEXT PRIMARY KEY, abbr TEXT, name TEXT, logo TEXT)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS wnba_player_game_logs (
        player_name TEXT, espn_id TEXT, team TEXT, opp TEXT, game_date TEXT,
        is_home INTEGER, min REAL, pts REAL, reb REAL, ast REAL, stl REAL,
        blk REAL, tov REAL, fg3m REAL, fp REAL, season_type TEXT)""")
    # Keep existing logs: ESPN's current response can omit completed prior
    # postseason blocks when a new season starts.
    cols = {row[1] for row in cur.execute(
        "PRAGMA table_info(wnba_player_game_logs)").fetchall()}
    if "season_type" not in cols:
        cur.execute("ALTER TABLE wnba_player_game_logs ADD COLUMN season_type TEXT")
        # Old rows have no source phase. Do not fabricate REGULAR: ESPN may
        # later identify a preserved row as a playoff game.
        cur.execute("UPDATE wnba_player_game_logs SET season_type='UNKNOWN' "
                    "WHERE season_type IS NULL OR season_type=''")
    cur.execute("DROP TABLE IF EXISTS wnba_player_stats")
    cur.execute("""CREATE TABLE wnba_player_stats (
        player_name TEXT PRIMARY KEY, espn_id TEXT, team TEXT, position TEXT,
        games INTEGER, min_avg REAL, min_sd REAL, min_l5 REAL,
        pts_avg REAL, pts_sd REAL, pts_l5 REAL,
        reb_avg REAL, reb_sd REAL, reb_l5 REAL,
        ast_avg REAL, ast_sd REAL, ast_l5 REAL,
        stl_avg REAL, stl_sd REAL, stl_l5 REAL,
        blk_avg REAL, blk_sd REAL, blk_l5 REAL,
        fg3m_avg REAL, fg3m_sd REAL, fg3m_l5 REAL,
        fp_avg REAL, fp_sd REAL, fp_l5 REAL,
        updated_at TEXT)""")


def _agg(logs, key):
    vals = [g[key] for g in logs]
    last5 = vals[:5] if len(vals) >= 1 else vals  # logs come newest-first
    return round(_mean(vals), 2), round(_sd(vals), 2), round(_mean(last5), 2)


def _current_regular_logs(logs):
    """Season snapshots exclude both playoffs and earlier regular seasons."""
    regular = sorted((g for g in logs if g["season_type"] == "REGULAR"
                      and g.get("game_date")), key=lambda g: g["game_date"], reverse=True)
    return ([g for g in regular if g["game_date"][:4] == regular[0]["game_date"][:4]]
            if regular else [])


def _upsert_gamelog(cur, player, aid, g, fallback_team):
    """Reconcile a source-labelled ESPN game with a legacy UNKNOWN row."""
    team = g["team"] or fallback_team
    identity = (player, aid, g["game_date"])
    old = cur.execute(
        "SELECT 1 FROM wnba_player_game_logs "
        "WHERE player_name=? AND espn_id=? AND game_date=? LIMIT 1", identity,
    ).fetchone()
    if old:
        cur.execute(
            "UPDATE wnba_player_game_logs SET team=?,opp=?,is_home=?,"
            "min=?,pts=?,reb=?,ast=?,stl=?,blk=?,tov=?,fg3m=?,fp=?,"
            "season_type=? WHERE player_name=? AND espn_id=? AND game_date=?",
            (team, g["opp"], g["is_home"], g["min"], g["pts"], g["reb"],
             g["ast"], g["stl"], g["blk"], g["tov"], g["fg3m"], g["fp"],
             g["season_type"], *identity),
        )
    else:
        cur.execute(
            "INSERT INTO wnba_player_game_logs "
            "(player_name,espn_id,team,opp,game_date,is_home,min,pts,reb,ast,"
            "stl,blk,tov,fg3m,fp,season_type) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (player, aid, team, g["opp"], g["game_date"], g["is_home"],
             g["min"], g["pts"], g["reb"], g["ast"], g["stl"], g["blk"],
             g["tov"], g["fg3m"], g["fp"], g["season_type"]),
        )


def main():
    print("=== WNBA game-log ingestion (ESPN) ===")
    teams = fetch_teams()
    print(f"Teams: {len(teams)}")
    if not teams:
        print("No teams fetched // aborting.")
        return

    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    _create_tables(cur)

    for t in teams:
        cur.execute("INSERT OR REPLACE INTO wnba_teams VALUES (?,?,?,?)",
                    (t["espn_id"], t["abbr"], t["name"], t["logo"]))
    conn.commit()

    now = datetime.now().isoformat()
    total_logs = 0
    total_players = 0
    for t in teams:
        roster = fetch_roster(t["espn_id"])
        for pl in roster:
            aid = pl["id"]
            if not aid:
                continue
            logs = fetch_gamelog(aid)
            time.sleep(0.12)
            if not logs:
                continue
            for g in logs:
                _upsert_gamelog(cur, pl["name"], aid, g, t["abbr"])
            total_logs += len(logs)
            # Aggregate only regular-season games.  Playoff rows are retained
            # for confidence/trend consumers but must not alter season baselines.
            regular_logs = _current_regular_logs(logs)
            if not regular_logs:
                continue
            pts = _agg(regular_logs, "pts")
            reb = _agg(regular_logs, "reb")
            ast = _agg(regular_logs, "ast")
            stl = _agg(regular_logs, "stl")
            blk = _agg(regular_logs, "blk")
            fg3 = _agg(regular_logs, "fg3m")
            mn = _agg(regular_logs, "min")
            fp = _agg(regular_logs, "fp")
            cur.execute(
                "INSERT OR REPLACE INTO wnba_player_stats "
                "(player_name, espn_id, team, position, games, "
                "min_avg, min_sd, min_l5, pts_avg, pts_sd, pts_l5, "
                "reb_avg, reb_sd, reb_l5, ast_avg, ast_sd, ast_l5, "
                "stl_avg, stl_sd, stl_l5, blk_avg, blk_sd, blk_l5, "
                "fg3m_avg, fg3m_sd, fg3m_l5, fp_avg, fp_sd, fp_l5, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 (pl["name"], aid, t["abbr"], pl["pos"], len(regular_logs),
                 mn[0], mn[1], mn[2],
                 pts[0], pts[1], pts[2],
                 reb[0], reb[1], reb[2],
                 ast[0], ast[1], ast[2],
                  stl[0], stl[1], stl[2],
                  blk[0], blk[1], blk[2],
                 fg3[0], fg3[1], fg3[2],
                 fp[0], fp[1], fp[2],
                 now))
            total_players += 1
        print(f"  {t['abbr']}: roster {len(roster)} processed")
        conn.commit()

    conn.commit()
    conn.close()
    print(f"Done. {total_players} players, {total_logs} game logs stored.")


if __name__ == "__main__":
    main()
