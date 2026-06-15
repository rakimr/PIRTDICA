"""
WNBA player shot-zone distribution builder for PIRTDICA SPORTS CO.

Mirrors the NBA Player Explorer shot chart (player_shot_zones) for the WNBA,
using REAL data from Basketball Reference WNBA pages:

  years/<YEAR>_shooting.html  -> per-player FGA distance distribution
                                 (pct_fga_00_03 = restricted area, 03_10 = paint,
                                  10_16 + 16_xx = mid-range, fg3a = three) plus
                                 FG% by the same buckets and the corner-3 share.
  years/<YEAR>_totals.html    -> raw FGA / FG volume to turn those percentages
                                 into absolute per-zone attempts/makes.

stats.wnba.com is blocked from this host (it times out), and Basketball
Reference does NOT publish player tracking data (catch-and-shoot / pull-up
creation) for the WNBA, so the WNBA Player Explorer intentionally drops the
creation + archetype columns the NBA chart shows -- the same honesty rule the
WNBA articles follow (position, not archetype).

Table (SQLite dfs_nba.db): wnba_player_shot_zones -- mirrors the NBA
player_shot_zones schema so the existing /api/player-shot-chart endpoint and the
front-end canvas renderer work unchanged when league=wnba.
"""
import sqlite3
import time
from datetime import datetime, date

import requests
from bs4 import BeautifulSoup, Comment

DB = "dfs_nba.db"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 Chrome/120 Safari/537.36"}
BASE = "https://www.basketball-reference.com/wnba/years/{year}_{page}.html"
MIN_FGA = 20  # only players with a meaningful shot sample


def _season_year():
    """WNBA season is a single calendar year (May-Oct). Before May, use last year."""
    today = date.today()
    return today.year if today.month >= 4 else today.year - 1


def _get_table(year, page, tid):
    url = BASE.format(year=year, page=page)
    for attempt in range(3):
        try:
            r = requests.get(url, headers=UA, timeout=30)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, "html.parser")
                t = soup.find("table", id=tid)
                if not t:
                    for c in soup.find_all(string=lambda s: isinstance(s, Comment)):
                        if f'id="{tid}"' in c:
                            t = BeautifulSoup(c, "html.parser").find("table", id=tid)
                            break
                if t is not None:
                    return t
        except Exception as e:
            if attempt == 2:
                print(f"  [WARN] GET failed {url}: {e}")
        time.sleep(0.6 * (attempt + 1))
    return None


def _data_rows(table):
    body = table.find("tbody")
    if not body:
        return []
    out = []
    for tr in body.find_all("tr"):
        cls = tr.get("class") or []
        if "thead" in cls:
            continue
        cells = tr.find_all(["th", "td"])
        if not cells:
            continue
        out.append(tr)
    return out


def _cell_map(tr):
    """Map data-stat -> text for one row. Player name + key handled specially
    because the player <th> get_text() is polluted on these pages."""
    d = {}
    for c in tr.find_all(["th", "td"]):
        ds = c.get("data-stat")
        if not ds or ds == "DUMMY":
            continue
        if ds == "player":
            a = c.find("a")
            d["player"] = a.get_text(strip=True) if a else ""
            d["key"] = c.get("data-append-csv") or d["player"].lower()
        else:
            d[ds] = c.get_text(strip=True)
    return d


def _f(v, default=0.0):
    if v is None or v == "":
        return default
    try:
        return float(v)
    except ValueError:
        return default


def _i(v, default=0):
    if v is None or v == "":
        return default
    try:
        return int(float(v))
    except ValueError:
        return default


def _ensure_table(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS wnba_player_shot_zones (
        player_name TEXT,
        player_id INTEGER,
        team TEXT,
        total_fga INTEGER,
        ra_fga INTEGER,
        ra_fgm INTEGER,
        paint_fga INTEGER,
        paint_fgm INTEGER,
        mid_fga INTEGER,
        mid_fgm INTEGER,
        three_fga INTEGER,
        three_fgm INTEGER,
        corner3_fga INTEGER,
        atb3_fga INTEGER,
        ra_pct REAL,
        paint_pct REAL,
        rim_paint_pct REAL,
        mid_pct REAL,
        three_pct REAL,
        scraped_at TEXT
    )""")


def build(conn):
    year = _season_year()
    print(f"WNBA player shot zones: fetching Basketball Reference {year} pages...")
    shooting = _get_table(year, "shooting", "shooting")
    totals = _get_table(year, "totals", "totals")
    if shooting is None or totals is None:
        # Fall back to prior season if the current year has no data yet.
        if shooting is None or totals is None:
            print(f"  [WARN] {year} incomplete; retrying {year - 1}")
            year -= 1
            shooting = _get_table(year, "shooting", "shooting")
            totals = _get_table(year, "totals", "totals")
    if shooting is None or totals is None:
        print("  [ERROR] could not load BBRef shooting/totals tables; aborting")
        return 0

    # totals: key -> (team, total_fga, total_fgm)
    tot = {}
    for tr in _data_rows(totals):
        d = _cell_map(tr)
        key = d.get("key")
        if not key:
            continue
        tot[key] = {
            "team": d.get("team", ""),
            "fga": _i(d.get("fga")),
            "fg": _i(d.get("fg")),
        }

    rows = []
    now = datetime.utcnow().isoformat()
    for tr in _data_rows(shooting):
        d = _cell_map(tr)
        key = d.get("key")
        name = d.get("player")
        if not key or not name:
            continue
        t = tot.get(key)
        if not t:
            continue
        total_fga = t["fga"]
        if total_fga < MIN_FGA:
            continue

        p_ra = _f(d.get("pct_fga_00_03"))
        p_paint = _f(d.get("pct_fga_03_10"))
        p_mid = _f(d.get("pct_fga_10_16")) + _f(d.get("pct_fga_16_xx"))
        p_three = _f(d.get("pct_fga_fg3a"))

        fg_ra = _f(d.get("fg_pct_00_03"))
        fg_paint = _f(d.get("fg_pct_03_10"))
        fg_10_16 = _f(d.get("fg_pct_10_16"))
        fg_16_xx = _f(d.get("fg_pct_16_xx"))
        fg_three = _f(d.get("fg_pct_fg3a"))
        p_corner_of_3 = _f(d.get("pct_fg3a_corner3"))

        ra_fga = round(total_fga * p_ra)
        paint_fga = round(total_fga * p_paint)
        fga_10_16 = total_fga * _f(d.get("pct_fga_10_16"))
        fga_16_xx = total_fga * _f(d.get("pct_fga_16_xx"))
        mid_fga = round(fga_10_16 + fga_16_xx)
        three_fga = round(total_fga * p_three)
        corner3_fga = round(three_fga * p_corner_of_3)
        atb3_fga = max(0, three_fga - corner3_fga)

        ra_fgm = round(ra_fga * fg_ra)
        paint_fgm = round(paint_fga * fg_paint)
        mid_fgm = round(fga_10_16 * fg_10_16 + fga_16_xx * fg_16_xx)
        three_fgm = round(three_fga * fg_three)

        ra_pct = round(ra_fga / total_fga * 100, 1) if total_fga else 0.0
        paint_pct = round(paint_fga / total_fga * 100, 1) if total_fga else 0.0
        rim_paint_pct = round((ra_fga + paint_fga) / total_fga * 100, 1) if total_fga else 0.0
        mid_pct = round(mid_fga / total_fga * 100, 1) if total_fga else 0.0
        three_pct = round(three_fga / total_fga * 100, 1) if total_fga else 0.0

        rows.append((
            name, None, t["team"], total_fga,
            ra_fga, ra_fgm, paint_fga, paint_fgm, mid_fga, mid_fgm,
            three_fga, three_fgm, corner3_fga, atb3_fga,
            ra_pct, paint_pct, rim_paint_pct, mid_pct, three_pct, now,
        ))

    cur = conn.cursor()
    _ensure_table(cur)
    cur.execute("DELETE FROM wnba_player_shot_zones")
    cur.executemany(
        "INSERT INTO wnba_player_shot_zones "
        "(player_name, player_id, team, total_fga, ra_fga, ra_fgm, paint_fga, "
        "paint_fgm, mid_fga, mid_fgm, three_fga, three_fgm, corner3_fga, atb3_fga, "
        "ra_pct, paint_pct, rim_paint_pct, mid_pct, three_pct, scraped_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    print(f"  wrote {len(rows)} WNBA players (>= {MIN_FGA} FGA) for {year}")
    rows.sort(key=lambda r: -r[3])
    for r in rows[:8]:
        print(f"    {r[0]:24s} {r[2]:4s} FGA={r[3]:4d} "
              f"RA {r[4]:3d} paint {r[6]:3d} mid {r[8]:3d} 3 {r[10]:3d} "
              f"(c3 {r[12]} atb3 {r[13]})")
    return len(rows)


def main():
    conn = sqlite3.connect(DB)
    build(conn)
    conn.close()


if __name__ == "__main__":
    main()
