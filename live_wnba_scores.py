"""Live WNBA scoreboard from ESPN's public JSON feed.

Powers the free live-scoring card under the standings table on the home page.
For every player in a live or finished game we compute FanDuel fantasy points and
a pace-based hot/cold read (live FP-per-minute vs their season FP-per-minute), plus
a highlight when the player is in tonight's PIRTDICA official picks (showing whether
that pick is currently winning or losing against the book line).

ESPN endpoints (the same provider already used for WNBA standings):
  scoreboard : .../basketball/wnba/scoreboard
  summary    : .../basketball/wnba/summary?event={id}
"""
import requests

from scrape_live_scores import calc_fanduel_fp
from utils.name_normalize import normalize_player_name
from utils.timezone import get_eastern_date_str

SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/scoreboard"
SUMMARY_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/summary"

# Thresholds for the pace-based hot/cold read.
MIN_MINUTES_FOR_TEMP = 6        # too few minutes is noise // no badge
HOT_RATIO = 1.15                # >= 15% above season pace = hot
COLD_RATIO = 0.85              # <= 15% below season pace = cold

# How a pick's stat maps onto the live box score we parse.
PICK_STAT_PARTS = {
    "PTS": ("pts",), "REB": ("reb",), "AST": ("ast",),
    "STL": ("stl",), "BLK": ("blk",), "3PM": ("threes",), "3PT": ("threes",),
    "PRA": ("pts", "reb", "ast"), "PR": ("pts", "reb"), "PA": ("pts", "ast"),
    "RA": ("reb", "ast"), "P+R": ("pts", "reb"), "P+A": ("pts", "ast"),
    "R+A": ("reb", "ast"), "P+R+A": ("pts", "reb", "ast"),
}


def _to_int(value):
    try:
        return int(str(value).strip())
    except (ValueError, AttributeError, TypeError):
        return 0


def _minutes_to_int(value):
    raw = str(value or "").strip()
    if not raw or raw == "--":
        return 0
    if ":" in raw:
        raw = raw.split(":")[0]
    return _to_int(raw)


def _fetch_scoreboard():
    resp = requests.get(SCOREBOARD_URL, timeout=12)
    resp.raise_for_status()
    return resp.json().get("events", [])


def _fetch_summary(event_id):
    resp = requests.get(SUMMARY_URL, params={"event": event_id}, timeout=12)
    resp.raise_for_status()
    return resp.json()


def _parse_competitors(competition):
    home, away = {}, {}
    for c in competition.get("competitors", []):
        team = c.get("team", {})
        side = {
            "abbr": team.get("abbreviation", ""),
            "name": team.get("shortDisplayName") or team.get("displayName", ""),
            "logo": team.get("logo", ""),
            "score": _to_int(c.get("score", 0)),
        }
        if c.get("homeAway") == "home":
            home = side
        else:
            away = side
    return home, away


def _parse_team_box(team_block):
    """Yield per-player live stat dicts from one team's boxscore block."""
    team_abbr = team_block.get("team", {}).get("abbreviation", "")
    out = []
    for statset in team_block.get("statistics", []):
        labels = statset.get("labels", [])
        idx = {label: i for i, label in enumerate(labels)}
        for ath in statset.get("athletes", []):
            if ath.get("didNotPlay"):
                continue
            stats = ath.get("stats") or []
            if not stats:
                continue

            def grab(label):
                i = idx.get(label)
                return stats[i] if i is not None and i < len(stats) else "0"

            minutes = _minutes_to_int(grab("MIN"))
            if minutes <= 0:
                continue
            pts = _to_int(grab("PTS"))
            reb = _to_int(grab("REB"))
            ast = _to_int(grab("AST"))
            to = _to_int(grab("TO"))
            stl = _to_int(grab("STL"))
            blk = _to_int(grab("BLK"))
            threes_raw = grab("3PT")
            threes = _to_int(str(threes_raw).split("-")[0]) if threes_raw else 0
            athlete = ath.get("athlete", {})
            name = athlete.get("displayName", "")
            if not name:
                continue
            out.append({
                "name": name,
                "normalized": normalize_player_name(name),
                "team": team_abbr,
                "min": minutes,
                "pts": pts, "reb": reb, "ast": ast,
                "stl": stl, "blk": blk, "to": to, "threes": threes,
                "fp": round(calc_fanduel_fp(pts, reb, ast, stl, blk, to), 1),
            })
    return out


def _temp_read(player, pace_map):
    """Pace-based hot/cold: live FP-per-minute vs season FP-per-minute."""
    if player["min"] < MIN_MINUTES_FOR_TEMP:
        return "neutral"
    pace = pace_map.get(player["normalized"])
    if not pace:
        return "neutral"
    fp_avg, min_avg = pace
    if not fp_avg or not min_avg:
        return "neutral"
    season_rate = fp_avg / min_avg
    if season_rate <= 0:
        return "neutral"
    ratio = (player["fp"] / player["min"]) / season_rate
    if ratio >= HOT_RATIO:
        return "hot"
    if ratio <= COLD_RATIO:
        return "cold"
    return "neutral"


def _pick_read(player, picks_map):
    """If the player is in tonight's picks, report live progress vs the line."""
    pick = picks_map.get(player["normalized"])
    if not pick:
        return None
    stat = str(pick.get("stat", "")).upper().replace(" ", "")
    parts = PICK_STAT_PARTS.get(stat)
    if not parts:
        return None
    current = sum(player.get(p, 0) for p in parts)
    line = pick.get("line")
    side = str(pick.get("side", "")).upper()
    status = "even"
    if line is not None and side in ("OVER", "UNDER"):
        if side == "OVER":
            status = "winning" if current > line else ("even" if current == line else "trailing")
        else:
            status = "winning" if current < line else ("even" if current == line else "trailing")
    return {"stat": pick.get("stat", stat), "side": side, "line": line,
            "current": current, "status": status}


def _build_pace_map():
    try:
        from backend import data_access
        df = data_access.get_wnba_fp_pace()
        if df is None or df.empty:
            return {}
        out = {}
        for _, r in df.iterrows():
            out[normalize_player_name(str(r["player_name"]))] = (
                float(r["fp_avg"]) if r["fp_avg"] is not None else 0.0,
                float(r["min_avg"]) if r["min_avg"] is not None else 0.0,
            )
        return out
    except Exception as e:
        print(f"[LIVE WNBA] pace map load failed ({e})")
        return {}


def _build_picks_map(game_date):
    try:
        from backend import data_access
        picks = data_access.get_wnba_official_picks(game_date)
        out = {}
        for p in picks or []:
            name = p.get("player") or p.get("name")
            if not name:
                continue
            line = p.get("book_line", p.get("line"))
            try:
                line = float(line) if line is not None and line != "" else None
            except (ValueError, TypeError):
                line = None
            out[normalize_player_name(str(name))] = {
                "stat": p.get("stat", ""),
                "side": (p.get("pick") or p.get("side") or p.get("recommendation") or "").upper(),
                "line": line,
            }
        return out
    except Exception as e:
        print(f"[LIVE WNBA] picks map load failed ({e})")
        return {}


def get_wnba_live_scoreboard(game_date=None):
    """Return today's WNBA games with live player box scores and hot/cold reads.

    Shape: {has_games, updated, games: [{id, state, status, home, away, players}]}.
    Degrades to {has_games: False} on any upstream failure.
    """
    if game_date is None:
        game_date = get_eastern_date_str()

    try:
        events = _fetch_scoreboard()
    except Exception as e:
        print(f"[LIVE WNBA] scoreboard fetch failed ({e})")
        return {"has_games": False, "games": [], "updated": game_date, "error": "feed_unavailable"}

    if not events:
        return {"has_games": False, "games": [], "updated": game_date}

    pace_map = _build_pace_map()
    picks_map = _build_picks_map(game_date)

    games = []
    for ev in events:
        try:
            comp = ev.get("competitions", [{}])[0]
            status = ev.get("status", {}).get("type", {})
            state = status.get("state", "pre")
            home, away = _parse_competitors(comp)
            game = {
                "id": ev.get("id"),
                "state": state,
                "status": status.get("shortDetail") or status.get("detail", ""),
                "home": home,
                "away": away,
                "players": [],
            }
            if state in ("in", "post"):
                summary = _fetch_summary(ev.get("id"))
                players = []
                for team_block in summary.get("boxscore", {}).get("players", []):
                    players.extend(_parse_team_box(team_block))
                for pl in players:
                    pl["temp"] = _temp_read(pl, pace_map)
                    pl["pick"] = _pick_read(pl, picks_map)
                players.sort(key=lambda p: p["fp"], reverse=True)
                game["players"] = players
            games.append(game)
        except Exception as e:
            print(f"[LIVE WNBA] game parse failed for {ev.get('id')} ({e})")
            continue

    return {
        "has_games": bool(games),
        "any_live": any(g["state"] == "in" for g in games),
        "updated": game_date,
        "games": games,
    }


if __name__ == "__main__":
    board = get_wnba_live_scoreboard()
    print(f"has_games={board['has_games']} any_live={board.get('any_live')} games={len(board['games'])}")
    for g in board["games"]:
        print(f"\n{g['away'].get('abbr')} {g['away'].get('score')} @ "
              f"{g['home'].get('abbr')} {g['home'].get('score')}  [{g['state']}] {g['status']}")
        for p in g["players"][:6]:
            badge = {"hot": "HOT", "cold": "COLD"}.get(p["temp"], "  ")
            pick = ""
            if p.get("pick"):
                pick = f"  PICK {p['pick']['side']} {p['pick']['line']} {p['pick']['stat']} -> {p['pick']['status']} ({p['pick']['current']})"
            print(f"  {badge:4s} {p['name']:24s} {p['fp']:5.1f}FP  "
                  f"{p['pts']}p {p['reb']}r {p['ast']}a{pick}")
