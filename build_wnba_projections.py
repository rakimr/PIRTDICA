"""
WNBA projection engine for PIRTDICA SPORTS CO.

This is the WNBA mirror of the NBA projection + prop-recommendation chain. It is
intentionally self-contained and league-agnostic in its math, fed entirely by the
WNBA game logs + season stats (scrape_wnba_gamelogs.py) and the WNBA FanDuel prop
lines (scrape_wnba_props.py).

Outputs (full-refresh SQLite tables):
  - wnba_dvp                 : per-team defensive factor by stat (from logs)
  - wnba_projections         : per-player per-stat projection (all players)
  - wnba_prop_recommendations: slate props joined to projections + edges, hit
                               rate, CV, composite score, confidence
  - wnba_player_value        : per-player value points for the charts
Also writes wnba_prop_recommendations.csv for the header generator / charts.

Honest scope: there is no WNBA referee data, Synergy play-type data, or NBA.com
shot-zone tracking, so those NBA adjustments are simply absent here. The matchup
adjustment (DVP) is derived purely from the game logs we DO have.
"""
import csv
import sqlite3
import unicodedata
from datetime import datetime

DB = "dfs_nba.db"

# prop stat code -> the column family we computed in wnba_player_stats / logs
STAT_MAP = {"PTS": "pts", "REB": "reb", "AST": "ast", "3PM": "fg3m"}


def norm(name):
    s = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode()
    return "".join(ch for ch in s.lower() if ch.isalnum())


def _mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


# ----------------------------------------------------------------------------
# DVP: how much each defending team inflates/deflates a stat vs league average,
# derived from the box lines opponents posted against them.
# ----------------------------------------------------------------------------
def build_dvp(cur):
    cur.execute("DROP TABLE IF EXISTS wnba_dvp")
    cur.execute("CREATE TABLE wnba_dvp (team TEXT, stat TEXT, factor REAL, PRIMARY KEY (team, stat))")
    # Valid WNBA opponents = the real franchises our players play for. Preseason
    # exhibitions vs national teams (e.g. NIGER, JPN) leak into the opp column;
    # restricting to actual franchises keeps those out of the DVP ratings.
    valid_teams = {
        r[0] for r in cur.execute(
            "SELECT DISTINCT team FROM wnba_player_game_logs WHERE team != ''"
        ).fetchall()
    }
    rows = cur.execute(
        "SELECT opp, pts, reb, ast, fg3m FROM wnba_player_game_logs WHERE opp != ''"
    ).fetchall()
    factors = {}
    for skey_idx, skey in enumerate(["pts", "reb", "ast", "fg3m"], start=1):
        by_team = {}
        all_vals = []
        for r in rows:
            opp = r[0]
            if opp not in valid_teams:
                continue
            val = r[skey_idx]
            by_team.setdefault(opp, []).append(val)
            all_vals.append(val)
        league = _mean(all_vals) or 1.0
        for team, vals in by_team.items():
            factor = (_mean(vals) / league) if league else 1.0
            factors[(team, skey)] = round(factor, 3)
    for (team, stat), factor in factors.items():
        cur.execute("INSERT INTO wnba_dvp VALUES (?,?,?)", (team, stat, factor))
    return factors


def build_dvp_position(cur):
    """Defense vs Position (G/F/C): how much of each stat a defense gives up to a
    position group vs the league average for that position. Unlike build_dvp (which
    is team x stat only), this buckets opponents' allowed box lines by the scoring
    player's position, so the chart and article can read a true positional matchup.
    Derived entirely from wnba_player_game_logs joined to the G/F/C label in
    wnba_player_stats // there is no third-party WNBA DvP source. Stat 'fp' powers
    the heatmap; pts/reb/ast/fg3m feed per-stat positional reads in the article."""
    cur.execute("DROP TABLE IF EXISTS wnba_dvp_position")
    cur.execute("CREATE TABLE wnba_dvp_position "
                "(team TEXT, position TEXT, stat TEXT, factor REAL, "
                "PRIMARY KEY (team, position, stat))")
    valid_teams = {
        r[0] for r in cur.execute(
            "SELECT DISTINCT team FROM wnba_player_game_logs WHERE team != ''"
        ).fetchall()
    }
    pos_map = {}
    for name, pos in cur.execute(
            "SELECT player_name, position FROM wnba_player_stats"):
        p = (pos or "").strip().upper()
        if name and p in ("G", "F", "C"):
            pos_map[name] = p
    rows = cur.execute(
        "SELECT player_name, opp, pts, reb, ast, fg3m, fp "
        "FROM wnba_player_game_logs WHERE opp != ''"
    ).fetchall()
    idx = {"pts": 2, "reb": 3, "ast": 4, "fg3m": 5, "fp": 6}
    factors = {}
    for stat, si in idx.items():
        by_tp = {}     # (team, position) -> [stat values that team allowed]
        by_pos = {}    # position -> [league-wide stat values for that position]
        for r in rows:
            opp = r[1]
            if opp not in valid_teams:
                continue
            pos = pos_map.get(r[0])
            if not pos:
                continue
            val = r[si]
            if val is None:
                continue
            by_tp.setdefault((opp, pos), []).append(val)
            by_pos.setdefault(pos, []).append(val)
        pos_league = {p: (_mean(v) or 0.0) for p, v in by_pos.items()}
        for (team, pos), vals in by_tp.items():
            if len(vals) < 5:        # too few games for a stable read // stay neutral
                factor = 1.0
            else:
                lg = pos_league.get(pos) or 0.0
                factor = (_mean(vals) / lg) if lg else 1.0
            factors[(team, pos, stat)] = round(factor, 3)
    for (team, pos, stat), factor in factors.items():
        cur.execute("INSERT INTO wnba_dvp_position VALUES (?,?,?,?)",
                    (team, pos, stat, factor))
    return factors


def applied_factor(factors, team, skey):
    """Dampen the raw DVP factor so a soft/hard matchup nudges, not dominates."""
    raw = factors.get((team, skey), 1.0)
    return 1.0 + 0.5 * (raw - 1.0)


# ----------------------------------------------------------------------------
# Projections: per-player per-stat, recency-weighted with a minutes-rate blend.
# ----------------------------------------------------------------------------
def project_value(avg, last5, sd, min_avg, min_l5):
    if avg <= 0:
        return 0.0
    proj_min = 0.6 * (min_l5 or min_avg) + 0.4 * (min_avg or min_l5)
    rate = (avg / min_avg) if min_avg else 0.0
    level = 0.6 * last5 + 0.4 * avg               # recency-weighted level
    minutes_proj = rate * proj_min                # minutes-driven projection
    return round(0.5 * level + 0.5 * minutes_proj, 1)


def build_projections(cur):
    cur.execute("DROP TABLE IF EXISTS wnba_projections")
    cur.execute("""CREATE TABLE wnba_projections (
        player_name TEXT, team TEXT, stat TEXT, games INTEGER,
        season_avg REAL, last5_avg REAL, sd REAL, cv REAL,
        min_avg REAL, projected REAL,
        PRIMARY KEY (player_name, stat))""")
    players = cur.execute("""SELECT player_name, team, games, min_avg, min_l5,
        pts_avg, pts_l5, pts_sd, reb_avg, reb_l5, reb_sd,
        ast_avg, ast_l5, ast_sd, fg3m_avg, fg3m_l5, fg3m_sd
        FROM wnba_player_stats WHERE games >= 1""").fetchall()
    cols = {
        "pts": (5, 6, 7), "reb": (8, 9, 10), "ast": (11, 12, 13), "fg3m": (14, 15, 16),
    }
    out = []
    for p in players:
        name, team, games, min_avg, min_l5 = p[0], p[1], p[2], p[3], p[4]
        for skey, (ia, il, isd) in cols.items():
            avg, last5, sd = p[ia], p[il], p[isd]
            cv = round(sd / avg, 3) if avg else 0.0
            proj = project_value(avg, last5, sd, min_avg, min_l5)
            out.append((name, team, skey, games, avg, last5, sd, cv, min_avg, proj))
    cur.executemany("INSERT INTO wnba_projections VALUES (?,?,?,?,?,?,?,?,?,?)", out)
    return len(out)


# ----------------------------------------------------------------------------
# Prop recommendations: slate props joined to projections + DVP, with hit rate,
# CV, composite score, and a strict-ish confidence gate.
# ----------------------------------------------------------------------------
def hit_rate(cur, espn_or_name, skey, line, side):
    """Fraction of the player's games that landed on the pick side of the line."""
    vals = cur.execute(
        f"SELECT {skey} FROM wnba_player_game_logs WHERE player_name = ?",
        (espn_or_name,)).fetchall()
    vals = [v[0] for v in vals]
    if not vals:
        return 0.0
    if side == "OVER":
        hits = sum(1 for v in vals if v > line)
    else:
        hits = sum(1 for v in vals if v < line)
    return round(100.0 * hits / len(vals), 0)


def composite(abs_edge, hr, cv, dva_supports, env_penalty=0.0):
    edge_c = min(abs_edge, 30.0) / 30.0 * 100.0
    cons_c = max(0.0, 1.0 - cv) * 100.0
    score = 0.40 * edge_c + 0.35 * hr + 0.25 * cons_c
    if dva_supports:
        score += 4.0
    score -= env_penalty
    return round(min(max(score, 0.0), 100.0), 1)


# Game-environment thresholds (LENIENT by design: close games are common but
# OTs are rare, so these only nudge the score, they never veto a pick).
TIGHT_SPREAD = 4.0       # |spread| <= this -> tight game, minutes/OT upside
BLOWOUT_SPREAD = 11.0    # |spread| >= this -> garbage-time benching risk
HIGH_MINUTES = 30.0      # players who carry the extra minutes in tight games
ENV_PENALTY = 3.0        # small composite nudge, ~worth one DVA support


def game_env_penalty(side, spread, min_avg):
    """Small composite penalty when the game script works against the pick.

    Tight game + UNDER on a heavy-minutes player: a close 4th (or the rare
    OT) keeps her on the floor piling up counting stats.
    Big spread + OVER: a blowout benches starters early."""
    if spread is None:
        return 0.0
    if side == "UNDER" and abs(spread) <= TIGHT_SPREAD and (min_avg or 0) >= HIGH_MINUTES:
        return ENV_PENALTY
    if side == "OVER" and abs(spread) >= BLOWOUT_SPREAD:
        return ENV_PENALTY
    return 0.0


def confidence(abs_edge, hr, cv, games, high_edge=8.0):
    if games >= 5 and abs_edge >= high_edge and hr >= 60 and cv <= 0.6:
        return "HIGH"
    if abs_edge >= 5 and hr >= 55 and cv <= 0.8:
        return "MEDIUM"
    return "LOW"


def build_prop_recs(cur, factors):
    cur.execute("DROP TABLE IF EXISTS wnba_prop_recommendations")
    cur.execute("""CREATE TABLE wnba_prop_recommendations (
        player TEXT, team TEXT, opponent TEXT, stat TEXT, book_line REAL,
        player_avg REAL, last5_avg REAL, projected_value REAL, vs_book_edge REAL,
        recommendation TEXT, hit_rate REAL, cv REAL, composite_score REAL,
        confidence TEXT, over_odds REAL, under_odds REAL, game_date TEXT,
        game_spread REAL, game_total REAL)""")

    # Lenient data-driven HIGH gate from the edge-honesty tracker. Any failure
    # (no network to Postgres, empty grades) falls back to the static default.
    try:
        from analysis.edge_calibration import get_dynamic_high_edge_threshold
        high_edge = get_dynamic_high_edge_threshold("wnba")
    except Exception:
        high_edge = 8.0

    # Game odds keyed by (home, away, game_date) so a rematch on a later date
    # in the fetched window can never overwrite tonight's odds.
    game_env = {}
    try:
        for home, away, gdate_g, spread, total in cur.execute(
                "SELECT home_team, away_team, game_date, home_spread, game_total FROM wnba_games"):
            game_env[(home, away, gdate_g)] = (spread, total)
    except sqlite3.OperationalError:
        pass  # pre-migration wnba_games without odds columns

    name_to_abbr = {r[0]: r[1] for r in cur.execute("SELECT name, abbr FROM wnba_teams").fetchall()}
    # projection lookup keyed by normalized name + stat
    proj_lookup = {}
    for r in cur.execute("""SELECT player_name, team, stat, games, season_avg,
        last5_avg, sd, cv, projected, min_avg FROM wnba_projections""").fetchall():
        proj_lookup[(norm(r[0]), r[2])] = r
    # exact-name lookup for game-log hit rates
    name_lookup = {norm(r[0]): r[0] for r in
                   cur.execute("SELECT DISTINCT player_name FROM wnba_player_game_logs").fetchall()}

    props = cur.execute("""SELECT player_name, stat, line, over_odds, under_odds,
        home_team, away_team, game_date FROM wnba_props""").fetchall()

    recs = []
    for pr in props:
        pname, pstat, line, oo, uo, home, away, gdate = pr
        skey = STAT_MAP.get(pstat)
        if not skey:
            continue
        proj_row = proj_lookup.get((norm(pname), skey))
        if not proj_row:
            continue
        _, pteam, _, games, season_avg, last5_avg, sd, cv, projected, min_avg = proj_row
        home_abbr = name_to_abbr.get(home, "")
        away_abbr = name_to_abbr.get(away, "")
        opponent = away_abbr if pteam == home_abbr else home_abbr
        # apply opponent DVP to the projection for THIS matchup
        af = applied_factor(factors, opponent, skey) if opponent else 1.0
        adj_proj = round(projected * af, 1)
        if line is None or line <= 0:
            continue
        edge = round((adj_proj - line) / line * 100.0, 1)
        side = "OVER" if adj_proj >= line else "UNDER"
        real_name = name_lookup.get(norm(pname), pname)
        hr = hit_rate(cur, real_name, skey, line, side)
        raw_factor = factors.get((opponent, skey), 1.0)
        dva_supports = (raw_factor > 1.0 and side == "OVER") or (raw_factor < 1.0 and side == "UNDER")
        spread, total = game_env.get((home, away, gdate), (None, None))
        env_pen = game_env_penalty(side, spread, min_avg)
        comp = composite(abs(edge), hr, cv, dva_supports, env_pen)
        conf = confidence(abs(edge), hr, cv, games, high_edge)
        recs.append((real_name, pteam, opponent, pstat, line, season_avg, last5_avg,
                     adj_proj, edge, side, hr, cv, comp, conf, oo, uo, gdate,
                     spread, total))

    recs.sort(key=lambda r: r[12], reverse=True)  # by composite desc
    cur.executemany(
        "INSERT INTO wnba_prop_recommendations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", recs)

    # CSV for the header generator / charts (mirror NBA prop_recommendations.csv cols)
    with open("wnba_prop_recommendations.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["player", "team", "opponent", "stat", "book_line", "player_avg",
                    "last5_avg", "projected_value", "vs_book_edge", "recommendation",
                    "hit_rate", "cv", "composite_score", "confidence",
                    "over_odds", "under_odds", "game_date", "game_spread", "game_total"])
        w.writerows(recs)
    return len(recs)


# ----------------------------------------------------------------------------
# Value table for the charts (usage proxy + fantasy points).
# ----------------------------------------------------------------------------
def build_player_value(cur):
    cur.execute("DROP TABLE IF EXISTS wnba_player_value")
    cur.execute("""CREATE TABLE wnba_player_value (
        player_name TEXT PRIMARY KEY, team TEXT, position TEXT, games INTEGER,
        min_avg REAL, fp_avg REAL, fp_sd REAL, fp_l5 REAL,
        pts_avg REAL, reb_avg REAL, ast_avg REAL, usage_proxy REAL)""")
    rows = cur.execute("""SELECT player_name, team, position, games, min_avg,
        fp_avg, fp_sd, fp_l5, pts_avg, reb_avg, ast_avg
        FROM wnba_player_stats WHERE games >= 1""").fetchall()
    out = []
    for r in rows:
        name, team, pos, games, min_avg, fp_avg, fp_sd, fp_l5, pts, reb, ast = r
        # usage proxy: scoring + playmaking load per minute (no tracking data exists)
        usage = round(((pts + 1.2 * ast + 0.5 * reb) / min_avg) if min_avg else 0.0, 3)
        out.append((name, team, pos, games, min_avg, fp_avg, fp_sd, fp_l5, pts, reb, ast, usage))
    cur.executemany("INSERT INTO wnba_player_value VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", out)
    return len(out)


def main():
    print("=== WNBA projection engine ===")
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    factors = build_dvp(cur)
    print(f"DVP factors: {len(factors)}")
    pos_factors = build_dvp_position(cur)
    print(f"DVP-by-position factors: {len(pos_factors)}")
    n_proj = build_projections(cur)
    print(f"Projections: {n_proj}")
    n_val = build_player_value(cur)
    print(f"Player value rows: {n_val}")
    n_recs = build_prop_recs(cur, factors)
    print(f"Prop recommendations: {n_recs}")
    conn.commit()
    conn.close()
    print(f"Done at {datetime.now().isoformat(timespec='seconds')}.")


if __name__ == "__main__":
    main()
