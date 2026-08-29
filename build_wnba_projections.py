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
import json
import sqlite3
import unicodedata
from datetime import datetime

DB = "dfs_nba.db"

# prop stat code -> the column family we computed in wnba_player_stats / logs
STAT_MAP = {
    "PTS": "pts", "REB": "reb", "AST": "ast", "3PM": "fg3m",
    "STL": "stl", "BLK": "blk",
}
MODEL_VERSION = "wnba_empirical_minutes_rates_v1"
POSITION_DVP_MIN = 10
TEAM_DVP_MIN = 20
UNAVAILABLE_COMPONENT = "unavailable_no_direct_evidence"

# The real WNBA franchises (ESPN abbreviations as they appear in the game
# logs). Exhibition opponents around All-Star weekend (national teams like
# JPN, NIGER) leak into the logs' team AND opp columns, so any whitelist
# derived from the data itself gets poisoned // keep this list explicit.
WNBA_FRANCHISES = (
    "ATL", "CHI", "CON", "DAL", "GS", "IND", "LA", "LV",
    "MIN", "NY", "PHX", "POR", "SEA", "TOR", "WSH",
)


def norm(name):
    s = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode()
    return "".join(ch for ch in s.lower() if ch.isalnum())


def _mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def _position_group(position):
    """Collapse roster labels such as PG, SG, F-C to the existing G/F/C grain."""
    p = (position or "").strip().upper()
    if p in ("G", "PG", "SG") or p.startswith("G"):
        return "G"
    if p in ("F", "PF", "SF") or p.startswith("F") or p.startswith("P"):
        return "F"
    if p == "C" or p.startswith("C"):
        return "C"
    return ""


# ----------------------------------------------------------------------------
# DVP: how much each defending team inflates/deflates a stat vs league average,
# derived from the box lines opponents posted against them.
# ----------------------------------------------------------------------------
def build_dvp(cur):
    cur.execute("DROP TABLE IF EXISTS wnba_dvp")
    cur.execute("CREATE TABLE wnba_dvp (team TEXT, stat TEXT, factor REAL, PRIMARY KEY (team, stat))")
    # Valid WNBA opponents = the real franchises only. Exhibitions vs national
    # teams (e.g. NIGER, JPN around All-Star weekend) leak into BOTH the team
    # and opp columns of the game logs, so deriving the whitelist from the data
    # self-poisons (JPN showed up as a "team" in the DVP heatmaps with garbage
    # factors). Use the explicit franchise list instead.
    valid_teams = WNBA_FRANCHISES
    rows = cur.execute(
        "SELECT opp, pts, reb, ast, fg3m, stl, blk FROM wnba_player_game_logs "
        "WHERE opp != '' AND team IN (%s)" % ",".join("?" * len(WNBA_FRANCHISES)),
        tuple(WNBA_FRANCHISES),
    ).fetchall()
    factors = {}
    for skey_idx, skey in enumerate(
            ["pts", "reb", "ast", "fg3m", "stl", "blk"], start=1):
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
    # Same explicit whitelist as build_dvp // see the exhibition-leak note there.
    valid_teams = WNBA_FRANCHISES
    pos_map = {}
    for name, pos in cur.execute(
            "SELECT player_name, position FROM wnba_player_stats"):
        p = _position_group(pos)
        if name and p:
            pos_map[name] = p
    rows = cur.execute(
        "SELECT player_name, opp, pts, reb, ast, fg3m, fp, stl, blk "
        "FROM wnba_player_game_logs "
        "WHERE opp != '' AND team IN (%s)" % ",".join("?" * len(WNBA_FRANCHISES)),
        tuple(WNBA_FRANCHISES),
    ).fetchall()
    idx = {
        "pts": 2, "reb": 3, "ast": 4, "fg3m": 5, "fp": 6,
        "stl": 7, "blk": 8,
    }
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
    raw = min(1.20, max(0.80, factors.get((team, skey), 1.0)))
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


def _quantile(values, q):
    """Deterministic linear empirical quantile (the input is never mutated)."""
    if not values:
        return None
    ordered = sorted(max(0.0, float(v)) for v in values)
    if len(ordered) == 1:
        return ordered[0]
    at = (len(ordered) - 1) * q
    lo = int(at)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (at - lo)


def empirical_profile(cur, player_name, skey, cutoff=None, model_mean=None):
    """Build a reproducible minutes x per-minute-rate predictive distribution.

    ``cutoff`` is exclusive, ensuring a slate can only use games completed
    before its date. Cross-joining observed minutes and rates avoids random
    simulation while retaining both sources of player-level uncertainty.
    Counting-stat scenarios are rounded to valid discrete outcomes, which
    makes integer prop pushes explicit rather than silently assigning them to
    one side.
    """
    where = "player_name = ? AND min > 0"
    params = [player_name]
    if cutoff:
        where += " AND game_date < ?"
        params.append(cutoff)
    try:
        rows = cur.execute(
            f"SELECT game_date, min, {skey} FROM wnba_player_game_logs "
            f"WHERE {where} AND {skey} IS NOT NULL ORDER BY game_date DESC",
            tuple(params),
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    rows = [r for r in rows if r[1] is not None and r[1] > 0 and r[2] is not None]
    if not rows:
        return None
    minutes = [float(r[1]) for r in rows]
    rates = [max(0.0, float(r[2])) / float(r[1]) for r in rows]
    recent_minutes = minutes[:5]
    recent_rates = rates[:5]
    projected_minutes = 0.6 * _mean(recent_minutes) + 0.4 * _mean(minutes)
    # This is a realized box-score outcome rate, not a physical opportunity
    # (touch, shot, potential assist, etc.) rate. We do not have tracking
    # inputs that would support splitting opportunity from conversion.
    outcome_rate = 0.6 * _mean(recent_rates) + 0.4 * _mean(rates)
    target = (float(model_mean) if model_mean is not None
              else projected_minutes * outcome_rate)
    raw = [m * rate for m in minutes for rate in rates]
    raw_mean = _mean(raw)
    scale = target / raw_mean if raw_mean > 0 else 0.0
    distribution = [float(round(max(0.0, value * scale))) for value in raw]
    n = len(rows)
    confidence = "HIGH" if n >= 20 else ("MEDIUM" if n >= 10 else "LOW")
    return {
        # Keep the exact empirical mean so it is arithmetically identical to
        # the stored scenario distribution; presentation layers may round it.
        "model_mean": _mean(distribution),
        "projected_minutes": round(projected_minutes, 3),
        "outcome_rate": round(outcome_rate, 5),
        "q10": round(_quantile(distribution, 0.10), 3),
        "q25": round(_quantile(distribution, 0.25), 3),
        "q50": round(_quantile(distribution, 0.50), 3),
        "q75": round(_quantile(distribution, 0.75), 3),
        "q90": round(_quantile(distribution, 0.90), 3),
        "distribution": distribution,
        "distribution_method": "deterministic_empirical_minutes_x_rate",
        "model_version": MODEL_VERSION,
        "profile_confidence": confidence,
        "sample_games": n,
        "cutoff": cutoff,
        "observed_values": [float(r[2]) for r in rows],
        "observed_minutes": minutes,
    }


def latest_player_team(cur, player_name, cutoff):
    """Return the player's latest logged team strictly before a slate cutoff."""
    try:
        row = cur.execute(
            "SELECT team FROM wnba_player_game_logs "
            "WHERE player_name = ? AND game_date < ? AND team != '' "
            "ORDER BY game_date DESC LIMIT 1",
            (player_name, cutoff),
        ).fetchone()
        return row[0] if row else None
    except sqlite3.OperationalError:
        return None


def distribution_probabilities(distribution, line):
    """Return over/under/push mass; all three sum to one."""
    if not distribution or line is None:
        return None, None, None
    n = float(len(distribution))
    over = sum(v > line for v in distribution) / n
    under = sum(v < line for v in distribution) / n
    push = sum(v == line for v in distribution) / n
    return round(over, 4), round(under, 4), round(push, 4)


def american_implied_probability(odds):
    """Convert valid American odds to implied probability, or return None."""
    try:
        odds = float(odds)
        if odds == 0:
            return None
        return ((100.0 / (odds + 100.0)) if odds > 0
                else (-odds / (-odds + 100.0)))
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def priced_distribution_sides(distribution, line, over_odds, under_odds):
    """Price both prop sides from an empirical distribution.

    A push is excluded from each side's *conditional* win rate, while expected
    ROI retains its true economic treatment: pushes return the stake. Market
    probabilities are normalized across the two outcomes to remove book vig.
    """
    p_over, p_under, p_push = distribution_probabilities(distribution, line)
    if p_over is None:
        return None
    denom = p_over + p_under
    conditional = {
        "OVER": p_over / denom if denom else 0.0,
        "UNDER": p_under / denom if denom else 0.0,
    }
    implied_over = american_implied_probability(over_odds)
    implied_under = american_implied_probability(under_odds)
    implied_total = (implied_over + implied_under
                     if implied_over is not None and implied_under is not None else None)
    market = ({"OVER": implied_over / implied_total,
               "UNDER": implied_under / implied_total} if implied_total else
              {"OVER": None, "UNDER": None})
    odds = {"OVER": over_odds, "UNDER": under_odds}
    masses = {"OVER": (p_over, p_under), "UNDER": (p_under, p_over)}
    sides = {}
    for side in ("OVER", "UNDER"):
        win, loss = masses[side]
        price = odds[side]
        try:
            price = float(price)
            profit_if_win = price / 100.0 if price > 0 else 100.0 / -price
            roi = win * profit_if_win - loss
        except (TypeError, ValueError, ZeroDivisionError):
            price, roi = None, None
        edge = (100.0 * (conditional[side] - market[side])
                if market[side] is not None else None)
        sides[side] = {
            "selected_probability": conditional[side],
            "market_no_vig_probability": market[side],
            "probability_edge_pp": edge,
            "expected_roi": roi,
            "selected_odds": price,
        }
    # Actual offered price is decisive when available. Without a priced side,
    # fall back to the probability comparison (or conditional win probability
    # when a two-sided market probability cannot be formed).
    priced = [(side, data) for side, data in sides.items()
              if data["expected_roi"] is not None]
    if priced:
        selected = max(priced, key=lambda item: (
            item[1]["expected_roi"], item[1]["probability_edge_pp"]
            if item[1]["probability_edge_pp"] is not None else -999.0,
            item[0] == "OVER"))[0]
    else:
        selected = max(sides, key=lambda side: (
            sides[side]["probability_edge_pp"]
            if sides[side]["probability_edge_pp"] is not None
            else sides[side]["selected_probability"],
            side == "OVER"))
    return p_over, p_under, p_push, selected, sides[selected]


def _matchup_factor(cur, opponent, position, skey, cutoff=None):
    """Choose one supported DVP level, shrink it, and bound its influence."""
    evidence = {"matchup_source": "neutral", "matchup_sample": 0,
                "raw_matchup_factor": 1.0, "applied_matchup_factor": 1.0}
    if not opponent:
        return 1.0, evidence
    date_sql = " AND l.game_date < ?" if cutoff else ""
    pos_map = {}
    try:
        pos_map = {name: _position_group(pos) for name, pos in cur.execute(
            "SELECT player_name, position FROM wnba_player_stats")}
        rows = cur.execute(
            f"SELECT l.player_name, l.opp, l.{skey} FROM wnba_player_game_logs l "
            f"WHERE l.team IN ({','.join('?' * len(WNBA_FRANCHISES))})"
            + date_sql,
            tuple(WNBA_FRANCHISES) + ((cutoff,) if cutoff else ()),
        ).fetchall()
    except sqlite3.OperationalError:
        return 1.0, evidence
    valid = [(name, opp, float(value)) for name, opp, value in rows
             if opp in WNBA_FRANCHISES and value is not None]
    candidate = None
    if position in ("G", "F", "C"):
        allowed = [v for name, opp, v in valid
                   if opp == opponent and pos_map.get(name) == position]
        baseline = [v for name, _opp, v in valid if pos_map.get(name) == position]
        if len(allowed) >= POSITION_DVP_MIN and baseline and _mean(baseline) > 0:
            candidate = ("position_dvp", allowed, baseline)
    if candidate is None:
        allowed = [v for _name, opp, v in valid if opp == opponent]
        baseline = [v for _name, _opp, v in valid]
        if len(allowed) >= TEAM_DVP_MIN and baseline and _mean(baseline) > 0:
            candidate = ("team_dvp", allowed, baseline)
    if candidate is None:
        return 1.0, evidence
    source, allowed, baseline = candidate
    raw = _mean(allowed) / _mean(baseline)
    bounded = min(1.20, max(0.80, raw))
    shrink = len(allowed) / (len(allowed) + 20.0)
    applied = 1.0 + shrink * (bounded - 1.0)
    evidence.update({
        "matchup_source": source, "matchup_sample": len(allowed),
        "raw_matchup_factor": round(raw, 4),
        "bounded_matchup_factor": round(bounded, 4),
        "shrinkage_weight": round(shrink, 4),
        "applied_matchup_factor": round(applied, 4),
    })
    return applied, evidence


def build_projections(cur, cutoff=None):
    cur.execute("DROP TABLE IF EXISTS wnba_projections")
    cur.execute("""CREATE TABLE wnba_projections (
        player_name TEXT, team TEXT, stat TEXT, games INTEGER,
        season_avg REAL, last5_avg REAL, sd REAL, cv REAL,
        min_avg REAL, projected REAL,
        model_mean REAL, q10 REAL, q25 REAL, q50 REAL, q75 REAL, q90 REAL,
        distribution_method TEXT, model_version TEXT, projected_minutes REAL,
        opportunity_rate REAL, outcome_rate REAL,
        opportunity_component TEXT, conversion_component TEXT,
        profile_confidence TEXT, evidence_json TEXT,
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
        if cutoff:
            team = latest_player_team(cur, name, cutoff)
        for skey, (ia, il, isd) in cols.items():
            if cutoff:
                # Every core input is reconstructed as-of cutoff. Current
                # aggregate rows must not leak later production into a
                # historical projection or become a rescaling target.
                profile = empirical_profile(cur, name, skey, cutoff)
                if profile:
                    observed = profile["observed_values"]
                    observed_minutes = profile["observed_minutes"]
                    games = len(observed)
                    avg = _mean(observed)
                    last5 = _mean(observed[:5])
                    sd = ((sum((v - avg) ** 2 for v in observed) / (games - 1)) ** 0.5
                          if games > 1 else 0.0)
                    min_avg = _mean(observed_minutes)
                    min_l5 = _mean(observed_minutes[:5])
                    proj = round(profile["model_mean"], 1)
                else:
                    avg = last5 = sd = min_avg = min_l5 = proj = None
                    games = 0
            else:
                avg, last5, sd = p[ia], p[il], p[isd]
                proj = project_value(avg, last5, sd, min_avg, min_l5)
                profile = empirical_profile(cur, name, skey, cutoff, proj)
            cv = round(sd / avg, 3) if avg else 0.0
            if profile:
                extra = (
                    profile["model_mean"], profile["q10"], profile["q25"],
                    profile["q50"], profile["q75"], profile["q90"],
                    profile["distribution_method"], profile["model_version"],
                    profile["projected_minutes"], None, profile["outcome_rate"],
                    UNAVAILABLE_COMPONENT, UNAVAILABLE_COMPONENT,
                    profile["profile_confidence"],
                    json.dumps({"source": "player_game_logs",
                                "sample_games": profile["sample_games"],
                                "pregame_cutoff": cutoff,
                                "model_mean_definition": "mean of discrete empirical scenarios",
                                "opportunity_component": {
                                    "status": UNAVAILABLE_COMPONENT,
                                    "reason": "no tracking-derived opportunities"},
                                "conversion_component": {
                                    "status": UNAVAILABLE_COMPONENT,
                                    "reason": "no tracking-derived opportunities"},}, sort_keys=True),
                )
            else:
                extra = (None,) * 11 + (UNAVAILABLE_COMPONENT, UNAVAILABLE_COMPONENT,
                                         "UNAVAILABLE", json.dumps(
                    {"error": "historical player minutes/rates unavailable",
                     "pregame_cutoff": cutoff,
                     "opportunity_component": {"status": UNAVAILABLE_COMPONENT},
                     "conversion_component": {"status": UNAVAILABLE_COMPONENT}},
                    sort_keys=True))
            out.append((name, team, skey, games, avg, last5, sd, cv, min_avg, proj) + extra)
        # STL/BLK are intentionally derived from authentic logs because the
        # legacy aggregate table predates those columns.
        for skey in ("stl", "blk"):
            profile = empirical_profile(cur, name, skey, cutoff)
            if not profile:
                continue
            vals = cur.execute(
                f"SELECT {skey} FROM wnba_player_game_logs WHERE player_name = ?"
                + (" AND game_date < ?" if cutoff else "")
                + " ORDER BY game_date DESC",
                (name, cutoff) if cutoff else (name,),
            ).fetchall()
            vals = [float(v[0]) for v in vals if v[0] is not None]
            avg = _mean(vals)
            last5 = _mean(vals[:5])
            sd = (sum((v - avg) ** 2 for v in vals) / (len(vals) - 1)) ** 0.5 if len(vals) > 1 else 0.0
            cv = round(sd / avg, 3) if avg else 0.0
            proj = round(profile["model_mean"], 1)
            evidence = json.dumps({
                "source": "player_game_logs",
                "sample_games": profile["sample_games"],
                "pregame_cutoff": cutoff,
                "model_mean_definition": "mean of discrete empirical scenarios",
                "opportunity_component": {
                    "status": UNAVAILABLE_COMPONENT,
                    "reason": "no tracking-derived opportunities"},
                "conversion_component": {
                    "status": UNAVAILABLE_COMPONENT,
                    "reason": "no tracking-derived opportunities"},
            }, sort_keys=True)
            profile_min_avg = _mean(profile["observed_minutes"])
            out.append((name, team, skey, len(vals), round(avg, 2), round(last5, 2),
                        round(sd, 2), cv, profile_min_avg, proj, profile["model_mean"],
                        profile["q10"], profile["q25"], profile["q50"],
                        profile["q75"], profile["q90"],
                        profile["distribution_method"], profile["model_version"],
                        profile["projected_minutes"], None, profile["outcome_rate"],
                        UNAVAILABLE_COMPONENT, UNAVAILABLE_COMPONENT,
                        profile["profile_confidence"], evidence))
    cur.executemany("INSERT INTO wnba_projections VALUES "
                    "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", out)
    return len(out)


# ----------------------------------------------------------------------------
# Prop recommendations: slate props joined to projections + DVP, with hit rate,
# CV, composite score, and a strict-ish confidence gate.
# ----------------------------------------------------------------------------
def hit_rate(cur, espn_or_name, skey, line, side, cutoff=None):
    """Fraction of the player's games that landed on the pick side of the line."""
    vals = cur.execute(
        f"SELECT {skey} FROM wnba_player_game_logs WHERE player_name = ?"
        + (" AND game_date < ?" if cutoff else ""),
        (espn_or_name, cutoff) if cutoff else (espn_or_name,)).fetchall()
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


def confidence(probability_edge_pp, hr, cv, games, high_edge=8.0):
    """Confidence gates never promote a negative no-vig probability edge."""
    if games >= 5 and probability_edge_pp >= high_edge and hr >= 60 and cv <= 0.6:
        return "HIGH"
    if probability_edge_pp >= 5 and hr >= 55 and cv <= 0.8:
        return "MEDIUM"
    return "LOW"


def build_prop_recs(cur, factors):
    cur.execute("DROP TABLE IF EXISTS wnba_prop_recommendations")
    cur.execute("""CREATE TABLE wnba_prop_recommendations (
        player TEXT, team TEXT, opponent TEXT, stat TEXT, book_line REAL,
        player_avg REAL, last5_avg REAL, projected_value REAL, vs_book_edge REAL,
        recommendation TEXT, hit_rate REAL, cv REAL, composite_score REAL,
        confidence TEXT, over_odds REAL, under_odds REAL, game_date TEXT,
        game_spread REAL, game_total REAL,
        model_mean REAL, p_over REAL, p_under REAL, p_push REAL,
        q10 REAL, q25 REAL, q50 REAL, q75 REAL, q90 REAL,
        distribution_method TEXT, model_version TEXT, projected_minutes REAL,
        opportunity_rate REAL, outcome_rate REAL,
        opportunity_component TEXT, conversion_component TEXT,
        profile_confidence TEXT, evidence_json TEXT,
        selected_probability REAL, market_no_vig_probability REAL,
        probability_edge_pp REAL, expected_roi REAL, selected_odds REAL)""")

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
    position_lookup = {norm(r[0]): _position_group(r[1]) for r in
                       cur.execute("SELECT player_name, position FROM wnba_player_stats").fetchall()}

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
        _, roster_team, _, games, season_avg, last5_avg, sd, cv, projected, min_avg = proj_row
        home_abbr = name_to_abbr.get(home, "")
        away_abbr = name_to_abbr.get(away, "")
        if line is None or line <= 0:
            continue
        real_name = name_lookup.get(norm(pname), pname)
        matchup_teams = {home_abbr, away_abbr} - {""}
        prior_team = latest_player_team(cur, real_name, gdate)
        is_historical = bool(gdate and gdate < datetime.now().date().isoformat())
        if prior_team in matchup_teams:
            pteam = prior_team
        elif not is_historical and roster_team in matchup_teams:
            # Current roster metadata is a safe pregame fallback for a recent
            # trade only on current/future slates.
            pteam = roster_team
        else:
            continue
        opponent = away_abbr if pteam == home_abbr else home_abbr
        # Rebuild from only information available before this game's date.
        profile = empirical_profile(cur, real_name, skey, gdate)
        if not profile:
            continue
        observed = profile["observed_values"]
        games = len(observed)
        season_avg = _mean(observed)
        last5_avg = _mean(observed[:5])
        sd = ((sum((v - season_avg) ** 2 for v in observed) / (games - 1)) ** 0.5
              if games > 1 else 0.0)
        cv = round(sd / season_avg, 3) if season_avg else 0.0
        min_avg = _mean(profile["observed_minutes"])
        # Historical roster positions are not stored as-of date. Using today's
        # label would leak a later position change, so historical slates use
        # only the cutoff-specific team DVP.
        matchup_position = None if is_historical else position_lookup.get(norm(pname))
        af, matchup_evidence = _matchup_factor(
            cur, opponent, matchup_position, skey, gdate)
        adjusted_distribution = [
            float(round(max(0.0, value * af))) for value in profile["distribution"]]
        adj_proj = round(_mean(adjusted_distribution), 1)
        priced = priced_distribution_sides(adjusted_distribution, line, oo, uo)
        p_over, p_under, p_push, side, side_price = priced
        probability_edge = side_price["probability_edge_pp"]
        # Keep the subscriber-facing Edge % definition stable: projected mean
        # versus the book line. Probability edge remains an internal field used
        # for side selection, scoring, and confidence.
        edge = round(((adj_proj - line) / line) * 100, 1) if line else 0.0
        hr = hit_rate(cur, real_name, skey, line, side, gdate)
        dva_supports = ((af > 1.0 and side == "OVER")
                        or (af < 1.0 and side == "UNDER"))
        spread, total = game_env.get((home, away, gdate), (None, None))
        env_pen = game_env_penalty(side, spread, min_avg)
        score_edge = max(0.0, probability_edge or 0.0)
        comp = composite(score_edge, hr, cv, dva_supports, env_pen)
        conf = confidence(score_edge, hr, cv, games, high_edge)
        quantiles = [_quantile(adjusted_distribution, q)
                     for q in (0.10, 0.25, 0.50, 0.75, 0.90)]
        evidence = {
            "source": "wnba_player_game_logs",
            "pregame_cutoff": gdate,
            "sample_games": profile["sample_games"],
            "outcome_per_minute_is_box_score_derived": True,
            "model_mean_definition": "mean of discrete empirical scenarios",
            "opportunity_component": {
                "status": UNAVAILABLE_COMPONENT,
                "reason": "no tracking-derived opportunities"},
            "conversion_component": {
                "status": UNAVAILABLE_COMPONENT,
                "reason": "no tracking-derived opportunities"},
            **matchup_evidence,
        }
        recs.append((real_name, pteam, opponent, pstat, line, season_avg, last5_avg,
                     adj_proj, edge, side, hr, cv, comp, conf, oo, uo, gdate,
                     spread, total, _mean(adjusted_distribution),
                     p_over, p_under, p_push,
                     *(round(q, 3) for q in quantiles),
                     profile["distribution_method"], profile["model_version"],
                     profile["projected_minutes"], None, profile["outcome_rate"],
                     UNAVAILABLE_COMPONENT, UNAVAILABLE_COMPONENT,
                     profile["profile_confidence"], json.dumps(evidence, sort_keys=True),
                     side_price["selected_probability"],
                     side_price["market_no_vig_probability"],
                     probability_edge, side_price["expected_roi"],
                     side_price["selected_odds"]))

    recs.sort(key=lambda r: r[12], reverse=True)  # by composite desc
    cur.executemany(
        "INSERT INTO wnba_prop_recommendations VALUES "
        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", recs)

    # CSV for the header generator / charts (mirror NBA prop_recommendations.csv cols)
    with open("wnba_prop_recommendations.csv", "w", newline="") as f:
        # Use repository-native LF endings. The csv module defaults to CRLF,
        # which makes every generated row fail `git diff --check` on Linux.
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["player", "team", "opponent", "stat", "book_line", "player_avg",
                    "last5_avg", "projected_value", "vs_book_edge", "recommendation",
                    "hit_rate", "cv", "composite_score", "confidence",
                     "over_odds", "under_odds", "game_date", "game_spread", "game_total",
                     "model_mean", "p_over", "p_under", "p_push",
                     "q10", "q25", "q50", "q75", "q90",
                     "distribution_method", "model_version", "projected_minutes",
                     "opportunity_rate", "outcome_rate", "opportunity_component",
                     "conversion_component", "profile_confidence", "evidence_json",
                     "selected_probability", "market_no_vig_probability",
                     "probability_edge_pp", "expected_roi", "selected_odds"])
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
