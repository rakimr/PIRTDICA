"""
WNBA daily article generator for PIRTDICA SPORTS CO.

Mirrors the NBA article subsystem: it selects HIGH-confidence WNBA prop picks,
writes a Claude-authored analysis for each, renders a Pillow header image of the
featured players, and saves everything to the `wnba_daily_articles` Postgres table
so /articles?league=wnba renders the SAME articles.html as the NBA page.

Inputs:
  - wnba_prop_recommendations.csv  (build_wnba_projections.py)
  - wnba_player_stats              (espn_id + position, for header + archetype line)
  - wnba_standings                 (records, for slate context)
Fallback: if Claude is unavailable, a template engine writes data-driven analysis
so the page never goes blank.
"""
import json
import os
import sqlite3
import time
import unicodedata
from datetime import datetime

import pandas as pd

import generate_header

DB = "dfs_nba.db"
POS_LABEL = {"G": "Guard", "F": "Forward", "C": "Center", "G/F": "Wing", "F/C": "Big"}
STAT_FULL = {"PTS": "points", "REB": "rebounds", "AST": "assists", "3PM": "three-pointers"}
# Prop stat -> wnba_dvp.stat key (DVP = how many of this stat the opponent allows
# vs the league average; factor > 1.0 = soft matchup that supports OVERs).
DVP_STAT = {"PTS": "pts", "REB": "reb", "AST": "ast", "3PM": "fg3m"}


def _norm(name):
    s = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode()
    return "".join(ch for ch in s.lower() if ch.isalnum())


def _load_recs():
    if not os.path.exists("wnba_prop_recommendations.csv"):
        return pd.DataFrame()
    return pd.read_csv("wnba_prop_recommendations.csv")


def _player_meta():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    meta = {}
    for name, espn_id, pos, team in cur.execute(
            "SELECT player_name, espn_id, position, team FROM wnba_player_stats").fetchall():
        meta[_norm(name)] = {"espn_id": espn_id, "pos": pos or "", "team": team}
    records = {}
    for abbr, wins, losses in cur.execute(
            "SELECT team, wins, losses FROM wnba_standings").fetchall():
        records[abbr] = f"{wins}-{losses}"
    conn.close()
    return meta, records


def _edge_str(edge):
    try:
        e = float(edge)
        return f"+{e:.1f}%" if e >= 0 else f"{e:.1f}%"
    except (TypeError, ValueError):
        return ""


def _zone_rank_label(rank, total):
    """1 = stingiest defense in the league, total = leakiest."""
    if not rank or not total:
        return ""
    if rank <= max(1, total // 3):
        return "stingy"
    if rank >= total - max(1, total // 3) + 1:
        return "leaky"
    return "average"


def _load_enrichment(slate_date=None):
    """Load WNBA shot-zone, opponent-defense-by-zone, and DVP context once.

    Mirrors the NBA article enrichment so Claude leads with shot-diet vs zone
    defense and DVP instead of leaning on last-5 / hit-rate. No archetype/DVA or
    usage-redistribution blocks // there is no WNBA tracking source for those.
    """
    caches = {"shot_zones": {}, "team_def": {}, "zone_ranks": {},
              "league_avg": {}, "dvp": {}, "dvp_rank": {}, "dvp_position": {},
              "referee_by_game": {}, "rest": {}, "fp": {}}
    try:
        conn = sqlite3.connect(DB)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        def _has(table):
            return cur.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,)).fetchone() is not None

        if _has("wnba_player_shot_zones"):
            for row in cur.execute("SELECT * FROM wnba_player_shot_zones"):
                caches["shot_zones"][_norm(row["player_name"])] = dict(row)

        if _has("wnba_team_defense_shot_zones"):
            for row in cur.execute("SELECT * FROM wnba_team_defense_shot_zones"):
                d = dict(row)
                # Player shot zones carry one combined "three" // synthesize the
                # opponent's combined three-point defense from corner3 + atb3.
                c3a, c3m = d.get("corner3_fga") or 0, d.get("corner3_fgm") or 0
                a3a, a3m = d.get("atb3_fga") or 0, d.get("atb3_fgm") or 0
                d["three_fg_pct"] = round(100.0 * (c3m + a3m) / (c3a + a3a), 1) if (c3a + a3a) else None
                d["three_freq"] = (d.get("corner3_freq") or 0) + (d.get("atb3_freq") or 0)
                caches["team_def"][d["team"]] = d

            # Per-zone league ranks: rank 1 = lowest FG% allowed (stingiest).
            zone_keys = ["ra_fg_pct", "paint_fg_pct", "mid_fg_pct", "three_fg_pct"]
            caches["zone_ranks"] = {t: {} for t in caches["team_def"]}
            for k in zone_keys:
                vals = [(t, dv.get(k)) for t, dv in caches["team_def"].items() if dv.get(k) is not None]
                vals.sort(key=lambda x: x[1])
                for i, (t, _v) in enumerate(vals, start=1):
                    caches["zone_ranks"][t][k] = (i, len(vals))

            # Data-driven league averages (attempts-weighted) so "soft vs tough"
            # is judged against the real WNBA baseline, not a hardcoded NBA one.
            for zone, (fgm_k, fga_k) in {
                "rim": ("ra_fgm", "ra_fga"),
                "paint": ("paint_fgm", "paint_fga"),
                "mid": ("mid_fgm", "mid_fga"),
            }.items():
                tm = sum((dv.get(fgm_k) or 0) for dv in caches["team_def"].values())
                ta = sum((dv.get(fga_k) or 0) for dv in caches["team_def"].values())
                caches["league_avg"][zone] = round(100.0 * tm / ta, 1) if ta else None
            tm = sum(((dv.get("corner3_fgm") or 0) + (dv.get("atb3_fgm") or 0)) for dv in caches["team_def"].values())
            ta = sum(((dv.get("corner3_fga") or 0) + (dv.get("atb3_fga") or 0)) for dv in caches["team_def"].values())
            caches["league_avg"]["three"] = round(100.0 * tm / ta, 1) if ta else None

        if _has("wnba_dvp"):
            by_stat = {}
            for team, stat, factor in cur.execute("SELECT team, stat, factor FROM wnba_dvp"):
                caches["dvp"][(team, stat)] = factor
                by_stat.setdefault(stat, []).append((team, factor))
            for stat, rows in by_stat.items():
                rows.sort(key=lambda x: x[1])  # rank 1 = toughest (lowest factor)
                for i, (team, _f) in enumerate(rows, start=1):
                    caches["dvp_rank"][(team, stat)] = (i, len(rows))

        # Defense vs Position (G/F/C): how a defense treats a position group for a
        # given stat vs the league average for that position. Built by
        # build_wnba_projections.build_dvp_position.
        if _has("wnba_dvp_position"):
            for team, position, stat, factor in cur.execute(
                    "SELECT team, position, stat, factor FROM wnba_dvp_position"):
                caches["dvp_position"][(team, position, stat)] = factor

        # Per-player season fantasy-point spread, for a ceiling/floor read. We do
        # not have a WNBA fp distribution model, so we derive an honest +/- 1 SD
        # band around the season FP average from wnba_player_stats.
        if _has("wnba_player_stats"):
            for row in cur.execute(
                "SELECT player_name, fp_avg, fp_sd, fp_l5 FROM wnba_player_stats"):
                d = dict(row)
                if d.get("fp_avg") is None:
                    continue
                caches["fp"][_norm(d["player_name"])] = {
                    "fp_avg": d.get("fp_avg"),
                    "fp_sd": d.get("fp_sd"),
                    "fp_l5": d.get("fp_l5"),
                }

        # Referee crew foul environment per game (matched by the two team abbrevs).
        if _has("wnba_referee_stats") and _has("wnba_referee_assignments"):
            ref_stats, fpgs = {}, []
            for name, fpg, fdiff in cur.execute(
                "SELECT referee, fouls_pg, foul_diff FROM wnba_referee_stats"):
                if not name:
                    continue
                ref_stats[name.strip()] = {"fouls_pg": fpg, "foul_diff": fdiff}
                if fpg is not None:
                    fpgs.append(fpg)
            lg_fpg = (sum(fpgs) / len(fpgs)) if fpgs else None
            # Prefer the assignments for the slate we are writing about; only fall
            # back to the most recent date when the slate has no rows.
            latest_date = None
            if slate_date is not None:
                sd = slate_date.isoformat() if hasattr(slate_date, "isoformat") else str(slate_date)
                if cur.execute(
                    "SELECT 1 FROM wnba_referee_assignments WHERE game_date = ? LIMIT 1",
                    (sd,)).fetchone():
                    latest_date = sd
            if not latest_date:
                latest = cur.execute(
                    "SELECT MAX(game_date) FROM wnba_referee_assignments").fetchone()
                latest_date = latest[0] if latest else None
            if latest_date:
                for home, away, chief, ref, ump in cur.execute(
                    "SELECT home_team, away_team, crew_chief, referee, umpire "
                    "FROM wnba_referee_assignments WHERE game_date = ?", (latest_date,)):
                    crew = [c.strip() for c in (chief, ref, ump) if c and str(c).strip()]
                    stats = [ref_stats[c] for c in crew if c in ref_stats]
                    if not (home and away) or not stats:
                        continue
                    cf = [s["fouls_pg"] for s in stats if s.get("fouls_pg") is not None]
                    block = {"crew": crew}
                    if cf:
                        avg_f = sum(cf) / len(cf)
                        block["avg_fouls_pg"] = round(avg_f, 1)
                        if lg_fpg:
                            if avg_f >= lg_fpg + 1.0:
                                block["whistle"] = "tight"
                            elif avg_f <= lg_fpg - 1.0:
                                block["whistle"] = "lenient"
                            else:
                                block["whistle"] = "average"
                    caches["referee_by_game"][frozenset((home.strip(), away.strip()))] = block

        # Team rest / back-to-back, derived the same way as build_wnba_slump_risk:
        # most-recent played date (game logs) vs the next scheduled game (schedule).
        if _has("wnba_games") and _has("wnba_teams") and _has("wnba_player_game_logs"):
            name2abbr = {}
            for abbr, nm in cur.execute("SELECT abbr, name FROM wnba_teams"):
                if nm:
                    name2abbr[nm.strip()] = abbr
            # Anchor "next game" to the slate we are writing about when known, so
            # rest/B2B reflects that slate rather than ET-today on backfill runs.
            if slate_date is not None:
                today = slate_date if hasattr(slate_date, "isoformat") else None
            else:
                today = None
            if today is None:
                try:
                    from utils.timezone import get_eastern_today
                    today = get_eastern_today()
                except Exception:
                    from zoneinfo import ZoneInfo
                    today = datetime.now(ZoneInfo("America/New_York")).date()
            prev_dates = {}
            for team, gdate in cur.execute(
                "SELECT team, MAX(game_date) FROM wnba_player_game_logs "
                "WHERE team != '' GROUP BY team"):
                if team and gdate:
                    prev_dates[team] = gdate
            next_games = {}
            for gdate, home, away in cur.execute(
                "SELECT game_date, home_team, away_team FROM wnba_games "
                "WHERE game_date >= ? ORDER BY game_date ASC", (today.isoformat(),)):
                h = name2abbr.get((home or "").strip())
                a = name2abbr.get((away or "").strip())
                if h and h not in next_games:
                    next_games[h] = (gdate, a)
                if a and a not in next_games:
                    next_games[a] = (gdate, h)
            from datetime import date as _date
            for team, (ndate, _opp) in next_games.items():
                prev = prev_dates.get(team)
                if not prev:
                    continue
                try:
                    rest_days = (_date.fromisoformat(ndate) - _date.fromisoformat(prev)).days
                except Exception:
                    continue
                caches["rest"][team] = {
                    "rest_days": rest_days,
                    "back_to_back": rest_days <= 1,
                }
        conn.close()
    except Exception as e:
        print(f"[WNBA ARTICLE] enrichment load failed ({e}) // continuing without it.")
    return caches


def _enrich_pick(player, opponent, stat, caches):
    """Build shot_diet, opp_def_zones, zone_matchup_edges, and dvp blocks for one
    pick. Returns only the blocks whose source data clears the sample threshold."""
    out = {}
    pz = caches["shot_zones"].get(_norm(player))
    opp_def = caches["team_def"].get(opponent)
    ranks = caches["zone_ranks"].get(opponent, {})
    lavg = caches["league_avg"]

    # zone label -> (player share key, player fgm/fga keys, opp def fg% key, lavg key)
    zone_map = [
        ("rim",   "ra_pct",    "ra_fgm",    "ra_fga",    "ra_fg_pct",    "rim"),
        ("paint", "paint_pct", "paint_fgm", "paint_fga", "paint_fg_pct", "paint"),
        ("mid",   "mid_pct",   "mid_fgm",   "mid_fga",   "mid_fg_pct",   "mid"),
        ("three", "three_pct", "three_fgm", "three_fga", "three_fg_pct", "three"),
    ]

    player_zones = []
    if pz and (pz.get("total_fga") or 0) >= 30:
        rows = []
        for label, share_k, fgm_k, fga_k, _opp_k, _l in zone_map:
            share = pz.get(share_k)
            if not share or share < 8:
                continue
            fga, fgm = pz.get(fga_k) or 0, pz.get(fgm_k) or 0
            fg = round(100.0 * fgm / fga, 1) if fga else None
            rows.append((label, round(share, 1), fg))
        rows.sort(key=lambda r: -(r[1] or 0))
        player_zones = rows[:3]
        if player_zones:
            out["shot_diet"] = {
                "season_fga": pz.get("total_fga"),
                "top_zones": [{"zone": z, "share_pct": s, "fg_pct": fg} for z, s, fg in player_zones],
            }

    if opp_def and player_zones:
        key_for = {"rim": "ra_fg_pct", "paint": "paint_fg_pct", "mid": "mid_fg_pct", "three": "three_fg_pct"}
        relevant = []
        for z, _s, _fg in player_zones:
            v = opp_def.get(key_for[z])
            if v is None:
                continue
            rk = ranks.get(key_for[z])
            relevant.append({
                "zone": z,
                "allowed_fg_pct": round(v, 1),
                "def_rank": f"{rk[0]}/{rk[1]} ({_zone_rank_label(*rk)})" if rk else None,
            })
        if relevant:
            out["opp_def_zones"] = relevant

    if pz and opp_def and (pz.get("total_fga") or 0) >= 30:
        edges = []
        for label, share_k, _fgm, _fga, opp_k, lavg_k in zone_map:
            share = pz.get(share_k) or 0
            allowed = opp_def.get(opp_k)
            base_avg = lavg.get(lavg_k)
            if share < 8 or allowed is None or base_avg is None:
                continue
            diff = allowed - base_avg
            mag = abs(share * diff)
            if mag <= 5:
                continue
            rk = ranks.get(opp_k)
            tone = "easier" if diff > 0 else "tougher"
            s = (f"{label}: takes {share:.0f}% of shots, opp allows {allowed:.1f}% "
                 f"(lg avg {base_avg:.0f}")
            if rk:
                s += f", def rank {rk[0]}/{rk[1]} // {_zone_rank_label(*rk)}"
            s += f" // {tone} than avg)"
            edges.append((mag, s))
        edges.sort(key=lambda e: -e[0])
        top = [t for _m, t in edges[:2]]
        if top:
            out["zone_matchup_edges"] = top

    dvp_key = DVP_STAT.get(stat)
    if dvp_key:
        factor = caches["dvp"].get((opponent, dvp_key))
        if factor is not None:
            rk = caches["dvp_rank"].get((opponent, dvp_key))
            edge_pct = round((factor - 1.0) * 100, 1)
            label = "soft" if edge_pct > 2 else "tough" if edge_pct < -2 else "neutral"
            out["dvp"] = {
                "stat": stat,
                "opp_allows_vs_avg_pct": edge_pct,
                "factor": round(factor, 3),
                "rank": f"{rk[0]}/{rk[1]} (1=toughest)" if rk else None,
                "read": label,
            }
    return out


def _load_slump_risk():
    """player-norm-key -> slump-risk context (build_wnba_slump_risk.py). Lets the
    article honestly flag a featured player who is showing cool-off warning signs
    (shrinking minutes/usage, hot-streak regression, tough matchup). Returns {} if
    the table is absent so the article still generates cleanly."""
    out = {}
    if not os.path.exists(DB):
        return out
    try:
        conn = sqlite3.connect(DB)
        cur = conn.cursor()
        has = cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='wnba_slump_risk'"
        ).fetchone()
        if not has:
            conn.close()
            return out
        for name, level, score, factors_json in cur.execute(
            "SELECT player_name, risk_level, overall_score, factors_json "
            "FROM wnba_slump_risk WHERE risk_level IN ('HIGH','MODERATE')"
        ):
            try:
                factors = json.loads(factors_json or "[]")
            except Exception:
                factors = []
            out[_norm(name)] = {
                "risk_level": level,
                "risk_score": round(score, 0) if score is not None else None,
                "signals": factors,
            }
        conn.close()
    except Exception as e:
        print(f"Slump-risk load failed ({e}) // continuing without it.")
    return out


def _build_briefing(recs, meta, records, caches=None):
    if caches is None:
        caches = _load_enrichment()
    slump = _load_slump_risk()
    prop_lines = []
    for _, r in recs.iterrows():
        nk = _norm(r["player"])
        pos = meta.get(nk, {}).get("pos", "")
        entry = {
            "player": r["player"],
            "team": r["team"],
            "team_record": records.get(r["team"], ""),
            "opponent": r["opponent"],
            "opponent_record": records.get(r["opponent"], ""),
            "position": POS_LABEL.get(pos, pos or "Player"),
            "stat": r["stat"],
            "book_line": r["book_line"],
            "projected": r["projected_value"],
            "season_avg": r["player_avg"],
            "last5_avg": r["last5_avg"],
            "edge_pct": r["vs_book_edge"],
            "model_side": r["recommendation"],
            "hit_rate": r["hit_rate"],
            "cv": r["cv"],
            "composite_score": r["composite_score"],
            "confidence": r["confidence"],
        }
        entry.update(_enrich_pick(r["player"], r["opponent"], r["stat"], caches))

        # Defense vs Position: how the opponent defends this player's position group
        # (G/F/C) for this stat, vs the league average for that position. Complements
        # the team-vs-stat `dvp` block with a true positional read.
        dvp_key = DVP_STAT.get(r["stat"])
        if pos and dvp_key:
            pf = caches.get("dvp_position", {}).get((r["opponent"], pos, dvp_key))
            if pf is not None:
                pct = round((pf - 1.0) * 100, 1)
                entry["dvp_position"] = {
                    "position": POS_LABEL.get(pos, pos),
                    "stat": r["stat"],
                    "opp_allows_to_pos_vs_avg_pct": pct,
                    "factor": round(pf, 3),
                    "read": "soft" if pct > 2 else "tough" if pct < -2 else "neutral",
                }

        # Fantasy-point ceiling/floor band (+/- 1 SD around the season FP avg).
        fp = caches.get("fp", {}).get(nk)
        if fp and fp.get("fp_avg") is not None:
            avg = float(fp["fp_avg"])
            block = {"season_fp_pg": round(avg, 1)}
            if fp.get("fp_l5") is not None:
                block["last5_fp_pg"] = round(float(fp["fp_l5"]), 1)
            if fp.get("fp_sd") is not None:
                sd = float(fp["fp_sd"])
                block["fp_ceiling"] = round(avg + sd, 1)
                block["fp_floor"] = round(max(0.0, avg - sd), 1)
            entry["fp_context"] = block

        # Team rest / back-to-back (this team and the opponent, for a rest edge).
        rest_map = caches.get("rest", {})
        rest = rest_map.get(r["team"])
        if rest:
            rblock = dict(rest)
            opp_rest = rest_map.get(r["opponent"])
            if opp_rest:
                rblock["opponent_rest_days"] = opp_rest.get("rest_days")
                if (rest.get("rest_days") is not None
                        and opp_rest.get("rest_days") is not None):
                    if rest["rest_days"] - opp_rest["rest_days"] >= 2:
                        rblock["rest_edge"] = "advantage"
                    elif opp_rest["rest_days"] - rest["rest_days"] >= 2:
                        rblock["rest_edge"] = "disadvantage"
            entry["rest"] = rblock

        # Referee crew foul environment for this game.
        ref = caches.get("referee_by_game", {}).get(frozenset((r["team"], r["opponent"])))
        if ref:
            entry["referee_crew"] = ref

        sr = slump.get(nk)
        if sr:
            entry["slump_risk"] = sr
        prop_lines.append(entry)
    return prop_lines


SYSTEM_PROMPT = """You are an elite WNBA DFS analyst for PIRTDICA SPORTS CO. You are given the full WNBA slate with model projections, shot-diet vs opponent-defense-by-zone matchups, DVP (defense vs the stat), recent form, and game environment.

You are competing against the sharpest analysts at FanDuel who set these player prop lines. Respect the lines. Only attack when you have genuine conviction backed by multiple converging signals.

YOUR JOB: Independently analyze the slate and select 4-8 HIGH confidence prop picks. You are NOT limited to what the model labeled HIGH; evaluate every prop line and find the sharpest edges yourself.

ANALYTICAL FRAMEWORK (this is how a sharp WNBA analyst reasons // follow it):
1. SHOT-DIET vs OPPONENT-DEFENSE-BY-ZONE (HEADLINE SIGNAL when present): If the pick has `zone_matchup_edges`, or `shot_diet` + `opp_def_zones`, LEAD with it. Cite the player's zone share and the opponent's allowed FG% and rank in that zone (e.g. "takes 35% of shots from mid // CON allows 38.4% there // def rank 13/15 // leaky"). When a player's high-frequency zones line up with the opponent's leaky zones, that is the strongest case for an OVER. This is the single most distinguishing piece of context per pick // never bury it.
2. DVP (defense vs the stat): `dvp.opp_allows_vs_avg_pct` is how much more/less of this stat the opponent gives up vs the WNBA average (positive = soft = supports OVER, negative = tough = supports UNDER). Cite it: "POR allows +7.1% more rebounds than average (rank 14/15)". Treat a soft/tough DVP as a primary matchup signal, not a footnote. If a pick also has `dvp_position`, that is how the opponent defends this player's POSITION GROUP (Guard/Forward/Center) for this stat // `opp_allows_to_pos_vs_avg_pct` positive = soft for that position group. When `dvp` (team vs the stat) and `dvp_position` (team vs this position) agree, the matchup edge is stronger and more trustworthy; when they diverge, lean on the position-specific read for a player who fills a clear positional role (e.g. a center attacking a team that bleeds rebounds to forwards/centers).
3. PROJECTION EDGE: the model's projected value vs the book line (`edge_pct`).
4. CONFIRMATION SIGNALS (secondary): hit rate, last-5 form, CV (consistency). Use these to CONFIRM a pick that the shot-diet/DVP/projection signals already support // do NOT lead with last-5 or hit rate. Lower CV is a tailwind; high CV demands a bigger edge.
5. SLUMP RISK (caution flag when present): if a pick has `slump_risk`, our Slump Risk model has flagged this player as MODERATE or HIGH risk of cooling off, with the observable `signals` that drove it (shrinking minutes/usage, an unsustainable hot streak due for regression, tough matchup, or short rest). For an OVER, treat a HIGH flag as a real reason for caution and either avoid it or explicitly acknowledge the risk and explain why the matchup still wins. For an UNDER, a slump-risk flag REINFORCES the case. Cite the specific signal honestly. Never hide it.
6. REST / BACK-TO-BACK (`rest` when present): `rest_days` is the days off before tonight for this player's team and `back_to_back` is true on the second night of a back-to-back. `opponent_rest_days` and `rest_edge` ('advantage'/'disadvantage') compare the two teams. A back-to-back is a real fatigue headwind (trimmed minutes, dip in efficiency) // lean it against aggressive OVERs and toward UNDERs. A clear rest advantage is a tailwind for volume OVERs. Use it as a supporting environment signal.
7. FANTASY-POINT CONTEXT (`fp_context` when present): `season_fp_pg` is the player's season fantasy output, `last5_fp_pg` is recent form, and `fp_ceiling`/`fp_floor` are an honest +/- 1 SD band around the season average (NOT a tonight projection). A wide band means a volatile, boom-or-bust profile (demand a bigger edge); a tight band supports confidence. If recent FP is running well above the season average, pair it with the slump-risk read before chasing an OVER.
8. REFEREE CREW (`referee_crew` when present): the officials assigned to this game with their foul environment. `avg_fouls_pg` is the crew's average fouls per game and `whistle` is tight/average/lenient vs the WNBA crew average. A `tight` whistle crew creates more free-throw and foul-out volume (supports PTS OVERs for foul-drawers, raises foul-trouble risk for bigs); a `lenient` crew suppresses FT-dependent scoring. This is a minor supporting signal, never the headline.

CORRELATION & STACKING (reason ACROSS your own picks, not just one pick at a time): picks in the SAME game share an opponent and game environment (referee whistle, rest, the pace of that matchup). When two of your picks are teammates, or fall in the same game, say so. Teammates both attacking the same leaky zone or a soft DVP, or both playing in a tight-whistle (more fouls, more free throws) game, are positively correlated and tend to rise together // that is real tournament upside, but they also bust together, so spreading picks across different games is the safer build. An OVER on one player and an UNDER on a teammate fighting for the same usage can offset // flag it. There are NO DFS ownership or implied-team-total inputs for the WNBA slate, so do not invent leverage, chalk, or Vegas-total claims.

RECENT RESULTS (`recent_pick_results` at the top of the slate data, when present): our OWN graded track record on past WNBA picks. `recent` is the last N days and `overall_to_date` is the full graded history, with `recent_by_stat` and `recent_by_direction` win-loss breakdowns (each carrying `decided` graded picks and `win_pct`). Use it as honest internal calibration only: be more selective on stats or directions that have been cold over a meaningful sample, and trust the ones we have been sharp on. Weight small samples lightly. NEVER mention this track record in the published analysis // it is internal calibration only.

PICK SELECTION:
- Prioritize picks where shot-diet vs zone defense and/or DVP point the same direction as the projection edge, then confirm with form.
- Do not write an analysis that only cites last-5 and hit rate when zone/DVP context is available. The zone matchup and DVP are why these are PIRTDICA picks.
- A soft opponent in that stat/zone supports OVERs, a tough one supports UNDERs.

WRITING STYLE:
- Talk to a sharp friend. Conversational, direct, confident.
- NEVER use em-dashes or double hyphens. Use periods, commas, colons, or "//" instead.
- Use "you" and "your". Short paragraphs (2-4 lines). One idea per paragraph.
- **Bold the key insight** in most sections.
- Back every claim with specific numbers (projection, edge, hit rate, last-5, composite score).
- Each analysis: 3-4 paragraphs, 150-250 words.
- End each analysis with: **The Call: OVER/UNDER X.X STAT** // We project [Player] at [Value] [stat] tonight ([edge]% edge vs. the book). Composite score: [X].

OUTPUT FORMAT:
Return a JSON object with two keys:
1. "picks": array of objects with: "player", "team", "opponent", "stat", "book_line", "projected", "avg", "edge" (e.g. "+17.8%"), "pick" ("OVER"/"UNDER"), "composite_score".
2. "analyses": array of objects with: "player", "stat", "call" ("OVER"/"UNDER"), "archetype", "team", "opponent", "analysis".

CRITICAL MATCHING RULES:
- "analyses" MUST have exactly one entry per pick, same order. Each is identified by (player, stat, call). The "call" must match the pick side exactly.
- Never write an UNDER analysis for an OVER pick or vice versa.

Return ONLY the JSON object. Order picks by edge strength (strongest first)."""


def _call_claude(prop_lines, game_count, slate_date):
    api_key = os.environ.get("AI_INTEGRATIONS_ANTHROPIC_API_KEY")
    base_url = os.environ.get("AI_INTEGRATIONS_ANTHROPIC_BASE_URL")
    if not api_key or not base_url:
        print("Claude not configured // using template fallback.")
        return None
    briefing = {
        "slate_date": str(slate_date),
        "game_count": game_count,
        "prop_lines": prop_lines,
    }
    try:
        from pick_feedback import load_recent_pick_results
        rpr = load_recent_pick_results("wnba", recent_days=14)
        if rpr:
            briefing["recent_pick_results"] = rpr
            rec = rpr.get("recent", {})
            print(f"[WNBA ARTICLE] recent pick results (last {rpr.get('window_days')}d): "
                  f"{rec.get('record')} ({rec.get('decided')} decided)")
    except Exception as _fb_err:
        print(f"[WNBA ARTICLE] recent pick results load failed ({_fb_err})")
    user_prompt = (
        "Analyze the full WNBA slate and select your HIGH confidence picks.\n\n"
        "HERE IS THE COMPLETE SLATE DATA (every prop line with model context):\n\n"
        f"{json.dumps(briefing, indent=2, default=str)}\n\n"
        "Remember: select 4-8 picks with the strongest convergence of signals, cite "
        "specific numbers, end each analysis with a bold **The Call:** line, and NEVER "
        "use em-dashes. Return ONLY a JSON object with \"picks\" and \"analyses\" keys."
    )
    try:
        from anthropic import Anthropic
        client = Anthropic(api_key=api_key, base_url=base_url)
        print("Calling Claude for WNBA pick selection + analysis...")
        t0 = time.time()
        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=8192,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
        text = msg.content[0].text.strip()
        print(f"Claude responded in {time.time() - t0:.1f}s ({len(text)} chars)")
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()
        result = json.loads(text)
        if not isinstance(result, dict) or "picks" not in result or "analyses" not in result:
            print("Claude returned unexpected format // template fallback.")
            return None
        if not isinstance(result["picks"], list) or len(result["picks"]) < 2:
            print("Claude returned too few picks // template fallback.")
            return None
        return result
    except Exception as e:
        print(f"Claude error ({e}) // template fallback.")
        return None


def _template_result(prop_lines):
    """Data-driven fallback when Claude is unavailable."""
    highs = [p for p in prop_lines if p["confidence"] in ("HIGH", "MEDIUM")][:6]
    if not highs:
        highs = prop_lines[:5]
    picks, analyses = [], []
    for p in highs:
        side = p["model_side"]
        stat_full = STAT_FULL.get(p["stat"], p["stat"].lower())
        edge = p["edge_pct"]
        # Lead with the matchup context (shot diet vs zone defense + DVP) when we
        # have it, mirroring the Claude framework, so the fallback is not just
        # last-5 / hit-rate either.
        matchup = ""
        zme = p.get("zone_matchup_edges")
        if zme:
            matchup += f"**Matchup edge:** {zme[0]}.\n\n"
        dvp = p.get("dvp")
        if dvp and dvp.get("read") in ("soft", "tough"):
            sign = "+" if dvp["opp_allows_vs_avg_pct"] >= 0 else ""
            matchup += (
                f"{p['opponent']} grades as a {dvp['read']} {stat_full} matchup: they allow "
                f"{sign}{dvp['opp_allows_vs_avg_pct']}% vs the WNBA average "
                f"(DVP rank {dvp.get('rank','')}).\n\n"
            )
        dvp_pos = p.get("dvp_position")
        if dvp_pos and dvp_pos.get("read") in ("soft", "tough"):
            psign = "+" if dvp_pos["opp_allows_to_pos_vs_avg_pct"] >= 0 else ""
            matchup += (
                f"Against {dvp_pos['position'].lower()}s specifically, {p['opponent']} allows "
                f"{psign}{dvp_pos['opp_allows_to_pos_vs_avg_pct']}% {stat_full} vs the WNBA "
                f"average for the position.\n\n"
            )
        analysis = (
            f"**{p['player']} is our {side.lower()} look on {stat_full} tonight.** "
            f"The model projects {p['projected']} against a book line of {p['book_line']}, "
            f"a {_edge_str(edge)} edge.\n\n"
            f"{matchup}"
            f"She profiles as a {p['position'].lower()} for {p['team']} ({p['team_record']}) "
            f"facing {p['opponent']} ({p['opponent_record']}). Season average sits at "
            f"{p['season_avg']} with a last-5 of {p['last5_avg']}, and the hit rate on this "
            f"side is {p['hit_rate']:.0f}%, confirming the matchup read.\n\n"
            f"Consistency reads at a {p['cv']} coefficient of variation. "
            f"**The Call: {side} {p['book_line']} {p['stat']}** // We project {p['player']} at "
            f"{p['projected']} {p['stat']} tonight ({_edge_str(edge)} edge vs. the book). "
            f"Composite score: {p['composite_score']}."
        )
        picks.append({
            "player": p["player"], "team": p["team"], "opponent": p["opponent"],
            "stat": p["stat"], "book_line": p["book_line"], "projected": p["projected"],
            "avg": p["season_avg"], "edge": _edge_str(edge), "pick": side,
            "composite_score": p["composite_score"],
        })
        analyses.append({
            "player": p["player"], "stat": p["stat"], "call": side,
            "archetype": p["position"], "team": p["team"], "opponent": p["opponent"],
            "analysis": analysis,
        })
    return {"picks": picks, "analyses": analyses}


def _to_template_shapes(result, meta):
    picks_data, analysis_data = [], []
    for i, pk in enumerate(result["picks"], start=1):
        picks_data.append({
            "rank": i,
            "player": pk.get("player", ""),
            "game": f"{pk.get('team','')} vs {pk.get('opponent','')}",
            "stat": pk.get("stat", ""),
            "avg": pk.get("avg", ""),
            "line": pk.get("book_line", ""),
            "projected": pk.get("projected", ""),
            "edge": pk.get("edge") if isinstance(pk.get("edge"), str) else _edge_str(pk.get("edge")),
            "pick": str(pk.get("pick", "OVER")).upper(),
        })
    for a in result["analyses"]:
        nk = _norm(a.get("player", ""))
        pos = meta.get(nk, {}).get("pos", "")
        analysis_data.append({
            "player": a.get("player", ""),
            "archetype": a.get("archetype") or POS_LABEL.get(pos, "Player"),
            "team": a.get("team", ""),
            "opponent": a.get("opponent", ""),
            "stat": a.get("stat", ""),
            "call": str(a.get("call", "OVER")).upper(),
            "analysis": a.get("analysis", ""),
        })
    return picks_data, analysis_data


_OFFICIAL_LOCK_WINDOW_MINUTES = 60
_OFFICIAL_LOCK_GRACE_AFTER_TIP_MINUTES = 30


def _first_tipoff_today_et(slate_date):
    """Return the earliest WNBA tipoff datetime in ET for `slate_date`, or None.

    Uses `wnba_games.commence_time` (stored as a UTC ISO timestamp) for the
    slate, converted to ET. Returns None on any failure or when no game times
    exist // callers must treat that as "no lock yet". Mirrors the NBA helper in
    generate_article.py so the lock window is consistent across both leagues.
    """
    try:
        from zoneinfo import ZoneInfo
        et_zone = ZoneInfo("America/New_York")
        if not os.path.exists(DB):
            return None
        conn = sqlite3.connect(DB)
        cur = conn.cursor()
        cur.execute(
            "SELECT commence_time FROM wnba_games WHERE game_date = ? AND commence_time IS NOT NULL",
            (slate_date.isoformat(),),
        )
        raw = [row[0] for row in cur.fetchall()]
        conn.close()
        parsed = []
        for ct in raw:
            try:
                ts = ct.replace("Z", "+00:00")
                dt = datetime.fromisoformat(ts)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ZoneInfo("UTC"))
                parsed.append(dt.astimezone(et_zone))
            except Exception:
                continue
        if not parsed:
            return None
        return min(parsed)
    except Exception:
        return None


def _should_lock_official_call(slate_date, now_et):
    """Return True iff we're inside the slate's lock window (mirror of NBA).

    Window: from T-60min before first tip through T+30min after. One-shot //
    caller checks official_picks_json is NULL first. If the window closes without
    a lock firing, the grader cleanly falls back to picks_json.
    """
    first_tip = _first_tipoff_today_et(slate_date)
    if first_tip is None:
        return False
    delta_min = (first_tip - now_et).total_seconds() / 60.0
    return -_OFFICIAL_LOCK_GRACE_AFTER_TIP_MINUTES <= delta_min <= _OFFICIAL_LOCK_WINDOW_MINUTES


def _save(slate_date, header_web_path, picks_data, analysis_data, game_count, claude_selected):
    from backend.database import SessionLocal
    from backend import models
    db = SessionLocal()
    try:
        row = db.query(models.WNBADailyArticle).filter(
            models.WNBADailyArticle.slate_date == slate_date).first()
        if row:
            row.header_image_path = header_web_path
            row.picks_json = json.dumps(picks_data)
            row.analysis_json = json.dumps(analysis_data)
            row.game_count = game_count
            row.claude_selected = claude_selected
        else:
            row = models.WNBADailyArticle(
                slate_date=slate_date,
                header_image_path=header_web_path,
                picks_json=json.dumps(picks_data),
                analysis_json=json.dumps(analysis_data),
                game_count=game_count,
                claude_selected=claude_selected,
            )
            db.add(row)

        # Official Call Snapshot — freeze picks_json into official_picks_json the
        # first time we save within the lock window (typically the pregame wave
        # ~60 min before the slate's first tipoff). Once locked, later regens
        # update picks_json only // the official snapshot is immutable until an
        # admin re-snapshots it. Mirrors generate_article.save_to_db.
        try:
            from zoneinfo import ZoneInfo as _ZI
            now_et = datetime.now(_ZI("America/New_York"))
            if not row.official_picks_json and _should_lock_official_call(slate_date, now_et):
                current_picks = row.picks_json or json.dumps([])
                try:
                    parsed_picks = json.loads(current_picks)
                except Exception:
                    parsed_picks = []
                if parsed_picks:
                    row.official_picks_json = current_picks
                    row.official_locked_at = now_et
                    print(f"[OFFICIAL CALL][WNBA][source=auto-lock] Locked snapshot for {slate_date} at {now_et.strftime('%Y-%m-%d %H:%M ET')} ({len(parsed_picks)} picks)")
        except Exception as e:
            print(f"[OFFICIAL CALL][WNBA][source=auto-lock] Lock skipped for {slate_date}: {e}")

        db.commit()
        print(f"Saved WNBA article for {slate_date}: {len(picks_data)} picks, claude={claude_selected}")
    finally:
        db.close()


def main():
    print("=== WNBA article generation ===")
    recs = _load_recs()
    if recs.empty:
        print("No WNBA prop recommendations // run build_wnba_projections.py first.")
        return
    meta, records = _player_meta()

    # Pick the ACTIVE slate explicitly instead of trusting row order. The recs
    # CSV can carry more than one game_date (e.g. an upcoming slate already has
    # odds), and recs["game_date"].iloc[0] could grab a future date // that would
    # make the official-call lock check the wrong day's tipoff and never fire.
    # Choose the earliest slate that is today-or-later in ET (the active/upcoming
    # slate); if every slate is in the past, fall back to the most recent one.
    from zoneinfo import ZoneInfo as _ZI
    et_today = datetime.now(_ZI("America/New_York")).date()
    slate_dates = sorted({
        datetime.strptime(str(d), "%Y-%m-%d").date()
        for d in recs["game_date"].dropna().unique()
    })
    if len(slate_dates) > 1:
        print(f"[WNBA ARTICLE] recs span {len(slate_dates)} slates: "
              f"{[d.isoformat() for d in slate_dates]} // selecting active slate.")
    upcoming = [d for d in slate_dates if d >= et_today]
    slate_date = upcoming[0] if upcoming else slate_dates[-1]
    slate_date_str = slate_date.isoformat()
    print(f"[WNBA ARTICLE] Active slate: {slate_date_str} (ET today: {et_today})")
    recs = recs[recs["game_date"].astype(str) == slate_date_str].reset_index(drop=True)
    if recs.empty:
        print(f"No WNBA recs for active slate {slate_date_str} after filtering.")
        return
    game_count = recs[["team", "opponent"]].apply(
        lambda r: frozenset([r["team"], r["opponent"]]), axis=1).nunique()

    prop_lines = _build_briefing(recs, meta, records, _load_enrichment(slate_date))
    result = _call_claude(prop_lines, game_count, slate_date)
    claude_selected = result is not None
    if not result:
        result = _template_result(prop_lines)

    picks_data, analysis_data = _to_template_shapes(result, meta)

    # Header image: featured players (dedup, top 6 by order) -> static/images.
    seen, header_players, espn_ids = set(), [], {}
    for pk in picks_data:
        name = pk["player"]
        if name in seen:
            continue
        seen.add(name)
        nk = _norm(name)
        m = meta.get(nk, {})
        header_players.append({
            "player": name, "team": m.get("team", pk["game"].split(" vs ")[0]),
            "stat": pk["stat"], "side": pk["pick"], "line": pk["line"], "edge": pk["edge"],
        })
        if m.get("espn_id"):
            espn_ids[name] = int(m["espn_id"])
        if len(header_players) >= 6:
            break

    out_path = f"static/images/wnba_article_header_{slate_date.isoformat()}.png"
    header_web_path = None
    try:
        sub = f"{slate_date.strftime('%B %-d, %Y').upper()} \u2014 WNBA HIGH CONFIDENCE PICKS"
        generate_header.generate(
            target_date=slate_date, out_path=out_path, player_data=header_players,
            subtitle_override=sub, espn_ids=espn_ids, league="wnba")
        if os.path.exists(out_path):
            header_web_path = "/" + out_path
    except Exception as e:
        print(f"Header generation failed ({e}) // continuing without header.")

    _save(slate_date, header_web_path, picks_data, analysis_data, game_count, claude_selected)
    print("Done.")


if __name__ == "__main__":
    main()
