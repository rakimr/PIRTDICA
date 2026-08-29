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
import re
import sqlite3
import time
import unicodedata
from datetime import datetime, date

import pandas as pd

import generate_header

DB = "dfs_nba.db"
POS_LABEL = {"G": "Guard", "F": "Forward", "C": "Center", "G/F": "Wing", "F/C": "Big"}
STAT_FULL = {
    "PTS": "points", "REB": "rebounds", "AST": "assists",
    "3PM": "three-pointers", "STL": "steals", "BLK": "blocks",
}
# Prop stat -> wnba_dvp.stat key (DVP = how many of this stat the opponent allows
# vs the league average; factor > 1.0 = soft matchup that supports OVERs).
DVP_STAT = {
    "PTS": "pts", "REB": "reb", "AST": "ast", "3PM": "fg3m",
    "STL": "stl", "BLK": "blk",
}
# ESPN's WNBA shot-chart feed can emit non-franchise aggregate buckets (for
# example COOP/SPO). They must never affect opponent context, league baselines,
# or rankings.
WNBA_FRANCHISES = {
    "ATL", "CHI", "CON", "DAL", "GS", "IND", "LA", "LV", "MIN",
    "NY", "PHX", "POR", "SEA", "TOR", "WSH",
}
OPTIONAL_MODEL_FIELDS = (
    "model_mean", "p_over", "p_under", "p_push", "q10", "q25", "q50", "q75",
    "q90", "distribution_method", "model_version", "projected_minutes",
    "opportunity_rate", "outcome_rate", "opportunity_component",
    "conversion_component", "profile_confidence", "evidence_json",
    "selected_probability", "market_no_vig_probability", "probability_edge_pp",
    "expected_roi", "selected_odds",
)


def _norm(name):
    s = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode()
    return "".join(ch for ch in s.lower() if ch.isalnum())


def _load_recs():
    if not os.path.exists("wnba_prop_recommendations.csv"):
        return pd.DataFrame()
    return pd.read_csv("wnba_prop_recommendations.csv")


def _as_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return datetime.fromisoformat(str(value)).date()
    except (TypeError, ValueError):
        return None


def _is_historical_backfill(slate_date):
    """Current aggregate tables are only safe for today's/future pregame slate."""
    slate = _as_date(slate_date)
    if slate is None:
        return False
    try:
        from zoneinfo import ZoneInfo
        today = datetime.now(ZoneInfo("America/New_York")).date()
    except Exception:
        today = date.today()
    return slate < today


def _player_meta(slate_date=None):
    # These are current snapshots with no as-of date. Do not leak current roster
    # or standings information into a historical article regeneration.
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    meta = {}
    historical = _is_historical_backfill(slate_date)
    for name, espn_id, pos, team in cur.execute(
            "SELECT player_name, espn_id, position, team FROM wnba_player_stats").fetchall():
        # Position is a current roster snapshot with no effective date. Retain
        # identity for matching, but never backfill a later position into an old
        # slate article.
        meta[_norm(name)] = {
            "espn_id": espn_id,
            "pos": None if historical else (pos or ""),
            "team": team,
        }
    records = {}
    # Standings is a current snapshot, not an as-of table. Identity and position
    # remain useful with a cutoff, but standings must never enter a slate briefing.
    if slate_date is None:
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


def _present(value):
    """Return JSON-safe availability without treating zero/False as missing."""
    if value is None:
        return False
    try:
        return not bool(pd.isna(value))
    except (TypeError, ValueError):
        return True


def _row_value(row, key):
    value = row.get(key) if hasattr(row, "get") else None
    return value.item() if _present(value) and hasattr(value, "item") else value


def _evidence_item(signal, value, mechanism, direction="neutral", sample=None,
                   confidence="unknown", counter_signals=None, stage="outcome"):
    """One auditable, explicitly typed piece of pregame evidence."""
    # The system prompt defines the evidence taxonomy. Keep only the mechanism
    # discriminator here rather than repeating prose for every prop.
    if "not rebound landing" in mechanism:
        mechanism = "shot-origin misses, not rebound landing/tracking"
    elif "not contest opportunity" in mechanism:
        mechanism = "shot location, not contest opportunity/tracking"
    elif "No separate opportunity" in mechanism:
        mechanism = "separate opportunity physics unavailable"
    elif "No separate conversion" in mechanism:
        mechanism = "separate conversion physics unavailable"
    elif "never two independent confirmations" in mechanism:
        mechanism = "overlapping aggregate DVP; one signal only"
    else:
        mechanism = mechanism.split(".", 1)[0][:40]
    item = {
        "signal": signal,
        "value": value,
        "mechanism": mechanism,
        "direction": direction,
        "sample": sample if _present(sample) else None,
        "confidence": confidence if _present(confidence) else None,
    }
    # Empty arrays repeated across every ledger item dominate a slate payload.
    # A counter-signal remains mandatory wherever one is actually supplied.
    if counter_signals:
        item["counter_signals"] = counter_signals
    return item


def _field_reference(field, **status):
    """Small ledger value pointing to the canonical top-level briefing field."""
    return {"field": field, **status}


def _build_evidence_ledger(entry, alternatives):
    """Classify supplied evidence without upgrading aggregate proxies to facts."""
    side = str(entry.get("model_side", "")).upper()
    over = side == "OVER"
    ledger = {"observed": [], "derived": [], "inferred_proxy": [], "unavailable": []}

    ledger["observed"].append(_evidence_item(
        "pregame_book_line", _field_reference("book_line"),
        "Pregame market threshold the modeled outcome must clear.",
        side.lower() if side else "neutral", confidence="market observation"))
    for signal, key, sample in (
        ("season_average", "season_avg", "season-to-date games"),
        ("last_five_average", "last5_avg", "last 5 games"),
    ):
        if _present(entry.get(key)):
            ledger["observed"].append(_evidence_item(
                signal, _field_reference(key),
                "Historical box-score production describes realized outcomes, not a guaranteed role.",
                "supports over" if float(entry[key]) > float(entry["book_line"]) else "supports under",
                sample=sample, confidence="descriptive", stage="outcome"))

    projection = entry.get("model_mean", entry.get("projected"))
    if _present(projection):
        ledger["derived"].append(_evidence_item(
            "model_projection", _field_reference(
                "model_mean" if _present(entry.get("model_mean")) else "projected"),
            "The pregame model combines supplied opportunity and conversion inputs into an outcome mean.",
            "supports over" if float(projection) > float(entry["book_line"]) else "supports under",
            confidence=entry.get("profile_confidence", entry.get("confidence")),
            counter_signals=["A mean does not describe tail risk; consult probabilities or quantiles when supplied."],
            stage="outcome"))
    if _present(entry.get("projected_minutes")):
        ledger["derived"].append(_evidence_item(
            "projected_minutes", _field_reference("projected_minutes"),
            "Expected playing time creates opportunity, but is a model estimate rather than a confirmed assignment.",
            "supports opportunity" if over else "opportunity context",
            confidence=entry.get("profile_confidence"), stage="opportunity"))
    if _present(entry.get("outcome_rate")):
        ledger["derived"].append(_evidence_item(
            "outcome_rate", _field_reference("outcome_rate"),
            "Model outcome per minute is a realized historical box-score outcome rate, not opportunities or chances.",
            "outcome-rate context",
            confidence=entry.get("profile_confidence"), stage="outcome"))
    for key, stage, description in (
        ("opportunity_component", "opportunity", "Separately supplied model opportunity component."),
        ("conversion_component", "conversion", "Separately supplied model conversion component."),
    ):
        if _present(entry.get(key)):
            ledger["derived"].append(_evidence_item(
                key, _field_reference(key), description, "model component",
                confidence=entry.get("profile_confidence"), stage=stage))

    probs = {k: entry[k] for k in ("p_over", "p_under", "p_push") if _present(entry.get(k))}
    quantiles = {k: entry[k] for k in ("q10", "q25", "q50", "q75", "q90")
                 if _present(entry.get(k))}
    if probs:
        ledger["derived"].append(_evidence_item(
            "outcome_probabilities", _field_reference("p_over", fields=list(probs)),
            "The fitted pregame distribution estimates each side of the posted line.",
            f"supports {side.lower()}" if side else "neutral",
            confidence=entry.get("profile_confidence"),
            counter_signals=["Distribution probabilities inherit model and minutes uncertainty."],
            stage="outcome"))
    if quantiles:
        ledger["derived"].append(_evidence_item(
            "outcome_quantiles", _field_reference("q10", fields=list(quantiles)),
            "Quantiles expose downside and upside around the central outcome instead of implying certainty.",
            f"supports {side.lower()}" if side else "neutral",
            confidence=entry.get("profile_confidence"), stage="outcome"))
    market_fields = {k: entry[k] for k in (
        "selected_probability", "market_no_vig_probability", "probability_edge_pp",
        "expected_roi", "selected_odds") if _present(entry.get(k))}
    if market_fields:
        ledger["derived"].append(_evidence_item(
            "selected_side_market_edge",
            _field_reference("selected_probability", fields=list(market_fields)),
            "Selected-side probability and no-vig market comparison",
            f"supports {side.lower()}" if side else "neutral",
            confidence=entry.get("profile_confidence"),
            counter_signals=["Probability edge is percentage points versus the no-vig market, not a mean percentage."],
            stage="outcome"))
    model_meta = {k: entry[k] for k in ("distribution_method", "model_version",
                                        "profile_confidence")
                  if _present(entry.get(k))}
    if model_meta:
        ledger["derived"].append(_evidence_item(
            "model_metadata", _field_reference("model_version", fields=list(model_meta)),
            "Supplied model provenance and profile confidence qualify the projection; they are not performance evidence.",
            "neutral", confidence=entry.get("profile_confidence"), stage="outcome"))

    shot = entry.get("shot_diet")
    if shot:
        ledger["observed"].append(_evidence_item(
            "historical_shot_diet", shot,
            "Historical shot locations describe where attempts were converted; they do not identify tonight's defender or assignment.",
            "conversion context", sample=shot.get("season_fga"),
            confidence="historical aggregate", stage="conversion"))
    if entry.get("zone_matchup_edges"):
        ledger["inferred_proxy"].append(_evidence_item(
            "zone_matchup_proxy", entry["zone_matchup_edges"],
            "Player shot-zone history is associated with opponent team zone results; this is not player tracking or a causal matchup assignment.",
            f"supports {side.lower()}" if side else "neutral",
            sample=shot.get("season_fga") if shot else None,
            confidence="aggregate proxy",
            counter_signals=["Team zone allowances include different shooters, lineups, and game contexts."],
            stage="conversion"))

    # Team DVP and position DVP are transformations of overlapping opponent
    # outcomes. Keep both visible for compatibility, but record a single proxy.
    dvp, pos_dvp = entry.get("dvp"), entry.get("dvp_position")
    if dvp or pos_dvp:
        preferred = pos_dvp or dvp
        value = {"position_dvp": pos_dvp, "team_dvp": dvp}
        ledger["inferred_proxy"].append(_evidence_item(
            "opponent_dvp_proxy", value,
            "Historical opponent allowance is an association-level proxy. Position and team DVP overlap and must count as one signal, never two independent confirmations.",
            ("supports over" if preferred.get("read") == "soft" else
             "supports under" if preferred.get("read") == "tough" else "neutral"),
            confidence="aggregate proxy",
            counter_signals=["No defender assignment, matchup tracking, or causal attribution is supplied."],
            stage="conversion" if entry.get("stat") in ("PTS", "3PM") else "opportunity"))
    if entry.get("cutoff_matchup_proxy"):
        ledger["inferred_proxy"].append(_evidence_item(
            "cutoff_model_matchup_proxy", entry["cutoff_matchup_proxy"],
            "Cutoff-specific model matchup input. It is the only permitted DVP/matchup proxy for this as-of slate and remains an association-level model input, not tracking or causation.",
            f"supports {side.lower()}" if side else "neutral",
            confidence="model evidence", stage="conversion"))
    offense = entry.get("opponent_rebound_miss_profile")
    if entry.get("stat") == "REB" and offense:
        ledger["inferred_proxy"].append(_evidence_item(
            "opponent_miss_supply_proxy", offense,
            "Opponent aggregate misses and long-versus-short miss locations describe expected miss supply, not rebound landing, tracking, or a claim that any big loses rebounds.",
            "rebound-environment context", confidence="aggregate proxy",
            counter_signals=["Rebound allocation, lineup context, and landing location are unknown."],
            stage="opportunity"))
    if entry.get("stat") == "BLK" and entry.get("blockable_shot_location_proxy"):
        ledger["inferred_proxy"].append(_evidence_item(
            "blockable_shot_location_proxy", entry["blockable_shot_location_proxy"],
            "Rim-plus-paint attempt share is an aggregate shot-location proxy, not contest opportunity or tracking.",
            "block-environment context", confidence="aggregate proxy", stage="opportunity"))
    if entry.get("stat") == "STL" and entry.get("opponent_turnovers_pg"):
        ledger["inferred_proxy"].append(_evidence_item(
            "opponent_turnover_exposure_proxy", entry["opponent_turnovers_pg"],
            "Opponent turnovers per played game are aggregate exposure only; turnover types and steal credit are unknown.",
            "steal-environment context", confidence="aggregate proxy", stage="opportunity"))
    missing_stat_context = {
        "REB": ("opponent_miss_supply_proxy",
                "Opponent aggregate miss-supply and miss-location context was not supplied."),
        "BLK": ("blockable_shot_location_proxy",
                "Opponent rim-plus-paint shot-location context was not supplied."),
        "STL": ("opponent_turnover_exposure_proxy",
                "Opponent turnovers per played game were not supplied."),
    }
    stat_context = missing_stat_context.get(entry.get("stat"))
    supplied_context = (
        (entry.get("stat") == "REB" and offense)
        or (entry.get("stat") == "BLK" and entry.get("blockable_shot_location_proxy"))
        or (entry.get("stat") == "STL" and entry.get("opponent_turnovers_pg"))
    )
    if stat_context and not supplied_context:
        ledger["unavailable"].append(_evidence_item(
            stat_context[0], None, stat_context[1], "neutral",
            confidence="unavailable", stage="opportunity"))
    # These blocks are retained in the ledger only. Do not double-serialize
    # context on a large slate.
    for key in ("opponent_rebound_miss_profile", "blockable_shot_location_proxy",
                "opponent_turnovers_pg"):
        entry.pop(key, None)

    for key, stage, mechanism in (
        ("rest", "opportunity", "Days between scheduled games can affect opportunity or efficiency but do not prove a minutes change."),
        ("game_environment", "opportunity", "Pregame spread and total are market-based game-script proxies, not realized pace or minutes."),
        ("referee_crew", "conversion", "Historical crew foul rates are associations and do not prove tonight's whistle."),
        ("slump_risk", "outcome", "A derived warning summarizes historical indicators; it is not a causal diagnosis."),
    ):
        if entry.get(key):
            ledger["inferred_proxy"].append(_evidence_item(
                key, entry[key], mechanism, "context",
                confidence="proxy", stage=stage))

    comparison = {
        "available": bool(alternatives),
        "alternatives": alternatives,
        "interpretation": (
            "Compare modeled edges for the same player; these outcomes share role inputs and are not independent."
            if alternatives else
            "No alternative stat line for this player was supplied on the slate."
        ),
    }
    entry["same_player_cross_stat_comparison"] = comparison
    target = "derived" if alternatives else "unavailable"
    ledger[target].append(_evidence_item(
        "same_player_cross_stat_comparison", _field_reference(
            "same_player_cross_stat_comparison", available=bool(alternatives)),
        "Alternative stats test whether the thesis is specific to this outcome or merely repeats a shared role assumption.",
        "relative comparison" if alternatives else "neutral",
        confidence="model comparison" if alternatives else "unavailable",
        counter_signals=["Same-player stat models may share minutes and role assumptions."],
        stage="outcome"))

    for key in ("projected_minutes", "opportunity_rate", "outcome_rate", "profile_confidence",
                "p_over", "p_under", "p_push", "q10", "q25", "q50", "q75", "q90",
                "selected_probability", "market_no_vig_probability", "probability_edge_pp",
                "expected_roi", "selected_odds"):
        if not _present(entry.get(key)):
            ledger["unavailable"].append(_evidence_item(
                key, None, f"{key} was not supplied; make no claim based on it.",
                "neutral", confidence="unavailable"))
    if not _present(entry.get("opportunity_component")):
        ledger["unavailable"].append(_evidence_item(
            "opportunity_physics", None,
            "No separate opportunity-volume measure was supplied; projected minutes and an outcome-per-minute rate do not establish chances.",
            "neutral", confidence="unavailable", stage="opportunity"))
    if not _present(entry.get("conversion_component")) and not shot:
        ledger["unavailable"].append(_evidence_item(
            "separate_conversion_physics", None,
            "No separate conversion-rate physics was supplied.",
            "neutral", confidence="unavailable", stage="conversion"))
    if entry.get("_as_of_cutoff"):
        ledger["unavailable"].append(_evidence_item(
            "current_aggregate_enrichment", None,
            "Current aggregate snapshots were omitted; only sources reconstructed exclusively before this slate date may appear.",
            "neutral", confidence="unavailable"))
    ledger["provenance_note"] = "Pregame typed evidence. Proxies are not tracking."
    return ledger


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
    historical_backfill = _is_historical_backfill(slate_date)
    caches = {"shot_zones": {}, "team_def": {}, "zone_ranks": {},
              "league_avg": {}, "dvp": {}, "dvp_rank": {}, "dvp_position": {},
              "referee_by_game": {}, "rest": {}, "fp": {}, "quarter": {},
              "game_env_by_game": {}, "opponent_offense": {},
              "team_turnovers_pg": {}, "as_of_exclusive": (
                  _as_date(slate_date).isoformat() if _as_date(slate_date) else None),
              "historical_backfill": historical_backfill}
    try:
        conn = sqlite3.connect(DB)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        def _has(table):
            return cur.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,)).fetchone() is not None

        # A supplied slate date is an exclusive cutoff, including same-day
        # staggered slates. Never touch current aggregate snapshots in this path.
        if slate_date is not None:
            cutoff = _as_date(slate_date).isoformat()
            keys = ("ra", "paint", "mid", "corner3", "atb3")
            def _aggregate_zone_json(column, offense=False):
                acc, names = {}, {}
                if not _has("wnba_game_shot_zones"):
                    return
                for (raw,) in cur.execute(
                        f"SELECT {column} FROM wnba_game_shot_zones "
                        "WHERE game_date < ? AND " + column + " IS NOT NULL", (cutoff,)):
                    try:
                        game = json.loads(raw)
                    except (TypeError, ValueError):
                        continue
                    for team, zone in game.items():
                        if team not in WNBA_FRANCHISES:
                            continue
                        names[team] = zone.get("name", team)
                        bucket = acc.setdefault(team, {f"{k}_{m}": 0
                                                       for k in keys for m in ("fga", "fgm")})
                        for key in keys:
                            bucket[f"{key}_fga"] += int(zone.get(f"{key}_fga", 0) or 0)
                            bucket[f"{key}_fgm"] += int(zone.get(f"{key}_fgm", 0) or 0)
                for team, z in acc.items():
                    total = sum(z[f"{k}_fga"] for k in keys)
                    made = sum(z[f"{k}_fgm"] for k in keys)
                    if not total:
                        continue
                    d = {"team": team, "team_name": names.get(team, team),
                         "total_fga": total, "total_fgm": made}
                    for key in keys:
                        fga, fgm = z[f"{key}_fga"], z[f"{key}_fgm"]
                        d[f"{key}_fga"], d[f"{key}_fgm"] = fga, fgm
                        d[f"{key}_freq"] = round(100 * fga / total, 1)
                        d[f"{key}_fg_pct"] = round(100 * fgm / fga, 1) if fga else None
                        if offense:
                            d[f"{key}_misses"] = fga - fgm
                    if offense:
                        misses = total - made
                        d["total_misses"] = misses
                        for key in keys:
                            d[f"{key}_miss_freq"] = round(
                                100 * d[f"{key}_misses"] / misses, 1) if misses else None
                        long = sum(d[f"{key}_misses"] for key in ("mid", "corner3", "atb3"))
                        d["long_rebound_miss_proxy_pct"] = round(100 * long / misses, 1) if misses else None
                        caches["opponent_offense"][team] = d
                    else:
                        c3a, c3m = d["corner3_fga"], d["corner3_fgm"]
                        a3a, a3m = d["atb3_fga"], d["atb3_fgm"]
                        d["three_fg_pct"] = round(100 * (c3m + a3m) / (c3a + a3a), 1) if c3a + a3a else None
                        caches["team_def"][team] = d

            _aggregate_zone_json("zones_json")
            _aggregate_zone_json("offense_zones_json", offense=True)
            # Recreate rank and league baseline exclusively from prior games.
            caches["zone_ranks"] = {team: {} for team in caches["team_def"]}
            for key in ("ra_fg_pct", "paint_fg_pct", "mid_fg_pct", "three_fg_pct"):
                vals = sorted((team, row[key]) for team, row in caches["team_def"].items()
                              if row.get(key) is not None)
                for idx, (team, _value) in enumerate(vals, 1):
                    caches["zone_ranks"][team][key] = (idx, len(vals))
            for label, fgm_key, fga_key in (
                    ("rim", "ra_fgm", "ra_fga"), ("paint", "paint_fgm", "paint_fga"),
                    ("mid", "mid_fgm", "mid_fga")):
                made = sum(row.get(fgm_key, 0) for row in caches["team_def"].values())
                attempts = sum(row.get(fga_key, 0) for row in caches["team_def"].values())
                caches["league_avg"][label] = round(100 * made / attempts, 1) if attempts else None
            made = sum(row.get("corner3_fgm", 0) + row.get("atb3_fgm", 0)
                       for row in caches["team_def"].values())
            attempts = sum(row.get("corner3_fga", 0) + row.get("atb3_fga", 0)
                           for row in caches["team_def"].values())
            caches["league_avg"]["three"] = round(100 * made / attempts, 1) if attempts else None

            if _has("wnba_player_game_logs"):
                logs = cur.execute(
                    "SELECT player_name, team, game_date, fp, tov FROM wnba_player_game_logs "
                    "WHERE game_date < ?", (cutoff,)).fetchall()
                fp_by_player, tov_by_team, dates_by_team = {}, {}, {}
                for player, team, game_date, fp, tov in logs:
                    if fp is not None:
                        fp_by_player.setdefault(_norm(player), []).append((game_date, float(fp)))
                    if team and tov is not None:
                        tov_by_team.setdefault(team, {}).setdefault(game_date, 0.0)
                        tov_by_team[team][game_date] += float(tov)
                    if team and game_date:
                        dates_by_team[team] = max(dates_by_team.get(team, ""), game_date)
                for player, values in fp_by_player.items():
                    values.sort(reverse=True)
                    nums = [v for _d, v in values]
                    caches["fp"][player] = {
                        "fp_avg": round(sum(nums) / len(nums), 2),
                        "fp_l5": round(sum(nums[:5]) / len(nums[:5]), 2),
                        "fp_sd": round(float(pd.Series(nums).std()), 2) if len(nums) > 1 else 0.0,
                    }
                caches["team_turnovers_pg"] = {
                    team: round(sum(games.values()) / len(games), 2)
                    for team, games in tov_by_team.items() if games
                }
                for team, previous in dates_by_team.items():
                    try:
                        days = (date.fromisoformat(cutoff) - date.fromisoformat(previous)).days
                    except ValueError:
                        continue
                    caches["rest"][team] = {"rest_days": days, "back_to_back": days <= 1}

            # Market fields are slate-specific, so exact-date rows are safe.
            if _has("wnba_games") and _has("wnba_teams"):
                names = {name.strip(): abbr for abbr, name in cur.execute(
                    "SELECT abbr, name FROM wnba_teams") if name}
                for home, away, spread, total in cur.execute(
                        "SELECT home_team, away_team, home_spread, game_total FROM wnba_games "
                        "WHERE game_date = ?", (cutoff,)):
                    h, a = names.get((home or "").strip()), names.get((away or "").strip())
                    if h and a and (spread is not None or total is not None):
                        caches["game_env_by_game"][frozenset((h, a))] = {
                            "home_abbr": h, "away_abbr": a, "home_spread": spread, "game_total": total}
            conn.close()
            return caches

        if _has("wnba_player_shot_zones"):
            for row in cur.execute("SELECT * FROM wnba_player_shot_zones"):
                caches["shot_zones"][_norm(row["player_name"])] = dict(row)

        if _has("wnba_team_defense_shot_zones"):
            for row in cur.execute("SELECT * FROM wnba_team_defense_shot_zones"):
                d = dict(row)
                if d.get("team") not in WNBA_FRANCHISES:
                    continue
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

        if _has("wnba_team_offense_shot_zones"):
            for row in cur.execute("SELECT * FROM wnba_team_offense_shot_zones"):
                d = dict(row)
                if d.get("team") in WNBA_FRANCHISES:
                    caches["opponent_offense"][d["team"]] = d

        # Team turnovers per played game are derived from player box scores.
        # They identify aggregate turnover supply only. Event type (live/dead
        # ball), steal credit, and transition outcome are not available here.
        if _has("wnba_player_game_logs"):
            rows = cur.execute(
                "SELECT team, game_date, SUM(tov) AS turnovers "
                "FROM wnba_player_game_logs WHERE team != '' AND game_date IS NOT NULL "
                "GROUP BY team, game_date").fetchall()
            by_team = {}
            for team, _game_date, turnovers in rows:
                if turnovers is not None:
                    by_team.setdefault(team, []).append(float(turnovers))
            caches["team_turnovers_pg"] = {
                team: round(sum(values) / len(values), 2)
                for team, values in by_team.items() if values
            }

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

        # Quarter-by-quarter scoring and late-game usage from ESPN play-by-play
        # (scrape_wnba_quarter_splits.py). Tells Claude WHEN a player scores and
        # whether she stays featured late in close games.
        if _has("wnba_player_quarter_splits"):
            for row in cur.execute(
                "SELECT player_name, games, q1_pts, q2_pts, q3_pts, q4_pts, "
                "q4_pts_share, q4_team_fga_share, clutch_pts_pg, clutch_fga_pg "
                "FROM wnba_player_quarter_splits WHERE games >= 5"):
                caches["quarter"][_norm(row["player_name"])] = dict(row)

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

        # Game environment (spread + total) per matchup, keyed by team-abbr pair.
        # Columns arrive via scrape_wnba_props.py; older DBs simply skip this.
        if _has("wnba_games") and _has("wnba_teams"):
            try:
                n2a = {}
                for abbr, nm in cur.execute("SELECT abbr, name FROM wnba_teams"):
                    if nm:
                        n2a[nm.strip()] = abbr
                for home, away, spread, total in cur.execute(
                        "SELECT home_team, away_team, home_spread, game_total FROM wnba_games"):
                    h = n2a.get((home or "").strip())
                    a = n2a.get((away or "").strip())
                    if not h or not a or (spread is None and total is None):
                        continue
                    caches["game_env_by_game"][frozenset((h, a))] = {
                        "home_abbr": h, "away_abbr": a,
                        "home_spread": spread, "game_total": total,
                    }
            except sqlite3.OperationalError:
                pass

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
    opp_offense = caches.get("opponent_offense", {}).get(opponent)

    if opp_offense and stat == "REB":
        total_misses = opp_offense.get("total_misses") or 0
        three_misses = ((opp_offense.get("corner3_misses") or 0)
                        + (opp_offense.get("atb3_misses") or 0))
        interior_misses = ((opp_offense.get("ra_misses") or 0)
                           + (opp_offense.get("paint_misses") or 0))
        out["opponent_rebound_miss_profile"] = {
            "label": "aggregate shot-origin misses, not rebound landing data or tracking",
            "total_misses": total_misses,
            "long_rebound_miss_proxy_pct": opp_offense.get("long_rebound_miss_proxy_pct"),
            "three_point_miss_share_pct": round(100 * three_misses / total_misses, 1) if total_misses else None,
            "midrange_miss_share_pct": round(100 * (opp_offense.get("mid_misses") or 0) / total_misses, 1) if total_misses else None,
            "interior_miss_share_pct": round(100 * interior_misses / total_misses, 1) if total_misses else None,
        }
    if opp_offense and stat == "BLK":
            total = opp_offense.get("total_fga") or 0
            blockable = (opp_offense.get("ra_fga") or 0) + (opp_offense.get("paint_fga") or 0)
            out["blockable_shot_location_proxy"] = {
                "rim_paint_attempt_share_pct": round(100 * blockable / total, 1) if total else None,
                "label": "aggregate rim-plus-paint shot-location proxy, not contest opportunity or tracking",
            }
    if stat == "STL":
        turnovers = caches.get("team_turnovers_pg", {}).get(opponent)
        if turnovers is not None:
            out["opponent_turnovers_pg"] = {
                "turnovers_per_played_game": turnovers,
                "label": "aggregate live-ball exposure proxy; turnover types and steal credit are unknown",
            }

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

    # Quarter/clutch profile: when she scores and whether she stays featured
    # late. Most useful for PTS lines // included for every stat as game-flow
    # context (a player benched in close 4th quarters has a softer ceiling).
    qs = caches.get("quarter", {}).get(_norm(player))
    if qs:
        block = {
            "pts_by_quarter": [qs.get("q1_pts"), qs.get("q2_pts"),
                               qs.get("q3_pts"), qs.get("q4_pts")],
        }
        if qs.get("q4_pts_share") is not None:
            block["q4_pts_share_pct"] = qs["q4_pts_share"]
        if qs.get("q4_team_fga_share") is not None:
            block["q4_team_fga_share_pct"] = qs["q4_team_fga_share"]
        if qs.get("clutch_pts_pg") is not None:
            block["clutch_pts_pg"] = qs["clutch_pts_pg"]
        if qs.get("clutch_fga_pg") is not None:
            block["clutch_fga_pg"] = qs["clutch_fga_pg"]
        out["quarter_profile"] = block

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
    # Slump risk is a current aggregate snapshot. Explicit as-of briefings must
    # not pull it into a same-day or historical slate.
    slump = {} if caches.get("as_of_exclusive") else _load_slump_risk()
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
            "position": POS_LABEL.get(pos, pos) if pos else None,
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
            "_historical_backfill": bool(caches.get("historical_backfill")),
            "_as_of_cutoff": bool(caches.get("as_of_exclusive")),
        }
        for field in OPTIONAL_MODEL_FIELDS:
            value = _row_value(r, field)
            if _present(value):
                if field == "evidence_json" and isinstance(value, str):
                    try:
                        value = json.loads(value)
                    except (TypeError, ValueError, json.JSONDecodeError):
                        value = {"status": "unavailable", "error": "invalid evidence_json"}
                entry[field] = value
                if field == "evidence_json" and isinstance(value, dict):
                    proxy = {key: value.get(key) for key in (
                        "matchup_source", "applied_matchup_factor", "matchup_sample")
                        if _present(value.get(key))}
                    if proxy:
                        entry["cutoff_matchup_proxy"] = proxy
        # Legacy field name is ambiguous and must never be interpreted as an
        # opportunity/chance measure. Keep a neutral null for schema continuity.
        entry["opportunity_rate"] = None
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

        # Vegas game environment (spread/total) for this game, from the pick's
        # team's perspective. Lenient read: script tags only at clear extremes.
        ge = caches.get("game_env_by_game", {}).get(frozenset((r["team"], r["opponent"])))
        if ge:
            hs, gt = ge.get("home_spread"), ge.get("game_total")
            team_spread = None
            if hs is not None:
                team_spread = hs if r["team"] == ge.get("home_abbr") else -hs
            env = {}
            if team_spread is not None:
                env["team_spread"] = round(team_spread, 1)
            if gt is not None:
                env["game_total"] = gt
                if team_spread is not None:
                    env["implied_team_total"] = round((gt - team_spread) / 2.0, 1)
            if team_spread is not None:
                if abs(team_spread) <= 4:
                    env["script"] = "tight game expected"
                elif abs(team_spread) >= 11:
                    env["script"] = ("heavy favorite // blowout benching risk"
                                     if team_spread < 0 else
                                     "big underdog // blowout benching risk")
            if env:
                entry["game_environment"] = env

        sr = slump.get(nk)
        if sr:
            entry["slump_risk"] = sr
        prop_lines.append(entry)
    for entry in prop_lines:
        alternatives = [
            {
                "stat": other.get("stat"),
                "book_line": other.get("book_line"),
                "projected": other.get("model_mean", other.get("projected")),
                "edge_pct": other.get("edge_pct"),
                "model_side": other.get("model_side"),
                **{k: other[k] for k in ("p_over", "p_under", "p_push", "q25", "q50", "q75")
                   if _present(other.get(k))},
            }
            for other in prop_lines
            if _norm(other.get("player")) == _norm(entry.get("player"))
            and str(other.get("stat")) != str(entry.get("stat"))
        ]
        entry["evidence_ledger"] = _build_evidence_ledger(entry, alternatives)
        # evidence_json is parsed solely for its cutoff-safe matchup fields.
        # The raw payload can be large and is not a second source of truth once
        # the structured ledger has been assembled.
        entry.pop("evidence_json", None)
        entry.pop("cutoff_matchup_proxy", None)
        entry.pop("_historical_backfill", None)
        entry.pop("_as_of_cutoff", None)
    return prop_lines


SYSTEM_PROMPT = """You are an elite WNBA DFS analyst for PIRTDICA SPORTS CO. You are given the full WNBA slate with model projections and a typed pregame evidence ledger.

You are competing against the sharpest analysts at FanDuel who set these player prop lines. Respect the lines. Only attack when you have genuine conviction backed by multiple converging signals.

YOUR JOB: Independently analyze the slate and select 4-8 HIGH confidence prop picks. You are NOT limited to what the model labeled HIGH; evaluate every prop line and find the sharpest edges yourself.

ANALYTICAL FRAMEWORK (this is how a sharp WNBA analyst reasons // follow it):
1. SHOT-DIET vs OPPONENT-DEFENSE-BY-ZONE (conversion signal when present): After establishing opportunity, use `zone_matchup_edges`, or `shot_diet` + `opp_def_zones`, as conversion context. Cite the player's historical zone share and the opponent's aggregate allowed FG% and rank. This is an association between separate historical aggregates, not tracking evidence, a defender assignment, or proof that the matchup causes tonight's result.
2. DVP (defense vs the stat): DVP is an aggregate association-level proxy, never a tracking fact or a known individual assignment. Team DVP and position DVP overlap. Treat them as ONE matchup signal and never count agreement between them as two confirmations. State disagreement when present.
For an explicit as-of slate, do not use any DVP table. Only `cutoff_matchup_proxy` fields emitted in that recommendation's evidence JSON may be discussed as matchup context.
3. PROJECTION EDGE: `edge_pct` is the model's projected value versus the book line.
4. CONFIRMATION SIGNALS (secondary): hit rate, last-5 form, CV (consistency). Use these to CONFIRM a pick that the shot-diet/DVP/projection signals already support // do NOT lead with last-5 or hit rate. Lower CV is a tailwind; high CV demands a bigger edge.
5. SLUMP RISK (caution flag when present): if a pick has `slump_risk`, our Slump Risk model has flagged this player as MODERATE or HIGH risk of cooling off, with the observable `signals` that drove it (shrinking minutes/usage, an unsustainable hot streak due for regression, tough matchup, or short rest). For an OVER, treat a HIGH flag as a real reason for caution and either avoid it or explicitly acknowledge the risk and explain why the matchup still wins. For an UNDER, a slump-risk flag REINFORCES the case. Cite the specific signal honestly. Never hide it.
6. REST / BACK-TO-BACK (`rest` when present): `rest_days` is the days off before tonight for this player's team and `back_to_back` is true on the second night of a back-to-back. `opponent_rest_days` and `rest_edge` ('advantage'/'disadvantage') compare the two teams. A back-to-back is a real fatigue headwind (trimmed minutes, dip in efficiency) // lean it against aggressive OVERs and toward UNDERs. A clear rest advantage is a tailwind for volume OVERs. Use it as a supporting environment signal.
7. FANTASY-POINT CONTEXT (`fp_context` when present): `season_fp_pg` is the player's season fantasy output, `last5_fp_pg` is recent form, and `fp_ceiling`/`fp_floor` are an honest +/- 1 SD band around the season average (NOT a tonight projection). A wide band means a volatile, boom-or-bust profile (demand a bigger edge); a tight band supports confidence. If recent FP is running well above the season average, pair it with the slump-risk read before chasing an OVER.
8. QUARTER / CLUTCH PROFILE (`quarter_profile` when present): built from play-by-play. `pts_by_quarter` is the player's average points in Q1-Q4, `q4_pts_share_pct` is the share of her scoring that comes in the 4th, `q4_team_fga_share_pct` is her share of her TEAM's 4th-quarter shots (late-game usage), and `clutch_pts_pg`/`clutch_fga_pg` are her scoring and volume in clutch time (Q4/OT, within 5 points, under 5:00). A player who keeps or grows her share late is safer for PTS OVERs (she closes games); a player whose scoring fades in the 4th or who loses late-game touches has a softer ceiling and a blowout-benching risk // that supports UNDERs on lines that need a full 40-minute run. Supporting signal, not the headline.
9. VEGAS GAME ENVIRONMENT (`game_environment` when present): `team_spread` is the pick's team's point spread (negative = favored), `game_total` is the Vegas over/under, and `implied_team_total` is the team's expected score. Use it with LENIENCE // spreads miss all the time and OTs are rare, so this is a supporting environment signal, never a veto. A tight spread (`script` = "tight game expected") keeps starters on the floor to the final whistle // that supports volume OVERs for heavy-minutes players and argues against UNDERs that need an early exit. A big spread (blowout benching risk) is a mild headwind for OVERs that need a full run. A high total or implied team total signals a scoring environment only: it may reflect pace, efficiency, or both, so never claim it proves possessions. Cite the actual numbers when you use them.
10. REFEREE CREW (`referee_crew` when present): the officials assigned to this game with their foul environment. `avg_fouls_pg` is the crew's average fouls per game and `whistle` is tight/average/lenient vs the WNBA crew average. A `tight` whistle crew creates more free-throw and foul-out volume (supports PTS OVERs for foul-drawers, raises foul-trouble risk for bigs); a `lenient` crew suppresses FT-dependent scoring. This is a minor supporting signal, never the headline.

CORRELATION & STACKING (reason ACROSS your own picks, not just one pick at a time): picks in the SAME game share an opponent and game environment (referee whistle, rest, and the market scoring environment). When two of your picks are teammates, or fall in the same game, say so. Teammates both attacking the same leaky zone or a soft DVP, or both playing in a tight-whistle (more fouls, more free throws) game, are positively correlated and tend to rise together // that is real tournament upside, but they also bust together, so spreading picks across different games is the safer build. An OVER on one player and an UNDER on a teammate fighting for the same usage can offset // flag it. There are NO DFS ownership inputs for the WNBA slate, so do not invent leverage or chalk claims. Vegas spread/total context arrives per pick in `game_environment` // only cite the numbers given there, never invent lines.

RECENT RESULTS (`recent_pick_results` at the top of the slate data, when present): our OWN graded track record on past WNBA picks. `recent` is the last N days and `overall_to_date` is the full graded history, with `recent_by_stat` and `recent_by_direction` win-loss breakdowns (each carrying `decided` graded picks and `win_pct`). Use it as honest internal calibration only: be more selective on stats or directions that have been cold over a meaningful sample, and trust the ones we have been sharp on. Weight small samples lightly. NEVER mention this track record in the published analysis // it is internal calibration only.

PICK SELECTION:
- Prioritize picks where shot-diet vs zone defense and/or DVP point the same direction as the projection edge, then confirm with form.
- Do not write an analysis that only cites last-5 and hit rate when zone/DVP context is available. The zone matchup and DVP are why these are PIRTDICA picks.
- A soft opponent in that stat/zone supports OVERs, a tough one supports UNDERs.
- For every pick, reason in order: OPPORTUNITY (separately supplied minutes/volume only) -> CONVERSION (separately supplied efficiency only) -> OUTCOME (distribution relative to the line). `outcome_rate` is a realized box-score outcome-per-minute rate, not opportunities or chances. Legacy `opportunity_rate` is unavailable. If a stage is unavailable, say so rather than filling the gap.
- Read `evidence_ledger` classifications literally. `inferred_proxy` supports an association only, not causation. Never turn team aggregates, zones, DVP, spread, rest, referee data, or quarter splits into claims about tracking, defensive assignments, play calls, role changes, or guaranteed minutes.
- Use `same_player_cross_stat_comparison` before selecting. Compare the chosen stat with alternatives for that player and say whether another stat has a cleaner edge. If none is available, state that the cross-stat check is unavailable.

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


WNBA_ANALYST_PATTERNS = """
ANALYSIS QUALITY PATTERNS
- Matchup convergence: Lead with the most player-specific zone or positional DVP mismatch. Then connect it to the projection and use form only as confirmation.
- Role and volume: Explain what creates the opportunities needed to clear or stay below the line. Use rest, quarter usage, and game script only when the supplied numbers materially affect that opportunity.
- Honest tension: Name the strongest counter-signal for every pick. Explain why the primary signals still outweigh it, or do not select the pick.
- Distinctive reasoning: Each analysis must have a different lead insight tied to that player's supplied context. Repeating a generic projection/last-five/hit-rate formula is a failure.
- Evidence discipline: Never invent an injury, role change, lineup fact, pace, spread, total, referee trend, or matchup number. If a field is absent, omit that angle.
- Causal discipline: Say aggregate proxies are associated with an outcome, not that they cause it. Never describe a proxy as player tracking or a defender assignment.
- Basketball chain: Explicitly connect opportunity -> conversion -> outcome. Do not skip a missing link; identify it as unavailable.
- Alternative-stat test: Cite the same-player cross-stat comparison and explain why this stat is cleaner, or state that no alternative was supplied.
"""


WNBA_ANALYSIS_BLUEPRINT = """
STRUCTURE EACH ANALYSIS LIKE THIS
Paragraph 1: A bold, player-specific opportunity hook using supplied numbers, or explicitly say opportunity evidence is unavailable.
Paragraph 2: Move from opportunity to conversion and then the modeled outcome relative to the line. Label association-level proxies honestly.
Paragraph 3: Name the single strongest counter-signal, then compare alternative stats for the same player. If no alternative was supplied, say that check is unavailable.
Final paragraph: State why the evidence still supports the side, then use the required **The Call:** sentence with the exact line, projection, edge, and composite score.

Do not copy this wording. It is a reasoning structure, not a prose template.
"""


def _analysis_word_count(text):
    """Count prose words without treating Markdown punctuation as content."""
    return len(re.findall(r"\b[\w%+.']+\b", str(text)))


def _validate_claude_result(result, prop_lines):
    """Reject shallow, mismatched, or repetitive output before it is published."""
    if not isinstance(result, dict):
        return False, "response is not an object"
    picks = result.get("picks")
    analyses = result.get("analyses")
    if not isinstance(picks, list) or not isinstance(analyses, list):
        return False, "picks/analyses are not arrays"

    if len(prop_lines) < 2:
        return False, "fewer than 2 available prop lines"
    minimum = min(4, len(prop_lines))
    if len(picks) < minimum or len(picks) > 8:
        return False, f"expected {minimum}-8 picks, received {len(picks)}"
    if len(analyses) != len(picks):
        return False, f"pick/analysis count mismatch ({len(picks)} vs {len(analyses)})"

    available = {
        (_norm(p.get("player")), str(p.get("stat", "")).upper(), str(p.get("model_side", "")).upper()): p
        for p in prop_lines
    }
    fingerprints = set()
    selected = set()
    for idx, (pick, analysis) in enumerate(zip(picks, analyses), start=1):
        pick_key = (
            _norm(pick.get("player")),
            str(pick.get("stat", "")).upper(),
            str(pick.get("pick", "")).upper(),
        )
        analysis_key = (
            _norm(analysis.get("player")),
            str(analysis.get("stat", "")).upper(),
            str(analysis.get("call", "")).upper(),
        )
        if pick_key not in available:
            return False, f"pick {idx} does not match an available player/stat/side"
        if pick_key in selected:
            return False, f"pick {idx} duplicates another selected player/stat/side"
        selected.add(pick_key)
        if analysis_key != pick_key:
            return False, f"analysis {idx} does not match its pick"

        text = str(analysis.get("analysis", "")).strip()
        call_match = re.search(
            r"\*\*The Call:\s*(OVER|UNDER)\s+(-?\d+(?:\.\d+)?)\s+([A-Za-z0-9]+)\*\*",
            text,
            flags=re.IGNORECASE,
        )
        if not call_match:
            return False, f"analysis {idx} is missing a parseable call line"
        if re.search(r"\n\s*\n", text[call_match.end():]):
            return False, f"analysis {idx} places prose after the final call paragraph"
        visible_side, visible_line, visible_stat = call_match.groups()
        source = available[pick_key]
        if visible_side.upper() != pick_key[2] or visible_stat.upper() != pick_key[1]:
            return False, f"analysis {idx} visible call contradicts its pick"
        try:
            if abs(float(visible_line) - float(source.get("book_line"))) > 0.001:
                return False, f"analysis {idx} visible call uses the wrong book line"
        except (TypeError, ValueError):
            return False, f"analysis {idx} has an invalid book line"

        words = _analysis_word_count(text)
        paragraphs = [p for p in re.split(r"\n\s*\n", text) if p.strip()]
        # Claude's word-count estimates are approximate. Keep the writing
        # target at 150-250, but allow a modest overrun so a strong 300-word
        # analysis is not replaced by the much less useful template fallback.
        if words < 140 or words > 350:
            return False, f"analysis {idx} has {words} words (expected 140-350)"
        if len(paragraphs) < 3:
            return False, f"analysis {idx} has fewer than 3 paragraphs"
        if len(re.findall(r"\d+(?:\.\d+)?%?", text)) < 4:
            return False, f"analysis {idx} lacks specific numerical evidence"

        fingerprint = re.sub(r"\W+", "", text.lower())[:180]
        if fingerprint in fingerprints:
            return False, f"analysis {idx} repeats another analysis"
        fingerprints.add(fingerprint)

        # Persist source-of-truth pick fields and a canonical visible call line.
        # Claude supplies the reasoning, but it never gets the final say on the
        # number, direction, or score subscribers see.
        pick.update({
            "player": source.get("player"),
            "team": source.get("team"),
            "opponent": source.get("opponent"),
            "stat": source.get("stat"),
            "book_line": source.get("book_line"),
            "projected": source.get("projected"),
            "avg": source.get("season_avg"),
            "edge": _edge_str(source.get("edge_pct")),
            "pick": source.get("model_side"),
            "composite_score": source.get("composite_score"),
        })
        canonical_call = (
            f"**The Call: {source.get('model_side')} {source.get('book_line')} {source.get('stat')}** "
            f"// We project {source.get('player')} at {source.get('projected')} "
            f"{source.get('stat')} tonight ({_edge_str(source.get('edge_pct'))} edge vs. the book). "
            f"Composite score: {source.get('composite_score')}."
        )
        canonical_text = re.sub(
            r"\*\*The Call:.*$",
            canonical_call,
            text,
            count=1,
            flags=re.IGNORECASE | re.DOTALL,
        )
        canonical_words = _analysis_word_count(canonical_text)
        canonical_paragraphs = [
            p for p in re.split(r"\n\s*\n", canonical_text) if p.strip()
        ]
        if canonical_words < 140 or canonical_words > 350:
            return False, (
                f"analysis {idx} has {canonical_words} words after canonical call "
                "replacement (expected 140-350)"
            )
        if len(canonical_paragraphs) < 3:
            return False, f"analysis {idx} has fewer than 3 paragraphs after canonicalization"
        if len(re.findall(r"\d+(?:\.\d+)?%?", canonical_text)) < 4:
            return False, f"analysis {idx} lacks numerical evidence after canonicalization"
        analysis["analysis"] = canonical_text
    return True, "ok"


def _recent_pick_results_before_slate(slate_date):
    """Use shared feedback only when its through-today window is pre-slate."""
    try:
        from zoneinfo import ZoneInfo
        today = datetime.now(ZoneInfo("America/New_York")).date()
    except Exception:
        today = date.today()
    cutoff = _as_date(slate_date)
    if not cutoff or cutoff <= today:
        return {}
    from pick_feedback import load_recent_pick_results
    return load_recent_pick_results("wnba", recent_days=14)


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
        # The shared loader aggregates through today and has no as-of parameter.
        # The wrapper omits same-day/historical slates so grades can never leak
        # from on/after this slate's exclusive cutoff.
        rpr = _recent_pick_results_before_slate(slate_date)
        if rpr:
            briefing["recent_pick_results"] = rpr
            rec = rpr.get("recent", {})
            print(f"[WNBA ARTICLE] recent pick results (last {rpr.get('window_days')}d): "
                  f"{rec.get('record')} ({rec.get('decided')} decided)")
    except Exception as _fb_err:
        print(f"[WNBA ARTICLE] recent pick results load failed ({_fb_err})")
    user_prompt = (
        "Analyze the full WNBA slate and select your HIGH confidence picks.\n\n"
        f"{WNBA_ANALYST_PATTERNS}\n\n"
        f"{WNBA_ANALYSIS_BLUEPRINT}\n\n"
        "HERE IS THE COMPLETE SLATE DATA (every prop line with model context):\n\n"
        f"{json.dumps(briefing, indent=2, default=str)}\n\n"
        "Remember: select 4-8 picks with the strongest convergence of signals. Every "
        "analysis must be 3-4 substantive paragraphs and 150-250 words, cite supplied "
        "numbers, include a real counter-signal, end with a bold **The Call:** line, "
        "and NEVER use em-dashes. Return ONLY a JSON object with \"picks\" and "
        "\"analyses\" keys."
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
        valid, reason = _validate_claude_result(result, prop_lines)
        if not valid:
            print(f"[WNBA ARTICLE][QUALITY REJECTED] {reason} // using visible template fallback.")
            return None
        print(f"[WNBA ARTICLE][QUALITY PASSED] {len(result['picks'])} picks with matched, substantive analyses.")
        return result
    except Exception as e:
        print(f"[WNBA ARTICLE][CLAUDE ERROR] {e} // using visible template fallback.")
        return None


def _template_result(prop_lines):
    """Data-driven fallback when Claude is unavailable or fails quality checks."""
    print("[WNBA ARTICLE][FALLBACK ACTIVE] Publishing deterministic data-driven analysis; claude_selected=false.")
    def _num(value, digits=1):
        if not _present(value):
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        text = f"{number:.{digits}f}"
        return text.rstrip("0").rstrip(".")

    def _prob(value):
        if not _present(value):
            return None
        number = float(value)
        return f"{number * 100 if abs(number) <= 1 else number:.1f}%"

    def _roi(value):
        if not _present(value):
            return None
        number = float(value)
        return f"{number * 100 if abs(number) <= 1 else number:.1f}%"

    def _odds(value):
        if not _present(value):
            return None
        number = float(value)
        return f"{number:+.0f}" if abs(number) >= 100 else _num(number, 2)

    highs = [p for p in prop_lines if p["confidence"] in ("HIGH", "MEDIUM")][:6]
    if not highs:
        highs = prop_lines[:5]
    picks, analyses = [], []
    for p in highs:
        side = p["model_side"]
        stat_full = STAT_FULL.get(p["stat"], p["stat"].lower())
        edge = p["edge_pct"]
        ledger = p.get("evidence_ledger", {})
        inferred = {x.get("signal"): x for x in ledger.get("inferred_proxy", [])}
        opportunity_component = _num(p.get("opportunity_component"), 2)
        if opportunity_component is not None:
            opportunity = (f"**Opportunity starts with the supplied volume component of "
                           f"{opportunity_component}.**")
        elif _present(p.get("projected_minutes")):
            opportunity = (f"**The model projects {_num(p['projected_minutes'])} minutes, but "
                           "direct opportunity volume is not measured.**")
        else:
            opportunity = "**Direct opportunity volume is not measured for this prop.**"
        season = p.get("season_avg", p.get("player_avg"))
        opportunity += (f" Her season result is {_num(season)} {p['stat']} per game and the "
                        f"last-five result is {_num(p.get('last5_avg'))}. These are descriptive "
                        "box-score outcomes, not evidence of tonight's role or defensive context.")
        if _present(p.get("outcome_rate")):
            opportunity += (f" The historical outcome rate is {_num(p['outcome_rate'], 3)} per "
                            "minute; it is not a count of chances.")

        conversion_component = _num(p.get("conversion_component"), 2)
        if conversion_component is not None:
            conversion = (f"**Conversion is represented by the supplied model component of "
                           f"{conversion_component}.**")
        elif p.get("shot_diet"):
            conversion = ("**Historical shot diet supplies conversion context.** It remains "
                          "descriptive and does not identify a defender or assignment.")
        else:
            conversion = ("**Direct conversion physics are not measured.** The projection can "
                          "still summarize outcomes, but it cannot establish how tonight's chances convert. "
                          "That missing split prevents claims about touches, attempt quality, or efficiency "
                          "and remains an explicit limitation on confidence.")
        proxy = inferred.get("opponent_miss_supply_proxy") or inferred.get(
            "blockable_shot_location_proxy") or inferred.get("opponent_turnover_exposure_proxy")
        if proxy:
            value = proxy.get("value", {})
            if p["stat"] == "REB":
                conversion += (f" The opponent profile records {value.get('total_misses')} aggregate misses "
                               f"and {_num(value.get('long_rebound_miss_proxy_pct'))}% long-origin misses, "
                               "not rebound landing data.")
            elif p["stat"] == "BLK":
                conversion += (f" Opponent rim-plus-paint attempt share is "
                               f"{_num(value.get('rim_paint_attempt_share_pct'))}%, a shot-location proxy "
                               "rather than contest opportunity.")
            else:
                conversion += (f" Opponent turnovers are {_num(value.get('turnovers_per_played_game'))} per "
                               "played game; turnover types and steal credit are unknown.")

        probability = p.get("selected_probability")
        market = p.get("market_no_vig_probability")
        pp_edge = p.get("probability_edge_pp")
        gap = float(p["projected"]) - float(p["book_line"])
        outcome = (f"**The outcome distribution centers on {_num(p['projected'])} against a "
                   f"{_num(p['book_line'])} line, a {gap:+.1f} {p['stat']} projection gap.**")
        if _present(probability):
            outcome += f" Selected-side probability is {_prob(probability)}."
        if _present(market) and _present(pp_edge):
            outcome += (f" The no-vig market is {_prob(market)}, leaving a "
                        f"{float(pp_edge):+.1f} percentage-point edge versus the no-vig market.")
        if _present(p.get("selected_odds")):
            outcome += f" The selected price is {_odds(p['selected_odds'])}."
        if _present(p.get("expected_roi")):
            outcome += f" Expected ROI is {_roi(p['expected_roi'])}."
        middle = [k for k in ("q25", "q50", "q75") if _present(p.get(k))]
        tails = middle if len(middle) == 3 else [
            k for k in ("q10", "q90") if _present(p.get(k))]
        if tails:
            outcome += " Tail range: " + ", ".join(f"{k} {_num(p[k])}" for k in tails) + "."

        last5 = p.get("last5_avg")
        low_conf = str(p.get("profile_confidence", "")).lower() == "low"
        recent_disagrees = (_present(last5) and
                            ((side == "OVER" and float(last5) < float(p["book_line"]))
                             or (side == "UNDER" and float(last5) > float(p["book_line"]))))
        crossing_key = "q25" if side == "OVER" else "q75"
        quantile_crosses = (_present(p.get(crossing_key)) and
                            ((side == "OVER" and float(p[crossing_key]) < float(p["book_line"]))
                             or (side == "UNDER" and float(p[crossing_key]) > float(p["book_line"]))))
        if recent_disagrees:
            uncertainty = (f"The strongest counter-signal is the last-five average of "
                           f"{_num(last5)}, which sits on the other side of the line.")
        elif low_conf:
            uncertainty = "The strongest counter-signal is the model's low profile confidence."
        elif quantile_crosses:
            uncertainty = (f"The strongest counter-signal is {crossing_key} at "
                           f"{_num(p[crossing_key])}, which crosses the book line.")
        else:
            uncertainty = "The strongest counter-signal is finite sample and model uncertainty."
        outcome += " " + uncertainty

        analysis = (
            f"**{p['player']} is our {side.lower()} look on {stat_full}.** {opportunity}\n\n"
            f"{conversion}\n\n"
            f"{outcome}\n\n"
            f"The supplied distribution and price support the modeled side, not certainty. "
            f"The call follows the available pregame evidence while preserving the stated tail and model risk. "
            f"**The Call: {side} {p['book_line']} {p['stat']}** // We project {p['player']} at "
            f"{p['projected']} {p['stat']} tonight. "
            + (f"Probability edge: {float(pp_edge):+.1f} percentage points versus the no-vig market. "
               if _present(pp_edge) else "") +
            f"Composite score: {p['composite_score']}."
        )
        picks.append({
            "player": p["player"], "team": p["team"], "opponent": p["opponent"],
            "stat": p["stat"], "book_line": p["book_line"], "projected": p["projected"],
            "avg": p.get("season_avg", p.get("player_avg")), "edge": _edge_str(edge), "pick": side,
            "composite_score": p["composite_score"],
        })
        analyses.append({
            "player": p["player"], "stat": p["stat"], "call": side,
            "archetype": p.get("position", "Player"), "team": p["team"], "opponent": p["opponent"],
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
    meta, records = _player_meta(slate_date)
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
