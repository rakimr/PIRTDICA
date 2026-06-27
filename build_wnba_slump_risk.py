"""
WNBA Slump Risk Engine for PIRTDICA SPORTS CO.

Predicts which rotation players are most likely to cool off BEFORE it shows up in
the box score, using only observable leading indicators:

  1. Minutes trend       -- last-5 minutes vs season baseline (shrinking role)
  2. Usage trend         -- last-5 usage proxy vs season usage proxy (fewer touches)
  3. Hot-streak regression -- last-5 fantasy points running above season mean
                              (mean reversion is the single most common cause of a
                              cool-off after a hot run)
  4. Schedule difficulty -- upcoming opponent defense (DVP) + rest / back-to-back

Each indicator becomes a 0-100 sub-score (higher = more risk). The blended
overall score maps to LOW / MODERATE / HIGH. A short honest narrative is written
for every elevated player via a three-tier system that mirrors the article
generator: Claude (primary) -> factor-based template (fallback) -> minimal note
(final fallback). Output is written to the `wnba_slump_risk` table in SQLite so
sync_to_postgres.py mirrors it to `wnba_slump_risk_live`.

This engine is deliberately scoped to observable, on-court drivers. It does NOT
attempt to infer anything about a player's body or personal life.

Inputs (SQLite dfs_nba.db):
  - wnba_player_stats        : min_avg/min_l5, fp_avg/fp_sd/fp_l5, games, position
  - wnba_player_value        : usage_proxy (season)
  - wnba_player_game_logs    : recent per-game min/pts/reb/ast for L5 usage + rest
  - wnba_games               : upcoming slate (next opponent + rest days)
  - wnba_dvp                 : opponent defense vs stat (matchup difficulty)
  - wnba_teams               : full-name <-> abbreviation map (games use full names)
"""
import json
import os
import sqlite3
import time
from datetime import datetime, date

DB = "dfs_nba.db"

# Only rate players who actually carry a rotation role. Below this we have too
# little signal and the trends are dominated by noise.
MIN_GAMES = 4
MIN_MPG = 12.0

# Risk bands.
HIGH_CUTOFF = 60.0
MODERATE_CUTOFF = 35.0

# Blend weights when every component is available. Missing components are dropped
# and the remaining weights renormalized so the score is always 0-100.
WEIGHTS = {
    "minutes": 0.30,
    "usage": 0.25,
    "regression": 0.25,
    "schedule": 0.20,
}

DVP_STATS = ("pts", "reb", "ast", "fg3m")


def _today_et():
    try:
        from utils.timezone import get_eastern_today
        return get_eastern_today()
    except Exception:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York")).date()


def _clamp(v, lo=0.0, hi=100.0):
    return max(lo, min(hi, v))


def _lerp_score(value, full_risk_at, no_risk_at):
    """Map `value` to 0-100 where it equals 100 at `full_risk_at` and 0 at
    `no_risk_at`, clamped. Works whether full_risk_at < or > no_risk_at."""
    if full_risk_at == no_risk_at:
        return 0.0
    frac = (no_risk_at - value) / (no_risk_at - full_risk_at)
    return _clamp(frac * 100.0)


# ----------------------------------------------------------------------------
# Schedule helpers
# ----------------------------------------------------------------------------

def _name_to_abbr(cur):
    """full team name -> abbr (wnba_games store full names, everything else uses
    abbreviations)."""
    m = {}
    for abbr, name in cur.execute("SELECT abbr, name FROM wnba_teams"):
        if name:
            m[name.strip()] = abbr
    return m


def _next_games(cur, name2abbr, today):
    """For each team abbr, the soonest upcoming game on/after `today`:
    {abbr: (game_date_str, opponent_abbr)}."""
    rows = cur.execute(
        "SELECT game_date, home_team, away_team FROM wnba_games "
        "WHERE game_date >= ? ORDER BY game_date ASC",
        (today.isoformat(),),
    ).fetchall()
    out = {}
    for gdate, home, away in rows:
        h = name2abbr.get((home or "").strip())
        a = name2abbr.get((away or "").strip())
        if h and h not in out:
            out[h] = (gdate, a)
        if a and a not in out:
            out[a] = (gdate, h)
    return out


def _prev_game_dates(cur):
    """abbr -> most recent played game_date (ISO str) from the game logs."""
    out = {}
    for team, gdate in cur.execute(
        "SELECT team, MAX(game_date) FROM wnba_player_game_logs "
        "WHERE team != '' GROUP BY team"
    ):
        if team and gdate:
            out[team] = gdate
    return out


def _dvp_toughness(cur):
    """opponent abbr -> average DVP factor across the tracked stats. factor < 1.0
    means the opponent allows LESS than league average (tough defense)."""
    acc = {}
    for team, stat, factor in cur.execute("SELECT team, stat, factor FROM wnba_dvp"):
        if stat in DVP_STATS and factor is not None:
            acc.setdefault(team, []).append(float(factor))
    return {t: (sum(v) / len(v)) for t, v in acc.items() if v}


def _recent_usage(cur):
    """player_name -> last-5-game usage proxy computed the same way as the season
    usage_proxy in build_wnba_projections (pts + 1.2*ast + 0.5*reb per minute)."""
    rows = cur.execute(
        "SELECT player_name, game_date, min, pts, reb, ast "
        "FROM wnba_player_game_logs WHERE min IS NOT NULL AND min > 0 "
        "ORDER BY player_name, game_date DESC"
    ).fetchall()
    by_player = {}
    for name, _gd, mn, pts, reb, ast in rows:
        if not name:
            continue
        bucket = by_player.setdefault(name, [])
        if len(bucket) < 5:
            bucket.append((mn or 0.0, pts or 0.0, reb or 0.0, ast or 0.0))
    out = {}
    for name, games in by_player.items():
        tot_min = sum(g[0] for g in games)
        if tot_min <= 0:
            continue
        prod = sum(g[1] + 1.2 * g[3] + 0.5 * g[2] for g in games)
        out[name] = round(prod / tot_min, 3)
    return out


# ----------------------------------------------------------------------------
# Sub-score computations
# ----------------------------------------------------------------------------

def _minutes_component(min_avg, min_l5):
    if not min_avg or min_l5 is None:
        return None, None
    pct = (min_l5 - min_avg) / min_avg
    score = _lerp_score(pct, full_risk_at=-0.25, no_risk_at=0.10)
    factor = None
    if pct <= -0.12:
        factor = f"Minutes down {abs(pct) * 100:.0f}% over last 5 ({min_avg:.1f} to {min_l5:.1f})"
    return score, factor


def _usage_component(season_usage, recent_usage):
    if not season_usage or recent_usage is None:
        return None, None
    pct = (recent_usage - season_usage) / season_usage
    score = _lerp_score(pct, full_risk_at=-0.30, no_risk_at=0.15)
    factor = None
    if pct <= -0.15:
        factor = f"Usage down {abs(pct) * 100:.0f}% over last 5 (fewer touches)"
    return score, factor


def _regression_component(fp_avg, fp_sd, fp_l5):
    if fp_avg is None or fp_l5 is None or not fp_sd:
        return None, None
    z = (fp_l5 - fp_avg) / fp_sd
    score = _lerp_score(z, full_risk_at=2.0, no_risk_at=0.0)
    factor = None
    if z >= 0.6:
        factor = (f"Running hot: last-5 FP {fp_l5:.1f} is {z:.1f} SD above "
                  f"season avg {fp_avg:.1f} (regression risk)")
    return score, factor


def _schedule_component(team, next_games, prev_dates, dvp_tough):
    info = next_games.get(team)
    if not info:
        return None, None, None, None, None
    next_date_str, opp = info
    matchup_score = None
    matchup_factor = None
    if opp and opp in dvp_tough:
        factor_val = dvp_tough[opp]
        matchup_score = _lerp_score(factor_val, full_risk_at=0.85, no_risk_at=1.15)
        if factor_val <= 0.95:
            matchup_factor = (f"Tough matchup: {opp} defense allows "
                              f"{(factor_val - 1.0) * 100:+.0f}% vs WNBA average")

    rest_days = None
    rest_score = None
    rest_factor = None
    prev = prev_dates.get(team)
    if prev and next_date_str:
        try:
            rest_days = (date.fromisoformat(next_date_str) - date.fromisoformat(prev)).days
        except Exception:
            rest_days = None
    if rest_days is not None:
        if rest_days <= 1:
            rest_score = 100.0
            rest_factor = f"Back-to-back: {rest_days} day rest before {opp or 'next game'}"
        elif rest_days == 2:
            rest_score = 35.0
        else:
            rest_score = 0.0

    parts, weights = [], []
    if matchup_score is not None:
        parts.append(matchup_score)
        weights.append(0.6)
    if rest_score is not None:
        parts.append(rest_score)
        weights.append(0.4)
    if not parts:
        return None, None, opp, next_date_str, rest_days
    sched_score = sum(p * w for p, w in zip(parts, weights)) / sum(weights)
    factor = matchup_factor or rest_factor
    if matchup_factor and rest_factor:
        factor = matchup_factor + " // " + rest_factor
    return sched_score, factor, opp, next_date_str, rest_days


def _blend(components):
    """components: {name: score-or-None}. Returns weighted 0-100 over available."""
    num, den = 0.0, 0.0
    for name, score in components.items():
        if score is None:
            continue
        w = WEIGHTS[name]
        num += score * w
        den += w
    if den == 0:
        return None
    return round(num / den, 1)


def _risk_level(score):
    if score is None:
        return "UNKNOWN"
    if score >= HIGH_CUTOFF:
        return "HIGH"
    if score >= MODERATE_CUTOFF:
        return "MODERATE"
    return "LOW"


# ----------------------------------------------------------------------------
# Narrative (three-tier: Claude -> template -> minimal)
# ----------------------------------------------------------------------------

def _template_narrative(rec):
    factors = rec.get("factors") or []
    if factors:
        lead = factors[0]
        if len(factors) > 1:
            return f"{lead}. Also: {factors[1].split(':')[0].split('(')[0].strip().lower()}."
        return f"{lead}."
    return "Elevated cool-off risk based on recent role and matchup signals."


def _minimal_narrative(rec):
    return "Elevated cool-off risk based on recent role and matchup signals."


def _claude_narratives(elevated):
    """Return {player_name: narrative} from Claude, or {} on any failure."""
    api_key = os.environ.get("AI_INTEGRATIONS_ANTHROPIC_API_KEY")
    base_url = os.environ.get("AI_INTEGRATIONS_ANTHROPIC_BASE_URL")
    if not api_key or not base_url:
        print("Claude not configured // using template narratives.")
        return {}
    payload = [{
        "player": r["player_name"],
        "team": r["team"],
        "position": r["position"],
        "risk_level": r["risk_level"],
        "score": r["overall_score"],
        "next_opponent": r["next_opponent"],
        "rest_days": r["rest_days"],
        "signals": r["factors"],
    } for r in elevated]
    system = (
        "You are a WNBA DFS analyst for PIRTDICA SPORTS CO. You are given players "
        "our Slump Risk model flagged as likely to COOL OFF soon, each with the "
        "observable signals that drove the flag (minutes trend, usage trend, "
        "hot-streak regression, and schedule difficulty). For each player write "
        "ONE honest, specific sentence (max 30 words) explaining why their "
        "production may dip, citing the concrete signals provided. Be measured, "
        "not alarmist. NEVER use em-dashes or double hyphens // use periods, "
        "commas, or '//'. Base every claim ONLY on the signals provided. Do not "
        "speculate about anything off the court. Return ONLY a JSON object mapping "
        "each exact player name to its sentence."
    )
    user = ("Players flagged by the Slump Risk model:\n\n"
            + json.dumps(payload, indent=2, default=str)
            + "\n\nReturn ONLY a JSON object: {\"Player Name\": \"one sentence\", ...}.")
    try:
        from anthropic import Anthropic
        client = Anthropic(api_key=api_key, base_url=base_url)
        print(f"Calling Claude for {len(elevated)} slump-risk narratives...")
        t0 = time.time()
        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        text = msg.content[0].text.strip()
        print(f"Claude responded in {time.time() - t0:.1f}s ({len(text)} chars)")
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()
        result = json.loads(text)
        if not isinstance(result, dict):
            print("Claude returned unexpected format // template narratives.")
            return {}
        return {str(k): str(v) for k, v in result.items() if v}
    except Exception as e:
        print(f"Claude error ({e}) // template narratives.")
        return {}


# ----------------------------------------------------------------------------
# Persistence
# ----------------------------------------------------------------------------

def _write_table(cur, records):
    # Build into a staging table and swap it in, so a failure mid-build never
    # leaves downstream readers (trends page, article) with a dropped/empty
    # table. The whole thing rides the single transaction committed in main(),
    # so an error before commit rolls back to the previous good copy.
    cur.execute("DROP TABLE IF EXISTS wnba_slump_risk_new")
    cur.execute("""CREATE TABLE wnba_slump_risk_new (
        player_name TEXT, team TEXT, position TEXT, games INTEGER,
        overall_score REAL, risk_level TEXT,
        minutes_score REAL, usage_score REAL, regression_score REAL, schedule_score REAL,
        min_avg REAL, min_l5 REAL, fp_avg REAL, fp_l5 REAL,
        next_opponent TEXT, next_game_date TEXT, rest_days INTEGER,
        factors_json TEXT, narrative TEXT, updated_at TEXT,
        PRIMARY KEY (player_name, team))""")
    now = datetime.now().isoformat()
    rows = []
    for r in records:
        rows.append((
            r["player_name"], r["team"], r["position"], r["games"],
            r["overall_score"], r["risk_level"],
            r["minutes_score"], r["usage_score"], r["regression_score"], r["schedule_score"],
            r["min_avg"], r["min_l5"], r["fp_avg"], r["fp_l5"],
            r["next_opponent"], r["next_game_date"], r["rest_days"],
            json.dumps(r["factors"]), r.get("narrative", ""), now,
        ))
    cur.executemany(
        "INSERT INTO wnba_slump_risk_new VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    cur.execute("DROP TABLE IF EXISTS wnba_slump_risk")
    cur.execute("ALTER TABLE wnba_slump_risk_new RENAME TO wnba_slump_risk")
    return len(rows)


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

def build(cur):
    today = _today_et()
    name2abbr = _name_to_abbr(cur)
    next_games = _next_games(cur, name2abbr, today)
    prev_dates = _prev_game_dates(cur)
    dvp_tough = _dvp_toughness(cur)
    recent_usage = _recent_usage(cur)

    season_usage = {}
    for name, usage in cur.execute(
        "SELECT player_name, usage_proxy FROM wnba_player_value"
    ):
        if name:
            season_usage[name] = usage

    players = cur.execute(
        "SELECT player_name, team, position, games, min_avg, min_l5, "
        "fp_avg, fp_sd, fp_l5 FROM wnba_player_stats "
        "WHERE games >= ? AND min_avg >= ?", (MIN_GAMES, MIN_MPG)
    ).fetchall()

    records = []
    for (name, team, pos, games, min_avg, min_l5, fp_avg, fp_sd, fp_l5) in players:
        min_score, min_factor = _minutes_component(min_avg, min_l5)
        usg_score, usg_factor = _usage_component(season_usage.get(name), recent_usage.get(name))
        reg_score, reg_factor = _regression_component(fp_avg, fp_sd, fp_l5)
        sched_score, sched_factor, opp, next_date, rest_days = _schedule_component(
            team, next_games, prev_dates, dvp_tough)

        overall = _blend({
            "minutes": min_score, "usage": usg_score,
            "regression": reg_score, "schedule": sched_score,
        })
        if overall is None:
            continue
        level = _risk_level(overall)

        factors = [f for f in (min_factor, usg_factor, reg_factor, sched_factor) if f]

        records.append({
            "player_name": name, "team": team, "position": pos or "", "games": games,
            "overall_score": overall, "risk_level": level,
            "minutes_score": round(min_score, 1) if min_score is not None else None,
            "usage_score": round(usg_score, 1) if usg_score is not None else None,
            "regression_score": round(reg_score, 1) if reg_score is not None else None,
            "schedule_score": round(sched_score, 1) if sched_score is not None else None,
            "min_avg": min_avg, "min_l5": min_l5, "fp_avg": fp_avg, "fp_l5": fp_l5,
            "next_opponent": opp, "next_game_date": next_date, "rest_days": rest_days,
            "factors": factors,
        })

    records.sort(key=lambda r: r["overall_score"], reverse=True)

    # Narratives only for elevated players (MODERATE+), capped for token sanity.
    elevated = [r for r in records if r["overall_score"] >= MODERATE_CUTOFF][:25]
    claude = _claude_narratives(elevated) if elevated else {}
    for r in records:
        if r["overall_score"] >= MODERATE_CUTOFF:
            # Three-tier fallback: Claude -> factor template -> minimal.
            narr = claude.get(r["player_name"])
            if not narr:
                try:
                    narr = _template_narrative(r)
                except Exception:
                    narr = ""
            if not narr:
                narr = _minimal_narrative(r)
            r["narrative"] = narr
        else:
            r["narrative"] = ""

    n = _write_table(cur, records)
    highs = sum(1 for r in records if r["risk_level"] == "HIGH")
    mods = sum(1 for r in records if r["risk_level"] == "MODERATE")
    print(f"Wrote {n} slump-risk rows ({highs} HIGH, {mods} MODERATE). "
          f"Claude narratives: {len(claude)}.")
    return n


def main():
    print("=== WNBA Slump Risk Engine ===")
    if not os.path.exists(DB):
        print(f"{DB} not found // run the WNBA pipeline first.")
        return
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    try:
        tables = {r[0] for r in cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        required = {"wnba_player_stats", "wnba_player_value", "wnba_player_game_logs"}
        missing = required - tables
        if missing:
            print(f"Missing required WNBA tables {missing} // nothing to do.")
            return
        build(cur)
        conn.commit()
    finally:
        conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
