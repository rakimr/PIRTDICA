"""
Recent pick-outcome feedback for the article generators.

Reads our own graded pick results from the Postgres `daily_pick_grades` (NBA) and
`wnba_daily_pick_grades` (WNBA) tables and aggregates an honest recent win-loss
summary // overall, by stat, by direction, and (NBA only) by a few signal
patterns (DVA support, usage-beneficiary, composite tier). The Claude analyst
gets this as INTERNAL calibration context so it can learn what kinds of picks
have actually been hitting lately, without ever citing the record in the
published article.

Everything is best-effort: any failure (no DB, empty tables, missing columns)
returns {} so the article still generates cleanly.
"""
from datetime import timedelta

# Pattern-level miss taxonomy (Task #64). Graders classify each MISS into one of
# these buckets; the feedback here only ever surfaces bucket-level counts, never
# "player X burned us on date Y".
MISS_REASONS = (
    "role_usage_shift",    # unexpected role/usage volatility (hub nights, promotions)
    "game_script",         # blowout / foul trouble / rotation cut short
    "shooting_variance",   # clean process, cold/hot shooting night
    "matchup_model_gap",   # playstyle-vs-defense read our DVP/matchup inputs missed
    "other",
)

# matchup_model_gap misses are model INPUT gaps (fix the DVP/matchup tables),
# not pick-type biases — they are logged as model_gap_notes and intentionally
# excluded from the Claude-facing miss-pattern buckets.
_CLAUDE_FACING_REASONS = tuple(r for r in MISS_REASONS if r != "matchup_model_gap")


def ensure_miss_reason_schema():
    """Idempotent, deployment-safe migration: add miss_reason to both existing
    grade tables and create model_gap_notes. Called by the graders/backfill
    before any read/write and mirrored in the app startup migration, so a
    pre-existing production database upgrades itself on first use."""
    try:
        from sqlalchemy import inspect as sa_inspect, text as sa_text
        from backend.database import Base, engine
        from backend import models  # noqa: F401 (registers ModelGapNote)
        Base.metadata.create_all(bind=engine)
        insp = sa_inspect(engine)
        for table in ("daily_pick_grades", "wnba_daily_pick_grades"):
            if table not in insp.get_table_names():
                continue
            cols = {c["name"] for c in insp.get_columns(table)}
            if "miss_reason" not in cols:
                with engine.begin() as conn:
                    conn.execute(sa_text(
                        f"ALTER TABLE {table} "
                        "ADD COLUMN IF NOT EXISTS miss_reason VARCHAR(40)"))
                print(f"[pick_feedback] Added miss_reason to {table}")
        return True
    except Exception as e:
        print(f"[pick_feedback] miss_reason schema ensure failed: {e}")
        return False


def _agg(rows):
    wins = sum(1 for r in rows if r["hit"] is True)
    losses = sum(1 for r in rows if r["hit"] is False)
    pushes = sum(1 for r in rows if r["hit"] is None)
    decided = wins + losses
    return {
        "record": f"{wins}-{losses}",
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "decided": decided,
        "win_pct": round(100.0 * wins / decided, 1) if decided else None,
    }


def _grouped(rows, keyfn, min_decided=3):
    """Aggregate rows into {key: record} buckets, dropping buckets whose decided
    sample is too thin to be worth showing (avoids noisy 1-1 splits)."""
    groups = {}
    for r in rows:
        k = keyfn(r)
        if k is None:
            continue
        groups.setdefault(k, []).append(r)
    out = {}
    for k, rs in groups.items():
        a = _agg(rs)
        if a["decided"] >= min_decided:
            out[k] = a
    return out


def load_recent_pick_results(league="nba", recent_days=14,
                             lookback_cap_days=400, min_window_decided=8):
    """Return an internal calibration dict of recent graded pick results, or {}.

    Shape (keys present only when there is enough data):
      {
        "window_days": 14,
        "note": "...internal calibration only...",
        "recent": {record, wins, losses, pushes, decided, win_pct},
        "overall_to_date": {..., since, through},
        "recent_by_stat": {"PTS": {...}, ...},
        "recent_by_direction": {"OVER": {...}, "UNDER": {...}},
        "recent_by_signal": {"dva_supported": {...}, "usage_beneficiary": {...}, ...}  # NBA only
      }
    """
    league = (league or "nba").strip().lower()
    is_nba = league != "wnba"
    try:
        from backend.database import engine
        from backend import models
        from sqlalchemy.orm import sessionmaker

        try:
            from utils.timezone import get_eastern_today
            today = get_eastern_today()
        except Exception:
            from datetime import date as _date
            today = _date.today()

        Model = models.WNBADailyPickGrade if not is_nba else models.DailyPickGrade
        Session = sessionmaker(bind=engine)
        db = Session()
        try:
            cutoff = today - timedelta(days=lookback_cap_days)
            rows = []
            for g in db.query(Model).filter(Model.slate_date >= cutoff).all():
                rows.append({
                    "slate_date": g.slate_date,
                    "stat": (g.stat or "").upper() or None,
                    "direction": (g.direction or "").upper() or None,
                    "hit": g.hit,
                    "composite_score": getattr(g, "composite_score", None),
                    "dva_edge": getattr(g, "dva_edge", None),
                    "usage_boost": getattr(g, "usage_boost", None),
                    "miss_reason": getattr(g, "miss_reason", None),
                })
        finally:
            db.close()
    except Exception as e:
        print(f"[pick_feedback] load failed for {league}: {e}")
        return {}

    if not rows:
        return {}

    recent_cut = today - timedelta(days=recent_days)
    recent = [r for r in rows if r["slate_date"] and r["slate_date"] >= recent_cut]

    overall = _agg(rows)
    dates = [r["slate_date"] for r in rows if r["slate_date"]]
    if dates:
        overall["since"] = min(dates).isoformat()
        overall["through"] = max(dates).isoformat()

    result = {
        "window_days": recent_days,
        "note": "Internal calibration only // never cite this track record in the published analysis.",
        "recent": _agg(recent),
        "overall_to_date": overall,
    }

    # Only break the recent window down by category once it carries a meaningful
    # sample // tiny samples produce misleading split records.
    if result["recent"]["decided"] >= min_window_decided:
        by_stat = _grouped(recent, lambda r: r["stat"])
        by_dir = _grouped(recent, lambda r: r["direction"])
        if by_stat:
            result["recent_by_stat"] = by_stat
        if by_dir:
            result["recent_by_direction"] = by_dir

        if is_nba:
            signal = {}
            signal.update(_grouped(
                recent,
                lambda r: None if r["dva_edge"] is None
                else ("dva_supported" if r["dva_edge"] > 0 else "dva_against")))
            signal.update(_grouped(
                recent,
                lambda r: None if r["usage_boost"] is None
                else ("usage_beneficiary" if r["usage_boost"] > 0 else "no_usage_boost")))
            comps = sorted(r["composite_score"] for r in recent
                           if r["composite_score"] is not None)
            if len(comps) >= 6:
                med = comps[len(comps) // 2]
                signal.update(_grouped(
                    recent,
                    lambda r: None if r["composite_score"] is None
                    else ("higher_composite" if r["composite_score"] >= med
                          else "lower_composite")))
            if signal:
                result["recent_by_signal"] = signal

    if result["recent"]["decided"] >= min_window_decided:
        patterns = _miss_patterns(recent)
        if patterns:
            result["recent_miss_patterns"] = patterns

    return result


def _miss_patterns(recent, min_misses=3):
    """Gated, bucket-level miss-reason records for the recent window.

    Guardrails (Task #64):
    - Pattern-level only: buckets are (reason) and (reason x stat) counts. No
      player names, no dates, no per-pick detail ever leaves this function.
    - Sample gates: a bucket only surfaces once it has >= min_misses classified
      misses. One game creates zero signal.
    - Avoidance/sizing, not steering: the note instructs Claude to demand a
      bigger edge on fragile categories, never to flip sides or target players.
    - matchup_model_gap is excluded (model input gap, logged separately as
      model_gap_notes for pipeline fixes, not a Claude nudge).
    """
    misses = [r for r in recent
              if r["hit"] is False and r.get("miss_reason") in _CLAUDE_FACING_REASONS]
    if not misses:
        return None
    total_misses = sum(1 for r in recent if r["hit"] is False)

    by_reason = {}
    by_reason_stat = {}
    for r in misses:
        by_reason[r["miss_reason"]] = by_reason.get(r["miss_reason"], 0) + 1
        if r.get("stat"):
            k = f"{r['miss_reason']}|{r['stat']}"
            by_reason_stat[k] = by_reason_stat.get(k, 0) + 1

    by_reason = {k: {"misses": v,
                     "share_of_recent_misses_pct": round(100.0 * v / total_misses, 1)}
                 for k, v in by_reason.items() if v >= min_misses}
    by_reason_stat = {k: v for k, v in by_reason_stat.items() if v >= min_misses}
    if not by_reason and not by_reason_stat:
        return None

    out = {
        "note": ("Internal calibration only. These are pattern-level miss buckets "
                 "from recent graded picks. Use them ONLY to demand a larger edge / "
                 "downgrade confidence on fragile pick categories (e.g. assist props "
                 "on volatile-role players). NEVER flip a pick's side because of "
                 "this, never avoid or target a specific player, and never cite "
                 "this in the published analysis."),
        "classified_misses": len(misses),
        "total_recent_misses": total_misses,
    }
    if by_reason:
        out["by_reason"] = by_reason
    if by_reason_stat:
        out["by_reason_and_stat"] = by_reason_stat
    return out
