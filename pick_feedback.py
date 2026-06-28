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

    return result
