"""
Scheduler for Intra-Day Player Prop Scrapes — fires `scrape_player_props.py
--force` at 11:00 AM, 2:00 PM, and 4:00 PM ET. These additional snapshots fill
the gap between the 1 AM post-game scrape (the "open") and the pre-game
refresh ~60 minutes before tipoff (the "current"), giving us 4-5 line snapshots
per day instead of 2.

Each scrape appends to `player_props_history`, so we can detect WHEN sharp money
moved (sudden swing vs gradual drift) — a stronger signal than total drift alone.

Reliability features (added 2026-04-26 for Task #31):
  - **Grace-window catch-up**: if the workflow restarts AFTER a target hour
    has already passed today, we still fire that window provided we're within
    `GRACE_WINDOW_MINUTES`. Without this, every restart silently skipped
    whichever window had just elapsed — that's why historical slates only
    have one snapshot per day.
  - **DB-backed de-dup**: a target hour is treated as already satisfied if
    `player_props_history` already has a snapshot within
    `WINDOW_BUCKET_MINUTES` of it today (e.g. the Daily Update or Pre-Game
    Refresh happened to scrape inside the same window). Saves API quota and
    avoids redundant rows.
  - **In-process attempt tracking**: once we attempt a window in this process,
    we don't retry it the same day even if it failed. Avoids hammering a
    broken API for an hour straight; a workflow restart still gets a fresh
    chance the next day.
  - **Short sleep cap (5m)**: every loop wakes at least every 5 minutes so the
    scheduler reacts quickly to clock changes, restarts, or new game data.

Rate-limit aware: the underlying scraper auto-skips when there are no games
today, and `has_games_today()` short-circuits before we even spawn it.

CLI:
  python scheduler_props.py             # run forever (the workflow entrypoint)
  python scheduler_props.py --status    # print today's snapshot coverage and exit
  python scheduler_props.py --once      # fire every currently-due window, then exit
"""
import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta

try:
    import zoneinfo
    ET = zoneinfo.ZoneInfo("US/Eastern")
    _USING_PYTZ = False
except ImportError:
    import pytz
    ET = pytz.timezone("US/Eastern")
    _USING_PYTZ = True


def _localize_et(naive_dt):
    if _USING_PYTZ:
        return ET.localize(naive_dt)
    return naive_dt.replace(tzinfo=ET)


SCRAPE_HOURS = [11, 14, 16]
GRACE_WINDOW_MINUTES = 90
WINDOW_BUCKET_MINUTES = 75
SLEEP_CAP_SECONDS = 300
DB_PATH = "/home/runner/workspace/dfs_nba.db"

# Movement-triggered article regen (Task #34). After every successful intra-day
# scrape we compare the fresh prop snapshot against the snapshot at the time of
# the last article generation. If enough lines have moved by enough points, we
# regenerate the article so subscribers see late-breaking sharp action /
# injury-driven moves before tipoff. Tunable via env vars so a flaky API or
# noisy slate doesn't burn Claude credits unnecessarily.
MOVEMENT_THRESHOLD = float(os.environ.get("PROPS_MOVE_THRESHOLD", "0.5"))
MIN_MOVERS = int(os.environ.get("PROPS_MOVE_MIN_MOVERS", "3"))
REGEN_COOLDOWN_MINUTES = int(os.environ.get("PROPS_REGEN_COOLDOWN_MIN", "30"))

# Durable cooldown state (Task #37). Without this, a Render redeploy or worker
# restart would wipe the in-memory cooldown and the very next scrape could fire
# a duplicate Claude regen even though nothing material happened. The state
# file is a tiny JSON blob: {"last_regen_at": "<isoformat ET>"}. Mirrors the
# pregame state pattern at /tmp/pirtdica_pregame_state.json.
MOVEMENT_STATE_PATH = "/tmp/pirtdica_movement_state.json"

_last_movement_regen = None  # in-process cache; durable copy lives on disk


def get_et_now():
    try:
        return datetime.now(ET)
    except Exception:
        from datetime import timezone
        return datetime.now(timezone.utc).astimezone(ET)


def _load_persisted_last_regen(state_path=None):
    """Load the durable last-regen timestamp (ET-aware) or None.

    A missing file, empty file, malformed JSON, or unparseable timestamp all
    fall through to None — we'd rather permit one extra regen than crash the
    scrape loop on a corrupted state file. Mirrors the lenient pattern in
    `scheduler_pregame._load_fired_state`.
    """
    path = state_path or MOVEMENT_STATE_PATH
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            blob = json.load(f) or {}
        ts = blob.get("last_regen_at")
        if not ts:
            return None
        parsed = datetime.fromisoformat(ts)
        if parsed.tzinfo is None:
            parsed = _localize_et(parsed)
        return parsed.astimezone(ET)
    except Exception as e:
        print(f"[movement] WARN: could not read cooldown state ({e}) — "
              f"treating as no prior regen", flush=True)
        return None


def _persist_last_regen(when_et, state_path=None):
    """Atomically write the last-regen timestamp so it survives a restart.

    Writes to a temp file and renames so a crash mid-write doesn't leave a
    half-written JSON blob behind.
    """
    path = state_path or MOVEMENT_STATE_PATH
    try:
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"last_regen_at": when_et.isoformat()}, f)
        os.replace(tmp, path)
    except Exception as e:
        print(f"[movement] WARN: could not persist cooldown state: {e}",
              flush=True)


def _effective_last_regen():
    """Return the most recent of (in-memory, on-disk) last-regen timestamps.

    We always read disk because a sibling process (or a previous incarnation
    of this same process before a restart) may have updated it. The in-memory
    cache is just an optimization for log-readable continuity within one
    process.
    """
    persisted = _load_persisted_last_regen()
    candidates = [t for t in (_last_movement_regen, persisted) if t is not None]
    if not candidates:
        return None
    return max(candidates)


def has_games_today():
    """Return True if there are NBA games on today's slate.

    Day-aware: requires `player_salaries` rows whose `scraped_at` falls on
    today's ET date AND have a non-null `game_time`. The post-game pipeline
    refreshes salaries every morning, so a same-day scrape is the strongest
    signal we have a live slate.

    Conservative: if we can't tell (no salary data yet, table missing, etc.)
    we still allow the scrape — the scraper itself fetches the events list and
    will short-circuit cleanly if no games are found, without spending much
    quota beyond the events lookup.
    """
    today_et = get_et_now().strftime("%Y-%m-%d")
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(DISTINCT game_time) FROM player_salaries "
            "WHERE game_time IS NOT NULL AND substr(scraped_at, 1, 10) = ?",
            (today_et,),
        )
        count = cur.fetchone()[0]
        conn.close()
        return count > 0
    except Exception:
        return True


def windows_satisfied_by_db():
    """Return the subset of SCRAPE_HOURS already covered today.

    Each snapshot in `player_props_history` is assigned to its **nearest**
    target hour and counts as satisfying only that target — provided it's
    within `WINDOW_BUCKET_MINUTES`. The nearest-target rule prevents a late
    14:00 snapshot at, say, 14:50 from also marking 16:00 satisfied (which
    would defeat the next intra-day window): 14:50 is 50min from 14:00 vs
    70min from 16:00, so it satisfies 14:00 only and 16:00 still fires.

    Covers cases where the Daily Update, Pre-Game Refresh, or a manual
    scrape happened to fire inside the same window — no need to re-scrape
    and burn API quota on a duplicate of an existing snapshot.
    """
    today_et = get_et_now().strftime("%Y-%m-%d")
    satisfied = set()
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT substr(scraped_at, 12, 5) FROM player_props_history "
            "WHERE substr(scraped_at, 1, 10) = ?",
            (today_et,),
        )
        snapshot_minutes = []
        for (hm,) in cur.fetchall():
            try:
                snapshot_minutes.append(int(hm[:2]) * 60 + int(hm[3:]))
            except Exception:
                continue
        conn.close()
    except Exception:
        return satisfied
    for sm in snapshot_minutes:
        nearest = min(SCRAPE_HOURS, key=lambda h: abs(h * 60 - sm))
        if abs(nearest * 60 - sm) <= WINDOW_BUCKET_MINUTES:
            satisfied.add(nearest)
    return satisfied


def snapshot_summary_today():
    """Return (distinct_minute_count, sorted_minute_strings) for today."""
    today_et = get_et_now().strftime("%Y-%m-%d")
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT substr(scraped_at, 12, 5) FROM player_props_history "
            "WHERE substr(scraped_at, 1, 10) = ? ORDER BY 1",
            (today_et,),
        )
        minutes = [row[0] for row in cur.fetchall() if row[0]]
        conn.close()
        return len(minutes), minutes
    except Exception:
        return 0, []


def _query_last_article_updated_at_et():
    """Return today's daily_articles.updated_at as an ET datetime, or None."""
    try:
        from backend.database import SessionLocal
        from backend import models
        today = get_et_now().date()
        with SessionLocal() as db:
            article = db.query(models.DailyArticle).filter(
                models.DailyArticle.slate_date == today
            ).first()
            if article and article.updated_at:
                ua = article.updated_at
                if ua.tzinfo is None:
                    from datetime import timezone
                    ua = ua.replace(tzinfo=timezone.utc)
                return ua.astimezone(ET)
    except Exception as e:
        print(f"[movement] could not load article updated_at: {e}", flush=True)
    return None


def detect_material_movement(article_updated_at, db_path=None):
    """Compare current props snapshot vs the snapshot at article_updated_at.

    For each (player, stat, bookmaker) triple, we pull:
      - the most recent snapshot today (the post-scrape line)
      - the most recent snapshot at-or-before the article time (what Claude saw)
    and return (count_of_movers, sample_string) where a "mover" is any prop
    whose absolute line delta is >= MOVEMENT_THRESHOLD.
    """
    if article_updated_at is None:
        return 0, ""
    today_et = get_et_now().strftime("%Y-%m-%d")
    # `scraped_at` is stored as ISO-8601 with offset (e.g.
    # "2026-04-25T11:30:00-04:00"). We MUST wrap both sides in SQLite's
    # `datetime()` so the comparison is normalized to UTC. A naive TEXT
    # comparison would compare a "T"-separated value against a space-separated
    # one, and silently exclude valid pre-article rows because 'T' (0x54) sorts
    # greater than ' ' (0x20). Same reason we wrap the ORDER BYs — the fall-DST
    # rollback hour can otherwise mis-rank snapshots within the same wall hour.
    article_dt_str = article_updated_at.isoformat()
    try:
        conn = sqlite3.connect(db_path or DB_PATH)
        cur = conn.cursor()
        cur.execute(
            """
            WITH latest AS (
                SELECT player_name, stat, bookmaker, line, scraped_at,
                       ROW_NUMBER() OVER (
                           PARTITION BY player_name, stat, bookmaker
                           ORDER BY datetime(scraped_at) DESC
                       ) AS rn
                FROM player_props_history
                WHERE game_date = ?
            ),
            article_time AS (
                SELECT player_name, stat, bookmaker, line, scraped_at,
                       ROW_NUMBER() OVER (
                           PARTITION BY player_name, stat, bookmaker
                           ORDER BY datetime(scraped_at) DESC
                       ) AS rn
                FROM player_props_history
                WHERE game_date = ? AND datetime(scraped_at) <= datetime(?)
            )
            SELECT l.player_name, l.stat, l.bookmaker,
                   a.line AS old_line, l.line AS new_line
            FROM latest l
            JOIN article_time a USING (player_name, stat, bookmaker)
            WHERE l.rn = 1 AND a.rn = 1
            """,
            (today_et, today_et, article_dt_str),
        )
        rows = cur.fetchall()
        conn.close()
    except Exception as e:
        print(f"[movement] query failed: {e}", flush=True)
        return 0, ""

    movers = []
    for player, stat, _book, old_line, new_line in rows:
        if old_line is None or new_line is None:
            continue
        try:
            delta = abs(float(new_line) - float(old_line))
        except (TypeError, ValueError):
            continue
        if delta >= MOVEMENT_THRESHOLD:
            movers.append((player, stat, float(old_line), float(new_line)))
    sample = "; ".join(
        f"{p} {s} {ol:.1f}->{nl:.1f}" for p, s, ol, nl in movers[:3]
    )
    return len(movers), sample


def maybe_regen_article_for_movement():
    """If material movement is detected since the last article, regen + push.

    Returns True if a regen actually fired. Honors REGEN_COOLDOWN_MINUTES so
    a back-to-back scrape (e.g. 14:00 and 14:30) can't burn Claude credits in
    rapid succession. The cooldown is durable: it survives a worker restart
    via the JSON state file at MOVEMENT_STATE_PATH, so a Render redeploy
    inside the cooldown window does NOT trigger a duplicate regen (Task #37).
    """
    global _last_movement_regen
    now = get_et_now()
    last_regen = _effective_last_regen()
    if last_regen is not None:
        elapsed = (now - last_regen).total_seconds() / 60.0
        if elapsed < REGEN_COOLDOWN_MINUTES:
            source = "disk" if _last_movement_regen is None else "memory"
            print(
                f"[movement] cooldown active "
                f"({REGEN_COOLDOWN_MINUTES - int(elapsed)}m left, "
                f"source={source}) — skipping check",
                flush=True,
            )
            return False

    article_ua = _query_last_article_updated_at_et()
    if article_ua is None:
        print("[movement] no article today yet — skipping movement check",
              flush=True)
        return False

    movers_count, sample = detect_material_movement(article_ua)
    print(
        f"[movement] {movers_count} props moved by >= {MOVEMENT_THRESHOLD} "
        f"since last article ({article_ua.strftime('%H:%M ET')})",
        flush=True,
    )
    if sample:
        print(f"[movement]   examples: {sample}", flush=True)

    if movers_count < MIN_MOVERS:
        return False

    print(
        f"[movement] >= {MIN_MOVERS} movers — triggering article regen",
        flush=True,
    )
    try:
        result = subprocess.run(
            [sys.executable, "-u", "generate_article.py"],
            cwd="/home/runner/workspace",
            text=True,
            timeout=600,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        if result.returncode != 0:
            print(
                f"[movement] article regen failed (exit {result.returncode})",
                flush=True,
            )
            return False
    except Exception as e:
        print(f"[movement] article regen error: {e}", flush=True)
        return False

    regen_completed_at = get_et_now()
    print(
        f"[movement] article regen succeeded at "
        f"{regen_completed_at.strftime('%H:%M ET')}",
        flush=True,
    )
    _last_movement_regen = regen_completed_at
    _persist_last_regen(regen_completed_at)
    try:
        push_result = subprocess.run(
            [
                sys.executable,
                "push_to_github.py",
                f"Movement-triggered article regen — {now.strftime('%b %d %H:%M ET')}",
            ],
            cwd="/home/runner/workspace",
            capture_output=True,
            text=True,
            timeout=180,
        )
        if push_result.returncode == 0:
            print("[movement] pushed regen to GitHub", flush=True)
        else:
            tail = (push_result.stderr or "").strip()[-200:]
            print(f"[movement] GitHub push exit {push_result.returncode}: {tail}",
                  flush=True)
    except Exception as e:
        print(f"[movement] push error: {e}", flush=True)
    return True


def run_scrape():
    now = get_et_now()
    print(f"\n{'='*50}")
    print(f"INTRA-DAY PROPS SCRAPE at {now.strftime('%Y-%m-%d %H:%M:%S ET')}")
    print(f"{'='*50}", flush=True)

    if not has_games_today():
        print("No games on today's slate — skipping scrape to preserve API quota")
        return

    try:
        result = subprocess.run(
            [sys.executable, "-u", "scrape_player_props.py", "--force"],
            cwd="/home/runner/workspace",
            text=True,
            timeout=600,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        if result.returncode == 0:
            print(f"\nIntra-day props scrape completed at "
                  f"{get_et_now().strftime('%H:%M ET')}")
            try:
                maybe_regen_article_for_movement()
            except Exception as e:
                print(f"[movement] post-scrape hook error: {e}", flush=True)
        else:
            print(f"\nIntra-day props scrape exited with code {result.returncode}")
    except subprocess.TimeoutExpired:
        print("\nIntra-day props scrape timed out after 10 minutes")
    except Exception as e:
        print(f"\nIntra-day props scrape error: {e}")


def _today_target(now, hour):
    return _localize_et(datetime(now.year, now.month, now.day, hour, 0, 0))


def _next_morning_first_target(now):
    tomorrow = now + timedelta(days=1)
    return _localize_et(datetime(
        tomorrow.year, tomorrow.month, tomorrow.day, SCRAPE_HOURS[0], 0, 0
    ))


def evaluate_and_fire(now, attempted_today):
    """Decide whether to fire a window now, return seconds to next check.

    A window is *due* if:
      - It hasn't been attempted yet this process-day, AND
      - It isn't already satisfied by a snapshot in the DB, AND
      - now >= target hour, AND
      - now <= target hour + GRACE_WINDOW_MINUTES.

    If a window is due, fire it and mark it attempted regardless of outcome
    (failed scrape attempts are tracked the same way so we don't loop on a
    broken API). Returns the seconds to sleep before the next evaluation
    (always capped at SLEEP_CAP_SECONDS).
    """
    satisfied = windows_satisfied_by_db()
    fired_now = False

    for h in SCRAPE_HOURS:
        if h in attempted_today or h in satisfied:
            continue
        target = _today_target(now, h)
        if now < target:
            continue
        late_minutes = (now - target).total_seconds() / 60.0
        if late_minutes > GRACE_WINDOW_MINUTES:
            # Past grace — give up on this window for today so we don't keep
            # re-checking it. Marking attempted has the same effect.
            attempted_today.add(h)
            print(f"\n[{now.strftime('%Y-%m-%d %H:%M ET')}] "
                  f"Skipping {h:02d}:00 ET window — {int(late_minutes)}m late, "
                  f"past {GRACE_WINDOW_MINUTES}m grace", flush=True)
            continue
        # Due — fire it.
        if late_minutes > 0:
            print(f"\n[{now.strftime('%Y-%m-%d %H:%M ET')}] "
                  f"Firing {h:02d}:00 ET window — {int(late_minutes)}m late, "
                  f"within {GRACE_WINDOW_MINUTES}m grace", flush=True)
        else:
            print(f"\n[{now.strftime('%Y-%m-%d %H:%M ET')}] "
                  f"Firing {h:02d}:00 ET window — on time", flush=True)
        run_scrape()
        attempted_today.add(h)
        fired_now = True
        # Refresh now so the next-target calculation uses the post-fire clock.
        now = get_et_now()
        break  # one fire per loop tick; re-evaluate immediately

    if fired_now:
        return 30  # short pause then re-evaluate (will pick up the next due window)

    # Compute time to next still-eligible window today.
    candidates = []
    satisfied = windows_satisfied_by_db()  # refresh after any fire
    for h in SCRAPE_HOURS:
        if h in attempted_today or h in satisfied:
            continue
        target = _today_target(now, h)
        if now < target:
            candidates.append((target - now).total_seconds())
        else:
            late_minutes = (now - target).total_seconds() / 60.0
            if late_minutes <= GRACE_WINDOW_MINUTES:
                # Already overdue but still in grace — re-check soon.
                candidates.append(30)
    if candidates:
        return max(30, min(min(candidates), SLEEP_CAP_SECONDS))

    # No more windows today: sleep toward tomorrow's first target, capped.
    secs = (_next_morning_first_target(now) - now).total_seconds()
    return max(60, min(secs, SLEEP_CAP_SECONDS))


def print_status():
    now = get_et_now()
    today = now.strftime("%Y-%m-%d")
    games = has_games_today()
    satisfied = windows_satisfied_by_db()
    n_minutes, minutes = snapshot_summary_today()
    print(f"INTRA-DAY PROPS SCHEDULER — STATUS")
    print(f"  Now (ET):              {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Slate has games today: {games}")
    print(f"  Target windows:        " + ", ".join(f"{h:02d}:00 ET" for h in SCRAPE_HOURS))
    print(f"  Grace window:          {GRACE_WINDOW_MINUTES} min after target")
    print(f"  DB-satisfied windows:  " + (
        ", ".join(f"{h:02d}:00" for h in sorted(satisfied)) if satisfied else "(none)"
    ))
    print(f"  Distinct snapshot minutes today ({today}): {n_minutes}")
    if minutes:
        for m in minutes:
            print(f"    - {m} ET")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--status", action="store_true",
                        help="Print today's snapshot coverage and exit.")
    parser.add_argument("--once", action="store_true",
                        help="Fire every currently-due window, then exit.")
    parser.add_argument("--force", action="store_true",
                        help="Bypass SCHEDULERS_ENABLED gate for manual runs.")
    args = parser.parse_args()

    if args.status:
        print_status()
        return

    # `--once` and `--force` bypass the gate: both are explicit, non-persistent
    # invocations (manual testing, cron, etc.) — they can't cause the silent
    # doubling problem the gate is meant to prevent.
    gate_open = (
        os.environ.get("SCHEDULERS_ENABLED", "").strip() == "1"
        or args.force
        or args.once
    )
    if not gate_open:
        print(
            "[scheduler_props] SCHEDULERS_ENABLED != 1 — exiting "
            "(set the env var, or pass --once/--force for a one-shot run).",
            flush=True,
        )
        return

    print("=" * 50)
    print("INTRA-DAY PROPS SCRAPE SCHEDULER")
    print(f"Targets: {', '.join(f'{h:02d}:00 ET' for h in SCRAPE_HOURS)}")
    print(f"Grace: {GRACE_WINDOW_MINUTES}m after target | "
          f"DB de-dup: ±{WINDOW_BUCKET_MINUTES}m | "
          f"Sleep cap: {SLEEP_CAP_SECONDS}s")
    print("=" * 50, flush=True)

    attempted_today = set()
    attempted_date = None

    while True:
        now = get_et_now()
        today = now.strftime("%Y-%m-%d")
        if attempted_date != today:
            attempted_today = set()
            attempted_date = today

        if not has_games_today():
            secs = (_next_morning_first_target(now) - now).total_seconds()
            wait_secs = max(60, min(secs, SLEEP_CAP_SECONDS))
            mins = int(wait_secs // 60)
            print(f"\n[{now.strftime('%Y-%m-%d %H:%M ET')}] "
                  f"No games on slate — re-checking in {mins}m", flush=True)
            if args.once:
                return
            time.sleep(wait_secs)
            continue

        wait_secs = evaluate_and_fire(now, attempted_today)
        if args.once:
            # `evaluate_and_fire` returns 30 after a fire (so the persistent
            # loop can pick up the next due window quickly). For --once we
            # want to drain every due window in one invocation, so keep
            # firing until we're back to a "no work right now" state.
            while wait_secs < 60:
                now = get_et_now()
                wait_secs = evaluate_and_fire(now, attempted_today)
            return
        now = get_et_now()
        next_check = now + timedelta(seconds=wait_secs)
        mins = int(wait_secs // 60)
        secs = int(wait_secs % 60)
        print(f"\n[{now.strftime('%Y-%m-%d %H:%M ET')}] "
              f"Next evaluation at {next_check.strftime('%H:%M:%S ET')} "
              f"({mins}m {secs}s)", flush=True)
        time.sleep(wait_secs)


if __name__ == "__main__":
    main()
