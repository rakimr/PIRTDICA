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


def get_et_now():
    try:
        return datetime.now(ET)
    except Exception:
        from datetime import timezone
        return datetime.now(timezone.utc).astimezone(ET)


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
    args = parser.parse_args()

    if args.status:
        print_status()
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
