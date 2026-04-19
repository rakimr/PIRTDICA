"""
Scheduler for Pre-Game Refresh — fires 60 minutes before the first NBA tipoff today.
Falls back to 6:20 PM ET if no game times are available.
Designed to run as a persistent Replit workflow.
"""
import subprocess
import sys
import time
import sqlite3
from datetime import datetime, timedelta
import os

try:
    import zoneinfo
    ET = zoneinfo.ZoneInfo('US/Eastern')
    _USING_PYTZ = False
except ImportError:
    import pytz
    ET = pytz.timezone('US/Eastern')
    _USING_PYTZ = True


def _localize_et(naive_dt):
    """Attach ET tzinfo to a naive datetime, handling pytz vs ZoneInfo correctly."""
    if _USING_PYTZ:
        return ET.localize(naive_dt)
    return naive_dt.replace(tzinfo=ET)

LEAD_MINUTES = 60
FALLBACK_HOUR = 18
FALLBACK_MINUTE = 20
GRACE_WINDOW_MINUTES = 180
DB_PATH = "/home/runner/workspace/dfs_nba.db"


def get_et_now():
    try:
        return datetime.now(ET)
    except Exception:
        from datetime import timezone
        return datetime.now(timezone.utc).astimezone(ET)


def get_first_game_today_et():
    """Returns earliest game datetime today in ET, or None if no games found."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT game_time FROM player_salaries WHERE game_time IS NOT NULL")
        times = [row[0] for row in cur.fetchall()]
        conn.close()
    except Exception:
        return None

    if not times:
        return None

    now = get_et_now()
    parsed_times = []
    for gt in times:
        try:
            parsed = datetime.strptime(gt, "%I:%M%p")
            naive = parsed.replace(year=now.year, month=now.month, day=now.day)
            parsed_times.append(_localize_et(naive))
        except Exception:
            continue

    if not parsed_times:
        return None
    return min(parsed_times)


def compute_target_today():
    """Returns (target_dt, source_label) for today's refresh trigger."""
    now = get_et_now()
    first_game = get_first_game_today_et()
    if first_game is not None:
        target = first_game - timedelta(minutes=LEAD_MINUTES)
        return target, f"60m before first tip ({first_game.strftime('%I:%M %p ET')})"
    target = now.replace(
        hour=FALLBACK_HOUR, minute=FALLBACK_MINUTE, second=0, microsecond=0
    )
    return target, f"fallback {FALLBACK_HOUR}:{FALLBACK_MINUTE:02d} ET (no game times found)"


def run_refresh():
    print(f"\n{'='*50}")
    print(f"TRIGGERING PRE-GAME REFRESH at {get_et_now().strftime('%Y-%m-%d %H:%M:%S ET')}")
    print(f"{'='*50}", flush=True)
    try:
        result = subprocess.run(
            [sys.executable, "-u", "run_pregame_refresh.py"],
            cwd="/home/runner/workspace",
            text=True,
            timeout=3600,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        if result.returncode == 0:
            print(f"\nPre-game refresh completed at {get_et_now().strftime('%H:%M ET')}")
        else:
            print(f"\nPre-game refresh finished with errors (exit {result.returncode})")
    except subprocess.TimeoutExpired:
        print("\nPre-game refresh timed out after 60 minutes")
    except Exception as e:
        print(f"\nPre-game refresh error: {e}")


def sleep_until_next_morning():
    """Sleep until the next 8 AM ET at-or-after now (handles post-midnight runs)."""
    now = get_et_now()
    today_8am_naive = datetime(now.year, now.month, now.day, 8, 0, 0)
    today_8am = _localize_et(today_8am_naive)
    if now < today_8am:
        next_morning = today_8am
    else:
        tomorrow = (now + timedelta(days=1))
        next_morning_naive = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 8, 0, 0)
        next_morning = _localize_et(next_morning_naive)
    wait_secs = max(60, (next_morning - now).total_seconds())
    hrs = int(wait_secs // 3600)
    mins = int((wait_secs % 3600) // 60)
    print(f"\n[{now.strftime('%Y-%m-%d %H:%M ET')}] "
          f"Refresh done. Sleeping until {next_morning.strftime('%Y-%m-%d %H:%M ET')} "
          f"({hrs}h {mins}m)", flush=True)
    time.sleep(wait_secs)


def main():
    print("=" * 50)
    print("PRE-GAME REFRESH SCHEDULER")
    print(f"Trigger: {LEAD_MINUTES}m before first tipoff (fallback {FALLBACK_HOUR}:{FALLBACK_MINUTE:02d} ET)")
    print(f"Grace window: {GRACE_WINDOW_MINUTES} minutes after target")
    print("=" * 50, flush=True)

    while True:
        now = get_et_now()
        target, source = compute_target_today()
        delta_minutes = (target - now).total_seconds() / 60

        if delta_minutes <= 0:
            minutes_late = -delta_minutes
            if minutes_late <= GRACE_WINDOW_MINUTES:
                print(f"\n[{now.strftime('%Y-%m-%d %H:%M ET')}] Target was {source}, "
                      f"{int(minutes_late)}m late but within {GRACE_WINDOW_MINUTES}m grace — running now",
                      flush=True)
                run_refresh()
                sleep_until_next_morning()
                continue
            else:
                print(f"\n[{now.strftime('%Y-%m-%d %H:%M ET')}] Target was {source}, "
                      f"{int(minutes_late)}m late — past grace window, skipping today",
                      flush=True)
                sleep_until_next_morning()
                continue

        # Not yet target time — sleep, but no longer than 5 minutes so we can
        # re-detect game times if they update mid-day (e.g. new slate posted).
        wait_secs = min(delta_minutes * 60, 300)
        hrs = int(delta_minutes // 60)
        mins = int(delta_minutes % 60)
        print(f"\n[{now.strftime('%Y-%m-%d %H:%M ET')}] "
              f"Next refresh at {target.strftime('%Y-%m-%d %H:%M ET')} ({hrs}h {mins}m) — "
              f"target source: {source}", flush=True)
        time.sleep(wait_secs)


if __name__ == "__main__":
    main()
