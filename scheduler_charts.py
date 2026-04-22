"""
Scheduler for Chart Gallery refresh — runs refresh_charts.py hourly during the
day. Keeps the static gallery PNGs current between full pipeline runs.

Active window: every hour from START_HOUR to END_HOUR ET. Outside that window
(overnight, and after the pre-game refresh has taken over for the slate), it
sleeps until the next active hour.
"""
import subprocess
import sys
import time
from datetime import datetime, timedelta
import os

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


START_HOUR = 8   # First refresh of the day at 8:00 AM ET
END_HOUR = 17    # Last refresh at 5:00 PM ET (pre-game refresh handles 6 PM+)
INTERVAL_MIN = 60


def get_et_now():
    try:
        return datetime.now(ET)
    except Exception:
        from datetime import timezone
        return datetime.now(timezone.utc).astimezone(ET)


def next_active_tick(now):
    """Return next time we should run, snapped to the top of the hour within window."""
    next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    if next_hour.hour < START_HOUR:
        target_naive = datetime(next_hour.year, next_hour.month, next_hour.day, START_HOUR, 0, 0)
        return _localize_et(target_naive)
    if next_hour.hour > END_HOUR:
        tomorrow = next_hour + timedelta(days=1)
        target_naive = datetime(tomorrow.year, tomorrow.month, tomorrow.day, START_HOUR, 0, 0)
        return _localize_et(target_naive)
    return next_hour


def in_active_window(now):
    return START_HOUR <= now.hour <= END_HOUR


def run_refresh():
    print(f"\n{'='*50}")
    print(f"TRIGGERING CHART REFRESH at {get_et_now().strftime('%Y-%m-%d %H:%M:%S ET')}")
    print(f"{'='*50}", flush=True)
    try:
        result = subprocess.run(
            [sys.executable, "-u", "refresh_charts.py"],
            cwd="/home/runner/workspace",
            text=True,
            timeout=2400,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        if result.returncode == 0:
            print(f"\nChart refresh completed at {get_et_now().strftime('%H:%M ET')}")
        else:
            print(f"\nChart refresh exited with code {result.returncode}")
    except subprocess.TimeoutExpired:
        print("\nChart refresh timed out after 15 minutes")
    except Exception as e:
        print(f"\nChart refresh error: {e}")


def main():
    print("=" * 50)
    print("CHART REFRESH SCHEDULER")
    print(f"Active window: {START_HOUR:02d}:00 - {END_HOUR:02d}:00 ET, every {INTERVAL_MIN} min")
    print("=" * 50, flush=True)

    # If we're booting inside the active window, run once immediately so the
    # gallery doesn't have to wait until the next top of hour.
    now = get_et_now()
    if in_active_window(now):
        print(f"\n[{now.strftime('%Y-%m-%d %H:%M ET')}] In active window — running now")
        run_refresh()

    while True:
        now = get_et_now()
        target = next_active_tick(now)
        wait_secs = max(30, (target - now).total_seconds())
        hrs = int(wait_secs // 3600)
        mins = int((wait_secs % 3600) // 60)
        print(f"\n[{now.strftime('%Y-%m-%d %H:%M ET')}] "
              f"Next refresh at {target.strftime('%Y-%m-%d %H:%M ET')} "
              f"({hrs}h {mins}m)", flush=True)
        time.sleep(wait_secs)
        run_refresh()


if __name__ == "__main__":
    main()
