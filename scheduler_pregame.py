"""
Scheduler for Pre-Game Refresh — waits until 6:20 PM ET daily, then runs.
Designed to run as a persistent Replit workflow.
If the scheduler starts after the target time but within a grace window,
it runs immediately to avoid missing today's refresh.
"""
import subprocess
import sys
import time
from datetime import datetime, timedelta
import os

try:
    import pytz
    ET = pytz.timezone('US/Eastern')
except ImportError:
    import zoneinfo
    ET = zoneinfo.ZoneInfo('US/Eastern')

TARGET_HOUR = 18
TARGET_MINUTE = 0
GRACE_WINDOW_MINUTES = 120

def get_et_now():
    try:
        return datetime.now(ET)
    except:
        from datetime import timezone
        utc_now = datetime.now(timezone.utc)
        return utc_now.astimezone(ET)

def seconds_until_target():
    now = get_et_now()
    target = now.replace(hour=TARGET_HOUR, minute=TARGET_MINUTE, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    delta = (target - now).total_seconds()
    return delta, target

def check_missed_window():
    now = get_et_now()
    target_today = now.replace(hour=TARGET_HOUR, minute=TARGET_MINUTE, second=0, microsecond=0)
    if now >= target_today:
        minutes_late = (now - target_today).total_seconds() / 60
        if minutes_late <= GRACE_WINDOW_MINUTES:
            return True, minutes_late
    return False, 0

def main():
    print("=" * 50)
    print("PRE-GAME REFRESH SCHEDULER")
    print(f"Target: {TARGET_HOUR}:{TARGET_MINUTE:02d} ET daily")
    print(f"Grace window: {GRACE_WINDOW_MINUTES} minutes")
    print("=" * 50, flush=True)

    missed, minutes_late = check_missed_window()
    if missed:
        print(f"\n[{get_et_now().strftime('%Y-%m-%d %H:%M ET')}] "
              f"Missed target by {int(minutes_late)}m — running immediately", flush=True)
        print(f"\n{'='*50}")
        print(f"TRIGGERING PRE-GAME REFRESH (missed window) at {get_et_now().strftime('%Y-%m-%d %H:%M:%S ET')}")
        print(f"{'='*50}", flush=True)

        try:
            result = subprocess.run(
                [sys.executable, "-u", "run_pregame_refresh.py"],
                cwd="/home/runner/workspace",
                text=True,
                timeout=3600,
                env={**os.environ, "PYTHONUNBUFFERED": "1"}
            )
            if result.returncode == 0:
                print(f"\nPre-game refresh completed successfully at {get_et_now().strftime('%H:%M ET')}")
            else:
                print(f"\nPre-game refresh finished with errors (exit code {result.returncode})")
        except subprocess.TimeoutExpired:
            print("\nPre-game refresh timed out after 60 minutes")
        except Exception as e:
            print(f"\nPre-game refresh error: {e}")

        time.sleep(60)

    while True:
        wait_secs, target_time = seconds_until_target()
        hours = int(wait_secs // 3600)
        mins = int((wait_secs % 3600) // 60)
        now = get_et_now()
        print(f"\n[{now.strftime('%Y-%m-%d %H:%M ET')}] "
              f"Next refresh at {target_time.strftime('%Y-%m-%d %H:%M ET')} "
              f"({hours}h {mins}m from now)", flush=True)

        time.sleep(wait_secs)

        print(f"\n{'='*50}")
        print(f"TRIGGERING PRE-GAME REFRESH at {get_et_now().strftime('%Y-%m-%d %H:%M:%S ET')}")
        print(f"{'='*50}", flush=True)

        try:
            result = subprocess.run(
                [sys.executable, "-u", "run_pregame_refresh.py"],
                cwd="/home/runner/workspace",
                text=True,
                timeout=3600,
                env={**os.environ, "PYTHONUNBUFFERED": "1"}
            )
            if result.returncode == 0:
                print(f"\nPre-game refresh completed successfully at {get_et_now().strftime('%H:%M ET')}")
            else:
                print(f"\nPre-game refresh finished with errors (exit code {result.returncode})")
        except subprocess.TimeoutExpired:
            print("\nPre-game refresh timed out after 60 minutes")
        except Exception as e:
            print(f"\nPre-game refresh error: {e}")

        time.sleep(60)

if __name__ == "__main__":
    main()
