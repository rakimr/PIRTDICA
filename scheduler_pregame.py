"""
Scheduler for Pre-Game Refresh — waits until 6:45 PM ET daily, then runs.
Designed to run as a persistent Replit workflow.
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
TARGET_MINUTE = 45

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

def main():
    print("=" * 50)
    print("PRE-GAME REFRESH SCHEDULER")
    print(f"Target: {TARGET_HOUR}:{TARGET_MINUTE:02d} ET daily")
    print("=" * 50, flush=True)

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
