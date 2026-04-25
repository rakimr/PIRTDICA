"""
Scheduler for mid-day sportsbook line snapshots — runs scrape_player_props.py
at fixed hours during the day to capture richer line-movement signal.

The existing pipeline already takes two snapshots per slate: 1 AM ET (post-game
pipeline) and ~60 min before tip (pre-game refresh). This scheduler adds three
mid-day snapshots so we can detect WHEN sharp money moved, not just total drift
from open to close.

Active fires (ET): 10:00, 13:00, 15:00.

If the scraper finds no games today, it exits cleanly (existing behavior). If
THE_ODDS_API_KEY is missing, the scraper exits with code 1 and we skip the
fire silently.
"""
import os
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


FIRE_HOURS = [10, 13, 15]
GRACE_MIN = 30


def get_et_now():
    try:
        return datetime.now(ET)
    except Exception:
        from datetime import timezone
        return datetime.now(timezone.utc).astimezone(ET)


def next_fire(now):
    """Return next datetime we should fire, snapped to the next FIRE_HOURS slot."""
    today_naive = datetime(now.year, now.month, now.day)
    for h in FIRE_HOURS:
        candidate = _localize_et(today_naive.replace(hour=h, minute=0, second=0, microsecond=0))
        if candidate > now:
            return candidate
    tomorrow_naive = today_naive + timedelta(days=1)
    return _localize_et(tomorrow_naive.replace(hour=FIRE_HOURS[0], minute=0, second=0, microsecond=0))


def within_grace(now):
    """If we just booted into the active window, check if we're still within
    GRACE_MIN of one of the fire times so we don't miss a slot to a restart."""
    for h in FIRE_HOURS:
        slot = _localize_et(datetime(now.year, now.month, now.day, h, 0, 0))
        delta_min = (now - slot).total_seconds() / 60.0
        if 0 <= delta_min <= GRACE_MIN:
            return slot
    return None


def run_scrape():
    print(f"\n{'='*50}")
    print(f"TRIGGERING PROPS SCRAPE at {get_et_now().strftime('%Y-%m-%d %H:%M:%S ET')}")
    print(f"{'='*50}", flush=True)
    try:
        result = subprocess.run(
            [sys.executable, "-u", "scrape_player_props.py", "--force"],
            cwd="/home/runner/workspace",
            text=True,
            timeout=900,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        if result.returncode == 0:
            print(f"\nProps scrape completed at {get_et_now().strftime('%H:%M ET')}")
        else:
            print(f"\nProps scrape exited with code {result.returncode} (likely no games or no API key)")
    except subprocess.TimeoutExpired:
        print("\nProps scrape timed out after 15 minutes")
    except Exception as e:
        print(f"\nProps scrape error: {e}")


def main():
    print("=" * 50)
    print("PROPS SNAPSHOT SCHEDULER")
    print(f"Fire times (ET): {', '.join(f'{h:02d}:00' for h in FIRE_HOURS)}")
    print("=" * 50, flush=True)

    now = get_et_now()
    grace_slot = within_grace(now)
    if grace_slot is not None:
        print(f"\n[{now.strftime('%Y-%m-%d %H:%M ET')}] Within grace of {grace_slot.strftime('%H:%M ET')} slot — running now")
        run_scrape()

    while True:
        now = get_et_now()
        target = next_fire(now)
        wait_secs = max(30, (target - now).total_seconds())
        hrs = int(wait_secs // 3600)
        mins = int((wait_secs % 3600) // 60)
        print(f"\n[{now.strftime('%Y-%m-%d %H:%M ET')}] "
              f"Next props snapshot at {target.strftime('%Y-%m-%d %H:%M ET')} "
              f"({hrs}h {mins}m)", flush=True)
        time.sleep(wait_secs)
        run_scrape()


if __name__ == "__main__":
    main()
