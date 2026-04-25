"""
Scheduler for Intra-Day Player Prop Scrapes — fires `scrape_player_props.py
--force` at 11:00 AM, 2:00 PM, and 4:00 PM ET. These additional snapshots fill
the gap between the 1 AM post-game scrape (the "open") and the pre-game
refresh ~60 minutes before tipoff (the "current"), giving us 4-5 line snapshots
per day instead of 2.

Each scrape appends to `player_props_history`, so we can detect WHEN sharp money
moved (sudden swing vs gradual drift) — a stronger signal than total drift alone.

Rate-limit aware: the underlying scraper auto-skips when there are no games
today, so we don't burn API quota on dark slates.
"""
import subprocess
import sys
import time
import sqlite3
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


SCRAPE_HOURS = [11, 14, 16]
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


def next_scrape_time(now):
    """Return the next scheduled scrape datetime (ET) after `now`."""
    today_targets = [
        _localize_et(datetime(now.year, now.month, now.day, h, 0, 0))
        for h in SCRAPE_HOURS
    ]
    for t in today_targets:
        if t > now:
            return t
    tomorrow = now + timedelta(days=1)
    return _localize_et(datetime(tomorrow.year, tomorrow.month, tomorrow.day,
                                 SCRAPE_HOURS[0], 0, 0))


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


def main():
    print("=" * 50)
    print("INTRA-DAY PROPS SCRAPE SCHEDULER")
    print(f"Targets: {', '.join(f'{h:02d}:00 ET' for h in SCRAPE_HOURS)}")
    print("=" * 50, flush=True)

    while True:
        now = get_et_now()
        target = next_scrape_time(now)
        wait_secs = max(30, (target - now).total_seconds())
        hrs = int(wait_secs // 3600)
        mins = int((wait_secs % 3600) // 60)
        print(f"\n[{now.strftime('%Y-%m-%d %H:%M ET')}] "
              f"Next scrape at {target.strftime('%Y-%m-%d %H:%M ET')} "
              f"({hrs}h {mins}m)", flush=True)
        time.sleep(wait_secs)
        run_scrape()


if __name__ == "__main__":
    main()
