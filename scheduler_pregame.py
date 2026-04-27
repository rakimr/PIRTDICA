"""
Scheduler for Pre-Game Refresh — fires once per "wave" today, where each wave
is `LEAD_MINUTES` (60) before a unique tip-off time on the slate. Multi-tip
slates with widely separated games (e.g. 1 PM, 7:30 PM) get TWO refreshes so
the late games run on data that is at most 60 min old.

Waves are deduped: tips within `WAVE_DEDUPE_MINUTES` (30) of each other share
a single wave (back-to-back games don't justify two refreshes).

Falls back to a single 6:20 PM ET wave if no game times are available.

Designed to run as a persistent Replit workflow.

Env gate (Task #34): the scheduler exits cleanly unless `SCHEDULERS_ENABLED=1`
is set, so dev workspaces don't double-fire alongside the production
deployment. Pass `--force` to override the gate for one-off testing. The
production launcher (`start_production.sh`) sets the env var automatically.

Reliability:
  - **Persistent fired state** at /tmp/pirtdica_pregame_state.json so a workflow
    restart mid-day doesn't re-fire waves that already ran. Auto-prunes
    entries older than 7 days.
  - **Grace window** (180m): a wave that's overdue but still within grace fires
    on the next loop tick (e.g. workspace just opened at 6:25 PM, the noon
    wave is past grace and skipped, but a 6:00 PM wave 25min late fires).
  - **Sleep cap** (5m): every loop wakes at least every 5 minutes so the
    scheduler reacts quickly to clock changes, restarts, or new game data.
  - **Wave folding**: 7:00 PM and 7:15 PM tips share a single 6:00 PM wave.

CLI:
  python scheduler_pregame.py           # run forever (the workflow entrypoint)
  python scheduler_pregame.py --status  # print today's wave plan and exit
  python scheduler_pregame.py --once    # fire every currently-due wave then exit
  python scheduler_pregame.py --force   # bypass SCHEDULERS_ENABLED gate
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
WAVE_DEDUPE_MINUTES = 30
FALLBACK_HOUR = 18
FALLBACK_MINUTE = 20
GRACE_WINDOW_MINUTES = 180
SLEEP_CAP_SECONDS = 300
DB_PATH = "/home/runner/workspace/dfs_nba.db"
STATE_PATH = "/tmp/pirtdica_pregame_state.json"


def get_et_now():
    try:
        return datetime.now(ET)
    except Exception:
        from datetime import timezone
        return datetime.now(timezone.utc).astimezone(ET)


def get_all_game_times_today_et(db_path=None):
    """Return sorted list of distinct tip-off datetimes today in ET, or []."""
    path = db_path or DB_PATH
    try:
        conn = sqlite3.connect(path)
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT game_time FROM player_salaries WHERE game_time IS NOT NULL"
        )
        raw_times = [row[0] for row in cur.fetchall()]
        conn.close()
    except Exception:
        return []

    now = get_et_now()
    parsed = []
    for gt in raw_times:
        try:
            t = datetime.strptime(gt, "%I:%M%p")
            naive = t.replace(year=now.year, month=now.month, day=now.day)
            parsed.append(_localize_et(naive))
        except Exception:
            continue
    return sorted(set(parsed))


def compute_waves_today(db_path=None):
    """Return list of (wave_dt, tip_dt, source_label) for today.

    Each wave fires `LEAD_MINUTES` before a unique tip-off time. Tips within
    `WAVE_DEDUPE_MINUTES` of an earlier wave are folded into that earlier
    wave (their tip is appended to the source label). Returns a single
    fallback wave if no game times are available.
    """
    tips = get_all_game_times_today_et(db_path=db_path)
    if not tips:
        now = get_et_now()
        target = now.replace(
            hour=FALLBACK_HOUR, minute=FALLBACK_MINUTE, second=0, microsecond=0
        )
        return [(
            target,
            None,
            f"fallback {FALLBACK_HOUR:02d}:{FALLBACK_MINUTE:02d} ET (no game times found)",
        )]

    waves = []
    for tip in tips:
        wave = tip - timedelta(minutes=LEAD_MINUTES)
        if waves:
            last_wave_dt, _, last_label = waves[-1]
            gap_min = (wave - last_wave_dt).total_seconds() / 60.0
            if gap_min < WAVE_DEDUPE_MINUTES:
                new_label = last_label + f" + {tip.strftime('%I:%M %p ET')}"
                waves[-1] = (last_wave_dt, waves[-1][1], new_label)
                continue
        waves.append((
            wave,
            tip,
            f"60m before {tip.strftime('%I:%M %p ET')}",
        ))
    return waves


def _load_fired_state(state_path=None):
    """Load JSON file: {date_str: [iso_wave_dt, ...]}."""
    path = state_path or STATE_PATH
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_fired_state(state, state_path=None):
    path = state_path or STATE_PATH
    try:
        with open(path, "w") as f:
            json.dump(state, f)
    except Exception as e:
        print(f"[pregame] WARN: failed to persist fired state: {e}", flush=True)


def _today_key():
    return get_et_now().strftime("%Y-%m-%d")


def has_fired(wave_dt, state=None, state_path=None):
    if state is None:
        state = _load_fired_state(state_path=state_path)
    return wave_dt.isoformat() in state.get(_today_key(), [])


def mark_fired(wave_dt, state_path=None):
    state = _load_fired_state(state_path=state_path)
    today = _today_key()
    waves = state.setdefault(today, [])
    if wave_dt.isoformat() not in waves:
        waves.append(wave_dt.isoformat())
    # Prune dates older than 7 days so the state file doesn't grow forever.
    now = get_et_now()
    keep = {}
    for d, lst in state.items():
        try:
            d_dt = datetime.strptime(d, "%Y-%m-%d").date()
            if (now.date() - d_dt).days <= 7:
                keep[d] = lst
        except Exception:
            continue
    _save_fired_state(keep, state_path=state_path)


def run_refresh(source_label):
    print(f"\n{'='*50}")
    print(f"TRIGGERING PRE-GAME REFRESH at {get_et_now().strftime('%Y-%m-%d %H:%M:%S ET')}")
    print(f"  Source: {source_label}")
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
            print(f"\nPre-game refresh completed at {get_et_now().strftime('%H:%M ET')}",
                  flush=True)
            return True
        print(f"\nPre-game refresh finished with errors (exit {result.returncode})",
              flush=True)
        return False
    except subprocess.TimeoutExpired:
        print("\nPre-game refresh timed out after 60 minutes", flush=True)
        return False
    except Exception as e:
        print(f"\nPre-game refresh error: {e}", flush=True)
        return False


def find_next_action(now, waves, state_path=None):
    """Return one of:
      ('fire', wave_dt, tip_dt, source_label)  -- a wave is due now
      ('wait', secs)                           -- there's a future wave today
      ('done', None)                           -- no more waves today
    Already-fired and past-grace waves are skipped (past-grace are auto-marked
    fired so they don't re-trigger this skip on every tick).
    """
    state = _load_fired_state(state_path=state_path)
    next_future_secs = None
    for wave_dt, tip_dt, label in waves:
        if has_fired(wave_dt, state=state):
            continue
        delta_secs = (wave_dt - now).total_seconds()
        if delta_secs <= 0:
            late_min = -delta_secs / 60.0
            if late_min <= GRACE_WINDOW_MINUTES:
                return ("fire", wave_dt, tip_dt, label)
            print(
                f"\n[{now.strftime('%Y-%m-%d %H:%M ET')}] "
                f"Skipping wave at {wave_dt.strftime('%H:%M ET')} "
                f"({label}) — {int(late_min)}m late, past "
                f"{GRACE_WINDOW_MINUTES}m grace",
                flush=True,
            )
            mark_fired(wave_dt, state_path=state_path)
            continue
        if next_future_secs is None or delta_secs < next_future_secs:
            next_future_secs = delta_secs

    if next_future_secs is None:
        return ("done", None)
    return ("wait", next_future_secs)


def sleep_until_next_morning(now):
    today_8am = _localize_et(datetime(now.year, now.month, now.day, 8, 0, 0))
    if now < today_8am:
        next_morning = today_8am
    else:
        tomorrow = now + timedelta(days=1)
        next_morning = _localize_et(
            datetime(tomorrow.year, tomorrow.month, tomorrow.day, 8, 0, 0)
        )
    wait_secs = max(60, (next_morning - now).total_seconds())
    hrs = int(wait_secs // 3600)
    mins = int((wait_secs % 3600) // 60)
    print(
        f"\n[{now.strftime('%Y-%m-%d %H:%M ET')}] "
        f"All waves done. Sleeping until "
        f"{next_morning.strftime('%Y-%m-%d %H:%M ET')} ({hrs}h {mins}m)",
        flush=True,
    )
    time.sleep(wait_secs)


def print_status():
    now = get_et_now()
    waves = compute_waves_today()
    fired = _load_fired_state().get(_today_key(), [])
    print("PRE-GAME REFRESH SCHEDULER — STATUS")
    print(f"  Now (ET):           {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  SCHEDULERS_ENABLED: {os.environ.get('SCHEDULERS_ENABLED', '(unset)')}")
    print(f"  Waves today ({len(waves)}):")
    for wave_dt, _, label in waves:
        status = "FIRED" if wave_dt.isoformat() in fired else "pending"
        print(f"    - {wave_dt.strftime('%H:%M ET')}  [{status}]  {label}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--status", action="store_true",
                        help="Print today's wave plan and exit.")
    parser.add_argument("--once", action="store_true",
                        help="Fire every currently-due wave then exit.")
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
            "[scheduler_pregame] SCHEDULERS_ENABLED != 1 — exiting "
            "(set the env var, or pass --once/--force for a one-shot run).",
            flush=True,
        )
        return

    print("=" * 50)
    print("PRE-GAME REFRESH SCHEDULER (multi-wave)")
    print(
        f"Lead: {LEAD_MINUTES}m before each tip | "
        f"Wave dedupe: {WAVE_DEDUPE_MINUTES}m | "
        f"Grace: {GRACE_WINDOW_MINUTES}m | "
        f"Sleep cap: {SLEEP_CAP_SECONDS}s"
    )
    print("=" * 50, flush=True)

    while True:
        now = get_et_now()
        waves = compute_waves_today()
        action = find_next_action(now, waves)

        if action[0] == "done":
            if args.once:
                return
            sleep_until_next_morning(now)
            continue

        if action[0] == "fire":
            _, wave_dt, _, label = action
            run_refresh(label)
            mark_fired(wave_dt)
            if args.once:
                continue
            time.sleep(5)
            continue

        # 'wait'
        _, wait_secs = action
        capped = min(max(30, wait_secs), SLEEP_CAP_SECONDS)
        next_check = now + timedelta(seconds=capped)
        wait_min_total = int(wait_secs / 60)
        print(
            f"\n[{now.strftime('%Y-%m-%d %H:%M ET')}] "
            f"Next wave in {wait_min_total}m. "
            f"Re-evaluating at {next_check.strftime('%H:%M:%S ET')} "
            f"(sleeping {int(capped)}s)",
            flush=True,
        )
        if args.once:
            return
        time.sleep(capped)


if __name__ == "__main__":
    main()
