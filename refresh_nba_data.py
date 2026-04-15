"""
NBA.com API Stale Data Refresh
Runs each NBA.com scraper sequentially with generous cooldowns between calls.
Resets the circuit breaker first so all scrapers get a fresh attempt.
Logs all output to a timestamped file in logs/ for later review.
"""
import subprocess
import sys
import os
import time
from datetime import datetime

SCRAPERS = [
    ("scrape_nba_gamelogs.py", "NBA Game Logs (Volatility)", 1800),
    ("scrape_shot_zones.py", "Shot Zones, Creation, Hustle & Tracking Stats", 600),
    ("scrape_team_defense_zones.py", "Team Defensive Shot Zones", 300),
    ("scrape_play_types.py", "Team Play Type Schemes (Synergy)", 600),
    ("scrape_measurements.py", "Player Physical Measurements (Basketball Reference)", 300),
]

COOLDOWN_BETWEEN = 75
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
PID_FILE = os.path.join(LOG_DIR, "nba_refresh.pid")


class TeeWriter:
    def __init__(self, log_file, original_stream):
        self.log_file = log_file
        self.original = original_stream

    def write(self, msg):
        self.original.write(msg)
        self.original.flush()
        self.log_file.write(msg)
        self.log_file.flush()

    def flush(self):
        self.original.flush()
        self.log_file.flush()


def main():
    os.makedirs(LOG_DIR, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(LOG_DIR, f"nba_refresh_{timestamp}.log")
    log_file = open(log_path, "w")

    sys.stdout = TeeWriter(log_file, sys.__stdout__)
    sys.stderr = TeeWriter(log_file, sys.__stderr__)

    with open(PID_FILE, "w") as pf:
        pf.write(str(os.getpid()))

    start = datetime.now()
    print(f"PID: {os.getpid()}")
    print(f"Log: {log_path}")
    print("=" * 60)
    print("NBA.COM STALE DATA REFRESH")
    print(f"Started: {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Scrapers: {len(SCRAPERS)}")
    print(f"Cooldown between calls: {COOLDOWN_BETWEEN}s")
    print("=" * 60)

    from utils.nba_api_helpers import reset_circuit
    reset_circuit()
    print("[OK] Circuit breaker reset\n")

    results = []
    for i, (script, label, timeout) in enumerate(SCRAPERS):
        print(f"\n{'=' * 60}")
        print(f"[{i+1}/{len(SCRAPERS)}] {label}")
        print(f"Script: {script} (timeout: {timeout}s)")
        print(f"{'=' * 60}", flush=True)

        script_start = time.time()
        try:
            result = subprocess.run(
                [sys.executable, "-u", script],
                capture_output=True,
                text=True,
                timeout=timeout,
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
            )
            if result.stdout:
                print(result.stdout, end="")
            if result.stderr:
                print(result.stderr, end="")
            elapsed = time.time() - script_start
            success = result.returncode == 0
            status = "OK" if success else f"FAILED (exit code {result.returncode})"
        except subprocess.TimeoutExpired as e:
            if e.stdout:
                print(e.stdout if isinstance(e.stdout, str) else e.stdout.decode(), end="")
            if e.stderr:
                print(e.stderr if isinstance(e.stderr, str) else e.stderr.decode(), end="")
            elapsed = time.time() - script_start
            success = False
            status = f"TIMEOUT after {timeout}s"

        results.append((label, success, status, elapsed))
        print(f"\n[{status}] {label} ({elapsed:.1f}s)")

        if i < len(SCRAPERS) - 1:
            from utils.nba_api_helpers import reset_circuit
            reset_circuit()
            print(f"\n[COOLDOWN] Waiting {COOLDOWN_BETWEEN}s before next scraper...")
            time.sleep(COOLDOWN_BETWEEN)

    end = datetime.now()
    total_min = (end - start).total_seconds() / 60

    print(f"\n\n{'=' * 60}")
    print("REFRESH SUMMARY")
    print(f"{'=' * 60}")
    for label, success, status, elapsed in results:
        icon = "PASS" if success else "FAIL"
        print(f"  [{icon}] {label}: {status} ({elapsed:.1f}s)")
    passed = sum(1 for _, s, _, _ in results if s)
    failed = len(results) - passed
    print(f"\nTotal: {passed} passed, {failed} failed")
    print(f"Duration: {total_min:.1f} minutes")
    print(f"Finished: {end.strftime('%Y-%m-%d %H:%M:%S')}")

    log_file.close()
    try:
        os.remove(PID_FILE)
    except OSError:
        pass


if __name__ == "__main__":
    main()
