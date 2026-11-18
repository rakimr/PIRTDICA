"""Export only recently fetched, current-season NBA shot-zone rows."""

import csv
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from utils.nba_season import nba_api_season


DB_PATH = Path("dfs_nba.db")
OUTPUT_PATH = Path("local-data-exports/player_shot_zones.csv")
FRESHNESS_WINDOW = timedelta(days=7)


def main():
    if not DB_PATH.exists():
        raise SystemExit("dfs_nba.db was not found. Run this from the PIRTDICA project folder.")

    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.execute("SELECT * FROM player_shot_zones")
        columns = [description[0] for description in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    except sqlite3.Error as exc:
        raise SystemExit(f"Could not read local player_shot_zones: {exc}") from exc
    finally:
        conn.close()

    required = {"source_season", "source_fetched_at", "player_id", "player_name"}
    if not required.issubset(columns):
        raise SystemExit(
            "The local table lacks source freshness metadata. Re-run scrape_shot_zones.py "
            "with the current project version."
        )

    now = datetime.now(timezone.utc)
    current_season = nba_api_season()
    fresh = []
    for row in rows:
        if row.get("source_season") != current_season:
            continue
        try:
            fetched_at = datetime.fromisoformat(
                str(row.get("source_fetched_at") or "").replace("Z", "+00:00")
            )
        except ValueError:
            continue
        if fetched_at.tzinfo is None:
            continue
        age = now - fetched_at.astimezone(timezone.utc)
        if timedelta(0) <= age <= FRESHNESS_WINDOW:
            fresh.append(row)

    if not fresh:
        raise SystemExit(
            f"No shot-zone rows were freshly fetched for {current_season} in the last "
            f"{FRESHNESS_WINDOW.days} days. Re-run scrape_shot_zones.py first."
        )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", newline="", encoding="utf-8-sig") as output:
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        writer.writerows(fresh)
    print(f"Exported {len(fresh)} current-season shot-zone rows to {OUTPUT_PATH}")
    print("The export contains no other SQLite tables.")


if __name__ == "__main__":
    main()
