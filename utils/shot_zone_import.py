"""Validate and safely merge a local NBA shot-zone refresh into live storage."""

import csv
import io
import math
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from utils.nba_season import nba_api_season
from utils.player_name_key import nba_player_name_key


MAX_CSV_BYTES = 2_000_000
MAX_ROWS = 1_000
FRESHNESS_WINDOW = timedelta(days=7)

SHOT_ZONE_COLUMNS = (
    "player_name", "player_id", "team", "total_fga",
    "ra_fga", "ra_fgm", "paint_fga", "paint_fgm",
    "mid_fga", "mid_fgm", "three_fga", "three_fgm",
    "corner3_fga", "atb3_fga", "ra_pct", "paint_pct",
    "rim_paint_pct", "mid_pct", "three_pct", "scraped_at",
)
INTEGER_COLUMNS = {
    "player_id", "total_fga", "ra_fga", "ra_fgm", "paint_fga",
    "paint_fgm", "mid_fga", "mid_fgm", "three_fga", "three_fgm",
    "corner3_fga", "atb3_fga",
}
PERCENT_COLUMNS = {
    "ra_pct", "paint_pct", "rim_paint_pct", "mid_pct", "three_pct",
}
REQUIRED_HEADERS = set(SHOT_ZONE_COLUMNS) - {"scraped_at"}
REQUIRED_HEADERS.update({"source_season", "source_fetched_at"})


def _parse_timestamp(value):
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("A row is missing its source fetch timestamp.")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("A row has an invalid source fetch timestamp.") from exc
    if parsed.tzinfo is None:
        raise ValueError("Source fetch timestamps must include a timezone.")
    return parsed.astimezone(timezone.utc)


def _parse_number(value, column, *, integer=False):
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        number = float(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid numeric value in {column}.") from exc
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"Invalid negative or non-finite value in {column}.")
    if integer:
        if not number.is_integer():
            raise ValueError(f"Expected a whole number in {column}.")
        return int(number)
    return number


def parse_shot_zone_csv(csv_text, *, expected_season=None, now=None):
    """Return only recent, current-season rows from a scraper CSV export."""
    if not isinstance(csv_text, str) or not csv_text.strip():
        raise ValueError("Choose a non-empty shot-zone CSV file.")
    if len(csv_text.encode("utf-8")) > MAX_CSV_BYTES:
        raise ValueError("The CSV is too large. Export only player_shot_zones.")

    reader = csv.DictReader(io.StringIO(csv_text.lstrip("\ufeff")))
    if not reader.fieldnames:
        raise ValueError("The CSV has no header row.")
    headers = [str(name or "").strip().lower() for name in reader.fieldnames]
    if len(headers) != len(set(headers)):
        raise ValueError("The CSV contains duplicate column names.")
    missing = sorted(REQUIRED_HEADERS - set(headers))
    if missing:
        raise ValueError(
            "This is not a fresh shot-zone export; missing columns: "
            + ", ".join(missing)
        )

    expected_season = expected_season or nba_api_season()
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)

    fresh_rows = []
    stale_rows = 0
    total_rows = 0
    seen_ids = set()
    seen_names = set()

    for raw_row in reader:
        if not raw_row or not any(str(value or "").strip() for value in raw_row.values()):
            continue
        total_rows += 1
        if total_rows > MAX_ROWS:
            raise ValueError(f"The CSV exceeds the {MAX_ROWS}-row safety limit.")
        row = {
            str(key or "").strip().lower(): str(value or "").strip()
            for key, value in raw_row.items()
        }
        if row.get("source_season") != expected_season:
            stale_rows += 1
            continue

        fetched_at = _parse_timestamp(row.get("source_fetched_at"))
        age = now - fetched_at
        if age < timedelta(minutes=-15) or age > FRESHNESS_WINDOW:
            stale_rows += 1
            continue

        player_name = row.get("player_name", "").strip()
        if not player_name or len(player_name) > 100:
            raise ValueError("A current-season row has an invalid player name.")
        try:
            player_id = int(row.get("player_id", ""))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{player_name} has an invalid NBA player ID.") from exc
        if player_id <= 0:
            raise ValueError(f"{player_name} has an invalid NBA player ID.")

        name_key = nba_player_name_key(player_name)
        if player_id in seen_ids or name_key in seen_names:
            raise ValueError("The CSV contains duplicate player IDs or names.")
        seen_ids.add(player_id)
        seen_names.add(name_key)

        team = row.get("team", "").upper()
        if len(team) > 10:
            raise ValueError(f"{player_name} has an invalid team abbreviation.")

        prepared = {
            "player_name": player_name,
            "player_id": player_id,
            "team": team or None,
            "scraped_at": fetched_at.isoformat(),
        }
        for column in SHOT_ZONE_COLUMNS:
            if column in {"player_name", "player_id", "team", "scraped_at"}:
                continue
            prepared[column] = _parse_number(
                row.get(column), column, integer=column in INTEGER_COLUMNS
            )
            if column in PERCENT_COLUMNS and prepared[column] is not None:
                if prepared[column] > 100:
                    raise ValueError(f"{column} must be between 0 and 100.")
        fresh_rows.append(prepared)

    if total_rows == 0:
        raise ValueError("The CSV contains no player rows.")
    if not fresh_rows:
        raise ValueError(
            f"No rows were fetched for the current NBA season ({expected_season}) "
            f"within the last {FRESHNESS_WINDOW.days} days. Re-run the local scraper."
        )

    return {
        "rows": fresh_rows,
        "total_rows": total_rows,
        "fresh_rows": len(fresh_rows),
        "skipped_rows": stale_rows,
        "season": expected_season,
        "latest_source_fetch": max(row["scraped_at"] for row in fresh_rows),
    }


def _plan_shot_zone_merge(conn, rows):
    existing_rows = conn.execute(text(
        "SELECT id, player_id, player_name FROM player_shot_zones_live"
    )).mappings().all()
    by_id = {}
    by_name = {}
    for existing in existing_rows:
        try:
            existing_player_id = int(existing["player_id"])
        except (TypeError, ValueError):
            existing_player_id = None
        by_id.setdefault(existing_player_id, []).append(existing)
        key = nba_player_name_key(existing["player_name"])
        if key:
            by_name.setdefault(key, []).append(existing)

    plan = []
    for row in rows:
        candidates = {
            item["id"]: item
            for item in (
                by_id.get(row["player_id"], [])
                + by_name.get(nba_player_name_key(row["player_name"]), [])
            )
        }
        if len(candidates) > 1:
            raise ValueError(
                f"Live shot-zone data has multiple records for {row['player_name']}; "
                "nothing was changed."
            )
        existing = next(iter(candidates.values()), None)
        if existing:
            try:
                existing_player_id = int(existing["player_id"])
            except (TypeError, ValueError):
                existing_player_id = None
            if existing_player_id not in (None, row["player_id"]):
                raise ValueError(
                    f"Player-name/ID mismatch for {row['player_name']} in live data; "
                    "nothing was changed."
                )
        plan.append((row, existing))
    return plan, len(existing_rows)


def preview_shot_zone_merge(conn, rows):
    """Read-only preview. Missing live table is an error; it is never created."""
    plan, live_count = _plan_shot_zone_merge(conn, rows)
    return {
        "updates": sum(1 for _, existing in plan if existing),
        "inserts": sum(1 for _, existing in plan if not existing),
        "live_rows_before": live_count,
        "incoming_rows": len(rows),
    }


def apply_shot_zone_merge(conn, rows):
    """Upsert by NBA player ID/name without deleting rows absent from the file."""
    plan, _ = _plan_shot_zone_merge(conn, rows)
    columns = list(SHOT_ZONE_COLUMNS)
    updates = 0
    inserts = 0
    set_clause = ", ".join(f'"{column}" = :{column}' for column in columns)
    insert_columns = ", ".join(f'"{column}"' for column in columns)
    insert_values = ", ".join(f":{column}" for column in columns)

    for row, existing in plan:
        values = {column: row.get(column) for column in columns}
        if existing:
            values["existing_id"] = existing["id"]
            conn.execute(
                text(
                    "UPDATE player_shot_zones_live SET "
                    f"{set_clause} WHERE id = :existing_id"
                ),
                values,
            )
            updates += 1
        else:
            conn.execute(
                text(
                    "INSERT INTO player_shot_zones_live "
                    f"({insert_columns}) VALUES ({insert_values})"
                ),
                values,
            )
            inserts += 1
    return {"updates": updates, "inserts": inserts, "incoming_rows": len(rows)}
