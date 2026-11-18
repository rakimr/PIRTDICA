from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, text

from utils.shot_zone_import import (
    apply_shot_zone_merge,
    parse_shot_zone_csv,
    preview_shot_zone_merge,
)


NOW = datetime(2026, 11, 18, 20, 0, tzinfo=timezone.utc)
HEADERS = [
    "player_name", "player_id", "team", "total_fga",
    "ra_fga", "ra_fgm", "paint_fga", "paint_fgm",
    "mid_fga", "mid_fgm", "three_fga", "three_fgm",
    "corner3_fga", "atb3_fga", "ra_pct", "paint_pct",
    "rim_paint_pct", "mid_pct", "three_pct", "scraped_at",
    "source_season", "source_fetched_at",
]


def csv_text(rows):
    lines = [",".join(HEADERS)]
    for row in rows:
        lines.append(",".join(str(row.get(header, "")) for header in HEADERS))
    return "\n".join(lines)


def fresh_row(name="Example Player", player_id=123):
    row = {header: "0" for header in HEADERS}
    row.update({
        "player_name": name,
        "player_id": str(player_id),
        "team": "BOS",
        "total_fga": "20",
        "ra_pct": "40.5",
        "source_season": "2026-27",
        "source_fetched_at": "2026-11-18T19:00:00+00:00",
    })
    return row


def test_parser_keeps_only_recent_current_season_rows():
    stale = fresh_row("Old Player", 456)
    stale["source_fetched_at"] = "2026-11-01T19:00:00+00:00"
    wrong_season = fresh_row("Prior Season Player", 789)
    wrong_season["source_season"] = "2025-26"

    parsed = parse_shot_zone_csv(
        csv_text([fresh_row(), stale, wrong_season]),
        expected_season="2026-27",
        now=NOW,
    )

    assert parsed["fresh_rows"] == 1
    assert parsed["skipped_rows"] == 2
    assert parsed["rows"][0]["player_id"] == 123


def test_parser_rejects_duplicate_player_identity():
    with pytest.raises(ValueError, match="duplicate"):
        parse_shot_zone_csv(
            csv_text([fresh_row(), fresh_row("Example Player Alias", 123)]),
            expected_season="2026-27",
            now=NOW,
        )


def test_parser_rejects_empty_or_stale_only_export():
    with pytest.raises(ValueError, match="No rows were fetched"):
        parse_shot_zone_csv(
            csv_text([fresh_row() | {"source_fetched_at": "2026-11-01T19:00:00+00:00"}]),
            expected_season="2026-27",
            now=NOW,
        )


def test_merge_updates_and_inserts_without_deleting_unmentioned_rows():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    column_defs = ", ".join(f'"{column}" TEXT' for column in HEADERS if column not in {
        "source_season", "source_fetched_at"
    })
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE player_shot_zones_live "
            f"(id INTEGER PRIMARY KEY AUTOINCREMENT, {column_defs}, "
            "updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)"
        ))
        conn.execute(text(
            "INSERT INTO player_shot_zones_live (player_name, player_id, team) "
            "VALUES ('Example Player', 123, 'OLD'), ('Keep Existing', 999, 'LAL')"
        ))

        parsed = parse_shot_zone_csv(
            csv_text([fresh_row(), fresh_row("New Player", 321)]),
            expected_season="2026-27",
            now=NOW,
        )
        preview = preview_shot_zone_merge(conn, parsed["rows"])
        assert preview["updates"] == 1
        assert preview["inserts"] == 1
        assert preview["live_rows_before"] == 2

        result = apply_shot_zone_merge(conn, parsed["rows"])
        assert result == {"updates": 1, "inserts": 1, "incoming_rows": 2}
        count = conn.execute(text(
            "SELECT COUNT(*) FROM player_shot_zones_live"
        )).scalar_one()
        keep = conn.execute(text(
            "SELECT team FROM player_shot_zones_live WHERE player_id = 999"
        )).scalar_one()
        updated = conn.execute(text(
            "SELECT team FROM player_shot_zones_live WHERE player_id = 123"
        )).scalar_one()

    assert count == 3
    assert keep == "LAL"
    assert updated == "BOS"
