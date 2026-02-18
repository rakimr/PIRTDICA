import sqlite3
import time
import pandas as pd
from datetime import datetime
from nba_api.stats.endpoints import shotchartdetail
from nba_api.stats.static import players as nba_players

DB_PATH = "dfs_nba.db"
SEASON = "2025-26"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.nba.com/",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.nba.com",
}

def get_player_ids():
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT player_name, player_id FROM player_shot_zones WHERE player_id IS NOT NULL AND player_id != 0"
    ).fetchall()
    conn.close()
    return {name: pid for name, pid in rows}

def scrape_league_averages():
    print("Fetching league average shot chart...")
    try:
        data = shotchartdetail.ShotChartDetail(
            team_id=0,
            player_id=0,
            season_nullable=SEASON,
            season_type_all_star="Regular Season",
            context_measure_simple="FGA",
            headers=HEADERS,
            timeout=60,
        )
        frames = data.get_data_frames()
        if len(frames) > 1:
            league_avg = frames[1]
            print(f"  League averages: {len(league_avg)} zone rows")
            return league_avg
    except Exception as e:
        print(f"  Failed to get league averages: {e}")
    return None

def scrape_player_shots(player_id, player_name):
    try:
        data = shotchartdetail.ShotChartDetail(
            team_id=0,
            player_id=player_id,
            season_nullable=SEASON,
            season_type_all_star="Regular Season",
            context_measure_simple="FGA",
            headers=HEADERS,
            timeout=60,
        )
        df = data.get_data_frames()[0]
        return df
    except Exception as e:
        print(f"  Error for {player_name}: {e}")
        return None

def main():
    print(f"Scraping NBA Shot Chart Detail for {SEASON}...")
    player_map = get_player_ids()
    print(f"Found {len(player_map)} players with IDs")

    conn = sqlite3.connect(DB_PATH)

    conn.execute("DROP TABLE IF EXISTS player_shot_chart_detail")
    conn.execute("""
        CREATE TABLE player_shot_chart_detail (
            player_name TEXT,
            player_id INTEGER,
            team_name TEXT,
            game_id TEXT,
            game_date TEXT,
            loc_x REAL,
            loc_y REAL,
            shot_made INTEGER,
            shot_type TEXT,
            shot_zone_basic TEXT,
            shot_zone_area TEXT,
            shot_zone_range TEXT,
            shot_distance INTEGER,
            action_type TEXT,
            period INTEGER,
            scraped_at TEXT
        )
    """)

    conn.execute("DROP TABLE IF EXISTS league_shot_zone_averages")
    conn.execute("""
        CREATE TABLE league_shot_zone_averages (
            shot_zone_basic TEXT,
            shot_zone_area TEXT,
            shot_zone_range TEXT,
            fga_frequency REAL,
            fgm REAL,
            fga REAL,
            fg_pct REAL
        )
    """)

    league_avg = scrape_league_averages()
    if league_avg is not None and len(league_avg) > 0:
        for _, row in league_avg.iterrows():
            conn.execute(
                "INSERT INTO league_shot_zone_averages VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    row.get("SHOT_ZONE_BASIC", ""),
                    row.get("SHOT_ZONE_AREA", ""),
                    row.get("SHOT_ZONE_RANGE", ""),
                    row.get("FGA_FREQUENCY", 0),
                    row.get("FGM", 0),
                    row.get("FGA", 0),
                    row.get("FG_PCT", 0),
                ),
            )
        conn.commit()
        print(f"  Saved {len(league_avg)} league average zone rows")
    time.sleep(1.0)

    total = len(player_map)
    saved = 0
    errors = 0
    batch = []
    now = datetime.now().isoformat()

    for i, (name, pid) in enumerate(sorted(player_map.items()), 1):
        if i % 25 == 0 or i == 1:
            print(f"  [{i}/{total}] Scraping {name} (ID={pid})...")

        df = scrape_player_shots(pid, name)
        if df is not None and len(df) > 0:
            for _, row in df.iterrows():
                batch.append((
                    row.get("PLAYER_NAME", name),
                    pid,
                    row.get("TEAM_NAME", ""),
                    row.get("GAME_ID", ""),
                    row.get("GAME_DATE", ""),
                    row.get("LOC_X", 0),
                    row.get("LOC_Y", 0),
                    int(row.get("SHOT_MADE_FLAG", 0)),
                    row.get("SHOT_TYPE", ""),
                    row.get("SHOT_ZONE_BASIC", ""),
                    row.get("SHOT_ZONE_AREA", ""),
                    row.get("SHOT_ZONE_RANGE", ""),
                    int(row.get("SHOT_DISTANCE", 0)),
                    row.get("ACTION_TYPE", ""),
                    int(row.get("PERIOD", 0)),
                    now,
                ))
            saved += 1
        else:
            errors += 1

        if len(batch) >= 5000:
            conn.executemany(
                "INSERT INTO player_shot_chart_detail VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                batch,
            )
            conn.commit()
            batch = []

        time.sleep(0.7)

    if batch:
        conn.executemany(
            "INSERT INTO player_shot_chart_detail VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            batch,
        )
        conn.commit()

    total_shots = conn.execute("SELECT COUNT(*) FROM player_shot_chart_detail").fetchone()[0]
    total_players = conn.execute("SELECT COUNT(DISTINCT player_id) FROM player_shot_chart_detail").fetchone()[0]
    conn.close()

    print(f"\nDone! {total_shots} shots from {total_players} players ({errors} errors)")

if __name__ == "__main__":
    main()
