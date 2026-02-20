import os
import sys
import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

SUPABASE_URL = os.environ.get("SUPABASE_DATABASE_URL")
DATABASE_URL = os.environ.get("DATABASE_URL")

SYNC_URL = SUPABASE_URL or DATABASE_URL
if not SYNC_URL:
    print("ERROR: Neither SUPABASE_DATABASE_URL nor DATABASE_URL is set")
    sys.exit(1)

if "supabase" in SYNC_URL and ":5432" in SYNC_URL:
    SYNC_URL = SYNC_URL.replace(":5432", ":6543")

if SUPABASE_URL:
    print("Syncing to Supabase PostgreSQL")
else:
    print("Syncing to local PostgreSQL (DATABASE_URL)")

engine = create_engine(SYNC_URL)
Session = sessionmaker(bind=engine)

SQLITE_DB = "dfs_nba.db"

CSV_TABLE_MAP = {
    "dfs_players.csv": "dfs_players_live",
    "dfs_players_valued.csv": "dfs_players_live",
    "prop_recommendations.csv": "prop_recommendations_live",
    "targeted_plays.csv": "targeted_plays_live",
    "ownership_projections.csv": "ownership_projections_live",
}

SQLITE_TABLE_MAP = {
    "player_salaries": "player_salaries_live",
    "injury_alerts": "injury_alerts_live",
    "player_archetypes": "player_archetypes_live",
    "player_per100": "player_per100_live",
    "player_positions": "player_positions_live",
    "player_stats": "player_stats_live",
    "player_game_logs": "player_game_logs_live",
    "player_shot_zones": "player_shot_zones_live",
    "player_shot_creation": "player_shot_creation_live",
    "player_hustle_stats": "player_hustle_stats_live",
    "dva_stats": "dva_stats_live",
    "archetype_profiles": "archetype_profiles_live",
    "team_defense_shot_zones": "team_defense_shot_zones_live",
    "team_play_types": "team_play_types_live",
    "player_headshots": "player_headshots_live",
    "game_odds": "game_odds_live",
}


def create_tables():
    from backend.models import Base
    Base.metadata.create_all(engine)
    print("PostgreSQL tables created/verified.")


def _calc_batch_size(num_cols):
    max_params = 400
    batch = max(1, max_params // max(num_cols, 1))
    return min(batch, 50)


def _insert_rows(conn, pg_table, df, common_cols):
    batch_size = _calc_batch_size(len(common_cols))
    num_cols = len(common_cols)
    col_list = ", ".join(common_cols)
    total = len(df)
    inserted = 0

    for i in range(0, total, batch_size):
        chunk = df.iloc[i:i + batch_size]
        chunk_size = len(chunk)
        value_groups = []
        params = {}
        for row_idx, (_, row) in enumerate(chunk.iterrows()):
            row_placeholders = []
            for j, col in enumerate(common_cols):
                key = f"p{row_idx}_{j}"
                row_placeholders.append(f":{key}")
                val = row[col]
                if pd.isna(val):
                    params[key] = None
                else:
                    params[key] = val
            value_groups.append(f"({', '.join(row_placeholders)})")
        values_sql = ", ".join(value_groups)
        conn.execute(
            text(f"INSERT INTO {pg_table} ({col_list}) VALUES {values_sql}"),
            params
        )
        inserted += chunk_size
        if total > 500 and inserted % 1000 == 0:
            print(f"    ... {inserted}/{total} rows inserted")


def sync_csv(csv_path, pg_table):
    if not os.path.exists(csv_path):
        print(f"  SKIP: {csv_path} not found")
        return 0

    try:
        df = pd.read_csv(csv_path)
        if df.empty:
            print(f"  SKIP: {csv_path} is empty")
            return 0

        df.columns = [c.strip().lower() for c in df.columns]

        inf_cols = df.select_dtypes(include=['float64', 'float32']).columns
        for col in inf_cols:
            df[col] = df[col].replace([float('inf'), float('-inf')], None)

        with engine.begin() as conn:
            conn.execute(text(f"DELETE FROM {pg_table}"))

            pg_cols_result = conn.execute(text(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{pg_table}' AND column_name NOT IN ('id', 'updated_at')"
            ))
            pg_cols = {row[0] for row in pg_cols_result}

            csv_cols = set(df.columns)
            common_cols = list(csv_cols & pg_cols)

            if not common_cols:
                print(f"  WARN: No matching columns for {csv_path} -> {pg_table}")
                return 0

            df_filtered = df[common_cols]
            df_filtered = df_filtered.where(pd.notnull(df_filtered), None)

            _insert_rows(conn, pg_table, df_filtered, common_cols)

        count = len(df_filtered)
        print(f"  OK: {csv_path} -> {pg_table} ({count} rows)")
        return count
    except Exception as e:
        print(f"  ERROR syncing {csv_path}: {e}")
        return 0


def sync_sqlite_table(sqlite_table, pg_table):
    if not os.path.exists(SQLITE_DB):
        print(f"  SKIP: {SQLITE_DB} not found")
        return 0

    try:
        sconn = sqlite3.connect(SQLITE_DB)
        tables = [r[0] for r in sconn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        if sqlite_table not in tables:
            print(f"  SKIP: SQLite table '{sqlite_table}' not found")
            sconn.close()
            return 0

        df = pd.read_sql_query(f"SELECT * FROM {sqlite_table}", sconn)
        sconn.close()

        if df.empty:
            print(f"  SKIP: SQLite table '{sqlite_table}' is empty")
            return 0

        df.columns = [c.strip().lower() for c in df.columns]

        inf_cols = df.select_dtypes(include=['float64', 'float32']).columns
        for col in inf_cols:
            df[col] = df[col].replace([float('inf'), float('-inf')], None)

        with engine.begin() as conn:
            conn.execute(text(f"DELETE FROM {pg_table}"))

            pg_cols_result = conn.execute(text(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{pg_table}' AND column_name NOT IN ('id', 'updated_at')"
            ))
            pg_cols = {row[0] for row in pg_cols_result}

            sqlite_cols = set(df.columns)
            if 'id' in sqlite_cols:
                sqlite_cols.discard('id')
            common_cols = list(sqlite_cols & pg_cols)

            if not common_cols:
                print(f"  WARN: No matching columns for {sqlite_table} -> {pg_table}")
                return 0

            df_filtered = df[common_cols]
            df_filtered = df_filtered.where(pd.notnull(df_filtered), None)

            _insert_rows(conn, pg_table, df_filtered, common_cols)

        count = len(df_filtered)
        print(f"  OK: {sqlite_table} -> {pg_table} ({count} rows)")
        return count
    except Exception as e:
        print(f"  ERROR syncing {sqlite_table}: {e}")
        return 0


def main():
    print("=" * 50)
    print("Syncing Pipeline Data to PostgreSQL")
    print("=" * 50)

    create_tables()

    total_rows = 0

    print("\n--- CSV Files ---")
    valued_path = "dfs_players_valued.csv"
    base_path = "dfs_players.csv"
    if os.path.exists(valued_path):
        total_rows += sync_csv(valued_path, "dfs_players_live")
    elif os.path.exists(base_path):
        total_rows += sync_csv(base_path, "dfs_players_live")
    else:
        print("  SKIP: No DFS players CSV found")

    total_rows += sync_csv("prop_recommendations.csv", "prop_recommendations_live")
    total_rows += sync_csv("targeted_plays.csv", "targeted_plays_live")
    total_rows += sync_csv("ownership_projections.csv", "ownership_projections_live")

    print("\n--- SQLite Tables ---")
    for sqlite_table, pg_table in SQLITE_TABLE_MAP.items():
        total_rows += sync_sqlite_table(sqlite_table, pg_table)

    print(f"\n{'=' * 50}")
    print(f"Sync Complete: {total_rows} total rows synced")
    print("=" * 50)


if __name__ == "__main__":
    main()
