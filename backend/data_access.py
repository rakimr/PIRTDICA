import os
import pandas as pd
from sqlalchemy import text
from backend.database import engine


def _pg_query(query, params=None):
    try:
        with engine.connect() as conn:
            if params:
                result = conn.execute(text(query), params)
            else:
                result = conn.execute(text(query))
            cols = list(result.keys())
            rows = result.fetchall()
            if not rows:
                return pd.DataFrame(columns=cols)
            return pd.DataFrame(rows, columns=cols)
    except Exception:
        return pd.DataFrame()


def _pg_table_has_data(table_name):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            return result.scalar() > 0
    except Exception:
        return False


def use_postgres():
    return _pg_table_has_data("dfs_players_live")


def get_dfs_players():
    if use_postgres():
        return _pg_query("SELECT * FROM dfs_players_live")
    for path in ["dfs_players_valued.csv", "dfs_players.csv"]:
        if os.path.exists(path):
            return pd.read_csv(path)
    return pd.DataFrame()


def get_prop_recommendations():
    if use_postgres():
        return _pg_query("SELECT * FROM prop_recommendations_live")
    if os.path.exists("prop_recommendations.csv"):
        return pd.read_csv("prop_recommendations.csv")
    return pd.DataFrame()


def get_targeted_plays():
    if use_postgres():
        return _pg_query("SELECT * FROM targeted_plays_live")
    if os.path.exists("targeted_plays.csv"):
        return pd.read_csv("targeted_plays.csv")
    return pd.DataFrame()


def get_ownership_projections():
    if use_postgres():
        return _pg_query("SELECT * FROM ownership_projections_live")
    if os.path.exists("ownership_projections.csv"):
        return pd.read_csv("ownership_projections.csv")
    return pd.DataFrame()


def get_player_salaries_game_times():
    if use_postgres():
        return _pg_query(
            "SELECT DISTINCT game, game_time FROM player_salaries_live WHERE game_time IS NOT NULL"
        )
    try:
        import sqlite3
        conn = sqlite3.connect("dfs_nba.db")
        df = pd.read_sql_query(
            "SELECT DISTINCT game, game_time FROM player_salaries WHERE game_time IS NOT NULL",
            conn
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame(columns=["game", "game_time"])


def get_injury_alerts():
    if use_postgres():
        return _pg_query(
            "SELECT player_name, status FROM injury_alerts_live WHERE status IN ('OUT', 'QUESTIONABLE', 'PROBABLE', 'DOUBTFUL', 'GTD')"
        )
    try:
        import sqlite3
        conn = sqlite3.connect("dfs_nba.db")
        df = pd.read_sql_query(
            "SELECT player_name, status FROM injury_alerts WHERE status IN ('OUT', 'QUESTIONABLE', 'PROBABLE', 'DOUBTFUL', 'GTD')",
            conn
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame(columns=["player_name", "status"])


def get_player_salary_count():
    if use_postgres():
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM player_salaries_live"))
                return result.scalar()
        except Exception:
            return 0
    try:
        import sqlite3
        conn = sqlite3.connect("dfs_nba.db")
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM player_salaries")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except Exception:
        return 0


def get_all_game_times():
    if use_postgres():
        try:
            with engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT DISTINCT game_time FROM player_salaries_live WHERE game_time IS NOT NULL"
                ))
                return [row[0] for row in result.fetchall()]
        except Exception:
            return []
    try:
        import sqlite3
        conn = sqlite3.connect("dfs_nba.db")
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT game_time FROM player_salaries WHERE game_time IS NOT NULL")
        times = [row[0] for row in cur.fetchall()]
        conn.close()
        return times
    except Exception:
        return []


def get_game_lock_rows():
    if use_postgres():
        try:
            with engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT DISTINCT game, game_time FROM player_salaries_live WHERE game_time IS NOT NULL"
                ))
                return result.fetchall()
        except Exception:
            return []
    try:
        import sqlite3
        conn = sqlite3.connect("dfs_nba.db")
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT game, game_time FROM player_salaries WHERE game_time IS NOT NULL")
        rows = cur.fetchall()
        conn.close()
        return rows
    except Exception:
        return []


def get_player_archetypes():
    composite_cols = ("creation_idx, playmaking_idx, interior_idx, perimeter_idx, "
                      "offball_idx, rebound_idx, defense_idx, size_idx")
    full_cols = f"player_name, team, archetype, cluster, {composite_cols}"
    basic_cols = "player_name, team, archetype, cluster"

    def _try_sqlite(with_composites=True):
        try:
            import sqlite3
            conn = sqlite3.connect("dfs_nba.db")
            try:
                df = pd.read_sql_query(f"SELECT {full_cols} FROM player_archetypes", conn)
            except Exception:
                if with_composites:
                    conn.close()
                    return pd.DataFrame()
                df = pd.read_sql_query(f"SELECT {basic_cols} FROM player_archetypes", conn)
            conn.close()
            return df
        except Exception:
            return pd.DataFrame()

    if use_postgres():
        df = _pg_query(f"SELECT {full_cols} FROM player_archetypes_live")
        if not df.empty and 'creation_idx' in df.columns:
            return df
        sqlite_df = _try_sqlite(with_composites=True)
        if not sqlite_df.empty and 'creation_idx' in sqlite_df.columns:
            return sqlite_df
        df = _pg_query(f"SELECT {basic_cols} FROM player_archetypes_live")
        if not df.empty:
            return df
        return _try_sqlite(with_composites=False)
    return _try_sqlite(with_composites=True)


def get_player_per100():
    if use_postgres():
        return _pg_query(
            "SELECT player_name, pts_per100, reb_per100, ast_per100, stl_per100, blk_per100 "
            "FROM player_per100_live WHERE games_played >= 10 AND mpg >= 12"
        )
    try:
        import sqlite3
        conn = sqlite3.connect("dfs_nba.db")
        df = pd.read_sql_query(
            "SELECT player_name, pts_per100, reb_per100, ast_per100, stl_per100, blk_per100 "
            "FROM player_per100 WHERE games_played >= 10 AND mpg >= 12", conn
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


def get_player_positions():
    if use_postgres():
        return _pg_query("SELECT player_name, pg_pct, sg_pct, sf_pct, pf_pct, c_pct FROM player_positions_live")
    try:
        import sqlite3
        conn = sqlite3.connect("dfs_nba.db")
        df = pd.read_sql_query("SELECT player_name, pg_pct, sg_pct, sf_pct, pf_pct, c_pct FROM player_positions", conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


def get_player_usage():
    if use_postgres():
        return _pg_query("SELECT player_name, usg_pct FROM player_stats_live")
    try:
        import sqlite3
        conn = sqlite3.connect("dfs_nba.db")
        df = pd.read_sql_query("SELECT player_name, usg_pct FROM player_stats", conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


def get_player_game_log_averages():
    if use_postgres():
        return _pg_query(
            "SELECT player_name, AVG(fg3m) as fg3m_pg, AVG(min) as min_pg "
            "FROM player_game_logs_live WHERE min >= 10 "
            "GROUP BY player_name HAVING COUNT(*) >= 5"
        )
    try:
        import sqlite3
        conn = sqlite3.connect("dfs_nba.db")
        df = pd.read_sql_query(
            "SELECT player_name, AVG(fg3m) as fg3m_pg, AVG(min) as min_pg "
            "FROM player_game_logs WHERE min >= 10 "
            "GROUP BY player_name HAVING COUNT(*) >= 5", conn
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


def get_player_shot_zones():
    if use_postgres():
        return _pg_query(
            "SELECT player_name, rim_paint_pct, three_pct, corner3_fga, atb3_fga, three_fga "
            "FROM player_shot_zones_live"
        )
    try:
        import sqlite3
        conn = sqlite3.connect("dfs_nba.db")
        df = pd.read_sql_query(
            "SELECT player_name, rim_paint_pct, three_pct, corner3_fga, atb3_fga, three_fga "
            "FROM player_shot_zones", conn
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


def get_player_shot_creation():
    if use_postgres():
        return _pg_query("SELECT player_name, cs_pct, pu_pct FROM player_shot_creation_live")
    try:
        import sqlite3
        conn = sqlite3.connect("dfs_nba.db")
        df = pd.read_sql_query("SELECT player_name, cs_pct, pu_pct FROM player_shot_creation", conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


def get_player_hustle_stats():
    if use_postgres():
        return _pg_query(
            "SELECT player_name, deflections_per48, contested_per48 FROM player_hustle_stats_live"
        )
    try:
        import sqlite3
        conn = sqlite3.connect("dfs_nba.db")
        df = pd.read_sql_query(
            "SELECT player_name, deflections_per48, contested_per48 FROM player_hustle_stats", conn
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


def get_dva_data():
    if use_postgres():
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM dva_stats_live"))
                if result.scalar() == 0:
                    return None, None

                rows = conn.execute(text(
                    "SELECT opp_team, archetype, fp_pm, fp_pm_diff, sample_n, "
                    "pts_pm_diff, reb_pm_diff, ast_pm_diff, stl_pm_diff, blk_pm_diff, fg3m_pm_diff, tov_pm_diff, "
                    "dvs_multiplier, "
                    "pts_component, reb_component, ast_component, stl_component, blk_component, fg3m_component, tov_component "
                    "FROM dva_stats_live ORDER BY opp_team, archetype"
                )).fetchall()

                profiles = {}
                prof_result = conn.execute(text("SELECT * FROM archetype_profiles_live"))
                prof_cols = list(prof_result.keys())
                for r in prof_result.fetchall():
                    rd = dict(zip(prof_cols, r))
                    profiles[rd['archetype']] = {k: rd[k] for k in rd if k not in ('archetype', 'id', 'updated_at')}

                return rows, profiles
        except Exception:
            return None, None
    try:
        import sqlite3
        conn = sqlite3.connect("dfs_nba.db")
        cur = conn.cursor()
        tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        if 'dva_stats' not in tables:
            conn.close()
            return None, None

        rows = cur.execute(
            "SELECT opp_team, archetype, fp_pm, fp_pm_diff, sample_n, "
            "pts_pm_diff, reb_pm_diff, ast_pm_diff, stl_pm_diff, blk_pm_diff, fg3m_pm_diff, tov_pm_diff, "
            "dvs_multiplier, "
            "pts_component, reb_component, ast_component, stl_component, blk_component, fg3m_component, tov_component "
            "FROM dva_stats ORDER BY opp_team, archetype"
        ).fetchall()

        profiles = {}
        if 'archetype_profiles' in tables:
            prof_rows = cur.execute("SELECT * FROM archetype_profiles").fetchall()
            prof_cols = [d[0] for d in cur.description]
            for r in prof_rows:
                rd = dict(zip(prof_cols, r))
                profiles[rd['archetype']] = {k: rd[k] for k in rd if k != 'archetype'}

        conn.close()
        return rows, profiles
    except Exception:
        return None, None


def get_player_game_log(player_name, stat_col, n=20):
    if use_postgres():
        try:
            with engine.connect() as conn:
                cols_result = conn.execute(text(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'player_game_logs_live'"
                ))
                existing_cols = {row[0] for row in cols_result.fetchall()}
                if stat_col not in existing_cols:
                    return None, f"{stat_col.upper()} data not yet available."

                result = conn.execute(text(
                    f"SELECT game_date, matchup, {stat_col} FROM player_game_logs_live "
                    f"WHERE player_name = :name ORDER BY game_date DESC LIMIT :n"
                ), {"name": player_name, "n": n})
                rows = result.fetchall()
                return rows, None
        except Exception as e:
            return None, str(e)
    try:
        import sqlite3
        conn = sqlite3.connect("dfs_nba.db")
        existing_cols = [r[1] for r in conn.execute("PRAGMA table_info(player_game_logs)").fetchall()]
        if stat_col not in existing_cols:
            conn.close()
            return None, f"{stat_col.upper()} data not yet available."
        rows = conn.execute(
            f"SELECT game_date, matchup, {stat_col} FROM player_game_logs WHERE player_name = ? ORDER BY game_date DESC LIMIT ?",
            (player_name, n)
        ).fetchall()
        conn.close()
        return rows, None
    except Exception as e:
        return None, str(e)


def get_player_shot_zone_detail(player_name_key):
    if use_postgres():
        try:
            df = _pg_query("SELECT * FROM player_shot_zones_live")
            return df if not df.empty else None
        except Exception:
            return None
    try:
        import sqlite3
        conn = sqlite3.connect("dfs_nba.db")
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        if "player_shot_zones" not in tables:
            conn.close()
            return None
        df = pd.read_sql_query("SELECT * FROM player_shot_zones", conn)
        conn.close()
        return df if not df.empty else None
    except Exception:
        return None


def get_team_defense_shot_zone(team):
    if use_postgres():
        try:
            with engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT team, team_name, total_fga, ra_fga, ra_fgm, paint_fga, paint_fgm, "
                    "mid_fga, mid_fgm, corner3_fga, corner3_fgm, atb3_fga, atb3_fgm, "
                    "ra_freq, paint_freq, mid_freq, corner3_freq, atb3_freq, "
                    "ra_fg_pct, paint_fg_pct, mid_fg_pct, corner3_fg_pct, atb3_fg_pct "
                    "FROM team_defense_shot_zones_live WHERE team = :team"
                ), {"team": team.upper()})
                row = result.fetchone()

                all_rows = conn.execute(text(
                    "SELECT total_fga, ra_fga, ra_fgm, paint_fga, paint_fgm, mid_fga, mid_fgm, "
                    "corner3_fga, corner3_fgm, atb3_fga, atb3_fgm FROM team_defense_shot_zones_live"
                )).fetchall()
                return row, all_rows
        except Exception:
            return None, []
    try:
        import sqlite3
        conn = sqlite3.connect("dfs_nba.db")
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        if "team_defense_shot_zones" not in tables:
            conn.close()
            return None, []

        row = conn.execute(
            "SELECT team, team_name, total_fga, ra_fga, ra_fgm, paint_fga, paint_fgm, "
            "mid_fga, mid_fgm, corner3_fga, corner3_fgm, atb3_fga, atb3_fgm, "
            "ra_freq, paint_freq, mid_freq, corner3_freq, atb3_freq, "
            "ra_fg_pct, paint_fg_pct, mid_fg_pct, corner3_fg_pct, atb3_fg_pct "
            "FROM team_defense_shot_zones WHERE team = ?",
            (team.upper(),)
        ).fetchone()

        all_rows = conn.execute(
            "SELECT total_fga, ra_fga, ra_fgm, paint_fga, paint_fgm, mid_fga, mid_fgm, "
            "corner3_fga, corner3_fgm, atb3_fga, atb3_fgm FROM team_defense_shot_zones"
        ).fetchall()
        conn.close()
        return row, all_rows
    except Exception:
        return None, []


def get_team_defense_teams():
    if use_postgres():
        try:
            with engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT DISTINCT team FROM team_defense_shot_zones_live ORDER BY team"
                ))
                return [row[0] for row in result.fetchall()]
        except Exception:
            return []
    try:
        import sqlite3
        conn = sqlite3.connect("dfs_nba.db")
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        if "team_defense_shot_zones" not in tables:
            conn.close()
            return []
        teams = [r[0] for r in conn.execute("SELECT DISTINCT team FROM team_defense_shot_zones ORDER BY team").fetchall()]
        conn.close()
        return teams
    except Exception:
        return []


def get_team_play_types():
    if use_postgres():
        try:
            with engine.connect() as conn:
                count = conn.execute(text("SELECT COUNT(*) FROM team_play_types_live")).scalar()
                if count == 0:
                    return None, None
                conn2 = engine.connect()
                conn2 = conn2.execution_options(stream_results=False)

                off_result = conn.execute(text(
                    "SELECT team, play_type, play_type_label, poss_pct, ppp, fg_pct, tov_poss_pct, "
                    "score_poss_pct, efg_pct, percentile "
                    "FROM team_play_types_live WHERE type_grouping='Offensive' ORDER BY team, poss_pct DESC"
                ))
                off_rows = off_result.fetchall()
                off_cols = list(off_result.keys())

                def_result = conn.execute(text(
                    "SELECT team, play_type, play_type_label, poss_pct, ppp, fg_pct, tov_poss_pct, "
                    "score_poss_pct, efg_pct, percentile "
                    "FROM team_play_types_live WHERE type_grouping='Defensive' ORDER BY team, poss_pct DESC"
                ))
                def_rows = def_result.fetchall()

                return off_rows, def_rows
        except Exception:
            return None, None
    try:
        import sqlite3
        conn = sqlite3.connect("dfs_nba.db")
        conn.row_factory = sqlite3.Row
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        if "team_play_types" not in tables:
            conn.close()
            return None, None

        off_rows = conn.execute(
            "SELECT team, play_type, play_type_label, poss_pct, ppp, fg_pct, tov_poss_pct, "
            "score_poss_pct, efg_pct, percentile FROM team_play_types "
            "WHERE type_grouping='Offensive' ORDER BY team, poss_pct DESC"
        ).fetchall()

        def_rows = conn.execute(
            "SELECT team, play_type, play_type_label, poss_pct, ppp, fg_pct, tov_poss_pct, "
            "score_poss_pct, efg_pct, percentile FROM team_play_types "
            "WHERE type_grouping='Defensive' ORDER BY team, poss_pct DESC"
        ).fetchall()
        conn.close()
        return off_rows, def_rows
    except Exception:
        return None, None


def get_player_headshots():
    if use_postgres():
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT player_name, headshot_url FROM player_headshots_live"))
                return result.fetchall()
        except Exception:
            return []
    try:
        import sqlite3
        conn = sqlite3.connect("dfs_nba.db")
        cursor = conn.cursor()
        cursor.execute("SELECT player_name, headshot_url FROM player_headshots")
        rows = cursor.fetchall()
        conn.close()
        return rows
    except Exception:
        return []


def write_manual_injury(player_name, normalized_name, status="OUT", reason="Manual override"):
    if use_postgres():
        try:
            from sqlalchemy.orm import Session
            from backend.database import SessionLocal
            from backend import models
            session = SessionLocal()
            existing = session.query(models.InjuryAlertLive).filter(
                models.InjuryAlertLive.player_name == normalized_name
            ).first()
            if existing:
                existing.status = status
                existing.reason = reason
            else:
                new_injury = models.InjuryAlertLive(
                    player_name=normalized_name,
                    status=status,
                    reason=reason,
                    alert_title=f"Manual: {normalized_name} - {status}"
                )
                session.add(new_injury)
            session.commit()
            session.close()
            return True
        except Exception:
            return False

    try:
        import sqlite3
        from utils.timezone import get_eastern_now
        conn = sqlite3.connect("dfs_nba.db")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS manual_injuries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_name TEXT UNIQUE,
                status TEXT DEFAULT 'OUT',
                reason TEXT,
                added_at TEXT
            )
        """)
        conn.execute(
            "INSERT OR REPLACE INTO manual_injuries (player_name, status, reason, added_at) VALUES (?, ?, ?, ?)",
            (normalized_name, status, reason, get_eastern_now().isoformat())
        )
        conn.commit()
        conn.close()
        return True
    except Exception:
        return False
