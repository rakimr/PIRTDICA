import sqlite3
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from utils.timezone import get_eastern_now
from utils.nba_season import nba_api_season
import warnings
from utils.player_name_key import nba_player_name_key as _ascii_key
warnings.filterwarnings('ignore')


DB_PATH = 'dfs_nba.db'

HEIGHT_THRESHOLD_INCHES = 82
POINT_CENTER_AST_THRESHOLD = 5.0
POINT_CENTER_PTS_THRESHOLD = 24.0
BALL_INITIATION_TOUCHES_PER_MIN = 2.0

COMPOSITE_FEATURES = [
    'creation_idx', 'playmaking_idx', 'interior_idx', 'perimeter_idx',
    'offball_idx', 'rebound_idx', 'defense_idx', 'size_idx',
]

FINAL_ARCHETYPES = frozenset({
    '3-and-D Wing', 'Shooting Wing', 'Scoring Wing', 'Combo Guard',
    'Playmaker', 'Traditional Big', 'Versatile Big', 'Stretch 4',
    'Stretch 5', 'Point Forward', 'Point Center',
})


def validate_archetype_labels(df):
    """Reject synthetic cluster names before they become model categories."""
    if df.empty:
        raise ValueError("Refusing to save an empty archetype build")
    for column in ('archetype', 'base_archetype'):
        if column not in df.columns:
            raise ValueError(f"Missing required archetype column: {column}")
        invalid = ~df[column].isin(FINAL_ARCHETYPES)
        if invalid.any():
            labels = df.loc[invalid, column].astype(str).unique().tolist()
            raise ValueError(
                f"Refusing to save unsupported {column} labels: {labels}"
            )


def _collapse_feature_rows(frame, source_name):
    """Collapse alias rows, preserving complementary fields without averaging duplicates."""
    if frame.empty:
        return frame

    frame = frame.copy().reset_index(drop=True)
    blank_key = frame['_merge_key'].isna() | frame['_merge_key'].astype(str).str.strip().eq('')
    frame.loc[blank_key, '_merge_key'] = [
        f'__missing_player_{index}' for index in frame.index[blank_key]
    ]
    if not frame['_merge_key'].duplicated().any():
        return frame

    duplicate_keys = int(frame.loc[frame['_merge_key'].duplicated(False), '_merge_key'].nunique())
    duplicate_rows = int(frame['_merge_key'].duplicated().sum())

    if source_name == 'player_shot_creation':
        activity_cols = [
            'cs_fga', 'cs_fgm', 'cs_3fga', 'cs_3fgm',
            'pu_fga', 'pu_fgm', 'pu_3fga', 'pu_3fgm',
            'paint_fga', 'paint_fgm',
        ]
        activity_cols = [col for col in activity_cols if col in frame.columns]
        frame['_activity_score'] = (
            frame[activity_cols].apply(pd.to_numeric, errors='coerce').fillna(0).sum(axis=1)
            if activity_cols else 0
        )
        sort_cols = ['_activity_score']
        ascending = [True]
        if 'scraped_at' in frame.columns:
            sort_cols.append('scraped_at')
            ascending.append(True)
    elif source_name == 'player_per100' and 'total_minutes' in frame.columns:
        sort_cols = ['total_minutes']
        ascending = [True]
        if 'scraped_at' in frame.columns:
            sort_cols.append('scraped_at')
            ascending.append(True)
    else:
        if 'scraped_at' in frame.columns:
            sort_cols = ['scraped_at']
            ascending = [True]
        else:
            frame['_completeness'] = frame.notna().sum(axis=1)
            sort_cols = ['_completeness']
            ascending = [True]

    frame = frame.sort_values(
        sort_cols,
        ascending=ascending,
        na_position='first',
        kind='stable',
    )
    frame = frame.groupby('_merge_key', sort=False, dropna=False).last().reset_index()
    frame = frame.drop(columns=['_activity_score', '_completeness'], errors='ignore')

    print(
        f"  {source_name}: collapsed {duplicate_rows} extra row(s) "
        f"across {duplicate_keys} normalized player key(s)"
    )
    return frame.reset_index(drop=True)


def _aggregate_game_log_features(game_logs):
    """Collapse spelling variants and duplicate same-game logs before modeling."""
    if game_logs.empty:
        return pd.DataFrame(columns=[
            'player_name', '_merge_key', 'fg3m_pg', 'pts_pg', 'reb_pg',
            'ast_pg', 'min_pg', 'log_games',
        ])

    logs = game_logs.copy()
    if 'season_type' not in logs.columns:
        logs['season_type'] = 'REGULAR'
    logs['season_type'] = logs['season_type'].fillna('REGULAR').astype(str)
    logs['_merge_key'] = logs['player_name'].map(_ascii_key)
    event_key = ['_merge_key', 'game_date', 'season_type']

    blank_key = logs['_merge_key'].isna() | logs['_merge_key'].astype(str).str.strip().eq('')
    logs.loc[blank_key, '_merge_key'] = [
        f'__missing_player_{index}' for index in logs.index[blank_key]
    ]

    if 'scraped_at' in logs.columns:
        logs = logs.sort_values('scraped_at', na_position='first', kind='stable')
    before = len(logs)
    logs = logs.groupby(event_key, sort=False, dropna=False).last().reset_index()
    removed = before - len(logs)
    if removed:
        print(f"  Game logs: removed {removed} duplicate normalized player-game row(s)")

    result = logs.groupby('_merge_key', as_index=False, sort=False).agg(
        player_name=('player_name', 'first'),
        fg3m_pg=('fg3m', 'mean'),
        pts_pg=('pts', 'mean'),
        reb_pg=('reb', 'mean'),
        ast_pg=('ast', 'mean'),
        min_pg=('min', 'mean'),
        log_games=('game_date', 'size'),
    )
    return result[result['log_games'] >= 5].reset_index(drop=True)

TARGET_K = 6

MIN_MINUTES_FOR_CENTROID = 800


def fetch_player_heights():
    try:
        from nba_api.stats.endpoints import leaguedashplayerbiostats
        from utils.nba_api_helpers import nba_api_call_with_retry
        df = nba_api_call_with_retry(
            leaguedashplayerbiostats.LeagueDashPlayerBioStats,
            "player bio stats",
            season=nba_api_season()
        )
        if df is None:
            print("  WARNING: Could not fetch height data from NBA.com")
            return {}
        height_map = {}
        for _, row in df.iterrows():
            name = row.get('PLAYER_NAME', '')
            height_inches = row.get('PLAYER_HEIGHT_INCHES')
            if name and height_inches and not pd.isna(height_inches):
                key = _ascii_key(name)
                height_map[key] = int(height_inches)
        print(f"  Fetched height data for {len(height_map)} players from NBA API")
        return height_map
    except Exception as e:
        print(f"  WARNING: Could not fetch height data: {e}")
        return {}


def load_feature_data(feature_season=None):
    conn = sqlite3.connect(DB_PATH)
    from utils.archetype_inputs import season_year, validate_input_season
    columns = {row[1] for row in conn.execute("PRAGMA table_info(player_per100)")}
    if feature_season is not None or "source_season" in columns:
        try:
            if feature_season is None:
                seasons = [row[0] for row in conn.execute(
                    "SELECT DISTINCT source_season FROM player_per100 WHERE source_season IS NOT NULL")]
                if len(seasons) != 1:
                    raise ValueError("Select one explicit archetype feature season")
                feature_season = seasons[0]
            validate_input_season(conn, feature_season)
        except Exception:
            conn.close()
            raise

    def read_performance(query):
        if feature_season is None:
            return pd.read_sql_query(query, conn)
        conjunction = " AND " if "WHERE" in query.upper() else " WHERE "
        return pd.read_sql_query(
            query.rstrip() + conjunction + "source_season=? AND season_type=?",
            conn, params=(feature_season, "Regular Season"))

    per100 = read_performance("""
        SELECT player_name, team, games_played, total_minutes, mpg,
               pts_per100, reb_per100, ast_per100, stl_per100, blk_per100, tov_per100
        FROM player_per100
        WHERE games_played >= 10 AND mpg >= 12
    """)

    positions = read_performance("""
        SELECT player_name, true_position, pg_pct, sg_pct, sf_pct, pf_pct, c_pct
        FROM player_positions
    """)

    game_log_columns = {
        row[1] for row in conn.execute("PRAGMA table_info(player_game_logs)").fetchall()
    }
    game_log_select = [
        'player_name', 'game_date', 'min', 'pts', 'reb', 'ast', 'fg3m',
    ]
    for optional_column in ('season_type', 'scraped_at'):
        if optional_column in game_log_columns:
            game_log_select.append(optional_column)
    game_query = f"SELECT {', '.join(game_log_select)} FROM player_game_logs WHERE min >= 10"
    game_params = ()
    if feature_season is not None:
        if "season_type" not in game_log_columns:
            conn.close()
            raise ValueError("Season-specific archetypes require verified game-log season types")
        start = season_year(feature_season)
        game_query += (
            " AND game_date >= ? AND game_date < ?"
            " AND UPPER(REPLACE(REPLACE(season_type, ' ', ''), '_', ''))"
            " IN ('REGULAR', 'REGULARSEASON')")
        game_params = (f"{start}-10-01", f"{start + 1}-10-01")
    game_log_rows = pd.read_sql_query(game_query, conn, params=game_params)
    game_logs = _aggregate_game_log_features(game_log_rows)

    usage = read_performance("""
        SELECT player_name, usg_pct
        FROM player_stats
    """)

    shot_zones = read_performance("""
        SELECT player_name, rim_paint_pct, three_pct, ra_pct, paint_pct, mid_pct,
               corner3_fga, atb3_fga, three_fga
        FROM player_shot_zones
    """)

    shot_creation = read_performance("""
        SELECT player_name, cs_pct, pu_pct, paint_pct as sc_paint_pct, cs_3_share,
               pu_3_share
        FROM player_shot_creation
    """)

    hustle = read_performance("""
        SELECT player_name, deflections_per48, contested_per48, loose_per48,
               charges_per48, screen_ast_per48, box_outs_per48
        FROM player_hustle_stats
    """)

    tracking = read_performance("""
        SELECT player_name, touches_pg, front_ct_touches_pg, time_of_poss_pg,
               avg_sec_per_touch, avg_drib_per_touch, touches_per_min, front_ct_per_min,
               post_touches_pg, paint_touches_pg
        FROM player_tracking_stats
    """)

    measurements = pd.read_sql_query("""
        SELECT player_name, height_inches, weight_lbs, wingspan_inches
        FROM player_measurements
    """, conn)

    conn.close()

    feature_frames = {
        'player_per100': per100,
        'player_positions': positions,
        'player_stats': usage,
        'player_shot_zones': shot_zones,
        'player_shot_creation': shot_creation,
        'player_hustle_stats': hustle,
        'player_tracking_stats': tracking,
        'player_measurements': measurements,
    }
    for source_name, frame in feature_frames.items():
        frame['_merge_key'] = frame['player_name'].map(_ascii_key)
        feature_frames[source_name] = _collapse_feature_rows(frame, source_name)

    per100 = feature_frames['player_per100']
    positions = feature_frames['player_positions']
    usage = feature_frames['player_stats']
    shot_zones = feature_frames['player_shot_zones']
    shot_creation = feature_frames['player_shot_creation']
    hustle = feature_frames['player_hustle_stats']
    tracking = feature_frames['player_tracking_stats']
    measurements = feature_frames['player_measurements']

    df = per100.merge(
        positions.drop(columns=['player_name']), on='_merge_key',
        how='inner', validate='one_to_one',
    )
    df = df.merge(
        game_logs.drop(columns=['player_name']), on='_merge_key',
        how='inner', validate='one_to_one',
    )
    df = df.merge(
        usage.drop(columns=['player_name']), on='_merge_key',
        how='left', validate='one_to_one',
    )
    df = df.merge(
        shot_zones.drop(columns=['player_name']), on='_merge_key',
        how='left', validate='one_to_one',
    )
    df = df.merge(
        shot_creation.drop(columns=['player_name']), on='_merge_key',
        how='left', validate='one_to_one',
    )
    df = df.merge(
        hustle.drop(columns=['player_name']), on='_merge_key',
        how='left', validate='one_to_one',
    )
    df = df.merge(
        tracking.drop(columns=['player_name']), on='_merge_key',
        how='left', validate='one_to_one',
    )
    df = df.merge(
        measurements.drop(columns=['player_name']), on='_merge_key',
        how='left', validate='one_to_one',
    )
    if df['_merge_key'].duplicated().any():
        duplicate_keys = df.loc[df['_merge_key'].duplicated(False), '_merge_key'].unique()
        raise ValueError(
            f"Archetype feature joins produced duplicate normalized players: "
            f"{list(duplicate_keys[:10])}"
        )
    df = df.drop(columns=['_merge_key'])

    df['usg_pct'] = df['usg_pct'].fillna(df['usg_pct'].median())

    df['rim_paint_pct'] = df['rim_paint_pct'].fillna(50.0)
    df['three_pct'] = df['three_pct'].fillna(25.0)
    df['ra_pct'] = df['ra_pct'].fillna(25.0)
    df['paint_pct'] = df['paint_pct'].fillna(15.0)
    df['mid_pct'] = df['mid_pct'].fillna(15.0)
    df['cs_pct'] = df['cs_pct'].fillna(30.0)
    df['pu_pct'] = df['pu_pct'].fillna(15.0)
    df['sc_paint_pct'] = df['sc_paint_pct'].fillna(40.0)
    df['cs_3_share'] = df['cs_3_share'].fillna(50.0)
    df['pu_3_share'] = df['pu_3_share'].fillna(30.0)

    df['corner3_pct_of_3'] = np.where(
        df['three_fga'].fillna(0) > 0,
        df['corner3_fga'].fillna(0) / df['three_fga'] * 100,
        35.0
    )
    df['corner3_pct_of_3'] = df['corner3_pct_of_3'].fillna(35.0)

    df['deflections_per48'] = df['deflections_per48'].fillna(df['deflections_per48'].median() if df['deflections_per48'].notna().any() else 3.0)
    df['contested_per48'] = df['contested_per48'].fillna(df['contested_per48'].median() if df['contested_per48'].notna().any() else 8.0)
    df['loose_per48'] = df['loose_per48'].fillna(1.0)
    df['charges_per48'] = df['charges_per48'].fillna(0.2)
    df['screen_ast_per48'] = df['screen_ast_per48'].fillna(1.0)
    df['box_outs_per48'] = df['box_outs_per48'].fillna(2.0)

    df['touches_per_min'] = df['touches_per_min'].fillna(1.5)
    df['front_ct_per_min'] = df['front_ct_per_min'].fillna(0.8)
    df['avg_sec_per_touch'] = df['avg_sec_per_touch'].fillna(2.5)
    df['avg_drib_per_touch'] = df['avg_drib_per_touch'].fillna(1.5)
    df['touches_pg'] = df['touches_pg'].fillna(40.0)
    df['front_ct_touches_pg'] = df['front_ct_touches_pg'].fillna(20.0)
    df['time_of_poss_pg'] = df['time_of_poss_pg'].fillna(2.0)
    df['post_touches_pg'] = df['post_touches_pg'].fillna(1.0)
    df['paint_touches_pg'] = df['paint_touches_pg'].fillna(3.0)

    df['height_inches'] = df['height_inches'].fillna(df['height_inches'].median() if df['height_inches'].notna().any() else 79)
    df['weight_lbs'] = df['weight_lbs'].fillna(df['weight_lbs'].median() if df['weight_lbs'].notna().any() else 215)
    df['wingspan_inches'] = df['wingspan_inches'].fillna(df['wingspan_inches'].median() if df['wingspan_inches'].notna().any() else 82)

    shot_merged = df['rim_paint_pct'].notna().sum()
    hustle_merged = df['deflections_per48'].notna().sum()
    tracking_merged = df['touches_pg'].notna().sum()
    size_merged = (df['height_inches'] != df['height_inches'].median()).sum() if df['height_inches'].notna().any() else 0
    print(f"  Shot zone data merged for {shot_merged}/{len(df)} players")
    print(f"  Hustle stats merged for {hustle_merged}/{len(df)} players")
    print(f"  Tracking stats merged for {tracking_merged}/{len(df)} players")
    print(f"  Measurements merged for {size_merged}/{len(df)} players")

    return df


def build_composite_indices(df):
    scaler = StandardScaler()

    raw_features = [
        'usg_pct', 'pu_pct', 'avg_sec_per_touch', 'avg_drib_per_touch',
        'ast_per100', 'touches_per_min',
        'rim_paint_pct', 'post_touches_pg', 'paint_touches_pg',
        'three_pct', 'cs_pct', 'pu_3_share',
        'cs_3_share', 'time_of_poss_pg',
        'reb_per100', 'box_outs_per48',
        'stl_per100', 'blk_per100', 'deflections_per48', 'contested_per48',
        'height_inches', 'weight_lbs', 'wingspan_inches',
    ]

    for col in raw_features:
        df[col] = df[col].fillna(0)

    z_df = pd.DataFrame(
        scaler.fit_transform(df[raw_features]),
        columns=[f'z_{c}' for c in raw_features],
        index=df.index
    )

    df['creation_idx'] = (
        z_df['z_usg_pct'] +
        z_df['z_pu_pct'] +
        z_df['z_avg_sec_per_touch'] +
        z_df['z_avg_drib_per_touch']
    )

    df['playmaking_idx'] = (
        z_df['z_ast_per100'] +
        z_df['z_touches_per_min']
    )

    df['interior_idx'] = (
        z_df['z_rim_paint_pct'] +
        z_df['z_post_touches_pg'] +
        z_df['z_paint_touches_pg']
    )

    df['perimeter_idx'] = (
        z_df['z_three_pct'] +
        z_df['z_cs_pct'] +
        z_df['z_pu_3_share']
    )

    df['offball_idx'] = (
        z_df['z_cs_3_share'] -
        z_df['z_time_of_poss_pg']
    )

    df['rebound_idx'] = (
        z_df['z_reb_per100'] +
        z_df['z_box_outs_per48']
    )

    df['defense_idx'] = (
        z_df['z_stl_per100'] +
        z_df['z_blk_per100'] +
        z_df['z_deflections_per48'] +
        z_df['z_contested_per48']
    )

    df['size_idx'] = (
        z_df['z_height_inches'] +
        z_df['z_weight_lbs'] +
        z_df['z_wingspan_inches']
    )

    corr = df[COMPOSITE_FEATURES].corr()
    print("\n  Composite index correlation matrix:")
    print(f"  {'':>16}", end='')
    for c in COMPOSITE_FEATURES:
        print(f" {c[:8]:>8}", end='')
    print()
    for i, row_name in enumerate(COMPOSITE_FEATURES):
        print(f"  {row_name:>16}", end='')
        for j, col_name in enumerate(COMPOSITE_FEATURES):
            val = corr.iloc[i, j]
            marker = '*' if abs(val) > 0.5 and i != j else ' '
            print(f" {val:>7.2f}{marker}", end='')
        print()

    high_corr = []
    for i in range(len(COMPOSITE_FEATURES)):
        for j in range(i+1, len(COMPOSITE_FEATURES)):
            r = abs(corr.iloc[i, j])
            if r > 0.5:
                high_corr.append((COMPOSITE_FEATURES[i], COMPOSITE_FEATURES[j], corr.iloc[i, j]))
    if high_corr:
        print(f"\n  WARNING: {len(high_corr)} feature pair(s) with |r| > 0.5:")
        for a, b, r in high_corr:
            print(f"    {a} <-> {b}: r={r:.3f}")
    else:
        print("\n  All composite indices have |r| < 0.5 — good orthogonality")

    return df, scaler


def label_cluster_scored(centroid, feature_names):
    c = dict(zip(feature_names, centroid))

    creation = c.get('creation_idx', 0)
    playmaking = c.get('playmaking_idx', 0)
    interior = c.get('interior_idx', 0)
    perimeter = c.get('perimeter_idx', 0)
    offball = c.get('offball_idx', 0)
    rebound = c.get('rebound_idx', 0)
    defense = c.get('defense_idx', 0)
    size = c.get('size_idx', 0)

    if size > 1.0 and interior > 0.5:
        if perimeter > 0.5:
            return 'Stretch Big'
        if playmaking > 0.5:
            return 'Versatile Big'
        return 'Traditional Big'

    if size > 0.5 and rebound > 0.5:
        if perimeter > 0.3:
            return 'Stretch Big'
        return 'Traditional Big'

    if playmaking > 1.0 and creation > 0.5:
        return 'Playmaker'

    if creation > 0.5 and defense < 0.0 and offball < 0.0:
        return 'Scoring Wing'

    if creation > 0.3 and perimeter > -0.5 and playmaking < 0.5:
        return 'Scoring Wing'

    if defense > 0.8 and perimeter > -0.5:
        return '3-and-D Wing'

    if offball > 0.5 and defense > 0.3:
        return '3-and-D Wing'

    if creation > 0.3 and playmaking > 0.3:
        return 'Combo Guard'

    if perimeter > 0.5 and offball > 0.0:
        # A shooting/off-ball cluster without the defensive signal is not 3-and-D.
        # This distinction must not depend on which cluster K-Means numbers first.
        return 'Shooting Wing' if defense < 0 else '3-and-D Wing'

    if perimeter > 0 and defense < -1.0:
        return 'Scoring Wing'

    return 'Combo Guard'


def refine_facilitating_wings(df):
    """Apply the existing Point Forward criteria independently of scoring usage."""
    wing_roles = {'Scoring Wing', 'Shooting Wing', '3-and-D Wing',
                  'Playmaker', 'Combo Guard'}
    forward_share = df['sf_pct'] + df['pf_pct']
    guard_share = df['pg_pct'] + df['sg_pct']
    eligible = (
        df['archetype'].isin(wing_roles)
        & (forward_share > guard_share)
        & (df['c_pct'] < 50)
        & (df['ast_per100'] >= 7.5)
        & (df['touches_per_min'] >= BALL_INITIATION_TOUCHES_PER_MIN)
    )
    df.loc[eligible, 'archetype'] = 'Point Forward'
    return df


def run_clustering(df, k=TARGET_K, *, height_map=None):
    feature_df = df[COMPOSITE_FEATURES].copy()
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(feature_df)

    high_min_mask = df['total_minutes'] >= MIN_MINUTES_FOR_CENTROID
    X_high_min = X_scaled[high_min_mask.values]

    print(f"  Centroid training on {high_min_mask.sum()} players with {MIN_MINUTES_FOR_CENTROID}+ minutes")
    print(f"  Full assignment for all {len(df)} players")

    print(f"\n  Using k={k} with {len(COMPOSITE_FEATURES)} composite features")
    print(f"  Features: {COMPOSITE_FEATURES}")
    print("\n  Silhouette scores (high-minute players):")
    for test_k in range(5, 12):
        km_test = KMeans(n_clusters=test_k, n_init=20, random_state=42, max_iter=300)
        labels_test = km_test.fit_predict(X_high_min)
        score = silhouette_score(X_high_min, labels_test)
        marker = " <-- chosen" if test_k == k else ""
        print(f"    k={test_k}: silhouette={score:.4f}{marker}")

    km = KMeans(n_clusters=k, n_init=30, random_state=42, max_iter=500)
    km.fit(X_high_min)

    df = df.copy()
    df['cluster'] = km.predict(X_scaled)

    distances = km.transform(X_scaled)
    inv_dist = 1.0 / (distances + 1e-8)
    cluster_probs = inv_dist / inv_dist.sum(axis=1, keepdims=True)

    for i in range(k):
        df[f'cluster_{i}_prob'] = cluster_probs[:, i]

    centroids_orig = scaler.inverse_transform(km.cluster_centers_)

    # Cluster IDs identify statistical groups, not additional basketball roles.
    # Multiple clusters may share a canonical role. Invented suffixes bypassed
    # the exact-label refinement rules below and fragmented the downstream DVA.
    cluster_labels = {
        i: label_cluster_scored(centroids_orig[i], COMPOSITE_FEATURES)
        for i in range(k)
    }

    print(f"\n  Cluster centroids (composite indices):")
    for i in range(k):
        c = dict(zip(COMPOSITE_FEATURES, centroids_orig[i]))
        n_total = len(df[df['cluster'] == i])
        n_high = high_min_mask[df['cluster'] == i].sum()
        print(f"    Cluster {i} [{cluster_labels[i]}] (n={n_total}, {n_high} high-min): "
              f"CRE={c['creation_idx']:.2f} PLY={c['playmaking_idx']:.2f} "
              f"INT={c['interior_idx']:.2f} PER={c['perimeter_idx']:.2f} "
              f"OFF={c['offball_idx']:.2f} REB={c['rebound_idx']:.2f} "
              f"DEF={c['defense_idx']:.2f} SIZ={c['size_idx']:.2f}")

    df['archetype'] = df['cluster'].map(cluster_labels)

    print("\n  Shot-zone-based big man reclassification...")
    big_archetypes = ['Traditional Big', 'Stretch Big', 'Traditional Big (Defensive)',
                      'Traditional Big (Role)', 'Stretch Big (Offensive)', 'Stretch Big (Role)',
                      'Traditional Big (Offensive)', 'Stretch Big (Defensive)']
    big_mask = df['archetype'].isin(big_archetypes)

    stretch_from_trad = 0
    trad_from_stretch = 0
    for idx in df[big_mask].index:
        player = df.loc[idx]
        current = player['archetype']
        rp = player.get('rim_paint_pct', 50)
        tp = player.get('three_pct', 25)
        csp = player.get('cs_pct', 30)
        fg3m = player.get('fg3m_pg', 0) / max(player.get('min_pg', 1), 1) * 100

        if 'Traditional' in current and tp >= 30 and fg3m >= 3.0:
            df.at[idx, 'archetype'] = 'Stretch Big'
            stretch_from_trad += 1
        elif 'Stretch' in current and rp >= 80 and tp < 15:
            df.at[idx, 'archetype'] = 'Traditional Big'
            trad_from_stretch += 1

    if stretch_from_trad or trad_from_stretch:
        print(f"    Shot zones: {stretch_from_trad} Traditional -> Stretch, {trad_from_stretch} Stretch -> Traditional")

    print("\n  Reclassifying facilitating bigs (Point Center / Point Forward)...")
    print(f"    Ball initiation gate: touches/min >= {BALL_INITIATION_TOUCHES_PER_MIN}")
    all_big_labels = ['Traditional Big', 'Stretch Big', 'Versatile Big']
    reclass_mask = df['archetype'].isin(all_big_labels)
    pc_count = 0
    pf_count = 0
    vb_count = 0
    initiation_blocked = 0
    for idx in df[reclass_mask].index:
        player = df.loc[idx]
        ast = player.get('ast_per100', 0)
        pts = player.get('pts_per100', 0)
        c_pct = player.get('c_pct', 0)
        tpm = player.get('touches_per_min', 1.5)

        if ast >= POINT_CENTER_AST_THRESHOLD and pts >= POINT_CENTER_PTS_THRESHOLD:
            if tpm < BALL_INITIATION_TOUCHES_PER_MIN:
                initiation_blocked += 1
                print(f"    {player['player_name']}: INITIATION GATE BLOCKED - stays {player['archetype']} "
                      f"(AST/100={ast:.1f}, PTS/100={pts:.1f}, T/Min={tpm:.3f} < {BALL_INITIATION_TOUCHES_PER_MIN})")
                continue
            if c_pct >= 50:
                df.at[idx, 'archetype'] = 'Point Center'
                pc_count += 1
                print(f"    {player['player_name']}: {player['archetype']} -> Point Center "
                      f"(AST/100={ast:.1f}, PTS/100={pts:.1f}, C%={c_pct:.0f}, T/Min={tpm:.3f})")
            else:
                df.at[idx, 'archetype'] = 'Point Forward'
                pf_count += 1
                print(f"    {player['player_name']}: {player['archetype']} -> Point Forward "
                      f"(AST/100={ast:.1f}, PTS/100={pts:.1f}, C%={c_pct:.0f}, T/Min={tpm:.3f})")
        elif ast >= POINT_CENTER_AST_THRESHOLD and pts >= 18.0:
            if player['archetype'] == 'Traditional Big':
                df.at[idx, 'archetype'] = 'Versatile Big'
                vb_count += 1
                print(f"    {player['player_name']}: Traditional Big -> Versatile Big "
                      f"(AST/100={ast:.1f}, PTS/100={pts:.1f})")

    print(f"  Facilitators: {pc_count} Point Center, {pf_count} Point Forward, {vb_count} -> Versatile Big, {initiation_blocked} blocked by initiation gate")

    print("\n  Position-based reclassification for frontcourt players in guard/wing archetypes...")
    non_big_archetypes = ['Scoring Wing', 'Scoring Guard', '3-and-D Wing', '3-and-D Guard',
                          'Shooting Wing', 'Scoring Wing (Offensive)', 'Scoring Wing (Role)',
                          'Combo Guard', 'Playmaker',
                          'Combo Guard (Offensive)', 'Combo Guard (Defensive)', 'Combo Guard (Role)']

    clear_big_mask = (
        df['archetype'].isin(non_big_archetypes) &
        ((df['c_pct'] + df['pf_pct']) >= 70)
    )

    tweener_big_mask = (
        df['archetype'].isin(non_big_archetypes) &
        ((df['c_pct'] + df['pf_pct']) >= 50) &
        ((df['c_pct'] + df['pf_pct']) < 70) &
        ((df['c_pct'] >= 15) | (df['reb_per100'] >= 9.0))
    )

    if height_map is None:
        height_map = fetch_player_heights()
    if height_map:
        df['_mk'] = df['player_name'].apply(_ascii_key)
        df['_height_check'] = df['_mk'].map(height_map)
        tall_borderline_mask = (
            df['archetype'].isin(non_big_archetypes) &
            df['_height_check'].notna() &
            (df['_height_check'] >= HEIGHT_THRESHOLD_INCHES) &
            ((df['c_pct'] + df['pf_pct']) >= 40) &
            ((df['c_pct'] + df['pf_pct']) < 50)
        )
        combined_mask = clear_big_mask | tweener_big_mask | tall_borderline_mask
        df = df.drop(columns=['_mk', '_height_check'])
    else:
        combined_mask = clear_big_mask | tweener_big_mask

    def _route_to_big(player):
        ast = player.get('ast_per100', 0)
        pts = player.get('pts_per100', 0)
        c_pct = player.get('c_pct', 0)
        rp = player.get('rim_paint_pct', 50)
        tp = player.get('three_pct', 25)
        csp = player.get('cs_pct', 30)

        tpm = player.get('touches_per_min', 1.5)
        if ast >= POINT_CENTER_AST_THRESHOLD and pts >= POINT_CENTER_PTS_THRESHOLD and tpm >= BALL_INITIATION_TOUCHES_PER_MIN:
            if c_pct >= 50:
                return 'Point Center'
            else:
                return 'Point Forward'
        elif ast >= POINT_CENTER_AST_THRESHOLD:
            return 'Versatile Big'
        elif rp >= 75 and tp < 15:
            return 'Traditional Big'
        elif tp >= 30 and csp >= 30:
            return 'Stretch Big'
        else:
            return 'Versatile Big'

    reclass_counts = {}
    wing_escape_count = 0
    for idx in df[combined_mask].index:
        player = df.loc[idx]
        old_arch = player['archetype']
        c_pct = player.get('c_pct', 0)
        pf_pct = player.get('pf_pct', 0)
        pu = player.get('pu_pct', 0)
        reb = player.get('reb_per100', 0)

        if c_pct < 10 and pu >= 20 and reb < 8:
            wing_escape_count += 1
            print(f"    {player['player_name']} (C%={c_pct:.0f} PF%={pf_pct:.0f}): "
                  f"WING ESCAPE - stays {old_arch} (PU={pu:.0f}% REB={reb:.1f})")
            continue

        new_arch = _route_to_big(player)
        df.at[idx, 'archetype'] = new_arch
        reclass_counts[new_arch] = reclass_counts.get(new_arch, 0) + 1
        rp = player.get('rim_paint_pct', 50)
        tp = player.get('three_pct', 25)
        csp = player.get('cs_pct', 30)
        print(f"    {player['player_name']} (C%={c_pct:.0f} PF%={pf_pct:.0f}): "
              f"{old_arch} -> {new_arch} (RimPaint={rp:.0f}% 3PT={tp:.0f}% C&S={csp:.0f}%)")

    total = sum(reclass_counts.values())
    parts = ', '.join(f"{v} {k}" for k, v in sorted(reclass_counts.items()))
    print(f"  Position reclass: {total} players ({parts}), {wing_escape_count} wing escapes")

    print("\n  Final Versatile Big shot-zone refinement...")
    vb_mask = df['archetype'] == 'Versatile Big'
    stretch_fix = 0
    trad_fix = 0
    for idx in df[vb_mask].index:
        player = df.loc[idx]
        rp = player.get('rim_paint_pct', 50)
        tp = player.get('three_pct', 25)
        csp = player.get('cs_pct', 30)
        fg3m = player.get('fg3m_pg', 0) / max(player.get('min_pg', 1), 1) * 100
        ast = player.get('ast_per100', 0)

        if ast >= POINT_CENTER_AST_THRESHOLD:
            continue

        if tp >= 40 and csp >= 35 and fg3m >= 4.0:
            df.at[idx, 'archetype'] = 'Stretch Big'
            stretch_fix += 1
            print(f"    {player['player_name']}: Versatile Big -> Stretch Big "
                  f"(3PT%={tp:.1f}, C&S%={csp:.1f}, 3PM/100={fg3m:.1f})")
        elif rp >= 85 and tp < 10:
            df.at[idx, 'archetype'] = 'Traditional Big'
            trad_fix += 1
            print(f"    {player['player_name']}: Versatile Big -> Traditional Big "
                  f"(RimPaint={rp:.1f}%, 3PT%={tp:.1f})")

    if stretch_fix or trad_fix:
        print(f"  VB refinement: {stretch_fix} -> Stretch, {trad_fix} -> Traditional")

    print("\n  Position/size guardrail: reclassifying guards/wings misclassified as bigs...")
    big_labels = ['Traditional Big', 'Versatile Big', 'Stretch Big']
    guard_wing_in_big_mask = (
        df['archetype'].isin(big_labels) &
        (df['true_position'].isin(['PG', 'SG', 'SF', 'PG-SG', 'SG-SF', 'SG-PG'])) &
        (df['size_idx'] < 1.0) &
        ((df.get('c_pct', pd.Series(0, index=df.index)) if 'c_pct' in df.columns else pd.Series(0, index=df.index)) < 15)
    )
    guard_wing_escape = 0
    for idx in df[guard_wing_in_big_mask].index:
        player = df.loc[idx]
        old_arch = player['archetype']
        creation = player.get('creation_idx', 0)
        perimeter = player.get('perimeter_idx', 0)
        defense = player.get('defense_idx', 0)
        interior = player.get('interior_idx', 0)
        playmaking = player.get('playmaking_idx', 0)
        pg_pct = player.get('pg_pct', 0)
        sg_pct = player.get('sg_pct', 0)
        sf_pct = player.get('sf_pct', 0)

        if defense >= 2.0 and perimeter > -1.0:
            new_arch = '3-and-D Wing'
        elif defense >= 2.0 and perimeter <= -1.0:
            new_arch = 'Scoring Wing'
        elif creation >= 1.5 or playmaking >= 1.5:
            if (pg_pct + sg_pct) >= 70:
                new_arch = 'Combo Guard'
            else:
                new_arch = 'Scoring Wing'
        elif perimeter >= 0.5:
            new_arch = 'Shooting Wing'
        elif interior >= 1.5:
            new_arch = 'Scoring Wing'
        else:
            new_arch = '3-and-D Wing'

        df.at[idx, 'archetype'] = new_arch
        guard_wing_escape += 1
        print(f"    {player['player_name']} ({player['true_position']}, size={player['size_idx']:.2f}): "
              f"{old_arch} -> {new_arch} "
              f"(CRE={creation:.2f} PER={perimeter:.2f} DEF={defense:.2f} INT={interior:.2f})")

    print(f"  Guard/wing guardrail: {guard_wing_escape} players reclassified out of big archetypes")

    print("\n  Hybrid branch routing for transcendent players...")
    elite_mask = (
        (df['pts_per100'] >= 30.0) &
        (df['ast_per100'] >= 7.0) &
        (df['usg_pct'] >= 26.0)
    )

    hybrid_routed = 0
    hybrid_routed_indices = set()
    for idx in df[elite_mask].index:
        player = df.loc[idx]
        old_arch = player['archetype']
        pts = player['pts_per100']
        ast = player['ast_per100']
        reb = player['reb_per100']
        usg = player['usg_pct']
        c_pct = player.get('c_pct', 0)
        pf_pct = player.get('pf_pct', 0)
        sf_pct = player.get('sf_pct', 0)
        guard_pct = player.get('pg_pct', 0) + player.get('sg_pct', 0)
        rp = player.get('rim_paint_pct', 50)
        tp = player.get('three_pct', 25)
        csp = player.get('cs_pct', 30)

        tpm = player.get('touches_per_min', 1.5)
        if (c_pct + pf_pct) >= 50:
            if ast >= POINT_CENTER_AST_THRESHOLD and c_pct >= 50 and tpm >= BALL_INITIATION_TOUCHES_PER_MIN:
                new_arch = 'Point Center'
            elif ast >= POINT_CENTER_AST_THRESHOLD and tpm >= BALL_INITIATION_TOUCHES_PER_MIN:
                new_arch = 'Point Forward'
            elif tp >= 30 and csp >= 30:
                new_arch = 'Stretch Big'
            else:
                new_arch = 'Versatile Big'
        elif (sf_pct + pf_pct) > guard_pct and ast >= 7.5 and tpm >= BALL_INITIATION_TOUCHES_PER_MIN:
            new_arch = 'Point Forward'
        elif player.get('pg_pct', 0) >= 70 and ast >= 10.0:
            new_arch = 'Playmaker'
        else:
            new_arch = 'Combo Guard'

        if new_arch != old_arch:
            df.at[idx, 'archetype'] = new_arch
            hybrid_routed += 1
            hybrid_routed_indices.add(idx)
            print(f"    {player['player_name']}: {old_arch} -> {new_arch} "
                  f"(PTS={pts:.1f} AST={ast:.1f} REB={reb:.1f} USG={usg:.1f} G%={guard_pct:.0f} F%={sf_pct+pf_pct:.0f})")
        else:
            hybrid_routed_indices.add(idx)

    print(f"  Hybrid branch routing: {hybrid_routed} players rerouted")

    previous_forwards = int(df['archetype'].eq('Point Forward').sum())
    df = refine_facilitating_wings(df)
    new_forwards = int(df['archetype'].eq('Point Forward').sum()) - previous_forwards
    print(f"  Facilitating wings outside scoring/usage gates: {new_forwards} -> Point Forward")

    print("\n  Guard playmaker reclassification (facilitator-first guards in Combo Guard)...")
    playmaker_reclass = 0
    combo_guard_variants = [a for a in df['archetype'].unique() if 'Combo Guard' in a]
    combo_guard_mask = df['archetype'].isin(combo_guard_variants)
    for idx in df[combo_guard_mask].index:
        if idx in hybrid_routed_indices:
            continue
        player = df.loc[idx]
        ast = player.get('ast_per100', 0)
        pg_pct = player.get('pg_pct', 0)
        guard_pct = player.get('pg_pct', 0) + player.get('sg_pct', 0)
        is_playmaker = False
        if pg_pct >= 70 and ast >= 6.0:
            is_playmaker = True
        elif guard_pct > 50 and ast >= 8.0:
            is_playmaker = True
        if is_playmaker:
            df.at[idx, 'archetype'] = 'Playmaker'
            playmaker_reclass += 1
            print(f"    {player['player_name']}: Combo Guard -> Playmaker "
                  f"(AST/100={ast:.1f}, PG%={pg_pct:.0f}, G%={guard_pct:.0f})")
    print(f"  Playmaker reclassification: {playmaker_reclass} guards reclassified")

    print("\n  Splitting Stretch Big into Stretch 4 / Stretch 5...")
    stretch_mask = df['archetype'] == 'Stretch Big'
    s4_count = 0
    s5_count = 0
    for idx in df[stretch_mask].index:
        player = df.loc[idx]
        c_pct = player.get('c_pct', 0)
        if c_pct >= 50:
            df.at[idx, 'archetype'] = 'Stretch 5'
            s5_count += 1
        else:
            df.at[idx, 'archetype'] = 'Stretch 4'
            s4_count += 1
    print(f"  Stretch split: {s4_count} Stretch 4, {s5_count} Stretch 5")

    df['base_archetype'] = df['archetype'].copy()

    return df, km, scaler, cluster_labels


def validate_archetypes(df):
    known_players = {
        'Karl-Anthony Towns': 'Stretch 5',
        'Rudy Gobert': 'Traditional Big',
        'Anthony Davis': 'Traditional Big',
        'James Harden': 'Combo Guard',
        'Kevin Durant': 'Scoring Wing',
        'Kawhi Leonard': 'Scoring Wing',
        'Victor Wembanyama': 'Stretch 5',
        'Draymond Green': 'Versatile Big',
        'Trae Young': 'Playmaker',
        'Jalen Brunson': 'Combo Guard',
        'Jaylen Brown': 'Combo Guard',
        'Donovan Mitchell': 'Combo Guard',
        'Norman Powell': 'Combo Guard',
        'Myles Turner': 'Stretch 5',
        'Brook Lopez': 'Stretch 5',
        'Domantas Sabonis': 'Point Center',
        'Mikal Bridges': '3-and-D Wing',
        'Klay Thompson': 'Combo Guard',
        'Julius Randle': 'Versatile Big',
        'Paolo Banchero': 'Point Forward',
        'Franz Wagner': 'Point Forward',
        'Jabari Smith': 'Stretch 4',
        'Lauri Markka': 'Stretch 4',
        'Alperen': 'Point Center',
        'Stephen Curry': 'Combo Guard',
        'Luka Don': 'Playmaker',
        'Shai Gilgeous': 'Combo Guard',
        'LaMelo Ball': 'Playmaker',
        'LeBron James': 'Point Forward',
        'Nikola Jok': 'Point Center',
        'Giannis Ante': 'Point Forward',
        'Miles Bridges': 'Versatile Big',
    }

    print("\nValidation against known archetypes:")
    ok_count = 0
    review_count = 0
    found_count = 0
    review_cases = []
    for player_fragment, expected in known_players.items():
        match = df[df['player_name'].str.contains(player_fragment, case=False, na=False)]
        if not match.empty:
            actual = match.iloc[0]['archetype']
            is_match = expected == actual
            status = "OK" if is_match else "REVIEW"
            if is_match:
                ok_count += 1
            else:
                review_count += 1
                review_cases.append({
                    "player_name": str(match.iloc[0]["player_name"]),
                    "expected": expected,
                    "candidate": str(actual),
                })
            found_count += 1
            prob_cols = [c for c in match.columns if c.startswith('cluster_') and c.endswith('_prob')]
            top_probs = match.iloc[0][prob_cols].sort_values(ascending=False).head(3)
            prob_str = ', '.join(f"C{c.split('_')[1]}={v:.0%}" for c, v in top_probs.items())
            print(f"  {match.iloc[0]['player_name']}: expected={expected}, got={actual} [{status}]  ({prob_str})")
        else:
            print(f"  {player_fragment}: NOT IN ELIGIBLE FEATURE DATA")

    if found_count > 0:
        error_rate = review_count / found_count * 100
        print(f"\n  Reference checks: {ok_count}/{found_count} matched, {review_count}/{found_count} need review ({error_rate:.0f}% mismatch rate)")
    return {
        'reference_players_found': found_count,
        'reference_matches': ok_count,
        'reference_mismatches': review_count,
        'review_cases': review_cases,
    }


def save_archetypes(df):
    validate_archetype_labels(df)
    normalized_keys = df['player_name'].map(_ascii_key)
    if normalized_keys.duplicated().any():
        duplicate_names = df.loc[
            normalized_keys.duplicated(False), 'player_name'
        ].astype(str).unique().tolist()
        raise ValueError(
            f"Refusing to save duplicate normalized archetype players: "
            f"{duplicate_names[:10]}"
        )

    conn = sqlite3.connect(DB_PATH)
    now = get_eastern_now().isoformat()

    prob_cols = [c for c in df.columns if c.startswith('cluster_') and c.endswith('_prob')]
    composite_cols = ['creation_idx', 'playmaking_idx', 'interior_idx', 'perimeter_idx',
                      'offball_idx', 'rebound_idx', 'defense_idx', 'size_idx']
    save_cols = ['player_name', 'team', 'true_position', 'archetype', 'base_archetype', 'cluster'] + prob_cols + composite_cols
    save_df = df[save_cols].copy()
    save_df['computed_at'] = now

    BREF_TO_ESPN = {
        'NOP': 'NO', 'CHO': 'CHA', 'BRK': 'BKN', 'NYK': 'NY',
        'SAS': 'SA', 'GSW': 'GS',
    }
    save_df['team'] = save_df['team'].replace(BREF_TO_ESPN)

    depth_charts = pd.read_sql_query(
        "SELECT DISTINCT player_name, team FROM depth_charts", conn
    )
    dc_map = {}
    for _, row in depth_charts.iterrows():
        key = _ascii_key(row['player_name'])
        if key not in dc_map:
            dc_map[key] = row['team']

    save_df['_mk'] = save_df['player_name'].apply(_ascii_key)
    updated_count = 0
    for idx, row in save_df.iterrows():
        dc_team = dc_map.get(row['_mk'])
        if dc_team and dc_team != row['team']:
            old_team = row['team']
            save_df.at[idx, 'team'] = dc_team
            updated_count += 1
            if old_team in ('2TM', '3TM', 'TOT') or dc_team != BREF_TO_ESPN.get(old_team, old_team):
                print(f"    {row['player_name']}: {old_team} -> {dc_team}")

    still_multi = save_df['team'].isin(['2TM', '3TM', 'TOT'])
    if still_multi.any():
        game_logs = pd.read_sql_query(
            "SELECT player_name, matchup, game_date FROM player_game_logs ORDER BY game_date DESC",
            conn
        )
        game_logs['_mk'] = game_logs['player_name'].apply(_ascii_key)
        latest = game_logs.drop_duplicates(subset='_mk', keep='first')

        def extract_team(matchup):
            if not matchup or not isinstance(matchup, str):
                return None
            parts = matchup.replace('@', 'vs.').split('vs.')
            return parts[0].strip() if len(parts) >= 2 else None

        latest['current_team'] = latest['matchup'].apply(extract_team)
        gl_map = dict(zip(latest['_mk'], latest['current_team']))

        for idx in save_df[still_multi].index:
            gl_team = gl_map.get(save_df.at[idx, '_mk'])
            if gl_team:
                print(f"    {save_df.at[idx, 'player_name']}: {save_df.at[idx, 'team']} -> {gl_team} (game log fallback)")
                save_df.at[idx, 'team'] = gl_team

    save_df = save_df.drop(columns=['_mk'])

    remaining_multi = save_df['team'].isin(['2TM', '3TM', 'TOT']).sum()
    print(f"  Team resolution: {updated_count} updated from depth charts, {remaining_multi} unresolved")

    from utils.current_nba_teams import (
        apply_current_player_teams,
        current_player_team_map_from_sqlite,
    )
    current_team_map = current_player_team_map_from_sqlite(conn)
    save_df = apply_current_player_teams(
        save_df, current_team_map, clear_unmatched=True
    )
    unresolved_current_teams = save_df["team"].astype(str).str.strip().eq("").sum()
    print(
        f"  Current roster mapping: {len(save_df) - unresolved_current_teams} "
        f"players assigned; {unresolved_current_teams} without a current roster match"
    )

    save_df.to_sql('player_archetypes', conn, if_exists='replace', index=False)

    print(f"\nSaved {len(save_df)} archetypes to player_archetypes table (with soft cluster probabilities)")

    print("\nArchetype distribution:")
    for arch, count in df['archetype'].value_counts().sort_index().items():
        print(f"  {arch}: {count} players")

    conn.close()


def main(*, dry_run=False, offline=False, feature_season=None):
    print("=" * 60)
    print("PHILLIPS-STYLE PLAYER ARCHETYPE CLASSIFICATION v2")
    print("8 Composite Indices + Minutes-Weighted Centroids + Soft Clustering")
    print("=" * 60)

    print("\n1. Loading feature data (per-100 + positions + shots + hustle + tracking + measurements)...")
    df = load_feature_data(feature_season) if feature_season else load_feature_data()
    print(f"   Loaded {len(df)} players with complete data")
    if df.empty:
        raise ValueError("No eligible archetype players; the 10-game / 12-MPG minimum is unchanged")

    print("\n2. Building 8 composite indices...")
    df, raw_scaler = build_composite_indices(df)

    print("\n3. Running minutes-weighted K-Means clustering...")
    cached_heights = (
        dict(zip(df['player_name'].map(_ascii_key), df['height_inches']))
        if offline else None
    )
    df, km, cluster_scaler, labels = run_clustering(df, height_map=cached_heights)

    print("\n4. Validating archetypes...")
    validate_archetype_labels(df)
    validation = validate_archetypes(df)

    if dry_run:
        print("\n5. DRY RUN: no archetypes or downstream outputs will be saved.")
    else:
        if not validation['reference_players_found'] or validation['reference_mismatches']:
            raise ValueError(
                "Archetype reference checks need review; refusing to replace "
                "stored classifications. Inspect with --dry-run --offline first."
            )
        print("\n5. Saving results (with soft cluster probabilities)...")
        from utils.archetype_inputs import validate_input_season
        with sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True) as source:
            validate_input_season(source, feature_season or nba_api_season())
            stored_tables = {
                row[0] for row in source.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")
            }
            if "player_archetypes" in stored_tables:
                old_players = {
                    _ascii_key(row[0]): row[0]
                    for row in source.execute("SELECT player_name FROM player_archetypes")
                }
                proposed_keys = {_ascii_key(name) for name in df["player_name"]}
                missing = sorted(name for key, name in old_players.items()
                                 if key not in proposed_keys)
                if missing:
                    raise ValueError(
                        f"Archetype rebuild would lose {len(missing)} previously"
                        f" classified players: {missing[:15]}")
        save_archetypes(df)

    print("\n6. Sample players by archetype:")
    for arch in sorted(df['archetype'].unique()):
        players = df[df['archetype'] == arch].nlargest(5, 'pts_per100')['player_name'].tolist()
        print(f"  {arch}: {', '.join(players)}")

    print("\n7. Big Man Shot Profile Summary:")
    big_archetypes = ['Traditional Big', 'Stretch 4', 'Stretch 5', 'Versatile Big', 'Point Center', 'Point Forward']
    for arch in big_archetypes:
        subset = df[df['archetype'] == arch]
        if subset.empty:
            continue
        n = len(subset)
        avg_rp = subset['rim_paint_pct'].mean()
        avg_tp = subset['three_pct'].mean()
        avg_cs = subset['cs_pct'].mean()
        avg_pu = subset['pu_pct'].mean()
        print(f"  {arch} (n={n}): RimPaint={avg_rp:.1f}% 3PT={avg_tp:.1f}% C&S={avg_cs:.1f}% PullUp={avg_pu:.1f}%")

    print("\n8. Defensive Hustle Profile Summary (per 48 min):")
    all_archetypes = ['3-and-D Wing', 'Combo Guard',
                      'Playmaker', 'Scoring Wing',
                      'Traditional Big', 'Stretch 4', 'Stretch 5', 'Versatile Big',
                      'Point Center', 'Point Forward']
    print(f"  {'Archetype':<20} {'N':>3} {'STL/100':>8} {'BLK/100':>8} {'DEFL/48':>8} {'CONTEST/48':>11}")
    print(f"  {'-'*62}")
    for arch in all_archetypes:
        subset = df[df['archetype'] == arch]
        if subset.empty:
            continue
        n = len(subset)
        avg_stl = subset['stl_per100'].mean()
        avg_blk = subset['blk_per100'].mean()
        avg_defl = subset['deflections_per48'].mean()
        avg_contest = subset['contested_per48'].mean()
        print(f"  {arch:<20} {n:>3} {avg_stl:>8.1f} {avg_blk:>8.1f} {avg_defl:>8.1f} {avg_contest:>11.1f}")

    print("\n9. Composite Index Profile by Archetype:")
    print(f"  {'Archetype':<20} {'N':>3} {'CRE':>6} {'PLY':>6} {'INT':>6} {'PER':>6} {'OFF':>6} {'REB':>6} {'DEF':>6} {'SIZ':>6}")
    print(f"  {'-'*74}")
    for arch in sorted(df['archetype'].unique()):
        subset = df[df['archetype'] == arch]
        n = len(subset)
        avgs = subset[COMPOSITE_FEATURES].mean()
        print(f"  {arch:<20} {n:>3}", end='')
        for feat in COMPOSITE_FEATURES:
            print(f" {avgs[feat]:>6.2f}", end='')
        print()

    print("\nDone!")
    return df


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="Build and validate NBA archetypes")
    parser.add_argument('--dry-run', action='store_true', help='Audit without saving results')
    parser.add_argument('--offline', action='store_true', help='Use cached height measurements')
    parser.add_argument('--feature-season', help='Explicit, uniformly sourced YYYY-YY feature season')
    args = parser.parse_args()
    main(dry_run=args.dry_run, offline=args.offline, feature_season=args.feature_season)
