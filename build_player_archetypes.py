import sqlite3
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from datetime import datetime
import unicodedata
import re
import warnings
warnings.filterwarnings('ignore')


_NICKNAME_MAP = {
    'ronald': 'ron', 'william': 'will', 'robert': 'rob',
    'kenneth': 'ken', 'nicholas': 'nick', 'christopher': 'chris',
    'timothy': 'tim', 'matthew': 'matt', 'daniel': 'dan',
    'michael': 'mike', 'joseph': 'joe', 'edward': 'ed',
    'anthony': 'tony', 'richard': 'rich', 'thomas': 'tom',
    'benjamin': 'ben', 'gregory': 'greg', 'gerald': 'gerry',
    'patrick': 'pat', 'jeffrey': 'jeff', 'cameron': 'cam',
}

def _ascii_key(name):
    """Create an ASCII merge key that handles double-encoded UTF-8 and diacritics."""
    if not name or not isinstance(name, str):
        return ""
    fixed = name
    for _ in range(2):
        try:
            fixed = fixed.encode('latin-1').decode('utf-8')
        except (UnicodeDecodeError, UnicodeEncodeError):
            break
    _cyr = {'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'e','ж':'zh',
            'з':'z','и':'i','й':'y','к':'k','л':'l','м':'m','н':'n','о':'o',
            'п':'p','р':'r','с':'s','т':'t','у':'u','ф':'f','х':'kh','ц':'ts',
            'ч':'ch','ш':'sh','щ':'shch','ъ':'','ы':'y','ь':'','э':'e','ю':'yu','я':'ya'}
    fixed = ''.join(_cyr.get(c, _cyr.get(c.lower(), c)) for c in fixed)
    nfkd = unicodedata.normalize('NFKD', fixed)
    ascii_name = ''.join(c for c in nfkd if not unicodedata.combining(c))
    ascii_name = re.sub(r'[^a-zA-Z\s]', '', ascii_name).lower().strip()
    ascii_name = re.sub(r'\s+', ' ', ascii_name)
    for suffix in [' iv', ' iii', ' ii', ' jr', ' sr', ' v']:
        if ascii_name.endswith(suffix):
            ascii_name = ascii_name[:-len(suffix)].strip()
            break
    parts = ascii_name.split(' ', 1)
    if len(parts) == 2 and parts[0] in _NICKNAME_MAP:
        ascii_name = _NICKNAME_MAP[parts[0]] + ' ' + parts[1]
    return ascii_name

DB_PATH = 'dfs_nba.db'

HEIGHT_THRESHOLD_INCHES = 81  # 6'9" = 81 inches
POINT_CENTER_AST_THRESHOLD = 5.0
POINT_CENTER_3PM_THRESHOLD = 2.0


def fetch_player_heights():
    """Fetch player heights from NBA API in bulk."""
    try:
        from nba_api.stats.endpoints import leaguedashplayerbiostats
        import time
        time.sleep(0.6)
        bio = leaguedashplayerbiostats.LeagueDashPlayerBioStats(season='2024-25')
        df = bio.get_data_frames()[0]
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

FEATURES = [
    'pts_per100', 'reb_per100', 'ast_per100', 'stl_per100', 'blk_per100',
    'fg3m_per100', 'usg_pct',
    'guard_pct', 'forward_pct', 'big_pct',
    'ast_to_reb_ratio', 'scoring_versatility',
]

TARGET_K = 6


def load_feature_data():
    conn = sqlite3.connect(DB_PATH)

    per100 = pd.read_sql_query("""
        SELECT player_name, team, games_played, total_minutes, mpg,
               pts_per100, reb_per100, ast_per100, stl_per100, blk_per100, tov_per100
        FROM player_per100
        WHERE games_played >= 10 AND mpg >= 12
    """, conn)

    positions = pd.read_sql_query("""
        SELECT player_name, true_position, pg_pct, sg_pct, sf_pct, pf_pct, c_pct
        FROM player_positions
    """, conn)

    game_logs = pd.read_sql_query("""
        SELECT player_name,
               AVG(fg3m) as fg3m_pg,
               AVG(pts) as pts_pg,
               AVG(reb) as reb_pg,
               AVG(ast) as ast_pg,
               AVG(min) as min_pg,
               COUNT(*) as log_games
        FROM player_game_logs
        WHERE min >= 10
        GROUP BY player_name
        HAVING COUNT(*) >= 5
    """, conn)

    usage = pd.read_sql_query("""
        SELECT player_name, usg_pct
        FROM player_stats
    """, conn)

    conn.close()

    for tbl in [per100, positions, game_logs, usage]:
        tbl['_merge_key'] = tbl['player_name'].apply(_ascii_key)

    df = per100.merge(positions.drop(columns=['player_name']), on='_merge_key', how='inner')
    df = df.merge(game_logs.drop(columns=['player_name']), on='_merge_key', how='inner')
    df = df.merge(usage.drop(columns=['player_name']), on='_merge_key', how='left')
    df = df.drop(columns=['_merge_key'])

    df['usg_pct'] = df['usg_pct'].fillna(df['usg_pct'].median())

    return df


def engineer_features(df):
    df['fg3m_per100'] = np.where(
        df['min_pg'] > 0,
        df['fg3m_pg'] / df['min_pg'] * 100,
        0
    )

    df['guard_pct'] = df['pg_pct'] + df['sg_pct']
    df['forward_pct'] = df['sf_pct'] + df['pf_pct']
    df['big_pct'] = df['c_pct']

    df['ast_to_reb_ratio'] = np.where(
        df['reb_per100'] > 0,
        df['ast_per100'] / df['reb_per100'],
        df['ast_per100']
    )

    df['scoring_versatility'] = np.where(
        df['pts_per100'] > 0,
        df['fg3m_per100'] / df['pts_per100'],
        0
    )

    for col in FEATURES:
        df[col] = df[col].fillna(0)

    return df


def label_cluster_scored(centroid, feature_names):
    """Label a cluster centroid as one of 6 archetypes.

    Target archetypes (k=6):
      1. Playmaker       - High-assist guards (pure PGs, floor generals)
      2. Combo Guard     - Scoring guards (high pts, guard-heavy)
      3. 3-and-D Wing    - Role forwards/wings (moderate stats, forward-heavy)
      4. Scoring Wing    - High-usage elite forwards (KD, Kawhi, LeBron, Giannis)
      5. Stretch Big     - Bigs who shoot 3s or play versatile roles
      6. Traditional Big - Rim protectors, rebounders, paint-dominant bigs
    """
    c = dict(zip(feature_names, centroid))

    pts = c.get('pts_per100', 0)
    reb = c.get('reb_per100', 0)
    ast = c.get('ast_per100', 0)
    stl = c.get('stl_per100', 0)
    blk = c.get('blk_per100', 0)
    fg3m = c.get('fg3m_per100', 0)
    usg = c.get('usg_pct', 0)
    gpct = c.get('guard_pct', 0)
    fpct = c.get('forward_pct', 0)
    bpct = c.get('big_pct', 0)
    ast_reb = c.get('ast_to_reb_ratio', 0)

    if bpct > 40:
        if fg3m > 4:
            return 'Stretch Big'
        return 'Traditional Big'

    if gpct > 60:
        if ast > 7:
            return 'Playmaker'
        return 'Combo Guard'

    if fpct > 40:
        if pts > 25 and usg > 22:
            return 'Scoring Wing'
        return '3-and-D Wing'

    if ast > 7:
        return 'Playmaker'
    if pts > 25:
        return 'Scoring Wing'
    if bpct > 20:
        return 'Stretch Big' if fg3m > 4 else 'Traditional Big'
    return 'Combo Guard'


def run_clustering(df, k=TARGET_K):
    feature_df = df[FEATURES].copy()
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(feature_df)

    print(f"Using k={k}")
    print("Silhouette scores for reference:")
    for test_k in range(6, 12):
        km_test = KMeans(n_clusters=test_k, n_init=20, random_state=42, max_iter=300)
        labels_test = km_test.fit_predict(X_scaled)
        score = silhouette_score(X_scaled, labels_test)
        marker = " <-- chosen" if test_k == k else ""
        print(f"  k={test_k}: silhouette={score:.4f}{marker}")

    km = KMeans(n_clusters=k, n_init=30, random_state=42, max_iter=500)
    df = df.copy()
    df['cluster'] = km.fit_predict(X_scaled)

    centroids_orig = scaler.inverse_transform(km.cluster_centers_)

    cluster_labels = {}
    label_counts = {}
    for i in range(k):
        label = label_cluster_scored(centroids_orig[i], FEATURES)
        label_counts[label] = label_counts.get(label, 0) + 1
        if label_counts[label] > 1:
            c = dict(zip(FEATURES, centroids_orig[i]))
            if c['stl_per100'] > 2.0 or c['usg_pct'] < 17:
                label = "3-and-D Guard"
            elif c['fg3m_per100'] > 6:
                label = "Scoring Guard"
            elif c['pts_per100'] > 25 or c['usg_pct'] > 25:
                label = f"{label} (Elite)"
            else:
                label = f"{label} (Role)"
        cluster_labels[i] = label

    print(f"\nCluster centroids (original scale):")
    for i in range(k):
        c = dict(zip(FEATURES, centroids_orig[i]))
        n = len(df[df['cluster'] == i])
        print(f"  Cluster {i} [{cluster_labels[i]}] (n={n}): "
              f"PTS={c['pts_per100']:.1f} REB={c['reb_per100']:.1f} AST={c['ast_per100']:.1f} "
              f"STL={c['stl_per100']:.1f} BLK={c['blk_per100']:.1f} 3PM={c['fg3m_per100']:.1f} "
              f"USG={c['usg_pct']:.1f} G%={c['guard_pct']:.0f} F%={c['forward_pct']:.0f} C%={c['big_pct']:.0f}")

    df['archetype'] = df['cluster'].map(cluster_labels)

    STRETCH_BIG_3PM_THRESHOLD = 4.0
    big_cluster_ids = [i for i, lbl in cluster_labels.items() if lbl == 'Traditional Big']
    if big_cluster_ids:
        big_mask = df['cluster'].isin(big_cluster_ids)
        stretch_mask = big_mask & (df['fg3m_per100'] >= STRETCH_BIG_3PM_THRESHOLD)
        reclassed = stretch_mask.sum()
        df.loc[stretch_mask, 'archetype'] = 'Stretch Big'
        print(f"\n  Player-level reclassification: {reclassed} bigs with 3PM/100 >= {STRETCH_BIG_3PM_THRESHOLD} -> Stretch Big")

    VERSATILE_BIG_PTS_THRESHOLD = 18.0
    trad_big_archetypes = ['Traditional Big']
    trad_mask = df['archetype'].isin(trad_big_archetypes)
    versatile_from_trad = 0
    pc_from_trad = 0
    for idx in df[trad_mask].index:
        player = df.loc[idx]
        ast = player.get('ast_per100', 0)
        pts = player.get('pts_per100', 0)
        fg3m = player.get('fg3m_per100', 0)
        if ast >= POINT_CENTER_AST_THRESHOLD and pts >= VERSATILE_BIG_PTS_THRESHOLD:
            if fg3m >= POINT_CENTER_3PM_THRESHOLD:
                df.at[idx, 'archetype'] = 'Point Center'
                pc_from_trad += 1
                print(f"    {player['player_name']}: Traditional Big -> Point Center "
                      f"(AST/100={ast:.1f}, PTS/100={pts:.1f}, 3PM/100={fg3m:.1f})")
            else:
                df.at[idx, 'archetype'] = 'Versatile Big'
                versatile_from_trad += 1
                print(f"    {player['player_name']}: Traditional Big -> Versatile Big "
                      f"(AST/100={ast:.1f}, PTS/100={pts:.1f})")
    if pc_from_trad or versatile_from_trad:
        print(f"  From Traditional Bigs: {pc_from_trad} -> Point Center, {versatile_from_trad} -> Versatile Big")

    print("\n  Height-based reclassification for bigs misclassified as wings...")
    height_map = fetch_player_heights()
    if height_map:
        df['_mk'] = df['player_name'].apply(_ascii_key)
        df['height_inches'] = df['_mk'].map(height_map)

        wing_archetypes = ['Scoring Wing', 'Scoring Guard', '3-and-D Wing', '3-and-D Guard',
                           'Scoring Wing (Elite)', 'Scoring Wing (Role)', 'Combo Guard']

        big_position_threshold = 40
        tall_big_mask = (
            df['height_inches'].notna() &
            (df['height_inches'] >= HEIGHT_THRESHOLD_INCHES) &
            df['archetype'].isin(wing_archetypes) &
            ((df['c_pct'] + df['pf_pct']) >= big_position_threshold)
        )

        point_center_count = 0
        versatile_big_count = 0
        for idx in df[tall_big_mask].index:
            player = df.loc[idx]
            ast = player.get('ast_per100', 0)
            fg3m = player.get('fg3m_per100', 0)
            c_pct = player.get('c_pct', 0)
            pf_pct = player.get('pf_pct', 0)
            height = int(player['height_inches'])
            ft_in = f"{height // 12}'{height % 12}\""

            if ast >= POINT_CENTER_AST_THRESHOLD and fg3m >= POINT_CENTER_3PM_THRESHOLD:
                df.at[idx, 'archetype'] = 'Point Center'
                point_center_count += 1
                print(f"    {player['player_name']} ({ft_in}, C%={c_pct:.0f} PF%={pf_pct:.0f}): "
                      f"{player['archetype']} -> Point Center (AST/100={ast:.1f}, 3PM/100={fg3m:.1f})")
            elif ast >= POINT_CENTER_AST_THRESHOLD:
                df.at[idx, 'archetype'] = 'Point Center'
                point_center_count += 1
                print(f"    {player['player_name']} ({ft_in}, C%={c_pct:.0f} PF%={pf_pct:.0f}): "
                      f"{player['archetype']} -> Point Center (AST/100={ast:.1f}, high facilitator)")
            else:
                df.at[idx, 'archetype'] = 'Versatile Big'
                versatile_big_count += 1
                print(f"    {player['player_name']} ({ft_in}, C%={c_pct:.0f} PF%={pf_pct:.0f}): "
                      f"{player['archetype']} -> Versatile Big (AST/100={ast:.1f}, 3PM/100={fg3m:.1f})")

        print(f"  Reclassified: {point_center_count} -> Point Center, {versatile_big_count} -> Versatile Big")
        df = df.drop(columns=['_mk', 'height_inches'])

    df['base_archetype'] = df['archetype'].copy()

    print("\n  Hybrid archetype reclassification (elite transcendent players only)...")

    path_a_mask = (
        (df['pts_per100'] >= 41.5) &
        (df['ast_per100'] >= 7.5) &
        (df['usg_pct'] >= 30.0)
    )

    path_b_mask = (
        (df['pts_per100'] >= 32.0) &
        (df['ast_per100'] >= 9.5) &
        (df['reb_per100'] >= 8.0) &
        (df['usg_pct'] >= 28.0)
    )

    elite_mask = path_a_mask | path_b_mask

    hybrid_guard_count = 0
    hybrid_forward_count = 0
    hybrid_big_count = 0

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
        guard = player['guard_pct']

        if (c_pct + pf_pct) >= 50:
            hybrid_type = 'Hybrid Big'
            hybrid_big_count += 1
        elif (sf_pct + pf_pct) > guard:
            hybrid_type = 'Hybrid Forward'
            hybrid_forward_count += 1
        else:
            hybrid_type = 'Hybrid Guard'
            hybrid_guard_count += 1

        path = 'A+B' if path_a_mask[idx] and path_b_mask[idx] else ('A' if path_a_mask[idx] else 'B')
        df.at[idx, 'archetype'] = hybrid_type
        print(f"    {player['player_name']}: {old_arch} -> {hybrid_type} [{path}] "
              f"(PTS={pts:.1f} AST={ast:.1f} REB={reb:.1f} USG={usg:.1f})")

    total = hybrid_guard_count + hybrid_forward_count + hybrid_big_count
    print(f"  Hybrid totals: {total} elite hybrids "
          f"({hybrid_guard_count} Guard, {hybrid_forward_count} Forward, {hybrid_big_count} Big)")

    return df, km, scaler, cluster_labels


def validate_archetypes(df):
    known_players = {
        'Karl-Anthony Towns': 'Stretch Big',
        'Rudy Gobert': 'Traditional Big',
        'Anthony Davis': 'Traditional Big',
        'James Harden': 'Playmaker',
        'Kevin Durant': 'Scoring Wing',
        'Kawhi Leonard': 'Scoring Wing',
        'Victor Wembanyama': 'Stretch Big',
        'Draymond Green': '3-and-D Wing',
        'Trae Young': 'Playmaker',
        'Jalen Brunson': 'Playmaker',
        'Jaylen Brown': 'Scoring Wing',
        'Donovan Mitchell': 'Playmaker',
        'Norman Powell': 'Scoring Guard',
        'Myles Turner': 'Stretch Big',
        'Brook Lopez': 'Stretch Big',
        'Domantas Sabonis': 'Versatile Big',
        'Mikal Bridges': '3-and-D Wing',
        'Klay Thompson': 'Scoring Guard',
        'Stephen Curry': 'Hybrid Guard',
        'Luka Don': 'Hybrid Guard',
        'Shai Gilgeous': 'Hybrid Guard',
        'LaMelo Ball': 'Hybrid Guard',
        'LeBron James': 'Hybrid Forward',
        'Nikola Jok': 'Hybrid Big',
        'Giannis Ante': 'Hybrid Big',
    }

    print("\nValidation against known archetypes:")
    ok_count = 0
    review_count = 0
    found_count = 0
    for player_fragment, expected in known_players.items():
        match = df[df['player_name'].str.contains(player_fragment, case=False, na=False)]
        if not match.empty:
            actual = match.iloc[0]['archetype']
            is_match = expected.lower() in actual.lower()
            status = "OK" if is_match else "REVIEW"
            if is_match:
                ok_count += 1
            else:
                review_count += 1
            found_count += 1
            print(f"  {match.iloc[0]['player_name']}: expected={expected}, got={actual} [{status}]")
        else:
            print(f"  {player_fragment}: NOT IN TODAY'S SLATE")

    if found_count > 0:
        error_rate = review_count / found_count * 100
        print(f"\n  Results: {ok_count}/{found_count} correct, {review_count}/{found_count} mismatched ({error_rate:.0f}% error rate)")


def save_archetypes(df):
    conn = sqlite3.connect(DB_PATH)
    now = datetime.now().isoformat()

    save_df = df[['player_name', 'team', 'true_position', 'archetype', 'base_archetype', 'cluster']].copy()
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

    save_df.to_sql('player_archetypes', conn, if_exists='replace', index=False)

    print(f"\nSaved {len(save_df)} archetypes to player_archetypes table")

    print("\nArchetype distribution:")
    for arch, count in df['archetype'].value_counts().sort_index().items():
        print(f"  {arch}: {count} players")

    conn.close()


def main():
    print("=" * 60)
    print("PHILLIPS-STYLE PLAYER ARCHETYPE CLASSIFICATION")
    print("K-Means Clustering on Per-100 Stats + Positions")
    print("=" * 60)

    print("\n1. Loading feature data...")
    df = load_feature_data()
    print(f"   Loaded {len(df)} players with complete data")

    print("\n2. Engineering features...")
    df = engineer_features(df)

    print("\n3. Running K-Means clustering...")
    df, km, scaler, labels = run_clustering(df)

    print("\n4. Validating archetypes...")
    validate_archetypes(df)

    print("\n5. Saving results...")
    save_archetypes(df)

    print("\n6. Sample players by archetype:")
    for arch in sorted(df['archetype'].unique()):
        players = df[df['archetype'] == arch].nlargest(5, 'pts_per100')['player_name'].tolist()
        print(f"  {arch}: {', '.join(players)}")

    print("\nDone!")
    return df


if __name__ == '__main__':
    main()
