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

HEIGHT_THRESHOLD_INCHES = 82
POINT_CENTER_AST_THRESHOLD = 5.0
POINT_CENTER_PTS_THRESHOLD = 24.0

FEATURES = [
    'pts_per100', 'reb_per100', 'ast_per100', 'stl_per100', 'blk_per100',
    'fg3m_per100', 'usg_pct',
    'guard_pct', 'forward_pct', 'big_pct',
    'ast_to_reb_ratio', 'scoring_versatility',
    'rim_paint_pct', 'three_pct',
    'cs_pct', 'pu_pct',
    'deflections_per48', 'contested_per48',
]

TARGET_K = 6


def fetch_player_heights():
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

    shot_zones = pd.read_sql_query("""
        SELECT player_name, rim_paint_pct, three_pct, ra_pct, paint_pct, mid_pct
        FROM player_shot_zones
    """, conn)

    shot_creation = pd.read_sql_query("""
        SELECT player_name, cs_pct, pu_pct, paint_pct as sc_paint_pct, cs_3_share
        FROM player_shot_creation
    """, conn)

    hustle = pd.read_sql_query("""
        SELECT player_name, deflections_per48, contested_per48, loose_per48,
               charges_per48, screen_ast_per48, box_outs_per48
        FROM player_hustle_stats
    """, conn)

    conn.close()

    for tbl in [per100, positions, game_logs, usage, shot_zones, shot_creation, hustle]:
        tbl['_merge_key'] = tbl['player_name'].apply(_ascii_key)

    df = per100.merge(positions.drop(columns=['player_name']), on='_merge_key', how='inner')
    df = df.merge(game_logs.drop(columns=['player_name']), on='_merge_key', how='inner')
    df = df.merge(usage.drop(columns=['player_name']), on='_merge_key', how='left')
    df = df.merge(shot_zones.drop(columns=['player_name']), on='_merge_key', how='left')
    df = df.merge(shot_creation.drop(columns=['player_name']), on='_merge_key', how='left')
    df = df.merge(hustle.drop(columns=['player_name']), on='_merge_key', how='left')
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

    df['deflections_per48'] = df['deflections_per48'].fillna(df['deflections_per48'].median() if df['deflections_per48'].notna().any() else 3.0)
    df['contested_per48'] = df['contested_per48'].fillna(df['contested_per48'].median() if df['contested_per48'].notna().any() else 8.0)
    df['loose_per48'] = df['loose_per48'].fillna(1.0)
    df['charges_per48'] = df['charges_per48'].fillna(0.2)
    df['screen_ast_per48'] = df['screen_ast_per48'].fillna(1.0)
    df['box_outs_per48'] = df['box_outs_per48'].fillna(2.0)

    shot_merged = df['rim_paint_pct'].notna().sum()
    hustle_merged = df['deflections_per48'].notna().sum()
    print(f"  Shot zone data merged for {shot_merged}/{len(df)} players")
    print(f"  Hustle stats merged for {hustle_merged}/{len(df)} players")

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
    rim_paint = c.get('rim_paint_pct', 0)
    three = c.get('three_pct', 0)
    cs = c.get('cs_pct', 0)
    pu = c.get('pu_pct', 0)
    defl = c.get('deflections_per48', 0)
    contest = c.get('contested_per48', 0)

    if bpct > 40 or (reb > 12 and blk > 1.5):
        if rim_paint > 75 and three < 15:
            return 'Traditional Big'
        if three > 30 and cs > 35:
            return 'Stretch Big'
        if fg3m > 4:
            return 'Stretch Big'
        return 'Traditional Big'

    if gpct > 60:
        if ast > 7:
            return 'Playmaker'
        if defl > 4.5 and stl > 1.8:
            return '3-and-D Wing'
        return 'Combo Guard'

    if fpct > 40:
        if pts > 25 and usg > 22:
            return 'Scoring Wing'
        if (defl > 4.0 or stl > 1.8) and contest > 6:
            return '3-and-D Wing'
        if pts > 20 and usg > 18:
            return 'Scoring Wing'
        return 'Combo Guard'

    if ast > 7:
        return 'Playmaker'
    if pts > 25:
        return 'Scoring Wing'
    if bpct > 20:
        if rim_paint > 75:
            return 'Traditional Big'
        if three > 30:
            return 'Stretch Big'
        return 'Traditional Big'
    if defl > 4.0 and stl > 1.5:
        return '3-and-D Wing'
    return 'Combo Guard'


def run_clustering(df, k=TARGET_K):
    feature_df = df[FEATURES].copy()
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(feature_df)

    print(f"Using k={k} with {len(FEATURES)} features (including shot zones + creation)")
    print("Features:", FEATURES)
    print("\nSilhouette scores for reference:")
    for test_k in range(5, 12):
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
            defl = c.get('deflections_per48', 0)
            contest = c.get('contested_per48', 0)
            stl = c.get('stl_per100', 0)
            if '3-and-D' in label:
                if defl > 4.0 and stl > 1.8:
                    label = "3-and-D Guard" if c['guard_pct'] > 50 else "3-and-D Wing"
                else:
                    label = "Combo Guard"
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
              f"USG={c['usg_pct']:.1f} G%={c['guard_pct']:.0f} F%={c['forward_pct']:.0f} C%={c['big_pct']:.0f} "
              f"RimPaint={c['rim_paint_pct']:.1f} 3PT%={c['three_pct']:.1f} "
              f"C&S={c['cs_pct']:.1f} PU={c['pu_pct']:.1f} "
              f"DEFL={c['deflections_per48']:.1f} CONTEST={c['contested_per48']:.1f}")

    df['archetype'] = df['cluster'].map(cluster_labels)

    print("\n  Shot-zone-based big man reclassification...")
    big_archetypes = ['Traditional Big', 'Stretch Big', 'Traditional Big (Elite)',
                      'Traditional Big (Role)', 'Stretch Big (Elite)', 'Stretch Big (Role)']
    big_mask = df['archetype'].isin(big_archetypes)

    stretch_from_trad = 0
    trad_from_stretch = 0
    for idx in df[big_mask].index:
        player = df.loc[idx]
        current = player['archetype']
        rp = player.get('rim_paint_pct', 50)
        tp = player.get('three_pct', 25)
        csp = player.get('cs_pct', 30)
        fg3m = player.get('fg3m_per100', 0)

        if 'Traditional' in current and tp >= 30 and fg3m >= 3.0:
            df.at[idx, 'archetype'] = 'Stretch Big'
            stretch_from_trad += 1
        elif 'Stretch' in current and rp >= 80 and tp < 15:
            df.at[idx, 'archetype'] = 'Traditional Big'
            trad_from_stretch += 1

    if stretch_from_trad or trad_from_stretch:
        print(f"    Shot zones: {stretch_from_trad} Traditional -> Stretch, {trad_from_stretch} Stretch -> Traditional")

    print("\n  Reclassifying facilitating bigs (Point Center / Point Forward)...")
    all_big_labels = ['Traditional Big', 'Stretch Big', 'Versatile Big']
    reclass_mask = df['archetype'].isin(all_big_labels)
    pc_count = 0
    pf_count = 0
    vb_count = 0
    for idx in df[reclass_mask].index:
        player = df.loc[idx]
        ast = player.get('ast_per100', 0)
        pts = player.get('pts_per100', 0)
        c_pct = player.get('c_pct', 0)
        fg3m = player.get('fg3m_per100', 0)

        if ast >= POINT_CENTER_AST_THRESHOLD and pts >= POINT_CENTER_PTS_THRESHOLD:
            if c_pct >= 50:
                df.at[idx, 'archetype'] = 'Point Center'
                pc_count += 1
                print(f"    {player['player_name']}: {player['archetype']} -> Point Center "
                      f"(AST/100={ast:.1f}, PTS/100={pts:.1f}, C%={c_pct:.0f})")
            else:
                df.at[idx, 'archetype'] = 'Point Forward'
                pf_count += 1
                print(f"    {player['player_name']}: {player['archetype']} -> Point Forward "
                      f"(AST/100={ast:.1f}, PTS/100={pts:.1f}, C%={c_pct:.0f})")
        elif ast >= POINT_CENTER_AST_THRESHOLD and pts >= 18.0:
            if player['archetype'] == 'Traditional Big':
                df.at[idx, 'archetype'] = 'Versatile Big'
                vb_count += 1
                print(f"    {player['player_name']}: Traditional Big -> Versatile Big "
                      f"(AST/100={ast:.1f}, PTS/100={pts:.1f})")

    print(f"  Facilitators: {pc_count} Point Center, {pf_count} Point Forward, {vb_count} -> Versatile Big")

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
        point_forward_count = 0
        versatile_big_count = 0
        stretch_big_count = 0
        trad_big_count = 0
        for idx in df[tall_big_mask].index:
            player = df.loc[idx]
            ast = player.get('ast_per100', 0)
            pts = player.get('pts_per100', 0)
            fg3m = player.get('fg3m_per100', 0)
            c_pct = player.get('c_pct', 0)
            pf_pct = player.get('pf_pct', 0)
            rp = player.get('rim_paint_pct', 50)
            tp = player.get('three_pct', 25)
            csp = player.get('cs_pct', 30)
            height = int(player['height_inches'])
            ft_in = f"{height // 12}'{height % 12}\""

            if ast >= POINT_CENTER_AST_THRESHOLD and pts >= POINT_CENTER_PTS_THRESHOLD:
                if c_pct >= 50:
                    new_arch = 'Point Center'
                    point_center_count += 1
                else:
                    new_arch = 'Point Forward'
                    point_forward_count += 1
            elif ast >= POINT_CENTER_AST_THRESHOLD and pts >= 18.0:
                new_arch = 'Versatile Big'
                versatile_big_count += 1
            elif rp >= 75 and tp < 15:
                new_arch = 'Traditional Big'
                trad_big_count += 1
            elif tp >= 30 and csp >= 30:
                new_arch = 'Stretch Big'
                stretch_big_count += 1
            else:
                new_arch = 'Versatile Big'
                versatile_big_count += 1

            df.at[idx, 'archetype'] = new_arch
            print(f"    {player['player_name']} ({ft_in}, C%={c_pct:.0f} PF%={pf_pct:.0f}): "
                  f"{player['archetype']} -> {new_arch} (RimPaint={rp:.0f}% 3PT={tp:.0f}% C&S={csp:.0f}%)")

        print(f"  Height reclass: {point_center_count} PC, {point_forward_count} PF, "
              f"{stretch_big_count} Stretch, {trad_big_count} Trad, {versatile_big_count} Versatile")
        df = df.drop(columns=['_mk', 'height_inches'])

    print("\n  Final Versatile Big shot-zone refinement...")
    vb_mask = df['archetype'] == 'Versatile Big'
    stretch_fix = 0
    trad_fix = 0
    for idx in df[vb_mask].index:
        player = df.loc[idx]
        rp = player.get('rim_paint_pct', 50)
        tp = player.get('three_pct', 25)
        csp = player.get('cs_pct', 30)
        fg3m = player.get('fg3m_per100', 0)
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
        guard_pct = player['guard_pct']
        fg3m = player.get('fg3m_per100', 0)
        rp = player.get('rim_paint_pct', 50)
        tp = player.get('three_pct', 25)
        csp = player.get('cs_pct', 30)

        if (c_pct + pf_pct) >= 50:
            if ast >= POINT_CENTER_AST_THRESHOLD and c_pct >= 50:
                new_arch = 'Point Center'
            elif ast >= POINT_CENTER_AST_THRESHOLD:
                new_arch = 'Point Forward'
            elif tp >= 30 and csp >= 30:
                new_arch = 'Stretch Big'
            else:
                new_arch = 'Versatile Big'
        elif (sf_pct + pf_pct) > guard_pct and ast >= 7.5:
            new_arch = 'Point Forward'
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

    print("\n  Guard playmaker reclassification (high-AST guards misclassified as Combo Guard)...")
    playmaker_reclass = 0
    combo_guard_mask = df['archetype'] == 'Combo Guard'
    for idx in df[combo_guard_mask].index:
        if idx in hybrid_routed_indices:
            continue
        player = df.loc[idx]
        ast = player.get('ast_per100', 0)
        guard_pct = player['guard_pct']
        usg = player.get('usg_pct', 0)
        if guard_pct > 50 and ast >= 8.0 and usg >= 22.0:
            df.at[idx, 'archetype'] = 'Playmaker'
            playmaker_reclass += 1
            print(f"    {player['player_name']}: Combo Guard -> Playmaker "
                  f"(AST/100={ast:.1f}, G%={guard_pct:.0f}, USG={usg:.1f})")
    print(f"  Playmaker reclassification: {playmaker_reclass} guards reclassified")

    df['base_archetype'] = df['archetype'].copy()

    return df, km, scaler, cluster_labels


def validate_archetypes(df):
    known_players = {
        'Karl-Anthony Towns': 'Stretch Big',
        'Rudy Gobert': 'Traditional Big',
        'Anthony Davis': 'Traditional Big',
        'James Harden': 'Combo Guard',
        'Kevin Durant': 'Scoring Wing',
        'Kawhi Leonard': 'Scoring Wing',
        'Victor Wembanyama': 'Stretch Big',
        'Draymond Green': '3-and-D Wing',
        'Trae Young': 'Combo Guard',
        'Jalen Brunson': 'Combo Guard',
        'Jaylen Brown': 'Combo Guard',
        'Donovan Mitchell': 'Combo Guard',
        'Norman Powell': 'Combo Guard',
        'Myles Turner': 'Stretch Big',
        'Brook Lopez': 'Stretch Big',
        'Domantas Sabonis': 'Versatile Big',
        'Mikal Bridges': '3-and-D Wing',
        'Klay Thompson': 'Combo Guard',
        'Julius Randle': 'Point Forward',
        'Paolo Banchero': 'Point Forward',
        'Franz Wagner': 'Point Forward',
        'Jabari Smith': 'Stretch Big',
        'Lauri Markka': 'Scoring Wing',
        'Alperen': 'Point Center',
        'Stephen Curry': 'Combo Guard',
        'Luka Don': 'Combo Guard',
        'Shai Gilgeous': 'Combo Guard',
        'LaMelo Ball': 'Combo Guard',
        'LeBron James': 'Point Forward',
        'Nikola Jok': 'Point Center',
        'Giannis Ante': 'Point Forward',
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
    print("K-Means + Shot Zones + Shot Creation + Hustle Stats")
    print("=" * 60)

    print("\n1. Loading feature data (per-100 + positions + shot zones + shot creation + hustle)...")
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

    print("\n7. Big Man Shot Profile Summary:")
    big_archetypes = ['Traditional Big', 'Stretch Big', 'Versatile Big', 'Point Center', 'Point Forward']
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
    all_archetypes = ['3-and-D Wing', '3-and-D Guard', 'Combo Guard', 'Scoring Guard',
                      'Playmaker', 'Scoring Wing',
                      'Traditional Big', 'Stretch Big', 'Versatile Big',
                      'Point Center', 'Point Forward',
                      'Hybrid Guard', 'Hybrid Forward', 'Hybrid Big']
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

    print("\nDone!")
    return df


if __name__ == '__main__':
    main()
