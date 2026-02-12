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

FEATURES = [
    'pts_per100', 'reb_per100', 'ast_per100', 'stl_per100', 'blk_per100',
    'fg3m_per100', 'usg_pct',
    'guard_pct', 'forward_pct', 'big_pct',
    'ast_to_reb_ratio', 'scoring_versatility',
]

TARGET_K = 9


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

    is_big_cluster = bpct > 30
    is_forward_cluster = fpct > 50 and bpct < 20
    is_guard_cluster = gpct > 50

    if is_big_cluster:
        if ast > 5 and reb > 8:
            return 'Point Center'
        if fg3m > 3:
            return 'Stretch Big'
        if reb > 10 and blk > 1.5 and fg3m < 2:
            return 'Traditional Big'
        if pts > 20 or (reb > 8 and ast > 3):
            return 'Versatile Big'
        return 'Traditional Big'

    if is_forward_cluster:
        if ast > 6.5:
            return 'Point Forward'
        if pts > 27 and usg > 24:
            return 'Scoring Wing'
        if fg3m > 4 and stl > 1.8 and pts < 20:
            return '3-and-D Wing'
        if fg3m > 4 and pts < 22:
            return '3-and-D Wing'
        if pts > 22:
            return 'Scoring Wing'
        if reb > 8 and blk > 0.8:
            return 'Athletic Wing'
        return 'Athletic Wing'

    if is_guard_cluster:
        if ast > 8 and pts < 24:
            return 'Playmaker'
        if pts > 25 and ast > 5:
            return 'Combo Guard'
        if pts > 25:
            return 'Combo Guard'
        if ast > 7:
            return 'Combo Guard'
        if fg3m > 4 and stl > 1.8 and pts < 20:
            return '3-and-D Wing'
        if ast > 5:
            return 'Combo Guard'
        return 'Combo Guard'

    if ast > 6:
        return 'Point Forward' if fpct > gpct else 'Playmaker'
    if pts > 25:
        return 'Scoring Wing'
    if fg3m > 4 and stl > 1.5:
        return '3-and-D Wing'
    return 'Athletic Wing'


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
            if c['pts_per100'] > 25 or c['usg_pct'] > 25:
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

    return df, km, scaler, cluster_labels


def validate_archetypes(df):
    known_players = {
        'Nikola Jok': 'Point Center',
        'Karl-Anthony Towns': 'Stretch Big',
        'Stephen Curry': 'Combo Guard',
        'LeBron James': 'Point Forward',
        'Rudy Gobert': 'Traditional Big',
        'Mikal Bridges': '3-and-D',
        'Anthony Davis': 'Versatile Big',
        'James Harden': 'Combo Guard',
        'Giannis Ante': 'Point Forward',
        'Kevin Durant': 'Scoring Wing',
        'Kawhi Leonard': 'Scoring Wing',
        'Victor Wembanyama': 'Traditional Big',
        'Draymond Green': 'Point Forward',
    }

    print("\nValidation against known archetypes:")
    for player_fragment, expected in known_players.items():
        match = df[df['player_name'].str.contains(player_fragment, case=False, na=False)]
        if not match.empty:
            actual = match.iloc[0]['archetype']
            status = "OK" if expected.lower() in actual.lower() else "REVIEW"
            print(f"  {match.iloc[0]['player_name']}: expected={expected}, got={actual} [{status}]")
        else:
            print(f"  {player_fragment}: NOT IN TODAY'S SLATE")


def save_archetypes(df):
    conn = sqlite3.connect(DB_PATH)
    now = datetime.now().isoformat()

    save_df = df[['player_name', 'team', 'true_position', 'archetype', 'cluster']].copy()
    save_df['computed_at'] = now

    multi_team = save_df['team'].isin(['2TM', '3TM', 'TOT'])
    if multi_team.any():
        game_logs = pd.read_sql_query("""
            SELECT player_name, matchup, game_date
            FROM player_game_logs
            ORDER BY game_date DESC
        """, conn)
        game_logs['_mk'] = game_logs['player_name'].apply(_ascii_key)
        latest = game_logs.drop_duplicates(subset='_mk', keep='first')

        def extract_team(matchup):
            if not matchup or not isinstance(matchup, str):
                return None
            parts = matchup.replace('@', 'vs.').split('vs.')
            return parts[0].strip() if len(parts) >= 2 else None

        latest['current_team'] = latest['matchup'].apply(extract_team)
        team_map = dict(zip(latest['_mk'], latest['current_team']))

        save_df.loc[multi_team, '_mk'] = save_df.loc[multi_team, 'player_name'].apply(_ascii_key)
        save_df.loc[multi_team, 'team'] = save_df.loc[multi_team, '_mk'].map(team_map)
        if '_mk' in save_df.columns:
            save_df = save_df.drop(columns=['_mk'])

        resolved = save_df.loc[multi_team & save_df['team'].notna()]
        if len(resolved):
            print(f"  Resolved current team for {len(resolved)} traded players")
            for _, row in resolved.iterrows():
                print(f"    {row['player_name']} -> {row['team']}")

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
