"""
Scrape NBA.com shot zone data and shot creation data for archetype classification.

Two data sources:
1. leaguedashplayershotlocations - Shot zone distribution (Restricted Area, Paint, Mid-Range, 3PT zones)
2. leaguedashplayerptshot - Shot creation type (Catch-and-Shoot, Pull-Up, Less Than 10ft)

These enable precise big man classification:
- Traditional Big: High rim/paint %, near-zero 3PT
- Stretch Big: High 3PT %, mostly catch-and-shoot (spot-up)
- Scoring Wing: High 3PT % but mostly pull-up/off-dribble (self-creator)
- Point Center: High assists + decent scoring from paint, facilitator hub
"""

import sqlite3
import pandas as pd
import numpy as np
import time
from datetime import datetime


DB_PATH = 'dfs_nba.db'
SEASON = '2025-26'


def scrape_shot_locations():
    from nba_api.stats.endpoints import leaguedashplayershotlocations

    print("Fetching shot zone data from NBA.com (leaguedashplayershotlocations)...")
    time.sleep(1)

    shot_locs = leaguedashplayershotlocations.LeagueDashPlayerShotLocations(
        season=SEASON,
        season_type_all_star='Regular Season',
        distance_range='By Zone',
        per_mode_detailed='Totals'
    )

    raw = shot_locs.get_data_frames()[0]
    print(f"  Got data for {len(raw)} players")

    raw.columns = [f"{c[0]}_{c[1]}".strip('_') if c[0] else c[1] for c in raw.columns]

    rows = []
    for _, r in raw.iterrows():
        player_name = r.get('PLAYER_NAME', '')
        player_id = r.get('PLAYER_ID', '')
        team = r.get('TEAM_ABBREVIATION', '')

        def _safe(val):
            if val is None or (isinstance(val, float) and np.isnan(val)):
                return 0
            return val

        ra_fga = _safe(r.get('Restricted Area_FGA', 0))
        ra_fgm = _safe(r.get('Restricted Area_FGM', 0))
        paint_fga = _safe(r.get('In The Paint (Non-RA)_FGA', 0))
        paint_fgm = _safe(r.get('In The Paint (Non-RA)_FGM', 0))
        mid_fga = _safe(r.get('Mid-Range_FGA', 0))
        mid_fgm = _safe(r.get('Mid-Range_FGM', 0))
        lc3_fga = _safe(r.get('Left Corner 3_FGA', 0))
        lc3_fgm = _safe(r.get('Left Corner 3_FGM', 0))
        rc3_fga = _safe(r.get('Right Corner 3_FGA', 0))
        rc3_fgm = _safe(r.get('Right Corner 3_FGM', 0))
        atb3_fga = _safe(r.get('Above the Break 3_FGA', 0))
        atb3_fgm = _safe(r.get('Above the Break 3_FGM', 0))

        total_fga = ra_fga + paint_fga + mid_fga + lc3_fga + rc3_fga + atb3_fga
        three_fga = lc3_fga + rc3_fga + atb3_fga
        three_fgm = lc3_fgm + rc3_fgm + atb3_fgm
        rim_paint_fga = ra_fga + paint_fga

        if total_fga < 20:
            continue

        rows.append({
            'player_name': player_name,
            'player_id': int(player_id),
            'team': team,
            'total_fga': int(total_fga),
            'ra_fga': int(ra_fga),
            'ra_fgm': int(ra_fgm),
            'paint_fga': int(paint_fga),
            'paint_fgm': int(paint_fgm),
            'mid_fga': int(mid_fga),
            'mid_fgm': int(mid_fgm),
            'three_fga': int(three_fga),
            'three_fgm': int(three_fgm),
            'corner3_fga': int(lc3_fga + rc3_fga),
            'atb3_fga': int(atb3_fga),
            'ra_pct': round(ra_fga / total_fga * 100, 1),
            'paint_pct': round(paint_fga / total_fga * 100, 1),
            'rim_paint_pct': round(rim_paint_fga / total_fga * 100, 1),
            'mid_pct': round(mid_fga / total_fga * 100, 1),
            'three_pct': round(three_fga / total_fga * 100, 1),
        })

    df = pd.DataFrame(rows)
    print(f"  Processed {len(df)} players with 20+ FGA")
    return df


def scrape_shot_creation():
    from nba_api.stats.endpoints import leaguedashplayerptshot

    print("\nFetching shot creation data from NBA.com (leaguedashplayerptshot)...")

    print("  Fetching Overall totals...")
    time.sleep(1)
    overall = leaguedashplayerptshot.LeagueDashPlayerPtShot(
        season=SEASON,
        season_type_all_star='Regular Season',
        per_mode_simple='Totals',
        general_range_nullable='Overall'
    ).get_data_frames()[0]

    print("  Fetching Catch and Shoot...")
    time.sleep(1)
    catch_shoot = leaguedashplayerptshot.LeagueDashPlayerPtShot(
        season=SEASON,
        season_type_all_star='Regular Season',
        per_mode_simple='Totals',
        general_range_nullable='Catch and Shoot'
    ).get_data_frames()[0]

    print("  Fetching Pull Ups...")
    time.sleep(1)
    pullups = leaguedashplayerptshot.LeagueDashPlayerPtShot(
        season=SEASON,
        season_type_all_star='Regular Season',
        per_mode_simple='Totals',
        general_range_nullable='Pullups'
    ).get_data_frames()[0]

    print("  Fetching Less Than 10ft...")
    time.sleep(1)
    paint = leaguedashplayerptshot.LeagueDashPlayerPtShot(
        season=SEASON,
        season_type_all_star='Regular Season',
        per_mode_simple='Totals',
        general_range_nullable='Less Than 10 ft'
    ).get_data_frames()[0]

    rows = []
    for _, ov in overall.iterrows():
        pid = ov['PLAYER_ID']
        name = ov['PLAYER_NAME']
        total_fga = ov['FGA']

        if total_fga < 20:
            continue

        cs = catch_shoot[catch_shoot['PLAYER_ID'] == pid]
        pu = pullups[pullups['PLAYER_ID'] == pid]
        pt = paint[paint['PLAYER_ID'] == pid]

        cs_fga = int(cs['FGA'].values[0]) if len(cs) > 0 else 0
        cs_fgm = int(cs['FGM'].values[0]) if len(cs) > 0 else 0
        cs_3fga = int(cs['FG3A'].values[0]) if len(cs) > 0 else 0
        cs_3fgm = int(cs['FG3M'].values[0]) if len(cs) > 0 else 0

        pu_fga = int(pu['FGA'].values[0]) if len(pu) > 0 else 0
        pu_fgm = int(pu['FGM'].values[0]) if len(pu) > 0 else 0
        pu_3fga = int(pu['FG3A'].values[0]) if len(pu) > 0 else 0
        pu_3fgm = int(pu['FG3M'].values[0]) if len(pu) > 0 else 0

        pt_fga = int(pt['FGA'].values[0]) if len(pt) > 0 else 0
        pt_fgm = int(pt['FGM'].values[0]) if len(pt) > 0 else 0

        gp = int(ov['GP'])
        total_fga_int = int(total_fga)

        rows.append({
            'player_name': name,
            'player_id': int(pid),
            'gp': gp,
            'total_fga': total_fga_int,
            'cs_fga': cs_fga,
            'cs_fgm': cs_fgm,
            'cs_3fga': cs_3fga,
            'cs_3fgm': cs_3fgm,
            'pu_fga': pu_fga,
            'pu_fgm': pu_fgm,
            'pu_3fga': pu_3fga,
            'pu_3fgm': pu_3fgm,
            'paint_fga': pt_fga,
            'paint_fgm': pt_fgm,
            'cs_pct': round(cs_fga / total_fga_int * 100, 1) if total_fga_int > 0 else 0,
            'pu_pct': round(pu_fga / total_fga_int * 100, 1) if total_fga_int > 0 else 0,
            'paint_pct': round(pt_fga / total_fga_int * 100, 1) if total_fga_int > 0 else 0,
            'cs_3_share': round(cs_3fga / (cs_3fga + pu_3fga) * 100, 1) if (cs_3fga + pu_3fga) > 0 else 0,
            'pu_3_share': round(pu_3fga / (cs_3fga + pu_3fga) * 100, 1) if (cs_3fga + pu_3fga) > 0 else 0,
        })

    df = pd.DataFrame(rows)
    print(f"  Processed {len(df)} players with 20+ FGA")
    return df


def save_to_db(zones_df, creation_df):
    conn = sqlite3.connect(DB_PATH)
    now = datetime.now().isoformat()

    zones_df['scraped_at'] = now
    zones_df.to_sql('player_shot_zones', conn, if_exists='replace', index=False)
    print(f"\nSaved {len(zones_df)} players to player_shot_zones table")

    creation_df['scraped_at'] = now
    creation_df.to_sql('player_shot_creation', conn, if_exists='replace', index=False)
    print(f"Saved {len(creation_df)} players to player_shot_creation table")

    conn.close()


def show_big_man_audit(zones_df, creation_df):
    conn = sqlite3.connect(DB_PATH)
    archetypes = pd.read_sql_query(
        "SELECT player_name, archetype FROM player_archetypes WHERE archetype IN "
        "('Traditional Big', 'Versatile Big', 'Stretch Big', 'Point Center', 'Point Forward')",
        conn
    )
    conn.close()

    if archetypes.empty:
        print("\nNo archetypes found for audit")
        return

    merged = archetypes.merge(zones_df[['player_name', 'ra_pct', 'rim_paint_pct', 'three_pct', 'total_fga']],
                               on='player_name', how='inner')
    merged = merged.merge(creation_df[['player_name', 'cs_pct', 'pu_pct', 'paint_pct', 'cs_3_share']],
                           on='player_name', how='inner')

    print(f"\n{'='*100}")
    print("BIG MAN SHOT PROFILE AUDIT")
    print(f"{'='*100}")
    print(f"{'Player':<25} {'Archetype':<18} {'RimPaint%':>9} {'3PT%':>5} {'C&S%':>5} {'PU%':>5} {'Paint%':>6} {'CS3Shr':>7} {'FGA':>5}")
    print('-'*100)

    for arch in ['Traditional Big', 'Stretch Big', 'Versatile Big', 'Point Center', 'Point Forward']:
        subset = merged[merged['archetype'] == arch].sort_values('three_pct', ascending=False)
        if subset.empty:
            continue
        for _, r in subset.iterrows():
            print(f"{r['player_name']:<25} {r['archetype']:<18} {r['rim_paint_pct']:>8.1f}% {r['three_pct']:>4.1f} {r['cs_pct']:>4.1f} {r['pu_pct']:>4.1f} {r['paint_pct']:>5.1f} {r['cs_3_share']:>6.1f}% {int(r['total_fga']):>5}")
        print()


def main():
    print("=" * 60)
    print("NBA.COM SHOT ZONE & CREATION SCRAPER")
    print(f"Season: {SEASON}")
    print("=" * 60)

    zones_df = scrape_shot_locations()
    creation_df = scrape_shot_creation()
    save_to_db(zones_df, creation_df)
    show_big_man_audit(zones_df, creation_df)

    print("\nDone!")


if __name__ == '__main__':
    main()
