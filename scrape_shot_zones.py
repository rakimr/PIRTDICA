"""
Scrape NBA.com shot zone data, shot creation data, and hustle stats for archetype classification.

Three data sources:
1. leaguedashplayershotlocations - Shot zone distribution (Restricted Area, Paint, Mid-Range, 3PT zones)
2. leaguedashplayerptshot - Shot creation type (Catch-and-Shoot, Pull-Up, Less Than 10ft)
3. leaguehustlestatsplayer - Defensive hustle stats (Deflections, Contested Shots, Loose Balls, Charges)

Shot data enables big man classification:
- Traditional Big: High rim/paint %, near-zero 3PT
- Stretch Big: High 3PT %, mostly catch-and-shoot (spot-up)
- Scoring Wing: High 3PT % but mostly pull-up/off-dribble (self-creator)
- Point Center: High assists + decent scoring from paint, facilitator hub

Hustle data enables guard/wing classification:
- 3-and-D Wing: High deflections + contested shots per minute (active defender)
- Scoring Wing: Low defensive activity, high USG (offensive-first)
- Combo Guard: Moderate defensive activity
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


def scrape_hustle_stats():
    from nba_api.stats.endpoints import leaguehustlestatsplayer

    print("\nFetching hustle stats from NBA.com (leaguehustlestatsplayer)...")
    time.sleep(1)

    hustle = leaguehustlestatsplayer.LeagueHustleStatsPlayer(
        season=SEASON,
        season_type_all_star='Regular Season',
        per_mode_time='Totals'
    )

    raw = hustle.get_data_frames()[0]
    print(f"  Got data for {len(raw)} players")

    rows = []
    for _, r in raw.iterrows():
        player_name = r.get('PLAYER_NAME', '')
        player_id = r.get('PLAYER_ID', '')
        team = r.get('TEAM_ABBREVIATION', '')
        gp = int(r.get('G', 0))
        minutes = float(r.get('MIN', 0))

        if gp < 5 or minutes < 50:
            continue

        contested = float(r.get('CONTESTED_SHOTS', 0) or 0)
        contested_2pt = float(r.get('CONTESTED_SHOTS_2PT', 0) or 0)
        contested_3pt = float(r.get('CONTESTED_SHOTS_3PT', 0) or 0)
        deflections = float(r.get('DEFLECTIONS', 0) or 0)
        charges = float(r.get('CHARGES_DRAWN', 0) or 0)
        screen_assists = float(r.get('SCREEN_ASSISTS', 0) or 0)
        loose_off = float(r.get('OFF_LOOSE_BALLS_RECOVERED', 0) or 0)
        loose_def = float(r.get('DEF_LOOSE_BALLS_RECOVERED', 0) or 0)
        loose_total = float(r.get('LOOSE_BALLS_RECOVERED', 0) or 0)
        box_outs = float(r.get('BOX_OUTS', 0) or 0)

        defl_per_min = deflections / minutes * 48 if minutes > 0 else 0
        contested_per_min = contested / minutes * 48 if minutes > 0 else 0
        loose_per_min = loose_total / minutes * 48 if minutes > 0 else 0
        charges_per_min = charges / minutes * 48 if minutes > 0 else 0
        screen_ast_per_min = screen_assists / minutes * 48 if minutes > 0 else 0
        box_outs_per_min = box_outs / minutes * 48 if minutes > 0 else 0

        rows.append({
            'player_name': player_name,
            'player_id': int(player_id),
            'team': team,
            'gp': gp,
            'minutes': round(minutes, 1),
            'contested_shots': int(contested),
            'contested_2pt': int(contested_2pt),
            'contested_3pt': int(contested_3pt),
            'deflections': int(deflections),
            'charges_drawn': int(charges),
            'screen_assists': int(screen_assists),
            'loose_balls_off': int(loose_off),
            'loose_balls_def': int(loose_def),
            'loose_balls_total': int(loose_total),
            'box_outs': int(box_outs),
            'deflections_per48': round(defl_per_min, 2),
            'contested_per48': round(contested_per_min, 2),
            'loose_per48': round(loose_per_min, 2),
            'charges_per48': round(charges_per_min, 2),
            'screen_ast_per48': round(screen_ast_per_min, 2),
            'box_outs_per48': round(box_outs_per_min, 2),
        })

    df = pd.DataFrame(rows)
    print(f"  Processed {len(df)} players with 5+ GP and 50+ MIN")
    return df


def scrape_tracking_stats():
    from nba_api.stats.endpoints import leaguedashptstats

    print("\nFetching player tracking stats from NBA.com (leaguedashptstats - Possessions)...")
    time.sleep(1)

    tracking = leaguedashptstats.LeagueDashPtStats(
        season=SEASON,
        season_type_all_star='Regular Season',
        per_mode_simple='PerGame',
        player_or_team='Player',
        pt_measure_type='Possessions'
    )

    raw = tracking.get_data_frames()[0]
    print(f"  Got data for {len(raw)} players")

    rows = []
    for _, r in raw.iterrows():
        player_name = r.get('PLAYER_NAME', '')
        player_id = r.get('PLAYER_ID', '')
        team = r.get('TEAM_ABBREVIATION', '')
        gp = int(r.get('GP', 0) or 0)
        minutes = float(r.get('MIN', 0) or 0)

        if gp < 5 or minutes < 5.0:
            continue

        touches = float(r.get('TOUCHES', 0) or 0)
        front_ct_touches = float(r.get('FRONT_CT_TOUCHES', 0) or 0)
        time_of_poss = float(r.get('TIME_OF_POSS', 0) or 0)
        avg_sec_per_touch = float(r.get('AVG_SEC_PER_TOUCH', 0) or 0)
        avg_drib_per_touch = float(r.get('AVG_DRIB_PER_TOUCH', 0) or 0)
        elbow_touches = float(r.get('ELBOW_TOUCHES', 0) or 0)
        post_touches = float(r.get('POST_TOUCHES', 0) or 0)
        paint_touches = float(r.get('PAINT_TOUCHES', 0) or 0)

        touches_per_min = touches / minutes if minutes > 0 else 0
        front_ct_per_min = front_ct_touches / minutes if minutes > 0 else 0
        time_of_poss_per_min = time_of_poss / minutes if minutes > 0 else 0

        rows.append({
            'player_name': player_name,
            'player_id': int(player_id),
            'team': team,
            'gp': gp,
            'minutes_pg': round(minutes, 1),
            'touches_pg': round(touches, 1),
            'front_ct_touches_pg': round(front_ct_touches, 1),
            'time_of_poss_pg': round(time_of_poss, 2),
            'avg_sec_per_touch': round(avg_sec_per_touch, 2),
            'avg_drib_per_touch': round(avg_drib_per_touch, 2),
            'elbow_touches_pg': round(elbow_touches, 1),
            'post_touches_pg': round(post_touches, 1),
            'paint_touches_pg': round(paint_touches, 1),
            'touches_per_min': round(touches_per_min, 3),
            'front_ct_per_min': round(front_ct_per_min, 3),
            'time_of_poss_pct': round(time_of_poss_per_min, 4),
        })

    df = pd.DataFrame(rows)
    print(f"  Processed {len(df)} players with 5+ GP and 5+ MIN/g")
    return df


def save_to_db(zones_df, creation_df, hustle_df=None, tracking_df=None):
    conn = sqlite3.connect(DB_PATH)
    now = datetime.now().isoformat()

    zones_df['scraped_at'] = now
    zones_df.to_sql('player_shot_zones', conn, if_exists='replace', index=False)
    print(f"\nSaved {len(zones_df)} players to player_shot_zones table")

    creation_df['scraped_at'] = now
    creation_df.to_sql('player_shot_creation', conn, if_exists='replace', index=False)
    print(f"Saved {len(creation_df)} players to player_shot_creation table")

    if hustle_df is not None:
        hustle_df['scraped_at'] = now
        hustle_df.to_sql('player_hustle_stats', conn, if_exists='replace', index=False)
        print(f"Saved {len(hustle_df)} players to player_hustle_stats table")

    if tracking_df is not None:
        tracking_df['scraped_at'] = now
        tracking_df.to_sql('player_tracking_stats', conn, if_exists='replace', index=False)
        print(f"Saved {len(tracking_df)} players to player_tracking_stats table")

    conn.close()


def show_big_man_audit(zones_df, creation_df):
    conn = sqlite3.connect(DB_PATH)
    archetypes = pd.read_sql_query(
        "SELECT player_name, archetype FROM player_archetypes WHERE archetype IN "
        "('Traditional Big', 'Versatile Big', 'Stretch 4', 'Stretch 5', 'Point Center', 'Point Forward')",
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

    for arch in ['Traditional Big', 'Stretch 4', 'Stretch 5', 'Versatile Big', 'Point Center', 'Point Forward']:
        subset = merged[merged['archetype'] == arch].sort_values('three_pct', ascending=False)
        if subset.empty:
            continue
        for _, r in subset.iterrows():
            print(f"{r['player_name']:<25} {r['archetype']:<18} {r['rim_paint_pct']:>8.1f}% {r['three_pct']:>4.1f} {r['cs_pct']:>4.1f} {r['pu_pct']:>4.1f} {r['paint_pct']:>5.1f} {r['cs_3_share']:>6.1f}% {int(r['total_fga']):>5}")
        print()


def main():
    print("=" * 60)
    print("NBA.COM SHOT ZONE, CREATION, HUSTLE & TRACKING STATS SCRAPER")
    print(f"Season: {SEASON}")
    print("=" * 60)

    zones_df = scrape_shot_locations()
    creation_df = scrape_shot_creation()
    hustle_df = scrape_hustle_stats()
    tracking_df = scrape_tracking_stats()
    save_to_db(zones_df, creation_df, hustle_df, tracking_df)
    show_big_man_audit(zones_df, creation_df)

    print("\n" + "=" * 80)
    print("DEFENSIVE HUSTLE LEADERS (per 48 min)")
    print("=" * 80)
    top = hustle_df.nlargest(15, 'deflections_per48')
    print(f"{'Player':<25} {'Defl/48':>8} {'Contest/48':>11} {'Loose/48':>9} {'Charges/48':>11}")
    print('-' * 70)
    for _, r in top.iterrows():
        print(f"{r['player_name']:<25} {r['deflections_per48']:>8.2f} {r['contested_per48']:>11.2f} "
              f"{r['loose_per48']:>9.2f} {r['charges_per48']:>11.2f}")

    print("\n" + "=" * 80)
    print("BALL INITIATION LEADERS (per game)")
    print("=" * 80)
    top_t = tracking_df.nlargest(20, 'touches_pg')
    print(f"{'Player':<25} {'Touch/G':>8} {'FrCt/G':>8} {'ToP/G':>7} {'Sec/Tch':>8} {'Drib/Tch':>9}")
    print('-' * 75)
    for _, r in top_t.iterrows():
        print(f"{r['player_name']:<25} {r['touches_pg']:>8.1f} {r['front_ct_touches_pg']:>8.1f} "
              f"{r['time_of_poss_pg']:>7.2f} {r['avg_sec_per_touch']:>8.2f} {r['avg_drib_per_touch']:>9.2f}")

    print("\nDone!")


if __name__ == '__main__':
    main()
