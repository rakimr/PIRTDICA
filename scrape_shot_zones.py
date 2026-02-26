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
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.nba_api_helpers import nba_api_call_with_retry, inter_call_delay

DB_PATH = 'dfs_nba.db'
SEASON = '2025-26'


def scrape_shot_locations():
    from nba_api.stats.endpoints import leaguedashplayershotlocations

    print("Fetching shot zone data from NBA.com (leaguedashplayershotlocations)...")

    raw = nba_api_call_with_retry(
        leaguedashplayershotlocations.LeagueDashPlayerShotLocations,
        "shot locations",
        season=SEASON,
        season_type_all_star='Regular Season',
        distance_range='By Zone',
        per_mode_detailed='Totals'
    )
    if raw is None:
        print("  Skipping shot locations - using cached data")
        return None
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
    overall = nba_api_call_with_retry(
        leaguedashplayerptshot.LeagueDashPlayerPtShot, "shot creation - Overall",
        season=SEASON, season_type_all_star='Regular Season',
        per_mode_simple='Totals', general_range_nullable='Overall'
    )
    if overall is None:
        print("  Skipping shot creation - using cached data")
        return None

    print("  Fetching Catch and Shoot...")
    inter_call_delay()
    catch_shoot = nba_api_call_with_retry(
        leaguedashplayerptshot.LeagueDashPlayerPtShot, "shot creation - C&S",
        season=SEASON, season_type_all_star='Regular Season',
        per_mode_simple='Totals', general_range_nullable='Catch and Shoot'
    )

    print("  Fetching Pull Ups...")
    inter_call_delay()
    pullups = nba_api_call_with_retry(
        leaguedashplayerptshot.LeagueDashPlayerPtShot, "shot creation - Pullups",
        season=SEASON, season_type_all_star='Regular Season',
        per_mode_simple='Totals', general_range_nullable='Pullups'
    )

    print("  Fetching Less Than 10ft...")
    inter_call_delay()
    paint = nba_api_call_with_retry(
        leaguedashplayerptshot.LeagueDashPlayerPtShot, "shot creation - Paint",
        season=SEASON, season_type_all_star='Regular Season',
        per_mode_simple='Totals', general_range_nullable='Less Than 10 ft'
    )

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
    time.sleep(2)

    raw = nba_api_call_with_retry(
        leaguehustlestatsplayer.LeagueHustleStatsPlayer, "hustle stats",
        season=SEASON, season_type_all_star='Regular Season',
        per_mode_time='Totals'
    )
    if raw is None:
        print("  Skipping hustle stats - using cached data")
        return None
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
    time.sleep(2)

    raw = nba_api_call_with_retry(
        leaguedashptstats.LeagueDashPtStats, "tracking stats",
        season=SEASON, season_type_all_star='Regular Season',
        per_mode_simple='PerGame', player_or_team='Player',
        pt_measure_type='Possessions'
    )
    if raw is None:
        print("  Skipping tracking stats - using cached data")
        return None
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


def upsert_table(conn, table_name, new_df, key_col='player_name'):
    now = datetime.now().isoformat()
    new_df = new_df.copy()
    new_df['scraped_at'] = now

    try:
        existing_df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    except Exception:
        existing_df = pd.DataFrame()

    if existing_df.empty:
        new_df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"  {table_name}: Inserted {len(new_df)} players (fresh table)")
        return

    new_keys = set(new_df[key_col].values)
    existing_keys = set(existing_df[key_col].values)

    updated = len(new_keys & existing_keys)
    new_additions = len(new_keys - existing_keys)
    preserved = len(existing_keys - new_keys)

    preserved_df = existing_df[~existing_df[key_col].isin(new_keys)]
    combined = pd.concat([new_df, preserved_df], ignore_index=True)
    combined.to_sql(table_name, conn, if_exists='replace', index=False)

    print(f"  {table_name}: {updated} updated, {new_additions} new, {preserved} preserved → {len(combined)} total")


def save_to_db(zones_df, creation_df, hustle_df=None, tracking_df=None):
    conn = sqlite3.connect(DB_PATH)

    if len(zones_df) > 0:
        upsert_table(conn, 'player_shot_zones', zones_df)

    if len(creation_df) > 0:
        upsert_table(conn, 'player_shot_creation', creation_df)

    if hustle_df is not None:
        upsert_table(conn, 'player_hustle_stats', hustle_df)

    if tracking_df is not None:
        upsert_table(conn, 'player_tracking_stats', tracking_df)

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


def get_top_player_ids(n=30):
    import unicodedata
    from nba_api.stats.static import players as nba_players

    def normalize_name(s):
        try:
            s = s.encode('latin-1').decode('utf-8')
        except (UnicodeDecodeError, UnicodeEncodeError):
            pass
        return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

    conn = sqlite3.connect(DB_PATH)
    try:
        top = pd.read_sql(
            f"SELECT player_name FROM player_stats ORDER BY fp_pg DESC LIMIT {n}", conn
        )
    except Exception as e:
        print(f"  Could not load top player list: {e}")
        return {}
    finally:
        conn.close()

    all_nba = {p['full_name']: p['id'] for p in nba_players.get_players() if p['is_active']}
    ascii_to_full = {normalize_name(full).lower(): full for full in all_nba}

    result = {}
    for name in top['player_name']:
        if name in all_nba:
            result[name] = all_nba[name]
        else:
            ascii_name = normalize_name(name).lower()
            if ascii_name in ascii_to_full:
                result[name] = all_nba[ascii_to_full[ascii_name]]
            else:
                last = name.split()[-1]
                matches = [full for full in all_nba if normalize_name(last).lower() in normalize_name(full).lower()]
                if len(matches) == 1:
                    result[name] = all_nba[matches[0]]
    return result


def backfill_missing_shot_zones(player_name, player_id):
    from nba_api.stats.endpoints import playerdashboardbyshootingsplits

    try:
        result = playerdashboardbyshootingsplits.PlayerDashboardByShootingSplits(
            player_id=player_id, season=SEASON,
            season_type_playoffs='Regular Season',
            per_mode_detailed='Totals',
            timeout=NBA_TIMEOUT, headers=NBA_HEADERS
        )
        dfs = result.get_data_frames()
        shot_area_df = dfs[2] if len(dfs) > 2 else None
        if shot_area_df is None or shot_area_df.empty:
            return None

        ra_row = shot_area_df[shot_area_df['GROUP_VALUE'] == 'Restricted Area']
        paint_row = shot_area_df[shot_area_df['GROUP_VALUE'] == 'In The Paint (Non-RA)']
        mid_row = shot_area_df[shot_area_df['GROUP_VALUE'] == 'Mid-Range']
        three_rows = shot_area_df[shot_area_df['GROUP_VALUE'].str.contains('3', na=False)]

        ra_fga = int(ra_row['FGA'].values[0]) if len(ra_row) > 0 else 0
        ra_fgm = int(ra_row['FGM'].values[0]) if len(ra_row) > 0 else 0
        paint_fga = int(paint_row['FGA'].values[0]) if len(paint_row) > 0 else 0
        paint_fgm = int(paint_row['FGM'].values[0]) if len(paint_row) > 0 else 0
        mid_fga = int(mid_row['FGA'].values[0]) if len(mid_row) > 0 else 0
        mid_fgm = int(mid_row['FGM'].values[0]) if len(mid_row) > 0 else 0
        three_fga = int(three_rows['FGA'].sum()) if len(three_rows) > 0 else 0
        three_fgm = int(three_rows['FGM'].sum()) if len(three_rows) > 0 else 0

        total_fga = ra_fga + paint_fga + mid_fga + three_fga
        if total_fga < 20:
            return None

        rim_paint_fga = ra_fga + paint_fga
        return {
            'player_name': player_name, 'player_id': int(player_id), 'team': '',
            'total_fga': total_fga,
            'ra_fga': ra_fga, 'ra_fgm': ra_fgm,
            'paint_fga': paint_fga, 'paint_fgm': paint_fgm,
            'mid_fga': mid_fga, 'mid_fgm': mid_fgm,
            'three_fga': three_fga, 'three_fgm': three_fgm,
            'corner3_fga': 0, 'atb3_fga': three_fga,
            'ra_pct': round(ra_fga / total_fga * 100, 1),
            'paint_pct': round(paint_fga / total_fga * 100, 1),
            'rim_paint_pct': round(rim_paint_fga / total_fga * 100, 1),
            'mid_pct': round(mid_fga / total_fga * 100, 1),
            'three_pct': round(three_fga / total_fga * 100, 1),
        }
    except Exception as e:
        print(f"    Shot zones backfill failed for {player_name}: {e}")
        return None


def backfill_missing_creation(player_name, player_id):
    from nba_api.stats.endpoints import playerdashptshots

    try:
        result = playerdashptshots.PlayerDashPtShots(
            player_id=player_id, team_id=0, season=SEASON,
            season_type_all_star='Regular Season',
            per_mode_simple='Totals',
            timeout=NBA_TIMEOUT, headers=NBA_HEADERS
        )
        dfs = result.get_data_frames()
        if not dfs or dfs[0].empty:
            return None

        general = dfs[0]
        overall = general[general['SHOT_TYPE'] == 'Overall'] if 'SHOT_TYPE' in general.columns else general
        cs = general[general['SHOT_TYPE'] == 'Catch and Shoot'] if 'SHOT_TYPE' in general.columns else pd.DataFrame()
        pu = general[general['SHOT_TYPE'] == 'Pullups'] if 'SHOT_TYPE' in general.columns else pd.DataFrame()

        total_fga = int(overall['FGA'].values[0]) if len(overall) > 0 else 0
        if total_fga < 20:
            return None

        cs_fga = int(cs['FGA'].values[0]) if len(cs) > 0 else 0
        cs_fgm = int(cs['FGM'].values[0]) if len(cs) > 0 else 0
        cs_3fga = int(cs['FG3A'].values[0]) if len(cs) > 0 and 'FG3A' in cs.columns else 0
        cs_3fgm = int(cs['FG3M'].values[0]) if len(cs) > 0 and 'FG3M' in cs.columns else 0
        pu_fga = int(pu['FGA'].values[0]) if len(pu) > 0 else 0
        pu_fgm = int(pu['FGM'].values[0]) if len(pu) > 0 else 0
        pu_3fga = int(pu['FG3A'].values[0]) if len(pu) > 0 and 'FG3A' in pu.columns else 0
        pu_3fgm = int(pu['FG3M'].values[0]) if len(pu) > 0 and 'FG3M' in pu.columns else 0
        gp = int(overall['GP'].values[0]) if len(overall) > 0 and 'GP' in overall.columns else 0

        return {
            'player_name': player_name, 'player_id': int(player_id),
            'gp': gp, 'total_fga': total_fga,
            'cs_fga': cs_fga, 'cs_fgm': cs_fgm,
            'cs_3fga': cs_3fga, 'cs_3fgm': cs_3fgm,
            'pu_fga': pu_fga, 'pu_fgm': pu_fgm,
            'pu_3fga': pu_3fga, 'pu_3fgm': pu_3fgm,
            'paint_fga': 0, 'paint_fgm': 0,
            'cs_pct': round(cs_fga / total_fga * 100, 1) if total_fga > 0 else 0,
            'pu_pct': round(pu_fga / total_fga * 100, 1) if total_fga > 0 else 0,
            'paint_pct': 0,
            'cs_3_share': round(cs_3fga / (cs_3fga + pu_3fga) * 100, 1) if (cs_3fga + pu_3fga) > 0 else 0,
            'pu_3_share': round(pu_3fga / (cs_3fga + pu_3fga) * 100, 1) if (cs_3fga + pu_3fga) > 0 else 0,
        }
    except Exception as e:
        print(f"    Shot creation backfill failed for {player_name}: {e}")
        return None


def backfill_missing_hustle(player_name, player_id):
    from nba_api.stats.endpoints import playerdashboardbygeneralsplits

    try:
        result = playerdashboardbygeneralsplits.PlayerDashboardByGeneralSplits(
            player_id=player_id, season=SEASON,
            season_type_all_star='Regular Season',
            per_mode_detailed='Totals',
            timeout=NBA_TIMEOUT, headers=NBA_HEADERS
        )
        dfs = result.get_data_frames()
        if not dfs or dfs[0].empty:
            return None

        overall = dfs[0].iloc[0]
        gp = int(overall.get('GP', 0))
        minutes = float(overall.get('MIN', 0))
        if gp < 5 or minutes < 50:
            return None

        return {
            'player_name': player_name, 'player_id': int(player_id), 'team': '',
            'gp': gp, 'minutes': round(minutes, 1),
            'contested_shots': 0, 'contested_2pt': 0, 'contested_3pt': 0,
            'deflections': 0, 'charges_drawn': 0, 'screen_assists': 0,
            'loose_balls_off': 0, 'loose_balls_def': 0, 'loose_balls_total': 0,
            'box_outs': 0,
            'deflections_per48': 0, 'contested_per48': 0,
            'loose_per48': 0, 'charges_per48': 0,
            'screen_ast_per48': 0, 'box_outs_per48': 0,
        }
    except Exception:
        return None


def run_backfill():
    print("\n" + "=" * 60)
    print("BACKFILL: Checking for missing top players...")
    print("=" * 60)

    top_players = get_top_player_ids(30)
    if not top_players:
        print("  No top player list available, skipping backfill")
        return

    conn = sqlite3.connect(DB_PATH)
    tables = {
        'player_shot_zones': ('player_name', backfill_missing_shot_zones),
        'player_shot_creation': ('player_name', backfill_missing_creation),
    }

    for table_name, (key_col, backfill_fn) in tables.items():
        try:
            existing = pd.read_sql(f"SELECT {key_col} FROM {table_name}", conn)
            existing_names = set(existing[key_col].values)
        except Exception:
            existing_names = set()

        missing = {name: pid for name, pid in top_players.items() if name not in existing_names}
        if not missing:
            print(f"  {table_name}: All top-30 players present")
            continue

        print(f"  {table_name}: {len(missing)} top players missing: {list(missing.keys())}")
        recovered = []
        for name, pid in missing.items():
            time.sleep(2)
            row = backfill_fn(name, pid)
            if row:
                recovered.append(row)
                print(f"    Recovered: {name}")

        if recovered:
            recovered_df = pd.DataFrame(recovered)
            upsert_table(conn, table_name, recovered_df, key_col)
            print(f"  Backfill recovered {len(recovered)}/{len(missing)} missing players for {table_name}")
        else:
            print(f"  Backfill: Could not recover any missing players for {table_name}")

    conn.close()


def main():
    print("=" * 60)
    print("NBA.COM SHOT ZONE, CREATION, HUSTLE & TRACKING STATS SCRAPER")
    print(f"Season: {SEASON}")
    print("=" * 60)

    zones_df = scrape_shot_locations()
    inter_call_delay()
    creation_df = scrape_shot_creation()
    inter_call_delay()
    hustle_df = scrape_hustle_stats()
    inter_call_delay()
    tracking_df = scrape_tracking_stats()

    if zones_df is None and creation_df is None and hustle_df is None and tracking_df is None:
        print("\nWARNING: All NBA.com endpoints unreachable - using cached data from SQLite")
        print("Done! (cached data preserved)")
        return

    if zones_df is not None and creation_df is not None:
        save_to_db(zones_df, creation_df, hustle_df, tracking_df)
        show_big_man_audit(zones_df, creation_df)
    elif zones_df is not None or creation_df is not None:
        save_to_db(
            zones_df if zones_df is not None else pd.DataFrame(),
            creation_df if creation_df is not None else pd.DataFrame(),
            hustle_df, tracking_df
        )

    run_backfill()

    if hustle_df is not None:
        print("\n" + "=" * 80)
        print("DEFENSIVE HUSTLE LEADERS (per 48 min)")
        print("=" * 80)
        top = hustle_df.nlargest(15, 'deflections_per48')
        print(f"{'Player':<25} {'Defl/48':>8} {'Contest/48':>11} {'Loose/48':>9} {'Charges/48':>11}")
        print('-' * 70)
        for _, r in top.iterrows():
            print(f"{r['player_name']:<25} {r['deflections_per48']:>8.2f} {r['contested_per48']:>11.2f} "
                  f"{r['loose_per48']:>9.2f} {r['charges_per48']:>11.2f}")

    if tracking_df is not None:
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
