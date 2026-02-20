"""
Scrape NBA.com team opponent shot zone data for defensive shot charts.

Data source: leaguedashteamshotlocations (opponent shooting by zone per team)
Shows where opponents shoot against each team relative to league average.
Zones: Restricted Area, Paint (Non-RA), Mid-Range, Above Break 3, Corner 3
"""

import sqlite3
import pandas as pd
import numpy as np
import time
from datetime import datetime


DB_PATH = 'dfs_nba.db'
SEASON = '2025-26'
MAX_RETRIES = 2
RETRY_DELAYS = [5, 15]
NBA_TIMEOUT = 60

NBA_HEADERS = {
    'Host': 'stats.nba.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'x-nba-stats-origin': 'stats',
    'x-nba-stats-token': 'true',
    'Connection': 'keep-alive',
    'Referer': 'https://www.nba.com/',
    'Origin': 'https://www.nba.com',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
}


TEAM_NAME_TO_ABBR = {
    'Atlanta Hawks': 'ATL', 'Boston Celtics': 'BOS', 'Brooklyn Nets': 'BKN',
    'Charlotte Hornets': 'CHA', 'Chicago Bulls': 'CHI', 'Cleveland Cavaliers': 'CLE',
    'Dallas Mavericks': 'DAL', 'Denver Nuggets': 'DEN', 'Detroit Pistons': 'DET',
    'Golden State Warriors': 'GSW', 'Houston Rockets': 'HOU', 'Indiana Pacers': 'IND',
    'LA Clippers': 'LAC', 'Los Angeles Lakers': 'LAL', 'Memphis Grizzlies': 'MEM',
    'Miami Heat': 'MIA', 'Milwaukee Bucks': 'MIL', 'Minnesota Timberwolves': 'MIN',
    'New Orleans Pelicans': 'NOP', 'New York Knicks': 'NYK', 'Oklahoma City Thunder': 'OKC',
    'Orlando Magic': 'ORL', 'Philadelphia 76ers': 'PHI', 'Phoenix Suns': 'PHX',
    'Portland Trail Blazers': 'POR', 'Sacramento Kings': 'SAC', 'San Antonio Spurs': 'SAS',
    'Toronto Raptors': 'TOR', 'Utah Jazz': 'UTA', 'Washington Wizards': 'WAS',
}


def scrape_team_defense_shot_zones():
    from nba_api.stats.endpoints import leaguedashteamshotlocations

    print("Fetching team opponent shot zone data from NBA.com...")
    time.sleep(1)

    raw = None
    for attempt in range(MAX_RETRIES):
        try:
            shot_locs = leaguedashteamshotlocations.LeagueDashTeamShotLocations(
                season=SEASON,
                season_type_all_star='Regular Season',
                distance_range='By Zone',
                per_mode_detailed='Totals',
                measure_type_simple='Opponent',
                timeout=NBA_TIMEOUT,
                headers=NBA_HEADERS
            )
            raw = shot_locs.get_data_frames()[0]
            break
        except Exception as e:
            delay = RETRY_DELAYS[attempt] if attempt < len(RETRY_DELAYS) else 15
            print(f"  Attempt {attempt+1}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                print(f"  Retrying in {delay}s...")
                time.sleep(delay)
    
    if raw is None:
        conn = sqlite3.connect(DB_PATH)
        try:
            cnt = conn.execute("SELECT COUNT(*) FROM team_defense_shot_zones").fetchone()[0]
        except:
            cnt = 0
        conn.close()
        if cnt > 0:
            print(f"WARNING: NBA.com unreachable - using cached data ({cnt} team records)")
        else:
            print("ERROR: NBA.com unreachable and no cached data available")
        return
    
    print(f"  Got data for {len(raw)} teams")

    flat_cols = []
    for c in raw.columns:
        if isinstance(c, tuple):
            zone = str(c[0]).strip()
            stat = str(c[1]).strip()
            if zone:
                flat_cols.append(f"{zone}_{stat}")
            else:
                flat_cols.append(stat)
        else:
            flat_cols.append(str(c))
    raw.columns = flat_cols

    rows = []
    for _, r in raw.iterrows():
        team_name = r.get('TEAM_NAME', '')
        team_id = r.get('TEAM_ID', '')
        team_abbr = TEAM_NAME_TO_ABBR.get(team_name, '')

        if not team_abbr:
            print(f"  WARNING: Unknown team name '{team_name}', skipping")
            continue

        def _safe(val):
            if val is None or (isinstance(val, float) and np.isnan(val)):
                return 0
            return val

        ra_fga = _safe(r.get('Restricted Area_OPP_FGA', 0))
        ra_fgm = _safe(r.get('Restricted Area_OPP_FGM', 0))
        paint_fga = _safe(r.get('In The Paint (Non-RA)_OPP_FGA', 0))
        paint_fgm = _safe(r.get('In The Paint (Non-RA)_OPP_FGM', 0))
        mid_fga = _safe(r.get('Mid-Range_OPP_FGA', 0))
        mid_fgm = _safe(r.get('Mid-Range_OPP_FGM', 0))
        corner3_fga = _safe(r.get('Corner 3_OPP_FGA', 0))
        corner3_fgm = _safe(r.get('Corner 3_OPP_FGM', 0))
        atb3_fga = _safe(r.get('Above the Break 3_OPP_FGA', 0))
        atb3_fgm = _safe(r.get('Above the Break 3_OPP_FGM', 0))

        total_fga = ra_fga + paint_fga + mid_fga + corner3_fga + atb3_fga

        if total_fga == 0:
            continue

        rows.append({
            'team': team_abbr,
            'team_name': team_name,
            'team_id': int(team_id),
            'total_fga': int(total_fga),
            'ra_fga': int(ra_fga),
            'ra_fgm': int(ra_fgm),
            'paint_fga': int(paint_fga),
            'paint_fgm': int(paint_fgm),
            'mid_fga': int(mid_fga),
            'mid_fgm': int(mid_fgm),
            'corner3_fga': int(corner3_fga),
            'corner3_fgm': int(corner3_fgm),
            'atb3_fga': int(atb3_fga),
            'atb3_fgm': int(atb3_fgm),
            'ra_freq': round(ra_fga / total_fga * 100, 1),
            'paint_freq': round(paint_fga / total_fga * 100, 1),
            'mid_freq': round(mid_fga / total_fga * 100, 1),
            'corner3_freq': round(corner3_fga / total_fga * 100, 1),
            'atb3_freq': round(atb3_fga / total_fga * 100, 1),
            'ra_fg_pct': round(ra_fgm / ra_fga * 100, 1) if ra_fga > 0 else 0,
            'paint_fg_pct': round(paint_fgm / paint_fga * 100, 1) if paint_fga > 0 else 0,
            'mid_fg_pct': round(mid_fgm / mid_fga * 100, 1) if mid_fga > 0 else 0,
            'corner3_fg_pct': round(corner3_fgm / corner3_fga * 100, 1) if corner3_fga > 0 else 0,
            'atb3_fg_pct': round(atb3_fgm / atb3_fga * 100, 1) if atb3_fga > 0 else 0,
        })

    df = pd.DataFrame(rows)
    print(f"  Processed {len(df)} teams")
    return df


def save_to_db(df):
    conn = sqlite3.connect(DB_PATH)
    now = datetime.now().isoformat()
    df['scraped_at'] = now
    df.to_sql('team_defense_shot_zones', conn, if_exists='replace', index=False)
    print(f"\nSaved {len(df)} teams to team_defense_shot_zones table")
    conn.close()


def main():
    print("=" * 60)
    print("NBA.COM TEAM DEFENSIVE SHOT ZONE SCRAPER")
    print(f"Season: {SEASON}")
    print("=" * 60)

    df = scrape_team_defense_shot_zones()
    if df is None:
        print("Done! (using cached data)")
        return
    save_to_db(df)

    print("\n" + "=" * 80)
    print("TEAM DEFENSIVE SHOT ZONE SUMMARY (Opponent FGA Distribution)")
    print("=" * 80)
    print(f"{'Team':<6} {'TotalFGA':>9} {'RA%':>6} {'Paint%':>7} {'Mid%':>6} {'Corner3%':>9} {'ATB3%':>6}")
    print('-' * 55)
    for _, r in df.sort_values('ra_freq').iterrows():
        print(f"{r['team']:<6} {r['total_fga']:>9} {r['ra_freq']:>5.1f}% {r['paint_freq']:>6.1f}% "
              f"{r['mid_freq']:>5.1f}% {r['corner3_freq']:>8.1f}% {r['atb3_freq']:>5.1f}%")

    print("\nDone!")


if __name__ == '__main__':
    main()
