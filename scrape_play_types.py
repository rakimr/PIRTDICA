import sqlite3
import time
from datetime import datetime
from nba_api.stats.endpoints import synergyplaytypes

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

PLAY_TYPES = [
    "Isolation", "Transition", "PRBallHandler", "PRRollman",
    "Postup", "Spotup", "Handoff", "Cut", "OffScreen", "OffRebound", "Misc"
]

PLAY_TYPE_LABELS = {
    "Isolation": "Isolation",
    "Transition": "Transition",
    "PRBallHandler": "PnR Ball Handler",
    "PRRollman": "PnR Roll Man",
    "Postup": "Post Up",
    "Spotup": "Spot Up",
    "Handoff": "Handoff",
    "Cut": "Cut",
    "OffScreen": "Off Screen",
    "OffRebound": "Putback",
    "Misc": "Miscellaneous",
}

SEASON_YEAR = "2025-26"

NBA_ABBREV_MAP = {
    "ATL": "ATL", "BOS": "BOS", "BKN": "BKN", "CHA": "CHA",
    "CHI": "CHI", "CLE": "CLE", "DAL": "DAL", "DEN": "DEN",
    "DET": "DET", "GSW": "GS", "HOU": "HOU", "IND": "IND",
    "LAC": "LAC", "LAL": "LAL", "MEM": "MEM", "MIA": "MIA",
    "MIL": "MIL", "MIN": "MIN", "NOP": "NO", "NYK": "NY",
    "OKC": "OKC", "ORL": "ORL", "PHI": "PHI", "PHX": "PHO",
    "POR": "POR", "SAC": "SAC", "SAS": "SA", "TOR": "TOR",
    "UTA": "UTA", "WAS": "WAS",
}

def normalize_team(nba_abbrev):
    return NBA_ABBREV_MAP.get(nba_abbrev, nba_abbrev)

def scrape_play_types():
    conn = sqlite3.connect("dfs_nba.db")
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS team_play_types (
        team TEXT,
        play_type TEXT,
        play_type_label TEXT,
        type_grouping TEXT,
        gp INTEGER,
        poss_pct REAL,
        ppp REAL,
        fg_pct REAL,
        ft_poss_pct REAL,
        tov_poss_pct REAL,
        score_poss_pct REAL,
        efg_pct REAL,
        poss INTEGER,
        pts REAL,
        fgm REAL,
        fga REAL,
        percentile REAL,
        scraped_at TEXT
    )
    """)

    cursor.execute("SELECT COUNT(*) FROM team_play_types")
    cached_rows = cursor.fetchone()[0]

    scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_rows = 0
    all_rows = []
    api_reachable = True

    for grouping in ["Offensive", "Defensive"]:
        if not api_reachable:
            break
        for play_type in PLAY_TYPES:
            if not api_reachable:
                break
            try:
                df = None
                for attempt in range(MAX_RETRIES):
                    try:
                        synergy = synergyplaytypes.SynergyPlayTypes(
                            league_id='00',
                            per_mode_simple='PerGame',
                            play_type_nullable=play_type,
                            player_or_team_abbreviation='T',
                            season_type_all_star='Regular Season',
                            season=SEASON_YEAR,
                            type_grouping_nullable=grouping,
                            timeout=NBA_TIMEOUT,
                            headers=NBA_HEADERS
                        )
                        df = synergy.get_data_frames()[0]
                        break
                    except Exception as retry_err:
                        err_str = str(retry_err).lower()
                        if 'timeout' in err_str or 'connection' in err_str or 'refused' in err_str or '403' in err_str or '429' in err_str:
                            delay = RETRY_DELAYS[attempt] if attempt < len(RETRY_DELAYS) else 15
                            if attempt < MAX_RETRIES - 1:
                                print(f"    Retry {attempt+1}/{MAX_RETRIES} for {grouping} {play_type}: {retry_err}")
                                time.sleep(delay)
                            else:
                                print(f"  NBA.com unreachable after {MAX_RETRIES} attempts for {grouping} {play_type}")
                                api_reachable = False
                                df = None
                        else:
                            delay = RETRY_DELAYS[attempt] if attempt < len(RETRY_DELAYS) else 15
                            if attempt < MAX_RETRIES - 1:
                                print(f"    Retry {attempt+1}/{MAX_RETRIES} for {grouping} {play_type}: {retry_err}")
                                time.sleep(delay)
                            else:
                                df = None

                if df is None or df.empty:
                    if api_reachable:
                        print(f"  Skipped/Empty: {grouping} {play_type}")
                    continue

                for _, row in df.iterrows():
                    team = normalize_team(row.get("TEAM_ABBREVIATION", ""))
                    all_rows.append((
                        team,
                        play_type,
                        PLAY_TYPE_LABELS.get(play_type, play_type),
                        grouping,
                        int(row.get("GP", 0)),
                        float(row.get("POSS_PCT", 0)),
                        float(row.get("PPP", 0)),
                        float(row.get("FG_PCT", 0)),
                        float(row.get("FT_POSS_PCT", 0)),
                        float(row.get("TOV_POSS_PCT", 0)),
                        float(row.get("SCORE_POSS_PCT", 0)),
                        float(row.get("EFG_PCT", 0)),
                        int(row.get("POSS", 0)),
                        float(row.get("PTS", 0)),
                        float(row.get("FGM", 0)),
                        float(row.get("FGA", 0)),
                        float(row.get("PERCENTILE", 0)),
                        scraped_at,
                    ))

                print(f"  {grouping} {play_type}: {len(df)} teams")

            except Exception as e:
                print(f"  ERROR {grouping} {play_type}: {e}")

            time.sleep(1.0)

    if not api_reachable:
        if cached_rows > 0:
            print(f"\nNBA.com unreachable — preserving {cached_rows} cached rows in team_play_types.")
            if len(all_rows) > 0:
                print(f"  (Discarding {len(all_rows)} partial rows from incomplete scrape)")
        else:
            print("\nNBA.com unreachable and no cached data available.")
        conn.close()
        return

    if len(all_rows) > 0:
        cursor.execute("DELETE FROM team_play_types")
        cursor.executemany("""
            INSERT INTO team_play_types VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, all_rows)
        total_rows = len(all_rows)
        conn.commit()
        print(f"\nDone. {total_rows} total rows saved to team_play_types.")
    else:
        print(f"\nNo new data scraped. Preserving {cached_rows} cached rows.")

    conn.close()

if __name__ == "__main__":
    print("Scraping NBA Synergy play type data...")
    scrape_play_types()
