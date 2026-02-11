import os
import requests
import sqlite3
import pandas as pd
from datetime import datetime
from utils.timezone import get_eastern_date_str, get_eastern_now

API_KEY = os.environ.get('THE_ODDS_API_KEY', '')
BASE_URL = 'https://api.the-odds-api.com/v4'
SPORT = 'basketball_nba'

MARKETS = [
    'player_points',
    'player_rebounds',
    'player_assists',
    'player_threes',
    'player_steals',
    'player_blocks',
    'player_turnovers',
]

MARKET_TO_STAT = {
    'player_points': 'PTS',
    'player_rebounds': 'REB',
    'player_assists': 'AST',
    'player_threes': '3PM',
    'player_steals': 'STL',
    'player_blocks': 'BLK',
    'player_turnovers': 'TO',
}

PREFERRED_BOOKS = ['fanduel', 'draftkings', 'betmgm', 'caesars']

conn = sqlite3.connect("dfs_nba.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS player_props (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_name TEXT,
    stat TEXT,
    line REAL,
    over_odds INTEGER,
    under_odds INTEGER,
    bookmaker TEXT,
    home_team TEXT,
    away_team TEXT,
    game_date TEXT,
    scraped_at TEXT
)
""")
conn.commit()

today = get_eastern_date_str()

import sys
force = '--force' in sys.argv

if not API_KEY:
    print("ERROR: THE_ODDS_API_KEY not set")
    conn.close()
    exit(1)

existing = cursor.execute("SELECT COUNT(*) FROM player_props WHERE game_date = ?", (today,)).fetchone()[0]
if existing > 0 and not force:
    print(f"Props already scraped for {today} ({existing} lines). Use --force to re-fetch.")
    conn.close()
    exit(0)

print(f"Fetching NBA events...")
events_url = f'{BASE_URL}/sports/{SPORT}/events'
events_resp = requests.get(events_url, params={'apiKey': API_KEY}, timeout=15)
events_resp.raise_for_status()
all_events = events_resp.json()

today_dt = datetime.strptime(today, '%Y-%m-%d')
today_events = []
for e in all_events:
    commence = datetime.fromisoformat(e['commence_time'].replace('Z', '+00:00'))
    from utils.timezone import EASTERN
    commence_et = commence.astimezone(EASTERN)
    if commence_et.strftime('%Y-%m-%d') == today:
        today_events.append(e)

print(f"Found {len(today_events)} games today ({today})")

if len(today_events) == 0:
    print("No games today, skipping props scrape")
    conn.close()
    exit(0)

all_props = []
markets_str = ','.join(MARKETS)

for event in today_events:
    event_id = event['id']
    home = event['home_team']
    away = event['away_team']
    print(f"  Fetching props: {away} @ {home}...")

    url = f'{BASE_URL}/sports/{SPORT}/events/{event_id}/odds'
    resp = requests.get(url, params={
        'apiKey': API_KEY,
        'regions': 'us',
        'markets': markets_str,
        'oddsFormat': 'american'
    }, timeout=20)

    remaining = resp.headers.get('x-requests-remaining', '?')
    print(f"    API requests remaining: {remaining}")

    if resp.status_code != 200:
        print(f"    Error: {resp.status_code}")
        continue

    data = resp.json()
    bookmakers = data.get('bookmakers', [])

    best_book = None
    for pref in PREFERRED_BOOKS:
        for bk in bookmakers:
            if bk['key'] == pref:
                best_book = bk
                break
        if best_book:
            break
    if not best_book and bookmakers:
        best_book = bookmakers[0]

    if not best_book:
        print(f"    No bookmakers available")
        continue

    print(f"    Using: {best_book['title']}")

    for market in best_book.get('markets', []):
        stat_label = MARKET_TO_STAT.get(market['key'], market['key'])
        outcomes = market.get('outcomes', [])

        players = {}
        for o in outcomes:
            player = o.get('description', '')
            direction = o.get('name', '')
            price = o.get('price', 0)
            point = o.get('point', 0)

            if player not in players:
                players[player] = {'line': point, 'over': None, 'under': None}
            if direction == 'Over':
                players[player]['over'] = price
                players[player]['line'] = point
            elif direction == 'Under':
                players[player]['under'] = price

        for player_name, vals in players.items():
            all_props.append({
                'player_name': player_name,
                'stat': stat_label,
                'line': vals['line'],
                'over_odds': vals['over'],
                'under_odds': vals['under'],
                'bookmaker': best_book['title'],
                'home_team': home,
                'away_team': away,
                'game_date': today,
                'scraped_at': get_eastern_now().isoformat()
            })

cursor.execute("DELETE FROM player_props WHERE game_date = ?", (today,))
conn.commit()

if all_props:
    df = pd.DataFrame(all_props)
    df.to_sql("player_props", conn, if_exists="append", index=False)
    print(f"\nSaved {len(df)} player prop lines to database")
    print(f"  Players: {df['player_name'].nunique()}")
    print(f"  Stats: {df['stat'].nunique()} ({', '.join(df['stat'].unique())})")
    print(f"  Bookmaker: {df['bookmaker'].iloc[0]}")
else:
    print("No props data collected")

conn.close()
