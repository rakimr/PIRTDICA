"""
Article header generator for PIRTDICA SPORTS CO.
Usage: python generate_header.py [YYYY-MM-DD]
       Defaults to today's date. Reads HIGH confidence picks from prop_recommendations.csv.
"""
import os, io, sys, sqlite3, requests
from datetime import date
from PIL import Image, ImageDraw, ImageFont

FONT_PATH = '/tmp/Righteous-Regular.ttf'
FONT_URL = 'https://github.com/google/fonts/raw/main/ofl/righteous/Righteous-Regular.ttf'
ESPN_HEADSHOT = 'https://a.espncdn.com/combiner/i?img=/i/headshots/nba/players/full/{id}.png&h=200&w=200&scale=crop'

ESPN_TEAM_IDS = {
    'ATL': 1, 'BOS': 2, 'BKN': 17, 'CHA': 30, 'CHI': 4,
    'CLE': 5, 'DAL': 6, 'DEN': 7, 'DET': 8, 'GS': 9,
    'HOU': 10, 'IND': 11, 'LAC': 12, 'LAL': 13, 'MEM': 29,
    'MIA': 14, 'MIL': 15, 'MIN': 16, 'NO': 3, 'NY': 18,
    'OKC': 25, 'ORL': 19, 'PHI': 20, 'PHX': 21, 'POR': 22,
    'SAC': 23, 'SA': 24, 'TOR': 28, 'UTA': 26, 'WAS': 27,
}


def ensure_font():
    if not os.path.exists(FONT_PATH):
        r = requests.get(FONT_URL, timeout=15)
        with open(FONT_PATH, 'wb') as f:
            f.write(r.content)


def get_espn_ids(player_names, teams):
    ids = {}
    fetched_rosters = {}
    for name, team in zip(player_names, teams):
        tid = ESPN_TEAM_IDS.get(team)
        if tid is None:
            continue
        if tid not in fetched_rosters:
            url = f'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{tid}/roster'
            try:
                fetched_rosters[tid] = requests.get(url, timeout=10).json().get('athletes', [])
            except Exception:
                fetched_rosters[tid] = []
        name_lower = name.lower()
        last = name.split()[-1].lower()
        exact = None
        partial = None
        for a in fetched_rosters[tid]:
            dn = a.get('displayName', '').lower()
            if dn == name_lower:
                exact = int(a['id'])
                break
            if partial is None and last in dn:
                partial = int(a['id'])
        ids[name] = exact or partial
        if ids[name] is None:
            del ids[name]
    return ids


def fetch_headshot(espn_id, d=88):
    url = ESPN_HEADSHOT.format(id=espn_id)
    r = requests.get(url, timeout=10)
    img = Image.open(io.BytesIO(r.content)).convert('RGBA').resize((d, d), Image.LANCZOS)
    white = Image.new('RGBA', (d, d), (255, 255, 255, 255))
    white.alpha_composite(img)
    return white.convert('RGB')


def generate(target_date=None, out_path=None, player_data=None, subtitle_override=None):
    import pandas as pd

    if target_date is None:
        target_date = date.today()
    date_str = target_date.strftime('%B %-d, %Y').upper()

    if out_path is None:
        out_path = f'articles/{target_date.strftime("%Y-%m-%d")}_header.png'

    ensure_font()

    if player_data is not None:
        player_list = player_data
    else:
        props = pd.read_csv('prop_recommendations.csv')
        players_df = pd.read_csv('dfs_players.csv')

        high = props[props['confidence'] == 'HIGH'].copy()
        if high.empty:
            print('No HIGH confidence picks found.')
            return
        high['abs_edge'] = high['vs_book_edge'].abs()
        high = high.sort_values('abs_edge', ascending=False).drop_duplicates(subset='player').head(6)

        player_list = []
        for _, row in high.iterrows():
            name = row['player']
            team_row = players_df[players_df['player_name'] == name]
            team = team_row.iloc[0]['team'] if len(team_row) else None
            if team:
                player_list.append((name, team))

    if not player_list:
        print('No players for header.')
        return

    names = [p[0] for p in player_list]
    teams = [p[1] for p in player_list]

    HEADSHOT_D = 109
    CARD_W = 175

    print(f'Building header for {len(names)} players...')
    espn_ids = get_espn_ids(names, teams)

    shots = []
    for name, team in player_list:
        espn_id = espn_ids.get(name)
        if espn_id:
            try:
                img = fetch_headshot(espn_id, HEADSHOT_D)
                parts = name.split()
                first = parts[0]
                last = ' '.join(parts[1:])
                shots.append((first, last, img))
                print(f'  {name}: OK')
            except Exception as e:
                print(f'  {name}: FAILED ({e})')
        else:
            print(f'  {name}: ESPN ID not found')

    if not shots:
        print('No headshots fetched.')
        return

    W = 1250
    H = 500

    TITLE_Y = 139
    SUB_Y = 177
    HEADSHOT_Y = 221
    NAME_Y_OFFSET = 9
    NAME_LINE_H = 14

    font_title = ImageFont.truetype(FONT_PATH, 26)
    font_sub = ImageFont.truetype(FONT_PATH, 12)
    font_name = ImageFont.truetype(FONT_PATH, 12)

    canvas = Image.new('RGB', (W, H), (255, 255, 255))
    draw = ImageDraw.Draw(canvas)

    title = 'PIRTDICA SPORTS CO.'
    sub = subtitle_override if subtitle_override else f'{date_str} \u2014 HIGH CONFIDENCE PICKS'

    tw = draw.textlength(title, font=font_title)
    draw.text(((W - tw) / 2, TITLE_Y), title, font=font_title, fill=(17, 17, 17))

    sw = draw.textlength(sub, font=font_sub)
    draw.text(((W - sw) / 2, SUB_Y), sub, font=font_sub, fill=(136, 136, 136))

    per_row = 6
    rows = [shots[i:i + per_row] for i in range(0, len(shots), per_row)]
    y = HEADSHOT_Y
    for row in rows:
        total_w = len(row) * CARD_W
        x_start = (W - total_w) // 2
        for i, (first, last, img) in enumerate(row):
            x = x_start + i * CARD_W + (CARD_W - HEADSHOT_D) // 2
            canvas.paste(img, (x, y))
            for li, line in enumerate([first, last]):
                lw = draw.textlength(line, font=font_name)
                lx = x_start + i * CARD_W + (CARD_W - lw) / 2
                draw.text((lx, y + HEADSHOT_D + NAME_Y_OFFSET + li * NAME_LINE_H), line, font=font_name, fill=(17, 17, 17))
        y += HEADSHOT_D + NAME_Y_OFFSET + 2 * NAME_LINE_H + 20

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    canvas.save(out_path, 'PNG')
    print(f'Saved: {out_path} ({os.path.getsize(out_path):,} bytes, {W}x{H}px)')
    return out_path


if __name__ == '__main__':
    from datetime import datetime
    d = datetime.strptime(sys.argv[1], '%Y-%m-%d').date() if len(sys.argv) > 1 else date.today()
    generate(d)
