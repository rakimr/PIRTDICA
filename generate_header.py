"""
Article header generator for PIRTDICA SPORTS CO.
Usage: python generate_header.py [YYYY-MM-DD]
       Defaults to today's date. Reads HIGH confidence picks from prop_recommendations.csv.

`player_data` accepts either:
  - list of (name, team) tuples (legacy)
  - list of dicts with keys: player, team, stat, side ("OVER"/"UNDER"),
    line (number), edge (string like "+18%" or "-44.0%")

Aesthetic: editorial sports magazine // white background // black title in
Righteous // muted gray subtitle and captions // square headshots // pick info
shown as a small all-caps caption beneath each player name.
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


def fetch_headshot(espn_id, d=109):
    url = ESPN_HEADSHOT.format(id=espn_id)
    r = requests.get(url, timeout=10)
    img = Image.open(io.BytesIO(r.content)).convert('RGBA').resize((d, d), Image.LANCZOS)
    white = Image.new('RGBA', (d, d), (255, 255, 255, 255))
    white.alpha_composite(img)
    return white.convert('RGB')


def _render_title_only(out_path, date_str, subtitle_override):
    W = 1250
    H = 220
    font_title = ImageFont.truetype(FONT_PATH, 32)
    font_sub = ImageFont.truetype(FONT_PATH, 14)

    canvas = Image.new('RGB', (W, H), (255, 255, 255))
    draw = ImageDraw.Draw(canvas)

    title = 'PIRTDICA SPORTS CO.'
    sub = subtitle_override if subtitle_override else f'{date_str} \u2014 ANALYSIS IN PROGRESS'

    tw = draw.textlength(title, font=font_title)
    draw.text(((W - tw) / 2, 90), title, font=font_title, fill=(17, 17, 17))

    sw = draw.textlength(sub, font=font_sub)
    draw.text(((W - sw) / 2, 138), sub, font=font_sub, fill=(136, 136, 136))

    os.makedirs(os.path.dirname(out_path) or '.', exist_ok=True)
    canvas.save(out_path, 'PNG')
    print(f'Saved fallback header: {out_path} ({os.path.getsize(out_path):,} bytes, {W}x{H}px)')
    return out_path


def _normalize_player_entry(entry):
    """Accept a (name, team) tuple OR a dict with rich pick info.

    Returns a dict with keys: name, team, stat, side, line, edge.
    Missing keys default to None so the renderer can decide what to draw.
    """
    if isinstance(entry, dict):
        name = entry.get('player') or entry.get('name')
        team = entry.get('team')
        stat = entry.get('stat')
        side = entry.get('side') or entry.get('pick')
        line = entry.get('line') or entry.get('book_line')
        edge = entry.get('edge')
        return {'name': name, 'team': team, 'stat': stat, 'side': side, 'line': line, 'edge': edge}
    if isinstance(entry, (list, tuple)) and len(entry) >= 2:
        return {'name': entry[0], 'team': entry[1], 'stat': None, 'side': None, 'line': None, 'edge': None}
    return None


def _format_line(line):
    if line is None:
        return ''
    try:
        f = float(line)
        if f.is_integer():
            return f'{int(f)}'
        return f'{f:.1f}'
    except (TypeError, ValueError):
        return str(line)


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
        player_list = []
        try:
            props = pd.read_csv('prop_recommendations.csv')
            players_df = pd.read_csv('dfs_players.csv')

            picks = props[props['confidence'] == 'HIGH'].copy()
            if picks.empty:
                # Soften: when no HIGH picks exist (common on tiny playoff
                # slates), fall back to the top picks by absolute edge
                # regardless of confidence so the header still shows real
                # players on tonight's slate instead of the blank
                # "ANALYSIS IN PROGRESS" fallback PNG.
                picks = props.copy()
                print(f'No HIGH confidence picks found — falling back to top picks by edge ({len(picks)} candidates).')
            if not picks.empty:
                picks['abs_edge'] = picks['vs_book_edge'].abs()
                picks = picks.sort_values('abs_edge', ascending=False).drop_duplicates(subset='player').head(6)
                for _, row in picks.iterrows():
                    name = row['player']
                    team_row = players_df[players_df['player_name'] == name]
                    team = team_row.iloc[0]['team'] if len(team_row) else None
                    if team:
                        player_list.append((name, team))
        except FileNotFoundError as e:
            print(f'CSV not available ({e}); will fall through to title-only header.')

    normalized = []
    for entry in player_list:
        n = _normalize_player_entry(entry)
        if n and n.get('name') and n.get('team'):
            normalized.append(n)

    HEADSHOT_D = 109
    CARD_W = 175

    shots = []
    if normalized:
        names = [p['name'] for p in normalized]
        teams = [p['team'] for p in normalized]
        print(f'Building header for {len(names)} players...')
        espn_ids = get_espn_ids(names, teams)

        for p in normalized:
            espn_id = espn_ids.get(p['name'])
            if espn_id:
                try:
                    img = fetch_headshot(espn_id, HEADSHOT_D)
                    shots.append((p, img))
                    print(f'  {p["name"]}: OK')
                except Exception as e:
                    print(f'  {p["name"]}: FAILED ({e})')
            else:
                print(f'  {p["name"]}: ESPN ID not found')
    else:
        print('No players supplied — rendering title-only fallback header.')

    if not shots:
        print('No headshots available — rendering title-only fallback header.')
        return _render_title_only(out_path, date_str, subtitle_override)

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

    rows = [shots[i:i + 6] for i in range(0, len(shots), 6)]
    y = HEADSHOT_Y
    for row in rows:
        total_w = len(row) * CARD_W
        x_start = (W - total_w) // 2
        for i, (p, img) in enumerate(row):
            x = x_start + i * CARD_W + (CARD_W - HEADSHOT_D) // 2
            canvas.paste(img, (x, y))

            name = p.get('name') or ''
            parts = name.split()
            first = parts[0] if parts else ''
            last = ' '.join(parts[1:]) if len(parts) > 1 else ''
            for li, line in enumerate([first, last]):
                lw = draw.textlength(line, font=font_name)
                lx = x_start + i * CARD_W + (CARD_W - lw) / 2
                draw.text(
                    (lx, y + HEADSHOT_D + NAME_Y_OFFSET + li * NAME_LINE_H),
                    line, font=font_name, fill=(17, 17, 17)
                )

        y += HEADSHOT_D + NAME_Y_OFFSET + 2 * NAME_LINE_H + 20

    os.makedirs(os.path.dirname(out_path) or '.', exist_ok=True)
    canvas.save(out_path, 'PNG')
    print(f'Saved: {out_path} ({os.path.getsize(out_path):,} bytes, {W}x{H}px)')
    return out_path


if __name__ == '__main__':
    from datetime import datetime
    d = datetime.strptime(sys.argv[1], '%Y-%m-%d').date() if len(sys.argv) > 1 else date.today()
    generate(d)
