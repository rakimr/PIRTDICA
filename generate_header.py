"""
Article header generator for PIRTDICA SPORTS CO.
Usage: python generate_header.py [YYYY-MM-DD]
       Defaults to today's date. Reads HIGH confidence picks from prop_recommendations.csv.

`player_data` accepts either:
  - list of (name, team) tuples (legacy)
  - list of dicts with keys: player, team, stat, side ("OVER"/"UNDER"),
    line (number), edge (string like "+18%" or "-44.0%")
"""
import os, io, sys, sqlite3, requests
from datetime import date
from PIL import Image, ImageDraw, ImageFont, ImageFilter

FONT_PATH = '/tmp/Righteous-Regular.ttf'
FONT_URL = 'https://github.com/google/fonts/raw/main/ofl/righteous/Righteous-Regular.ttf'
INTER_PATH = '/tmp/Inter-Bold.ttf'
INTER_URL = 'https://github.com/rsms/inter/raw/master/docs/font-files/Inter-Bold.ttf'
INTER_REG_PATH = '/tmp/Inter-Regular.ttf'
INTER_REG_URL = 'https://github.com/rsms/inter/raw/master/docs/font-files/Inter-Regular.ttf'
ESPN_HEADSHOT = 'https://a.espncdn.com/combiner/i?img=/i/headshots/nba/players/full/{id}.png&h=240&w=240&scale=crop'

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
    if not os.path.exists(INTER_PATH):
        try:
            r = requests.get(INTER_URL, timeout=15)
            with open(INTER_PATH, 'wb') as f:
                f.write(r.content)
        except Exception:
            pass
    if not os.path.exists(INTER_REG_PATH):
        try:
            r = requests.get(INTER_REG_URL, timeout=15)
            with open(INTER_REG_PATH, 'wb') as f:
                f.write(r.content)
        except Exception:
            pass


def _load_font(path, size):
    try:
        return ImageFont.truetype(path, size)
    except Exception:
        return ImageFont.truetype(FONT_PATH, size)


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


def fetch_headshot(espn_id, d=140):
    url = ESPN_HEADSHOT.format(id=espn_id)
    r = requests.get(url, timeout=10)
    img = Image.open(io.BytesIO(r.content)).convert('RGBA').resize((d, d), Image.LANCZOS)
    return img


def _make_circle_avatar(headshot_rgba, d, ring_color=(212, 164, 77, 255), ring_w=3):
    """Crop a square headshot into a circle with a thin colored ring."""
    canvas = Image.new('RGBA', (d, d), (0, 0, 0, 0))
    mask = Image.new('L', (d, d), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, d, d), fill=255)
    inner_d = d - ring_w * 2
    bg_circle = Image.new('RGBA', (d, d), (26, 26, 26, 255))
    canvas.paste(bg_circle, (0, 0), mask)
    inner = headshot_rgba.resize((inner_d, inner_d), Image.LANCZOS)
    inner_mask = Image.new('L', (inner_d, inner_d), 0)
    ImageDraw.Draw(inner_mask).ellipse((0, 0, inner_d, inner_d), fill=255)
    canvas.paste(inner, (ring_w, ring_w), inner_mask)
    ring = Image.new('RGBA', (d, d), (0, 0, 0, 0))
    ImageDraw.Draw(ring).ellipse((0, 0, d - 1, d - 1), outline=ring_color, width=ring_w)
    canvas = Image.alpha_composite(canvas, ring)
    return canvas


def _gradient_bg(W, H):
    """Vertical dark gradient with a subtle radial highlight."""
    bg = Image.new('RGB', (W, H), (10, 10, 12))
    px = bg.load()
    top = (10, 10, 12)
    bottom = (24, 24, 28)
    for y in range(H):
        t = y / max(H - 1, 1)
        r = int(top[0] + (bottom[0] - top[0]) * t)
        g = int(top[1] + (bottom[1] - top[1]) * t)
        b = int(top[2] + (bottom[2] - top[2]) * t)
        for x in range(W):
            px[x, y] = (r, g, b)
    glow = Image.new('RGBA', (W, H), (0, 0, 0, 0))
    gd = ImageDraw.Draw(glow)
    cx, cy = W // 2, int(H * 0.22)
    max_r = int(W * 0.55)
    for r in range(max_r, 0, -8):
        alpha = int(18 * (1 - r / max_r))
        if alpha <= 0:
            continue
        gd.ellipse((cx - r, cy - r, cx + r, cy + r), fill=(212, 164, 77, alpha))
    glow = glow.filter(ImageFilter.GaussianBlur(40))
    bg = Image.alpha_composite(bg.convert('RGBA'), glow).convert('RGB')
    return bg


def _render_title_only(out_path, date_str, subtitle_override):
    W = 1600
    H = 360
    bg = _gradient_bg(W, H)
    draw = ImageDraw.Draw(bg)

    f_brand = _load_font(INTER_PATH, 18)
    f_title = _load_font(FONT_PATH, 64)
    f_sub = _load_font(INTER_REG_PATH, 18)
    f_tag = _load_font(INTER_PATH, 13)

    brand = 'PIRTDICA SPORTS CO.'
    bw = draw.textlength(brand, font=f_brand)
    draw.text(((W - bw) / 2, 70), brand, font=f_brand, fill=(212, 164, 77))

    title = "TODAY'S BOARD"
    tw = draw.textlength(title, font=f_title)
    draw.text(((W - tw) / 2, 110), title, font=f_title, fill=(245, 245, 245))

    sub = subtitle_override if subtitle_override else f'{date_str} // ANALYSIS IN PROGRESS'
    sw = draw.textlength(sub, font=f_sub)
    draw.text(((W - sw) / 2, 200), sub, font=f_sub, fill=(160, 160, 160))

    tag = 'CHECK BACK SHORTLY FOR HIGH-CONFIDENCE PLAYS'
    gw = draw.textlength(tag, font=f_tag)
    bx, by = (W - gw) / 2 - 14, 252
    pad_x, pad_y = 14, 8
    draw.rounded_rectangle(
        (bx, by, bx + gw + pad_x * 2, by + 28),
        radius=6, outline=(212, 164, 77), width=1
    )
    draw.text((bx + pad_x, by + pad_y - 1), tag, font=f_tag, fill=(212, 164, 77))

    os.makedirs(os.path.dirname(out_path) or '.', exist_ok=True)
    bg.save(out_path, 'PNG')
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


def _draw_player_card(canvas, draw, x, y, card_w, p, headshot_rgba, fonts):
    """Render a single player card centered at (x, y) top-left within card_w."""
    f_name_first, f_name_last, f_team, f_pick = fonts
    HEADSHOT_D = 140

    side = (p.get('side') or '').upper()
    if side == 'OVER':
        ring = (34, 197, 94, 255)
        pick_color = (34, 197, 94)
        arrow = '\u25B2'
    elif side == 'UNDER':
        ring = (239, 68, 68, 255)
        pick_color = (239, 68, 68)
        arrow = '\u25BC'
    else:
        ring = (212, 164, 77, 255)
        pick_color = (212, 164, 77)
        arrow = ''

    avatar = _make_circle_avatar(headshot_rgba, HEADSHOT_D, ring_color=ring, ring_w=3)
    ax = x + (card_w - HEADSHOT_D) // 2
    canvas.paste(avatar, (ax, y), avatar)

    name = p.get('name') or ''
    parts = name.split()
    first = parts[0] if parts else ''
    last = ' '.join(parts[1:]) if len(parts) > 1 else ''

    cy = y + HEADSHOT_D + 12
    fw = draw.textlength(first, font=f_name_first)
    draw.text((x + (card_w - fw) / 2, cy), first, font=f_name_first, fill=(180, 180, 180))
    cy += 18
    lw = draw.textlength(last, font=f_name_last)
    draw.text((x + (card_w - lw) / 2, cy), last, font=f_name_last, fill=(245, 245, 245))
    cy += 26

    team = p.get('team') or ''
    if team:
        tw = draw.textlength(team, font=f_team)
        pad_x = 8
        chip_w = tw + pad_x * 2
        chip_h = 18
        chip_x = x + (card_w - chip_w) / 2
        draw.rounded_rectangle(
            (chip_x, cy, chip_x + chip_w, cy + chip_h),
            radius=4, fill=(40, 40, 44)
        )
        draw.text((chip_x + pad_x, cy + 2), team, font=f_team, fill=(180, 180, 180))
        cy += chip_h + 10

    stat = p.get('stat')
    line = _format_line(p.get('line'))
    if side and stat and line:
        pick_text = f'{arrow} {side} {line} {stat}'
        pw = draw.textlength(pick_text, font=f_pick)
        draw.text((x + (card_w - pw) / 2, cy), pick_text, font=f_pick, fill=pick_color)
        cy += 20
        edge = p.get('edge')
        if edge:
            edge_str = str(edge).strip()
            ew = draw.textlength(edge_str, font=f_team)
            draw.text((x + (card_w - ew) / 2, cy), edge_str, font=f_team, fill=(140, 140, 140))


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

            high = props[props['confidence'] == 'HIGH'].copy()
            if high.empty:
                print('No HIGH confidence picks found.')
            else:
                high['abs_edge'] = high['vs_book_edge'].abs()
                high = high.sort_values('abs_edge', ascending=False).drop_duplicates(subset='player').head(6)
                for _, row in high.iterrows():
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
                    img = fetch_headshot(espn_id, 140)
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

    W = 1600
    n = len(shots)
    has_pick_info = any(s[0].get('side') and s[0].get('stat') and s[0].get('line') is not None for s in shots)
    H = 600 if has_pick_info else 540

    bg = _gradient_bg(W, H)
    draw = ImageDraw.Draw(bg)

    f_brand = _load_font(INTER_PATH, 18)
    f_title = _load_font(FONT_PATH, 56)
    f_sub = _load_font(INTER_REG_PATH, 18)
    f_count = _load_font(INTER_PATH, 13)

    f_name_first = _load_font(INTER_REG_PATH, 14)
    f_name_last = _load_font(INTER_PATH, 18)
    f_team = _load_font(INTER_PATH, 11)
    f_pick = _load_font(INTER_PATH, 14)

    brand = 'PIRTDICA SPORTS CO.'
    bw = draw.textlength(brand, font=f_brand)
    draw.text(((W - bw) / 2, 48), brand, font=f_brand, fill=(212, 164, 77))

    title = "TODAY'S BOARD"
    tw = draw.textlength(title, font=f_title)
    draw.text(((W - tw) / 2, 80), title, font=f_title, fill=(245, 245, 245))

    sub = subtitle_override if subtitle_override else f'{date_str} // HIGH-CONFIDENCE PLAYS'
    sw = draw.textlength(sub, font=f_sub)
    draw.text(((W - sw) / 2, 158), sub, font=f_sub, fill=(160, 160, 160))

    accent_w = 60
    ay = 195
    draw.rectangle(((W - accent_w) / 2, ay, (W + accent_w) / 2, ay + 2), fill=(212, 164, 77))

    per_row = min(6, n)
    rows = [shots[i:i + 6] for i in range(0, n, 6)]

    CARD_W = 200 if n <= 6 else 180
    CARD_H = 290 if has_pick_info else 230

    HEADSHOT_Y = 230
    fonts = (f_name_first, f_name_last, f_team, f_pick)

    y = HEADSHOT_Y
    for row in rows:
        total_w = len(row) * CARD_W
        x_start = (W - total_w) // 2
        for i, (p, img) in enumerate(row):
            x = x_start + i * CARD_W
            _draw_player_card(bg, draw, x, y, CARD_W, p, img, fonts)
        y += CARD_H

    label = f'{n} HIGH-CONFIDENCE PLAY{"S" if n != 1 else ""}'
    lw = draw.textlength(label, font=f_count)
    pad_x, pad_h = 14, 26
    bx = (W - lw - pad_x * 2) / 2
    by = H - 56
    draw.rounded_rectangle(
        (bx, by, bx + lw + pad_x * 2, by + pad_h),
        radius=6, outline=(212, 164, 77), width=1
    )
    draw.text((bx + pad_x, by + 6), label, font=f_count, fill=(212, 164, 77))

    os.makedirs(os.path.dirname(out_path) or '.', exist_ok=True)
    bg.save(out_path, 'PNG')
    print(f'Saved: {out_path} ({os.path.getsize(out_path):,} bytes, {W}x{H}px)')
    return out_path


if __name__ == '__main__':
    from datetime import datetime
    d = datetime.strptime(sys.argv[1], '%Y-%m-%d').date() if len(sys.argv) > 1 else date.today()
    generate(d)
