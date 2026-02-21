"""Scrape live NBA box scores from plaintextsports.com and calculate FanDuel FP."""
import requests
from bs4 import BeautifulSoup
import re
from utils.name_normalize import normalize_player_name
from utils.timezone import get_eastern_date_str

BASE_URL = "https://plaintextsports.com"

def get_today_games(game_date=None):
    if game_date is None:
        game_date = get_eastern_date_str()
    url = f"{BASE_URL}/nba/"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    
    game_urls = list(set(re.findall(r'/nba/\d{4}-\d{2}-\d{2}/[a-z]+-[a-z]+', resp.text)))
    game_urls = [g for g in game_urls if game_date in g]
    return game_urls

def calc_fanduel_fp(pts, reb, ast, stl, blk, to):
    return pts + (reb * 1.2) + (ast * 1.5) + (stl * 3) + (blk * 3) - to

def parse_stat_line(stat_div):
    text = stat_div.get_text()
    text_stripped = text.strip()
    if 'Has not entered' in text_stripped or 'Inactive' in text_stripped or 'DNP' in text_stripped:
        return None
    if re.match(r'^\s*-\s+-\s+-', text_stripped):
        return None
    
    bold = stat_div.find('b')
    pts = 0
    if bold:
        try:
            pts = int(bold.get_text(strip=True))
        except ValueError:
            return None
    
    parts = text.split()
    if len(parts) < 11:
        return None
    
    try:
        minutes_str = parts[0]
        if not re.match(r'\d+:\d+', minutes_str):
            return None
        
        reb_raw = parts[4]
        if '/' in reb_raw:
            reb = int(reb_raw.split('/')[-1])
        else:
            reb = int(reb_raw)
        
        ast = int(parts[5])
        stl = int(parts[6])
        blk = int(parts[7])
        to = int(parts[8])
        
        return {
            'minutes': minutes_str,
            'reb': reb,
            'ast': ast,
            'stl': stl,
            'blk': blk,
            'to': to,
            'pts': pts,
        }
    except (ValueError, IndexError):
        return None

def extract_player_name(name_div):
    texts = []
    for child in name_div.children:
        if hasattr(child, 'name') and child.name == 'span':
            continue
        if hasattr(child, 'string') and child.string:
            texts.append(child.string.strip())
        elif isinstance(child, str):
            texts.append(child.strip())
    return ' '.join(t for t in texts if t)

def section_total_pts(section):
    total = 0
    for b in section.find_all('b'):
        try:
            total += int(b.get_text(strip=True))
        except ValueError:
            pass
    return total

def parse_section_players(section):
    players = {}
    divs = section.find_all('div', recursive=False)
    
    i = 1
    while i < len(divs) - 1:
        name_div = divs[i]
        stat_div = divs[i + 1]
        
        player_name = extract_player_name(name_div)
        
        pos_span = name_div.find('span', class_='text-gray')
        position = pos_span.get_text(strip=True) if pos_span else ''
        
        if not player_name:
            i += 2
            continue
        
        stats = parse_stat_line(stat_div)
        
        if stats:
            fp = calc_fanduel_fp(
                stats['pts'], stats['reb'], stats['ast'],
                stats['stl'], stats['blk'], stats['to']
            )
            normalized = normalize_player_name(player_name)
            
            players[normalized] = {
                'name': player_name,
                'normalized': normalized,
                'position': position,
                'minutes': stats['minutes'],
                'pts': stats['pts'],
                'reb': stats['reb'],
                'ast': stats['ast'],
                'stl': stats['stl'],
                'blk': stats['blk'],
                'to': stats['to'],
                'fp': fp,
            }
        
        i += 2
    
    return players

def scrape_game_box_score(game_path):
    url = f"{BASE_URL}{game_path}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    
    soup = BeautifulSoup(resp.text, 'html.parser')
    
    box_sections = soup.find_all('div', class_='box-score-players')
    
    grouped = {}
    for section in box_sections:
        header_div = section.find('div', class_='sticky')
        if not header_div:
            continue
        header_b = header_div.find('b')
        if not header_b:
            continue
        header_key = header_b.get_text(strip=True)
        if header_key not in grouped:
            grouped[header_key] = []
        grouped[header_key].append(section)
    
    best_sections = {}
    for header_key, sections_list in grouped.items():
        best = max(sections_list, key=section_total_pts)
        best_sections[header_key] = best
    
    players = {}
    for header_key, section in best_sections.items():
        section_players = parse_section_players(section)
        players.update(section_players)
    
    return players

def get_all_live_scores(game_date=None):
    game_paths = get_today_games(game_date)
    all_players = {}
    
    for path in game_paths:
        try:
            game_players = scrape_game_box_score(path)
            all_players.update(game_players)
        except Exception as e:
            print(f"Error scraping {path}: {e}")
            continue
    
    return all_players

def get_live_scores_summary(game_date=None):
    all_players = get_all_live_scores(game_date)
    
    result = {}
    for norm_name, data in all_players.items():
        result[norm_name] = {
            'name': data['name'],
            'fp': round(data['fp'], 1),
            'pts': data['pts'],
            'reb': data['reb'],
            'ast': data['ast'],
            'stl': data['stl'],
            'blk': data['blk'],
            'to': data['to'],
        }
    
    return result


if __name__ == "__main__":
    scores = get_live_scores_summary()
    print(f"\nLive scores for {len(scores)} players:\n")
    sorted_players = sorted(scores.items(), key=lambda x: x[1]['fp'], reverse=True)
    for norm, data in sorted_players[:30]:
        print(f"  {data['name']:25s} {data['fp']:6.1f} FP  ({data['pts']}pts {data['reb']}reb {data['ast']}ast {data['stl']}stl {data['blk']}blk {data['to']}to)")
