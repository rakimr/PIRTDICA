import requests
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
import re
from datetime import datetime, date
from team_map import TEAM_MAP
from utils.timezone import get_eastern_date_str
from utils.name_normalize import normalize_player_name

conn = sqlite3.connect("dfs_nba.db")
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS player_salaries")
cursor.execute("""
CREATE TABLE IF NOT EXISTS player_salaries (
    player_name TEXT,
    team TEXT,
    position TEXT,
    salary INTEGER,
    status TEXT,
    roster_order INTEGER,
    game TEXT,
    game_time TEXT,
    scraped_at TEXT
)
""")
conn.commit()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

today = get_eastern_date_str()

def scrape_rotogrinders():
    URL = f"https://rotogrinders.com/lineups/nba?site=fanduel&date={today}"
    print(f"Fetching FanDuel lineups from RotoGrinders for {today}...")

    try:
        response = requests.get(URL, headers=HEADERS, timeout=30)
        if response.status_code != 200:
            print(f"RotoGrinders error: HTTP {response.status_code}")
            return []
    except Exception as e:
        print(f"RotoGrinders request failed: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    rows = []

    game_cards = soup.find_all("div", class_="game-card")
    print(f"RotoGrinders: Found {len(game_cards)} games")

    for game_card in game_cards:
        teams_div = game_card.find("div", class_="game-card-teams")
        team_abbrs = []
        if teams_div:
            nameplates = teams_div.find_all("div", class_="team-nameplate")
            for np in nameplates:
                title = np.find("span", class_="team-nameplate-title")
                if title and title.get("data-abbr"):
                    team_abbrs.append(title.get("data-abbr"))

        if len(team_abbrs) >= 2:
            game_title = f"{team_abbrs[0]} @ {team_abbrs[1]}"
            away_team = team_abbrs[0]
            home_team = team_abbrs[1]
        else:
            game_title = "Unknown"
            away_team = None
            home_team = None

        card_text = game_card.get_text()
        time_matches = re.findall(r'(\d{1,2}:\d{2}\s*(?:AM|PM))', card_text, re.IGNORECASE)
        game_time = time_matches[0].upper().replace(' ', '') if time_matches else None

        lineup_cards = game_card.find_all("div", class_="lineup-card")
        if not lineup_cards:
            continue

        for idx, lineup_card in enumerate(lineup_cards):
            current_team = away_team if idx == 0 else home_team
            players_div = lineup_card.find("div", class_="lineup-card-players")
            if not players_div:
                continue

            current_status = None
            team_order = 0
            for child in players_div.children:
                if not hasattr(child, 'name') or not child.name:
                    continue

                if child.name == 'span':
                    text = child.get_text(strip=True).lower()
                    if 'starter' in text:
                        current_status = 'Starter'
                    elif 'bench' in text:
                        current_status = 'Bench'

                elif child.name == 'ul':
                    players = child.find_all("li", class_="lineup-card-player")

                    for player in players:
                        nameplate = player.find("span", class_="player-nameplate")
                        if not nameplate:
                            continue

                        name_elem = nameplate.find("a", class_="player-nameplate-name")
                        if not name_elem:
                            continue

                        player_name = name_elem.get_text(strip=True)
                        position = nameplate.get("data-position")

                        salary = nameplate.get("data-salary")
                        if salary:
                            try:
                                salary = int(salary)
                            except:
                                salary = None

                        team = TEAM_MAP.get(current_team, current_team) if current_team else None
                        team_order += 1

                        rows.append({
                            "player_name": player_name,
                            "team": team,
                            "position": position,
                            "salary": salary,
                            "status": current_status,
                            "roster_order": team_order,
                            "game": game_title,
                            "game_time": game_time,
                            "scraped_at": datetime.utcnow().isoformat()
                        })

    print(f"RotoGrinders: {len(rows)} players scraped")
    return rows


def normalize_team(raw_team):
    if not raw_team:
        return None
    raw_team = raw_team.strip()
    return TEAM_MAP.get(raw_team, raw_team)


def scrape_fantasypros():
    URL = "https://www.fantasypros.com/daily-fantasy/nba/fanduel-salary-changes.php"
    print("Fetching FanDuel salaries from FantasyPros...")

    try:
        response = requests.get(URL, headers=HEADERS, timeout=30)
        if response.status_code != 200:
            print(f"FantasyPros error: HTTP {response.status_code}")
            return []
    except Exception as e:
        print(f"FantasyPros request failed: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table")
    if not table:
        print("FantasyPros: No table found")
        return []

    rows = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

        player_link = tds[1].find("a", class_="fp-player-link")
        if not player_link:
            continue

        player_name = player_link.get("fp-player-name") or player_link.get_text(strip=True)

        small = tds[1].find("small")
        team = None
        position = None
        if small:
            info_text = small.get_text(strip=True).strip("()")
            parts = info_text.split(" - ")
            if len(parts) == 2:
                raw_team = parts[0].strip()
                team = normalize_team(raw_team)
                position = parts[1].strip()

        salary_td = tds[4]
        salary_val = salary_td.get("data-salary")
        if salary_val:
            try:
                salary = int(float(salary_val))
            except:
                salary = None
        else:
            salary_text = salary_td.get_text(strip=True).replace("$", "").replace(",", "")
            try:
                salary = int(salary_text)
            except:
                salary = None

        game_time_text = tds[2].get_text(strip=True) if len(tds) > 2 else None
        opponent = tds[3].get_text(strip=True) if len(tds) > 3 else None

        rows.append({
            "player_name": player_name,
            "team": team,
            "position": position,
            "salary": salary,
            "game_time": game_time_text,
            "opponent": opponent,
        })

    print(f"FantasyPros: {len(rows)} players scraped")
    return rows


def get_depth_chart_status(player_name, team):
    try:
        dc_df = pd.read_sql("SELECT * FROM depth_charts", conn)
    except:
        return None, None

    if dc_df.empty:
        return None, None

    norm_target = normalize_player_name(player_name)
    dc_df["norm_name"] = dc_df["player_name"].apply(normalize_player_name)

    matches = dc_df[dc_df["norm_name"] == norm_target]
    if team:
        team_matches = matches[matches["team"] == team]
        if not team_matches.empty:
            matches = team_matches

    if matches.empty:
        return None, None

    has_starter_slot = False
    best_order = 99
    for _, row in matches.iterrows():
        slot = str(row.get("position_slot", ""))
        slot_match = re.match(r'^[A-Z]+(\d+)$', slot)
        if slot_match:
            slot_num = int(slot_match.group(1))
            if slot_num == 1:
                has_starter_slot = True
            best_order = min(best_order, slot_num)

    status = "Starter" if has_starter_slot else "Bench"
    return status, best_order


rg_rows = scrape_rotogrinders()
fp_rows = scrape_fantasypros()

rg_by_name_team = {}
rg_by_name_only = {}
for row in rg_rows:
    norm = normalize_player_name(row["player_name"])
    team = row.get("team")
    if team:
        rg_by_name_team[(norm, team)] = row
    if norm not in rg_by_name_only:
        rg_by_name_only[norm] = []
    rg_by_name_only[norm].append(row)

salary_patches = 0
players_added = 0

for fp_row in fp_rows:
    fp_norm = normalize_player_name(fp_row["player_name"])
    fp_team = fp_row.get("team")

    matched_rg = None
    if fp_team:
        matched_rg = rg_by_name_team.get((fp_norm, fp_team))

    if not matched_rg:
        candidates = rg_by_name_only.get(fp_norm, [])
        if len(candidates) == 1:
            matched_rg = candidates[0]
        elif len(candidates) > 1:
            print(f"  SKIP ambiguous name match: {fp_row['player_name']} ({fp_team}) - {len(candidates)} RG candidates")
            continue

    if matched_rg:
        if not matched_rg.get("salary") and fp_row.get("salary"):
            matched_rg["salary"] = fp_row["salary"]
            salary_patches += 1
            print(f"  Patched salary: {matched_rg['player_name']} -> ${fp_row['salary']:,}")
    else:
        if not fp_row.get("salary"):
            continue

        status = None
        roster_order = None
        if fp_team:
            status, roster_order = get_depth_chart_status(fp_row["player_name"], fp_team)
            if status is None:
                print(f"  No depth chart match for {fp_row['player_name']} ({fp_team}) - status unknown")

        opponent = fp_row.get("opponent", "")
        if fp_team and opponent:
            game = f"{fp_team} vs {opponent}"
        else:
            game = "Unknown"

        new_row = {
            "player_name": fp_row["player_name"],
            "team": fp_team,
            "position": fp_row.get("position"),
            "salary": fp_row["salary"],
            "status": status,
            "roster_order": roster_order if roster_order and roster_order < 99 else None,
            "game": game,
            "game_time": fp_row.get("game_time"),
            "scraped_at": datetime.utcnow().isoformat()
        }
        rg_rows.append(new_row)
        players_added += 1

        if fp_team:
            rg_by_name_team[(fp_norm, fp_team)] = new_row
        if fp_norm not in rg_by_name_only:
            rg_by_name_only[fp_norm] = []
        rg_by_name_only[fp_norm].append(new_row)
        print(f"  Added from FantasyPros: {fp_row['player_name']} ({fp_team}) ${fp_row['salary']:,} [{status or 'Unknown'}]")

print(f"\nSalary patches applied: {salary_patches}")
print(f"Players added from FantasyPros: {players_added}")

df = pd.DataFrame(rg_rows)

if not df.empty:
    df = df.drop_duplicates(subset=["player_name", "team"], keep="first")
    df.to_sql("player_salaries", conn, if_exists="replace", index=False)
    print(f"\nFinal player_salaries table: {len(df)} players saved.")

    missing_salary = df[df["salary"].isna() | (df["salary"] == 0)]
    if not missing_salary.empty:
        print(f"WARNING: {len(missing_salary)} players still missing salary:")
        for _, row in missing_salary.iterrows():
            print(f"  - {row['player_name']} ({row.get('team', '?')})")
    else:
        print("All players have valid salaries.")
else:
    print("No player salary data found from any source.")

conn.close()
