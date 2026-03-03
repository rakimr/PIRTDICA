"""
Basketball Reference game log scraper.
Fallback data source when NBA.com stats API is unreachable.
Scrapes box scores for missing dates and produces the same output
format as scrape_nba_gamelogs.py (player_game_logs + player_volatility).
"""

import requests
import pandas as pd
import numpy as np
import sqlite3
import time
import re
import io
import os
import sys
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}
REQUEST_DELAY = 3.0

BREF_TO_STANDARD = {
    "BRK": "BKN",
    "CHO": "CHA",
    "NJN": "BKN",
    "NOH": "NOP",
    "NOK": "NOP",
}


def bref_team(abbr):
    return BREF_TO_STANDARD.get(abbr, abbr)


def parse_minutes(mp_str):
    if not mp_str or not isinstance(mp_str, str):
        return 0
    if ":" in mp_str:
        parts = mp_str.split(":")
        try:
            return int(parts[0]) + int(parts[1]) / 60
        except (ValueError, IndexError):
            return 0
    try:
        return float(mp_str)
    except ValueError:
        return 0


def calc_fanduel_fp(pts, reb, ast, stl, blk, tov):
    return pts + (reb * 1.2) + (ast * 1.5) + (stl * 3) + (blk * 3) - tov


def get_latest_game_date():
    conn = sqlite3.connect("dfs_nba.db")
    try:
        result = conn.execute("SELECT MAX(game_date) FROM player_game_logs").fetchone()
        if result and result[0]:
            return result[0]
    except Exception:
        pass
    finally:
        conn.close()
    return None


def get_games_for_date(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    url = f"https://www.basketball-reference.com/boxscores/?month={dt.month}&day={dt.day}&year={dt.year}"

    resp = requests.get(url, headers=HEADERS, timeout=30)
    if resp.status_code != 200:
        print(f"  WARNING: Got status {resp.status_code} for {date_str} schedule page")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    game_links = set()
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if (
            "/boxscores/20" in href
            and href.endswith(".html")
            and "pbp" not in href
            and "shot-chart" not in href
            and "plus-minus" not in href
        ):
            game_links.add(href)

    return sorted(game_links)


def parse_box_score(box_url, game_date):
    full_url = f"https://www.basketball-reference.com{box_url}"

    resp = requests.get(full_url, headers=HEADERS, timeout=30)
    if resp.status_code != 200:
        print(f"    WARNING: Got status {resp.status_code} for {box_url}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    tables = soup.find_all("table", id=re.compile(r"box-(\w+)-game-basic"))
    if len(tables) < 2:
        print(f"    WARNING: Found {len(tables)} box score tables in {box_url}, expected 2")
        return []

    team_abbrs = []
    for t in tables:
        match = re.search(r"box-(\w+)-game-basic", t["id"])
        if match:
            team_abbrs.append(bref_team(match.group(1)))

    home_team = None
    away_team = None
    scorebox = soup.find("div", class_="scorebox")
    if scorebox:
        team_links = scorebox.find_all("a", href=re.compile(r"/teams/\w+/"))
        if len(team_links) >= 2:
            away_abbr = bref_team(re.search(r"/teams/(\w+)/", team_links[0]["href"]).group(1))
            home_abbr = bref_team(re.search(r"/teams/(\w+)/", team_links[1]["href"]).group(1))
            away_team = away_abbr
            home_team = home_abbr

    if not home_team or not away_team:
        away_team = team_abbrs[0]
        home_team = team_abbrs[1]

    rows = []
    for idx, table in enumerate(tables):
        team = team_abbrs[idx]
        is_home = team == home_team
        opp = away_team if is_home else home_team

        matchup = f"{team} vs. {opp}" if is_home else f"{team} @ {opp}"

        try:
            df = pd.read_html(io.StringIO(str(table)))[0]
        except Exception as e:
            print(f"    WARNING: Could not parse table for {team}: {e}")
            continue

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[-1] for c in df.columns]

        name_col = df.columns[0]

        skip_values = {"Reserves", "Team Totals", "Starters"}
        df = df[~df[name_col].isin(skip_values)]

        df = df[~df["MP"].astype(str).str.contains("Did Not|Player Suspended|Not With|Inactive", case=False, na=False)]

        for _, row in df.iterrows():
            player_name = str(row[name_col]).strip()
            if not player_name or player_name == "nan":
                continue

            minutes = parse_minutes(str(row.get("MP", "0")))
            if minutes <= 0:
                continue

            try:
                pts = int(float(row.get("PTS", 0) or 0))
                reb = int(float(row.get("TRB", 0) or 0))
                ast = int(float(row.get("AST", 0) or 0))
                stl = int(float(row.get("STL", 0) or 0))
                blk = int(float(row.get("BLK", 0) or 0))
                tov = int(float(row.get("TOV", 0) or 0))
                fg3m = int(float(row.get("3P", 0) or 0))
            except (ValueError, TypeError):
                continue

            fp = calc_fanduel_fp(pts, reb, ast, stl, blk, tov)

            rows.append({
                "player_name": player_name,
                "game_date": game_date,
                "matchup": matchup,
                "min": round(minutes, 1),
                "pts": pts,
                "reb": reb,
                "ast": ast,
                "stl": stl,
                "blk": blk,
                "tov": tov,
                "fg3m": fg3m,
                "fp": round(fp, 1),
            })

    return rows


def get_missing_dates(latest_date_str):
    if not latest_date_str:
        start = datetime(2025, 10, 22)
    else:
        start = datetime.strptime(latest_date_str[:10], "%Y-%m-%d") + timedelta(days=1)

    today = datetime.now()
    yesterday = today - timedelta(days=1)

    dates = []
    current = start
    while current <= yesterday:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    return dates


def scrape_gamelogs_bref(max_days=None):
    latest = get_latest_game_date()
    missing_dates = get_missing_dates(latest)

    if not missing_dates:
        print("  Basketball Reference: No missing dates to backfill")
        return None

    if max_days:
        missing_dates = missing_dates[:max_days]

    print(f"  Basketball Reference fallback: backfilling {len(missing_dates)} days ({missing_dates[0]} to {missing_dates[-1]})")

    total_lines = 0
    dates_with_games = 0

    for i, date_str in enumerate(missing_dates):
        print(f"  [{i+1}/{len(missing_dates)}] Fetching games for {date_str}...", end="", flush=True)
        time.sleep(REQUEST_DELAY)

        try:
            game_links = get_games_for_date(date_str)
        except Exception as e:
            print(f" ERROR getting schedule: {e}")
            continue

        if not game_links:
            print(f" no games")
            continue

        date_rows = []
        for j, link in enumerate(game_links):
            time.sleep(REQUEST_DELAY)
            try:
                rows = parse_box_score(link, date_str)
                date_rows.extend(rows)
            except Exception as e:
                print(f"\n    ERROR parsing {link}: {e}")
                continue

        if date_rows:
            merge_and_save(pd.DataFrame(date_rows))
            dates_with_games += 1
            total_lines += len(date_rows)
            print(f" {len(game_links)} games, {len(date_rows)} lines — saved")
        else:
            print(f" {len(game_links)} games but 0 parsed lines")

    if total_lines == 0:
        print("  Basketball Reference: No game data found")
        return None

    print(f"\n  Basketball Reference: scraped {total_lines} game log entries across {dates_with_games} game days")
    return pd.DataFrame({"saved": [True]})


def merge_and_save(new_logs):
    conn = sqlite3.connect("dfs_nba.db")

    try:
        existing_logs = pd.read_sql("SELECT * FROM player_game_logs", conn)
    except Exception:
        existing_logs = pd.DataFrame()

    if len(existing_logs) > 0 and len(new_logs) > 0:
        new_logs["scraped_at"] = datetime.now().isoformat()

        new_keys = set(zip(new_logs["player_name"], new_logs["game_date"]))
        existing_keys = set(zip(existing_logs["player_name"], existing_logs["game_date"]))
        preserved_keys = existing_keys - new_keys
        preserved = existing_logs[
            existing_logs.apply(lambda r: (r["player_name"], r["game_date"]) in preserved_keys, axis=1)
        ].copy()

        merged = pd.concat([new_logs, preserved], ignore_index=True)
        merged = merged.sort_values(["player_name", "game_date"], ascending=[True, False])

        new_count = len(new_keys - existing_keys)
        updated_count = len(new_keys & existing_keys)
        preserved_count = len(preserved_keys)
        print(f"  [player_game_logs] Upsert: {updated_count} updated, {new_count} new, {preserved_count} preserved")
    elif len(new_logs) > 0:
        new_logs["scraped_at"] = datetime.now().isoformat()
        merged = new_logs
        print(f"  [player_game_logs] Fresh insert: {len(new_logs)} entries")
    else:
        conn.close()
        return

    merged.to_sql("player_game_logs", conn, if_exists="replace", index=False)
    print(f"  Saved {len(merged)} total game log entries")

    recalc_volatility(merged, conn)
    conn.close()


def recalc_volatility(all_logs, conn):
    df = all_logs.copy()
    df["min"] = pd.to_numeric(df["min"], errors="coerce").fillna(0)
    df = df[df["min"] > 0]

    for col in ["pts", "reb", "ast", "stl", "blk", "tov", "fg3m"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["fp_calc"] = df.apply(
        lambda r: calc_fanduel_fp(r["pts"], r["reb"], r["ast"], r["stl"], r["blk"], r["tov"]),
        axis=1,
    )
    df["fppm"] = df["fp_calc"] / df["min"]

    stats = df.groupby("player_name").agg(
        games_played=("min", "count"),
        avg_min=("min", "mean"),
        min_sd=("min", "std"),
        avg_fp=("fp_calc", "mean"),
        fp_sd=("fp_calc", "std"),
        avg_fppm=("fppm", "mean"),
        fppm_sd=("fppm", "std"),
        max_fp=("fp_calc", "max"),
        min_fp=("fp_calc", "min"),
    ).reset_index()

    stats["min_sd"] = stats["min_sd"].fillna(10.0).round(2)
    stats["avg_min"] = stats["avg_min"].round(2)
    stats["fp_sd"] = stats["fp_sd"].fillna(15.0).round(2)
    stats["avg_fp"] = stats["avg_fp"].round(2)
    stats["avg_fppm"] = stats["avg_fppm"].round(3)
    stats["fppm_sd"] = stats["fppm_sd"].fillna(0.5).round(3)
    stats["max_fp"] = stats["max_fp"].round(1)
    stats["min_fp"] = stats["min_fp"].round(1)

    max_games = 50

    def calc_omega(row):
        gp = min(row["games_played"] / max_games, 1.0)
        sd = row["min_sd"]
        sd_factor = max(0, min(1, 1 - (sd - 3) / 7))
        return round(max(0.10, min(0.90, (gp * 0.5) + (sd_factor * 0.5))), 3)

    stats["omega"] = stats.apply(calc_omega, axis=1)
    stats["scraped_at"] = datetime.now().isoformat()

    stats.to_sql("player_volatility", conn, if_exists="replace", index=False)
    print(f"  [player_volatility] Recalculated for {len(stats)} players")


def backfill_incremental():
    latest = get_latest_game_date()
    missing_dates = get_missing_dates(latest)

    if not missing_dates:
        print("  No missing dates to backfill")
        return

    print(f"  Backfilling {len(missing_dates)} days ({missing_dates[0]} to {missing_dates[-1]})")

    for i, date_str in enumerate(missing_dates):
        print(f"  [{i+1}/{len(missing_dates)}] {date_str}...", end="", flush=True)
        time.sleep(REQUEST_DELAY)

        try:
            game_links = get_games_for_date(date_str)
        except Exception as e:
            print(f" ERROR: {e}")
            continue

        if not game_links:
            print(" no games")
            continue

        date_rows = []
        for link in game_links:
            time.sleep(REQUEST_DELAY)
            try:
                rows = parse_box_score(link, date_str)
                date_rows.extend(rows)
            except Exception as e:
                print(f" ERR", end="")
                continue

        if date_rows:
            new_logs = pd.DataFrame(date_rows)
            merge_and_save(new_logs)
            print(f" {len(game_links)} games, {len(date_rows)} lines — saved")
        else:
            print(f" {len(game_links)} games but 0 parsed lines")

    print("\nBackfill complete!")


if __name__ == "__main__":
    print("Basketball Reference Game Log Scraper")
    print("=" * 50)
    import sys
    if "--incremental" in sys.argv:
        backfill_incremental()
    else:
        new_logs = scrape_gamelogs_bref()
        if new_logs is not None:
            merge_and_save(new_logs)
            print("\nDone!")
        else:
            print("\nNo new data to save.")
