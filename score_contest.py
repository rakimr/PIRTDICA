"""
Score completed contests by fetching actual fantasy points and updating the database.
Run this after games complete to record actual vs projected performance.
"""
import os
import sys
import argparse
from datetime import datetime, date, timedelta
import pandas as pd
import requests
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from backend.database import Base, engine
from backend import models
from utils.timezone import get_eastern_today

def fetch_actual_stats_nba(game_date: date) -> pd.DataFrame:
    """Fetch actual player stats from NBA.com stats API (primary source).
    Returns official FanDuel FP values and minutes directly."""
    import time
    import random
    try:
        from nba_api.stats.endpoints import PlayerGameLogs
        from utils.nba_api_helpers import MAX_RETRIES, RETRY_DELAYS, NBA_TIMEOUT, get_nba_headers
        
        date_str = game_date.strftime("%m/%d/%Y")
        df = None
        for attempt in range(MAX_RETRIES):
            try:
                warmup = random.uniform(2, 5)
                time.sleep(warmup)
                headers = get_nba_headers()
                logs = PlayerGameLogs(
                    season_nullable='2025-26',
                    season_type_nullable='Regular Season',
                    date_from_nullable=date_str,
                    date_to_nullable=date_str,
                    league_id_nullable='00',
                    timeout=NBA_TIMEOUT,
                    headers=headers
                )
                df = logs.get_data_frames()[0]
                break
            except Exception as retry_err:
                base_delay = RETRY_DELAYS[attempt] if attempt < len(RETRY_DELAYS) else 60
                jitter = random.uniform(-3, 3)
                delay = max(3, base_delay + jitter)
                print(f"  NBA.com attempt {attempt+1}/{MAX_RETRIES} failed: {retry_err}")
                if attempt < MAX_RETRIES - 1:
                    print(f"  Retrying in {delay:.0f}s...")
                    time.sleep(delay)
        
        if df is None:
            print(f"NBA.com unreachable after {MAX_RETRIES} attempts")
            return pd.DataFrame()
        
        if df.empty:
            print(f"No NBA.com data for {game_date}")
            return pd.DataFrame()
        
        players = []
        for _, row in df.iterrows():
            fp = row.get('NBA_FANTASY_PTS', 0) or 0
            mins = row.get('MIN', 0) or 0
            players.append({
                'player_name': row['PLAYER_NAME'],
                'FP': round(float(fp), 1),
                'MIN': float(mins),
                'PTS': float(row.get('PTS', 0) or 0),
                'REB': float(row.get('REB', 0) or 0),
                'AST': float(row.get('AST', 0) or 0),
                'STL': float(row.get('STL', 0) or 0),
                'BLK': float(row.get('BLK', 0) or 0),
                'TOV': float(row.get('TOV', 0) or 0)
            })
        
        result = pd.DataFrame(players)
        print(f"Fetched stats for {len(result)} players from NBA.com for {game_date}")
        return result
        
    except Exception as e:
        print(f"NBA.com API error: {e}")
        return pd.DataFrame()


def fetch_actual_stats_bbref(game_date: date) -> pd.DataFrame:
    """Fetch actual player stats from Basketball Reference (fallback source).
    Manually calculates FanDuel FP from box score stats."""
    from bs4 import BeautifulSoup
    
    url = f"https://www.basketball-reference.com/friv/dailyleaders.fcgi?month={game_date.month}&day={game_date.day}&year={game_date.year}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        table = soup.find('table', {'id': 'stats'})
        if not table:
            print(f"No stats table found on BBRef for {game_date}")
            return pd.DataFrame()
        
        rows = table.find('tbody').find_all('tr', class_=lambda x: x != 'thead')
        
        players = []
        for row in rows:
            cells = {td.get('data-stat'): td for td in row.find_all('td')}
            if not cells or 'player' not in cells:
                continue
            
            try:
                name = cells['player'].get_text(strip=True)
                pts = float(cells.get('pts', {}).get_text(strip=True) or 0) if cells.get('pts') else 0
                trb = float(cells.get('trb', {}).get_text(strip=True) or 0) if cells.get('trb') else 0
                ast = float(cells.get('ast', {}).get_text(strip=True) or 0) if cells.get('ast') else 0
                stl = float(cells.get('stl', {}).get_text(strip=True) or 0) if cells.get('stl') else 0
                blk = float(cells.get('blk', {}).get_text(strip=True) or 0) if cells.get('blk') else 0
                tov = float(cells.get('tov', {}).get_text(strip=True) or 0) if cells.get('tov') else 0
                mins = cells.get('mp', {}).get_text(strip=True) if cells.get('mp') else '0'
                
                fp = pts + (trb * 1.2) + (ast * 1.5) + (stl * 3) + (blk * 3) - tov
                
                players.append({
                    'player_name': name,
                    'FP': round(fp, 1),
                    'MIN': mins,
                    'PTS': pts,
                    'REB': trb,
                    'AST': ast,
                    'STL': stl,
                    'BLK': blk,
                    'TOV': tov
                })
            except (ValueError, IndexError, AttributeError):
                continue
        
        if not players:
            print(f"No player data parsed from BBRef for {game_date}")
            return pd.DataFrame()
        
        df = pd.DataFrame(players)
        print(f"Fetched stats for {len(df)} players from BBRef for {game_date}")
        return df
        
    except Exception as e:
        print(f"BBRef error: {e}")
        return pd.DataFrame()


def fetch_actual_stats(game_date: date) -> pd.DataFrame:
    """Fetch actual player stats, trying NBA.com first, then Basketball Reference."""
    df = fetch_actual_stats_nba(game_date)
    if not df.empty:
        return df
    
    print("Falling back to Basketball Reference...")
    return fetch_actual_stats_bbref(game_date)

from utils.name_normalize import normalize_player_name as normalize_name

def score_contest(contest_date: date = None, force: bool = False):
    """Score a contest by updating actual FP values."""
    
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    db = Session()
    
    if contest_date is None:
        contest_date = get_eastern_today() - timedelta(days=1)
    
    contest = db.query(models.Contest).filter(
        models.Contest.slate_date == contest_date
    ).first()
    
    if not contest:
        print(f"No contest found for {contest_date}")
        db.close()
        return
    
    if contest.status == 'completed' and not force:
        print(f"Contest for {contest_date} already scored. Use --force to rescore.")
        db.close()
        return
    
    print(f"Scoring contest for {contest_date}...")
    
    actual_stats = fetch_actual_stats(contest_date)
    
    def parse_minutes_str(min_str):
        """Convert minutes string like '32:15' or '32' to float minutes."""
        if not min_str:
            return 0.0
        try:
            if ':' in str(min_str):
                parts = str(min_str).split(':')
                return float(parts[0]) + float(parts[1]) / 60.0
            return float(min_str)
        except (ValueError, IndexError):
            return 0.0
    
    name_to_fp = {}
    name_to_min = {}
    if not actual_stats.empty:
        for _, row in actual_stats.iterrows():
            normalized = normalize_name(row['player_name'])
            if normalized in name_to_fp:
                name_to_fp[normalized] += row['FP']
            else:
                name_to_fp[normalized] = row['FP']
            mins = parse_minutes_str(row.get('MIN', 0))
            if normalized in name_to_min:
                name_to_min[normalized] += mins
            else:
                name_to_min[normalized] = mins
        print(f"Loaded {len(name_to_fp)} players with actual stats")
    else:
        print("NBA.com and BBRef unavailable, trying live scores...")
        try:
            from scrape_live_scores import get_all_live_scores
            live_players = get_all_live_scores(contest_date.strftime("%Y-%m-%d"))
            if live_players:
                for norm_name, data in live_players.items():
                    name_to_fp[norm_name] = round(data['fp'], 1)
                    name_to_min[norm_name] = parse_minutes_str(data.get('minutes', 0))
                print(f"Using live scores data ({len(name_to_fp)} players)")
            else:
                print("No live scores available either. Games may still be in progress.")
                db.close()
                return
        except Exception as e:
            print(f"Error fetching live scores: {e}")
            db.close()
            return
    
    if not name_to_fp:
        print("No scoring data available.")
        db.close()
        return
    
    house_players = db.query(models.HouseLineupPlayer).filter(
        models.HouseLineupPlayer.contest_id == contest.id
    ).all()
    
    house_total = 0
    matched = 0
    for player in house_players:
        normalized = normalize_name(player.player_name)
        if normalized in name_to_fp:
            player.actual_fp = name_to_fp[normalized]
            house_total += name_to_fp[normalized]
            matched += 1
        else:
            player.actual_fp = 0
    
    print(f"Matched {matched}/{len(house_players)} house players")
    contest.house_lineup_score = house_total
    
    snapshots = db.query(models.ProjectionSnapshot).filter(
        models.ProjectionSnapshot.contest_id == contest.id
    ).all()
    
    snapshot_matched = 0
    minutes_matched = 0
    for snapshot in snapshots:
        normalized = snapshot.player_name_normalized or normalize_name(snapshot.player_name)
        if normalized in name_to_fp:
            snapshot.actual_fp = name_to_fp[normalized]
            if snapshot.proj_fp and snapshot.proj_fp > 0:
                snapshot.prediction_error = name_to_fp[normalized] - snapshot.proj_fp
            snapshot_matched += 1
        if normalized in name_to_min:
            snapshot.actual_min = name_to_min[normalized]
            if snapshot.proj_min and snapshot.proj_min > 0:
                snapshot.minutes_error = name_to_min[normalized] - snapshot.proj_min
            minutes_matched += 1
    
    print(f"Updated {snapshot_matched} projection snapshots with actual FP, {minutes_matched} with actual minutes")
    
    entries = db.query(models.ContestEntry).filter(
        models.ContestEntry.contest_id == contest.id
    ).all()
    
    for entry in entries:
        entry_players = db.query(models.EntryPlayer).filter(
            models.EntryPlayer.entry_id == entry.id
        ).all()
        
        entry_total = 0
        for player in entry_players:
            normalized = normalize_name(player.player_name)
            if normalized in name_to_fp:
                player.actual_fp = name_to_fp[normalized]
                entry_total += name_to_fp[normalized]
            else:
                player.actual_fp = 0
        
        entry.actual_score = entry_total
        
        if entry.house_lineup_snapshot:
            import json
            try:
                snapshot_players = json.loads(entry.house_lineup_snapshot)
                snapshot_actual = 0
                for sp in snapshot_players:
                    norm = normalize_name(sp['player_name'])
                    snapshot_actual += name_to_fp.get(norm, 0)
                entry.house_actual_score = snapshot_actual
                entry.beat_house = entry_total > snapshot_actual
            except:
                entry.house_actual_score = house_total
                entry.beat_house = entry_total > house_total
        else:
            entry.house_actual_score = house_total
            entry.beat_house = entry_total > house_total
    
    contest.status = 'completed'
    db.commit()
    
    print(f"\n=== Contest Results for {contest_date} ===")
    print(f"House lineup score: {house_total:.1f} FP")
    print(f"Total entries: {len(entries)}")
    winners = sum(1 for e in entries if e.beat_house)
    print(f"Winners: {winners} ({winners/len(entries)*100:.1f}%)" if entries else "Winners: 0")
    
    try:
        from backend.achievements import check_scoring_achievements
        badges_awarded = 0
        for entry in entries:
            try:
                check_scoring_achievements(db, entry.user_id, entry)
                badges_awarded += 1
            except Exception as e:
                print(f"Achievement check error for user {entry.user_id}: {e}")
        db.commit()
        print(f"Achievement checks completed for {len(entries)} entries")
    except Exception as e:
        db.rollback()
        print(f"Achievement system error: {e}")
    
    try:
        from backend.main import settle_h2h_challenges
        settle_h2h_challenges(db)
    except Exception as e:
        print(f"H2H settlement: {e}")
    
    db.close()

def update_adjustment_factors():
    """Calculate and update player adjustment factors based on historical performance."""
    
    Session = sessionmaker(bind=engine)
    db = Session()
    
    snapshots = db.query(models.ProjectionSnapshot).filter(
        models.ProjectionSnapshot.actual_fp.isnot(None),
        models.ProjectionSnapshot.proj_fp.isnot(None),
        models.ProjectionSnapshot.proj_fp > 0
    ).all()
    
    player_errors = {}
    player_actuals = {}
    player_display_names = {}
    player_minutes_errors = {}
    for snap in snapshots:
        normalized = snap.player_name_normalized or normalize_name(snap.player_name)
        if normalized not in player_errors:
            player_errors[normalized] = []
            player_actuals[normalized] = []
            player_display_names[normalized] = snap.player_name
            player_minutes_errors[normalized] = []
        error_pct = (snap.actual_fp - snap.proj_fp) / snap.proj_fp
        player_errors[normalized].append(error_pct)
        player_actuals[normalized].append(snap.actual_fp)
        if snap.actual_min is not None and snap.proj_min and snap.proj_min > 0:
            min_error_pct = (snap.actual_min - snap.proj_min) / snap.proj_min
            player_minutes_errors[normalized].append(min_error_pct)
    
    print(f"\nUpdating adjustment factors for {len(player_errors)} players...")
    
    import statistics
    
    bias_updated = 0
    variance_updated = 0
    minutes_updated = 0
    
    for normalized_name, errors in player_errors.items():
        if len(errors) < 3:
            continue
        
        avg_error = sum(errors) / len(errors)
        adjustment = 1.0 + (avg_error * 0.5)
        adjustment = max(0.7, min(1.3, adjustment))
        
        error_std = statistics.stdev(errors) if len(errors) > 1 else 0
        consistency = 1.0 / (1.0 + error_std) if len(errors) > 1 else 0.5
        
        prediction_variance = error_std
        
        if error_std < 0.2:
            variance_dampening = 1.0
        elif error_std < 0.4:
            variance_dampening = 0.95
        elif error_std < 0.6:
            variance_dampening = 0.88
        elif error_std < 0.8:
            variance_dampening = 0.80
        else:
            variance_dampening = 0.72
        
        avg_actual = sum(player_actuals[normalized_name]) / len(player_actuals[normalized_name])
        
        min_errors = player_minutes_errors.get(normalized_name, [])
        min_sample = len(min_errors)
        avg_min_error = 0.0
        min_adjustment = 1.0
        min_consistency = 0.0
        
        if min_sample >= 2:
            avg_min_error = sum(min_errors) / min_sample
            min_adjustment = 1.0 + (avg_min_error * 0.6)
            min_adjustment = max(0.6, min(1.4, min_adjustment))
            min_std = statistics.stdev(min_errors) if min_sample > 1 else 0
            min_consistency = 1.0 / (1.0 + min_std)
            minutes_updated += 1
        
        existing = db.query(models.PlayerAdjustmentFactor).filter(
            models.PlayerAdjustmentFactor.player_name_normalized == normalized_name
        ).first()
        
        display_name = player_display_names.get(normalized_name, normalized_name)
        
        if existing:
            existing.player_name = display_name
            existing.sample_size = len(errors)
            existing.avg_prediction_error = avg_error
            existing.adjustment_factor = adjustment
            existing.consistency_score = consistency
            existing.prediction_variance = prediction_variance
            existing.variance_dampening = variance_dampening
            existing.avg_actual_fp = avg_actual
            existing.minutes_sample_size = min_sample
            existing.avg_minutes_error = avg_min_error
            existing.minutes_adjustment_factor = min_adjustment
            existing.minutes_consistency = min_consistency
        else:
            factor = models.PlayerAdjustmentFactor(
                player_name=display_name,
                player_name_normalized=normalized_name,
                sample_size=len(errors),
                avg_prediction_error=avg_error,
                adjustment_factor=adjustment,
                consistency_score=consistency,
                prediction_variance=prediction_variance,
                variance_dampening=variance_dampening,
                avg_actual_fp=avg_actual,
                minutes_sample_size=min_sample,
                avg_minutes_error=avg_min_error,
                minutes_adjustment_factor=min_adjustment,
                minutes_consistency=min_consistency
            )
            db.add(factor)
        
        bias_updated += 1
        if variance_dampening < 1.0:
            variance_updated += 1
    
    db.commit()
    db.close()
    print(f"ML Model 1 (Bias): Updated {bias_updated} players with bias correction factors")
    print(f"ML Model 2 (Variance): {variance_updated} players will have variance dampening applied")
    print(f"ML Model 3 (Minutes): {minutes_updated} players with minutes correction factors")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Score completed contests")
    parser.add_argument('--date', type=str, help='Contest date (YYYY-MM-DD). Defaults to yesterday.')
    parser.add_argument('--force', action='store_true', help='Force rescore even if already completed')
    parser.add_argument('--update-factors', action='store_true', help='Update player adjustment factors')
    args = parser.parse_args()
    
    if args.date:
        contest_date = datetime.strptime(args.date, '%Y-%m-%d').date()
    else:
        contest_date = None
    
    score_contest(contest_date, args.force)
    
    if args.update_factors:
        update_adjustment_factors()
