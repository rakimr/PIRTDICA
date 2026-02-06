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

def fetch_actual_stats(game_date: date) -> pd.DataFrame:
    """Fetch actual player stats from Basketball Reference for a given date."""
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
            print(f"No stats table found for {game_date}")
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
            print(f"No player data parsed for {game_date}")
            return pd.DataFrame()
        
        df = pd.DataFrame(players)
        print(f"Fetched stats for {len(df)} players from {game_date}")
        return df
        
    except Exception as e:
        print(f"Error fetching stats: {e}")
        return pd.DataFrame()

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
    
    if actual_stats.empty:
        print("No stats available yet. Games may still be in progress.")
        db.close()
        return
    
    name_to_fp = {}
    for _, row in actual_stats.iterrows():
        normalized = normalize_name(row['player_name'])
        if normalized in name_to_fp:
            name_to_fp[normalized] += row['FP']
        else:
            name_to_fp[normalized] = row['FP']
    
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
    for snapshot in snapshots:
        normalized = snapshot.player_name_normalized or normalize_name(snapshot.player_name)
        if normalized in name_to_fp:
            snapshot.actual_fp = name_to_fp[normalized]
            if snapshot.proj_fp and snapshot.proj_fp > 0:
                snapshot.prediction_error = name_to_fp[normalized] - snapshot.proj_fp
            snapshot_matched += 1
    
    print(f"Updated {snapshot_matched} projection snapshots with actual FP")
    
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
        entry.beat_house = entry_total > house_total
    
    contest.status = 'completed'
    db.commit()
    
    print(f"\n=== Contest Results for {contest_date} ===")
    print(f"House lineup score: {house_total:.1f} FP")
    print(f"Total entries: {len(entries)}")
    winners = sum(1 for e in entries if e.beat_house)
    print(f"Winners: {winners} ({winners/len(entries)*100:.1f}%)" if entries else "Winners: 0")
    
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
    player_display_names = {}
    for snap in snapshots:
        normalized = snap.player_name_normalized or normalize_name(snap.player_name)
        if normalized not in player_errors:
            player_errors[normalized] = []
            player_display_names[normalized] = snap.player_name
        error_pct = (snap.actual_fp - snap.proj_fp) / snap.proj_fp
        player_errors[normalized].append(error_pct)
    
    print(f"\nUpdating adjustment factors for {len(player_errors)} players...")
    
    import statistics
    
    for normalized_name, errors in player_errors.items():
        if len(errors) < 3:
            continue
        
        avg_error = sum(errors) / len(errors)
        
        adjustment = 1.0 + (avg_error * 0.5)
        adjustment = max(0.7, min(1.3, adjustment))
        
        consistency = 1.0 / (1.0 + statistics.stdev(errors)) if len(errors) > 1 else 0.5
        
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
        else:
            factor = models.PlayerAdjustmentFactor(
                player_name=display_name,
                player_name_normalized=normalized_name,
                sample_size=len(errors),
                avg_prediction_error=avg_error,
                adjustment_factor=adjustment,
                consistency_score=consistency
            )
            db.add(factor)
    
    db.commit()
    db.close()
    print("Adjustment factors updated successfully")

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
