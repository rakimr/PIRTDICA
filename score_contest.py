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
    """Fetch actual player stats from NBA.com for a given date."""
    date_str = game_date.strftime("%m/%d/%Y")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Referer': 'https://www.nba.com/',
        'Origin': 'https://www.nba.com'
    }
    
    url = "https://stats.nba.com/stats/leaguegamelog"
    params = {
        "Counter": 0,
        "DateFrom": date_str,
        "DateTo": date_str,
        "Direction": "DESC",
        "LeagueID": "00",
        "PlayerOrTeam": "P",
        "Season": "2025-26",
        "SeasonType": "Regular Season",
        "Sorter": "FGM"
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        headers_list = data['resultSets'][0]['headers']
        rows = data['resultSets'][0]['rowSet']
        
        if not rows:
            print(f"No game data found for {game_date}")
            return pd.DataFrame()
        
        df = pd.DataFrame(rows, columns=headers_list)
        
        df['FP'] = (
            df['PTS'] + 
            df['REB'] * 1.2 + 
            df['AST'] * 1.5 + 
            df['STL'] * 3 + 
            df['BLK'] * 3 - 
            df['TOV']
        )
        
        df['player_name'] = df['PLAYER_NAME']
        
        return df[['player_name', 'FP', 'MIN', 'PTS', 'REB', 'AST', 'STL', 'BLK', 'TOV']]
        
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
    print(f"Winners: {winners} ({winners/len(entries)*100:.1f}% if entries else 0)")
    
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
