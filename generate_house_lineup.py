"""
Generate the daily house lineup using Monte Carlo simulation and save to database.
Run this after run_daily_update.py completes.
"""
import os
import sys
from datetime import datetime, date, timedelta
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from backend.database import Base, engine
from backend import models
from utils.timezone import get_eastern_today, get_eastern_now

def generate_house_lineup(force=False, exclude_teams=None):
    """Generate today's house lineup using Monte Carlo and save to DB.
    
    Args:
        force: Force regenerate even if contest exists
        exclude_teams: List of team abbreviations to exclude (e.g. ['SA', 'DEN'])
    """
    
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    db = Session()
    
    today = get_eastern_today()
    
    existing = db.query(models.Contest).filter(models.Contest.slate_date == today).first()
    if existing:
        if force:
            print(f"Force refresh: deleting existing contest for {today}")
            db.query(models.HouseLineupPlayer).filter(models.HouseLineupPlayer.contest_id == existing.id).delete()
            db.query(models.EntryPlayer).filter(
                models.EntryPlayer.entry_id.in_(
                    db.query(models.ContestEntry.id).filter(models.ContestEntry.contest_id == existing.id)
                )
            ).delete(synchronize_session=False)
            db.query(models.ContestEntry).filter(models.ContestEntry.contest_id == existing.id).delete()
            db.query(models.Contest).filter(models.Contest.id == existing.id).delete()
            db.commit()
        else:
            print(f"Contest already exists for {today}")
            db.close()
            return
    
    try:
        players_df = pd.read_csv("dfs_players.csv")
        if len(players_df) == 0:
            print("No players in dfs_players.csv")
            db.close()
            return
    except Exception as e:
        print(f"Error loading players: {e}")
        db.close()
        return
    
    print(f"Loaded {len(players_df)} players")
    
    # Filter out excluded teams
    if exclude_teams:
        before_count = len(players_df)
        players_df = players_df[~players_df['team'].isin(exclude_teams)]
        print(f"Excluded teams {exclude_teams}: filtered {before_count - len(players_df)} players")
    
    import sqlite3
    try:
        conn = sqlite3.connect("dfs_nba.db")
        game_times_df = pd.read_sql_query(
            "SELECT DISTINCT game, game_time FROM player_salaries WHERE game_time IS NOT NULL", 
            conn
        )
        conn.close()
        game_times = dict(zip(game_times_df['game'], game_times_df['game_time']))
        
        from zoneinfo import ZoneInfo
        eastern = ZoneInfo("America/New_York")
        now = datetime.now(eastern)
        
        def is_game_locked(team, opponent):
            for game_key, game_time_str in game_times.items():
                if team in game_key and opponent in game_key:
                    try:
                        game_dt = datetime.strptime(game_time_str, "%I:%M%p")
                        game_dt = game_dt.replace(year=now.year, month=now.month, day=now.day, tzinfo=eastern)
                        return now >= game_dt
                    except:
                        pass
            return False
        
        players_df['is_locked'] = players_df.apply(
            lambda row: is_game_locked(row['team'], row['opponent']), axis=1
        )
        
        locked_count = players_df['is_locked'].sum()
        if locked_count > 0:
            print(f"Filtering out {locked_count} players from locked games")
            players_df = players_df[~players_df['is_locked']]
        
        if len(players_df) == 0:
            print("No unlocked players available")
            db.close()
            return
            
    except Exception as e:
        print(f"Warning: Could not check game locks: {e}")
    
    players_df['score'] = players_df['proj_fp']
    
    try:
        from monte_carlo_optimizer import generate_diverse_lineups, simulate_outcomes, evaluate_lineups
        
        print("Running Monte Carlo simulation...")
        lineups = generate_diverse_lineups(players_df, num_lineups=25)
        print(f"Generated {len(lineups)} lineups")
        
        simulated_scores = simulate_outcomes(players_df, num_sims=10000)
        print("Ran 10,000 simulations")
        
        results = evaluate_lineups(lineups, simulated_scores)
        
        best_lineup = results[0]['players']
        win_rate = results[0]['win_rate']
        print(f"Best lineup has {win_rate:.2f}% win rate")
        
    except Exception as e:
        print(f"Monte Carlo failed, using LP optimizer: {e}")
        from optimize_fanduel import optimize_lineup
        best_lineup = optimize_lineup(players_df)
    
    lock_time = datetime.combine(today, datetime.strptime("19:00", "%H:%M").time())
    
    contest = models.Contest(
        slate_date=today,
        lock_time=lock_time,
        status="open"
    )
    db.add(contest)
    db.flush()
    
    total_proj = 0
    for player_name in best_lineup:
        player_data = players_df[players_df['player_name'] == player_name].iloc[0]
        
        house_player = models.HouseLineupPlayer(
            contest_id=contest.id,
            player_name=player_name,
            position=str(player_data.get('position', '')),
            team=str(player_data.get('team', '')),
            salary=int(player_data.get('salary', 0)),
            proj_fp=float(player_data.get('proj_fp', 0))
        )
        db.add(house_player)
        total_proj += float(player_data.get('proj_fp', 0))
    
    contest.house_lineup_score = total_proj
    db.commit()
    
    print(f"\nCreated contest for {today}")
    print(f"House lineup: {', '.join(best_lineup)}")
    print(f"Projected score: {total_proj:.1f} FP")
    
    db.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--force', action='store_true', help='Force regenerate even if contest exists')
    parser.add_argument('--exclude', nargs='+', help='Team abbreviations to exclude (e.g. SA DEN)')
    args = parser.parse_args()
    generate_house_lineup(force=args.force, exclude_teams=args.exclude)
