from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from backend import models
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import func


def award_achievement(db: Session, user_id: int, code: str):
    existing = db.query(models.UserAchievement).filter(
        models.UserAchievement.user_id == user_id,
        models.UserAchievement.achievement_code == code
    ).first()
    if existing:
        return False

    achievement = db.query(models.Achievement).filter(models.Achievement.code == code).first()
    if not achievement:
        return False

    try:
        ua = models.UserAchievement(user_id=user_id, achievement_code=code)
        db.add(ua)

        if achievement.coin_reward > 0:
            user = db.query(models.User).filter(models.User.id == user_id).first()
            if user:
                user.coins += achievement.coin_reward
                tx = models.CurrencyTransaction(
                    user_id=user_id,
                    amount=achievement.coin_reward,
                    transaction_type="achievement",
                    description=f"Achievement unlocked: {achievement.name}"
                )
                db.add(tx)

        db.flush()
        return True
    except IntegrityError:
        db.rollback()
        return False


def check_contest_achievements(db: Session, user_id: int, entry: models.ContestEntry):
    total_entries = db.query(models.ContestEntry).filter(
        models.ContestEntry.user_id == user_id
    ).count()

    award_achievement(db, user_id, "first_entry")

    if total_entries >= 25:
        award_achievement(db, user_id, "entries_25")
    if total_entries >= 50:
        award_achievement(db, user_id, "entries_50")
    if total_entries >= 100:
        award_achievement(db, user_id, "entries_100")

    now = datetime.now(ZoneInfo("America/New_York"))
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    monthly_entries = db.query(models.ContestEntry).filter(
        models.ContestEntry.user_id == user_id,
        models.ContestEntry.created_at >= month_start
    ).count()
    if monthly_entries >= 10:
        award_achievement(db, user_id, "monthly_grinder")

    if entry.created_at and hasattr(entry, 'contest') and entry.contest and entry.contest.lock_time:
        time_diff = entry.contest.lock_time - entry.created_at
        if time_diff.total_seconds() > 7200:
            award_achievement(db, user_id, "early_bird")


def check_scoring_achievements(db: Session, user_id: int, entry: models.ContestEntry):
    if not entry.beat_house:
        return

    award_achievement(db, user_id, "first_win")

    total_wins = db.query(models.ContestEntry).filter(
        models.ContestEntry.user_id == user_id,
        models.ContestEntry.beat_house == True
    ).count()

    if total_wins >= 10:
        award_achievement(db, user_id, "wins_10")
    if total_wins >= 50:
        award_achievement(db, user_id, "wins_50")
    if total_wins >= 100:
        award_achievement(db, user_id, "wins_100")

    if entry.actual_score >= 400:
        award_achievement(db, user_id, "perfect_slate")

    salary_remaining = 60000 - (entry.total_salary or 0)
    if salary_remaining >= 5000:
        award_achievement(db, user_id, "value_king")
    if salary_remaining >= 7000:
        award_achievement(db, user_id, "salary_saver")

    margin = (entry.actual_score or 0) - (entry.house_actual_score or 0)
    if margin >= 50:
        award_achievement(db, user_id, "blowout_win")

    _check_win_streak(db, user_id)

    _check_six_x_king(db, entry)

    _check_archetype_badges(db, user_id, entry)


def _check_win_streak(db: Session, user_id: int):
    recent_entries = db.query(models.ContestEntry).filter(
        models.ContestEntry.user_id == user_id,
        models.ContestEntry.actual_score > 0
    ).order_by(models.ContestEntry.created_at.desc()).limit(10).all()

    streak = 0
    for e in recent_entries:
        if e.beat_house:
            streak += 1
        else:
            break

    if streak >= 3:
        award_achievement(db, user_id, "streak_3")
    if streak >= 5:
        award_achievement(db, user_id, "streak_5")
    if streak >= 10:
        award_achievement(db, user_id, "streak_10")


def _check_six_x_king(db: Session, entry: models.ContestEntry):
    players = db.query(models.EntryPlayer).filter(
        models.EntryPlayer.entry_id == entry.id
    ).all()
    for p in players:
        if p.salary and p.salary > 0 and p.actual_fp:
            value = p.actual_fp / (p.salary / 1000)
            if value >= 6.0:
                award_achievement(db, entry.user_id, "six_x_king")
                return


def _check_archetype_badges(db: Session, user_id: int, entry: models.ContestEntry):
    players = db.query(models.EntryPlayer).filter(
        models.EntryPlayer.entry_id == entry.id
    ).all()

    positions = [p.position for p in players if p.position]
    guard_count = sum(1 for p in positions if p in ('PG', 'SG'))
    wing_count = sum(1 for p in positions if p in ('SF', 'SF/PF'))
    big_count = sum(1 for p in positions if p in ('PF', 'C'))

    all_scored_25 = all(
        (p.actual_fp or 0) >= 25 for p in players if p.position in ('PG', 'SG', 'SF', 'PF', 'C')
    )
    if all_scored_25 and len(players) >= 5:
        award_achievement(db, user_id, "arch_balanced")

    if guard_count >= 4:
        _check_archetype_win_count(db, user_id, "guard", "arch_guard")
    if wing_count >= 4:
        _check_archetype_win_count(db, user_id, "wing", "arch_wing")
    if big_count >= 4:
        _check_archetype_win_count(db, user_id, "big", "arch_big")


def _check_archetype_win_count(db: Session, user_id: int, arch_type: str, badge_code: str):
    winning_entries = db.query(models.ContestEntry).filter(
        models.ContestEntry.user_id == user_id,
        models.ContestEntry.beat_house == True
    ).all()

    count = 0
    for e in winning_entries:
        players = db.query(models.EntryPlayer).filter(
            models.EntryPlayer.entry_id == e.id
        ).all()
        positions = [p.position for p in players if p.position]
        if arch_type == "guard":
            if sum(1 for p in positions if p in ('PG', 'SG')) >= 4:
                count += 1
        elif arch_type == "wing":
            if sum(1 for p in positions if p in ('SF', 'SF/PF')) >= 4:
                count += 1
        elif arch_type == "big":
            if sum(1 for p in positions if p in ('PF', 'C')) >= 4:
                count += 1
    if count >= 10:
        award_achievement(db, user_id, badge_code)


def check_h2h_achievements(db: Session, user_id: int, challenge: models.H2HChallenge):
    if challenge.winner_id != user_id:
        return

    award_achievement(db, user_id, "h2h_first")

    h2h_wins = db.query(models.H2HChallenge).filter(
        models.H2HChallenge.winner_id == user_id,
        models.H2HChallenge.status == "completed"
    ).count()

    if h2h_wins >= 10:
        award_achievement(db, user_id, "h2h_wins_10")
    if h2h_wins >= 25:
        award_achievement(db, user_id, "h2h_wins_25")

    if challenge.wager >= 100:
        award_achievement(db, user_id, "h2h_high_roller")

    opponent_id = challenge.opponent_id if challenge.challenger_id == user_id else challenge.challenger_id
    if opponent_id:
        user_rank = db.query(models.LeaderboardCache).filter(
            models.LeaderboardCache.user_id == user_id,
            models.LeaderboardCache.period == "all_time"
        ).first()
        opp_rank = db.query(models.LeaderboardCache).filter(
            models.LeaderboardCache.user_id == opponent_id,
            models.LeaderboardCache.period == "all_time"
        ).first()
        if user_rank and opp_rank and user_rank.rank > opp_rank.rank and opp_rank.rank > 0:
            award_achievement(db, user_id, "giant_killer")

    _check_h2h_streak(db, user_id)


def check_ranked_achievements(db: Session, user_id: int, challenge: models.H2HChallenge, ranking_result: dict):
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        return

    is_winner = (challenge.winner_id == user_id)

    if is_winner:
        ranked_wins = user.ranked_wins or 0
        if ranked_wins == 1:
            award_achievement(db, user_id, "first_blood")
        if ranked_wins >= 10:
            award_achievement(db, user_id, "ranked_wins_10")
        if ranked_wins >= 25:
            award_achievement(db, user_id, "ranked_wins_25")
        if ranked_wins >= 50:
            award_achievement(db, user_id, "ranked_wins_50")
        if ranked_wins >= 100:
            award_achievement(db, user_id, "ranked_wins_100")

    streak = user.ranked_streak or 0
    if streak >= 5:
        award_achievement(db, user_id, "win_streak_5")
    if streak >= 10:
        award_achievement(db, user_id, "win_streak_10")

    ranked_wins = user.ranked_wins or 0
    ranked_losses = user.ranked_losses or 0
    total_ranked = ranked_wins + ranked_losses
    if total_ranked >= 100 and ranked_wins / total_ranked >= 0.60:
        award_achievement(db, user_id, "win_rate_60")

    if ranking_result.get("promo_clutch"):
        award_achievement(db, user_id, "promo_clutch")

    _check_division_achievements(db, user_id, user)

    if is_winner:
        _check_ranked_statistical(db, user_id, challenge)
        _check_secret_achievements(db, user_id, challenge, user)


def _check_division_achievements(db: Session, user_id: int, user):
    from backend.ranking import DIVISIONS
    division = user.division or "Bronze"
    div_badges = {
        "Silver": "reach_silver",
        "Gold": "reach_gold",
        "Platinum": "reach_platinum",
        "Diamond": "reach_diamond",
        "Master": "reach_master",
        "Grandmaster": "reach_grandmaster",
        "Champion": "reach_champion",
    }
    div_idx = DIVISIONS.index(division) if division in DIVISIONS else 0
    for div_name, badge_code in div_badges.items():
        target_idx = DIVISIONS.index(div_name) if div_name in DIVISIONS else 99
        if div_idx >= target_idx:
            award_achievement(db, user_id, badge_code)


def _check_ranked_statistical(db: Session, user_id: int, challenge: models.H2HChallenge):
    winner_score = 0
    winner_proj = 0
    winner_salary = 0
    players = db.query(models.H2HLineupPlayer).filter(
        models.H2HLineupPlayer.challenge_id == challenge.id,
        models.H2HLineupPlayer.user_id == user_id
    ).all()
    for p in players:
        winner_score += (p.actual_fp or 0)
        winner_proj += (p.proj_fp or 0)
        winner_salary += (p.salary or 0)

    if winner_proj > 0 and winner_score > winner_proj * 1.25:
        award_achievement(db, user_id, "projection_crusher")

    if winner_salary > 0 and winner_salary < 55000:
        award_achievement(db, user_id, "efficiency_king")

    if winner_score >= 350:
        award_achievement(db, user_id, "slate_dominator")

    from sqlalchemy import or_
    recent_ranked = db.query(models.H2HChallenge).filter(
        models.H2HChallenge.status == "completed",
        models.H2HChallenge.match_type.in_(["ranked", "match_night"]),
        models.H2HChallenge.winner_id == user_id,
        or_(
            models.H2HChallenge.challenger_id == user_id,
            models.H2HChallenge.opponent_id == user_id
        )
    ).order_by(models.H2HChallenge.created_at.desc()).limit(5).all()

    if len(recent_ranked) >= 5:
        all_positive = True
        for c in recent_ranked:
            if c.challenger_id == user_id:
                diff = (c.challenger_score or 0) - (c.opponent_score or 0)
            else:
                diff = (c.opponent_score or 0) - (c.challenger_score or 0)
            if diff <= 0:
                all_positive = False
                break
        if all_positive:
            award_achievement(db, user_id, "consistency_5")


def _check_secret_achievements(db: Session, user_id: int, challenge: models.H2HChallenge, user):
    opponent_id = challenge.opponent_id if challenge.challenger_id == user_id else challenge.challenger_id
    if opponent_id:
        opponent = db.query(models.User).filter(models.User.id == opponent_id).first()
        if opponent:
            mmr_diff = (opponent.mmr or 1000) - (user.mmr or 1000)
            if mmr_diff >= 400:
                award_achievement(db, user_id, "upset_artist")

    winner_proj = 0
    loser_proj = 0
    players = db.query(models.H2HLineupPlayer).filter(
        models.H2HLineupPlayer.challenge_id == challenge.id
    ).all()
    for p in players:
        if p.user_id == user_id:
            winner_proj += (p.proj_fp or 0)
        else:
            loser_proj += (p.proj_fp or 0)
    if loser_proj > winner_proj + 30:
        award_achievement(db, user_id, "comeback_king")

    streak = user.ranked_streak or 0
    if streak >= 5:
        from sqlalchemy import or_
        recent = db.query(models.H2HChallenge).filter(
            models.H2HChallenge.status == "completed",
            models.H2HChallenge.match_type.in_(["ranked", "match_night"]),
            or_(
                models.H2HChallenge.challenger_id == user_id,
                models.H2HChallenge.opponent_id == user_id
            )
        ).order_by(models.H2HChallenge.created_at.desc()).limit(15).all()

        loss_streak = 0
        found_loss_streak = False
        win_after = 0
        for c in reversed(recent):
            if c.winner_id != user_id:
                if not found_loss_streak:
                    loss_streak += 1
                    if loss_streak >= 5:
                        found_loss_streak = True
                        win_after = 0
                else:
                    break
            else:
                if found_loss_streak:
                    win_after += 1
                else:
                    loss_streak = 0
        if found_loss_streak and win_after >= 5:
            award_achievement(db, user_id, "redemption_arc")


def _check_h2h_streak(db: Session, user_id: int):
    recent = db.query(models.H2HChallenge).filter(
        models.H2HChallenge.status == "completed",
        (models.H2HChallenge.challenger_id == user_id) | (models.H2HChallenge.opponent_id == user_id)
    ).order_by(models.H2HChallenge.created_at.desc()).limit(5).all()

    streak = 0
    for c in recent:
        if c.winner_id == user_id:
            streak += 1
        else:
            break

    if streak >= 3:
        award_achievement(db, user_id, "h2h_streak_3")
    if streak >= 5:
        award_achievement(db, user_id, "h2h_streak_5")
