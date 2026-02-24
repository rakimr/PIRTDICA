"""
Seed the database with initial shop items and achievements.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backend.database import Base, engine
from sqlalchemy.orm import sessionmaker
from backend import models

def seed_data():
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    db = Session()

    achievements = [
        {"code": "first_entry", "name": "First Steps", "description": "Submit your first lineup", "icon": "🏀", "coin_reward": 10, "badge_type": "competitive_earned", "rarity": "common"},
        {"code": "entries_25", "name": "Dedicated", "description": "Submit 25 lineups", "icon": "📋", "coin_reward": 25, "badge_type": "competitive_earned", "rarity": "common"},
        {"code": "entries_50", "name": "Committed", "description": "Submit 50 lineups", "icon": "📊", "coin_reward": 50, "badge_type": "competitive_earned", "rarity": "uncommon"},
        {"code": "entries_100", "name": "Centurion", "description": "Submit 100 lineups", "icon": "💯", "coin_reward": 100, "badge_type": "competitive_earned", "rarity": "rare"},
        {"code": "monthly_grinder", "name": "Monthly Grinder", "description": "Submit 10+ lineups in a single month", "icon": "📅", "coin_reward": 30, "badge_type": "statistical_earned", "rarity": "common"},
        {"code": "early_bird", "name": "Early Bird", "description": "Submit a lineup 2+ hours before lock", "icon": "🐦", "coin_reward": 15, "badge_type": "statistical_earned", "rarity": "common"},
        {"code": "first_win", "name": "Winner!", "description": "Beat the house for the first time", "icon": "🏆", "coin_reward": 25, "badge_type": "competitive_earned", "rarity": "common"},
        {"code": "wins_10", "name": "Double Digits", "description": "Win 10 total contests", "icon": "🔟", "coin_reward": 50, "badge_type": "competitive_earned", "rarity": "uncommon"},
        {"code": "wins_50", "name": "Veteran", "description": "Win 50 total contests", "icon": "⭐", "coin_reward": 200, "badge_type": "competitive_earned", "rarity": "rare"},
        {"code": "wins_100", "name": "Legend", "description": "Win 100 total contests", "icon": "👑", "coin_reward": 500, "badge_type": "competitive_earned", "rarity": "epic"},
        {"code": "perfect_slate", "name": "Perfect Slate", "description": "Score 400+ fantasy points in a single contest", "icon": "💎", "coin_reward": 100, "badge_type": "statistical_earned", "rarity": "rare"},
        {"code": "value_king", "name": "Value King", "description": "Beat the house with $5,000+ salary remaining", "icon": "💰", "coin_reward": 75, "badge_type": "statistical_earned", "rarity": "uncommon"},
        {"code": "salary_saver", "name": "Salary Saver", "description": "Beat the house with $7,000+ salary remaining", "icon": "🏦", "coin_reward": 100, "badge_type": "statistical_earned", "rarity": "rare"},
        {"code": "blowout_win", "name": "Blowout", "description": "Beat the house by 50+ points", "icon": "💥", "coin_reward": 75, "badge_type": "statistical_earned", "rarity": "uncommon"},
        {"code": "six_x_king", "name": "6x King", "description": "Get 6.0x value from a single player", "icon": "6️⃣", "coin_reward": 50, "badge_type": "statistical_earned", "rarity": "uncommon"},
        {"code": "streak_3", "name": "Hot Hand", "description": "Win 3 contests in a row", "icon": "🔥", "coin_reward": 50, "badge_type": "competitive_earned", "rarity": "uncommon"},
        {"code": "streak_5", "name": "On Fire", "description": "Win 5 contests in a row", "icon": "🔥", "coin_reward": 100, "badge_type": "competitive_earned", "rarity": "rare"},
        {"code": "streak_10", "name": "Unstoppable", "description": "Win 10 contests in a row", "icon": "🔥", "coin_reward": 250, "badge_type": "competitive_earned", "rarity": "epic"},
        {"code": "arch_balanced", "name": "Balanced Attack", "description": "All 5 starters score 25+ FP in a win", "icon": "⚖️", "coin_reward": 75, "badge_type": "statistical_earned", "rarity": "rare"},
        {"code": "arch_guard", "name": "Guard Whisperer", "description": "Win 10 contests with 4+ guards", "icon": "🎯", "coin_reward": 100, "badge_type": "statistical_earned", "rarity": "rare"},
        {"code": "arch_wing", "name": "Wing Commander", "description": "Win 10 contests with 4+ wings", "icon": "🦅", "coin_reward": 100, "badge_type": "statistical_earned", "rarity": "rare"},
        {"code": "arch_big", "name": "Big Man Era", "description": "Win 10 contests with 4+ bigs", "icon": "🏔️", "coin_reward": 100, "badge_type": "statistical_earned", "rarity": "rare"},
        {"code": "h2h_first", "name": "Challenger", "description": "Win your first Head-to-Head match", "icon": "⚔️", "coin_reward": 25, "badge_type": "competitive_earned", "rarity": "common"},
        {"code": "h2h_wins_10", "name": "Duelist", "description": "Win 10 Head-to-Head matches", "icon": "🗡️", "coin_reward": 75, "badge_type": "competitive_earned", "rarity": "uncommon"},
        {"code": "h2h_wins_25", "name": "Gladiator", "description": "Win 25 Head-to-Head matches", "icon": "🛡️", "coin_reward": 150, "badge_type": "competitive_earned", "rarity": "rare"},
        {"code": "h2h_high_roller", "name": "High Roller", "description": "Win a Head-to-Head match with 100+ coin wager", "icon": "🎰", "coin_reward": 50, "badge_type": "competitive_earned", "rarity": "uncommon"},
        {"code": "giant_killer", "name": "Giant Killer", "description": "Beat a higher-ranked opponent in H2H", "icon": "🐉", "coin_reward": 75, "badge_type": "competitive_earned", "rarity": "rare"},
        {"code": "h2h_streak_3", "name": "H2H Hot Streak", "description": "Win 3 Head-to-Head matches in a row", "icon": "3️⃣", "coin_reward": 50, "badge_type": "competitive_earned", "rarity": "uncommon"},
        {"code": "h2h_streak_5", "name": "H2H Dominator", "description": "Win 5 Head-to-Head matches in a row", "icon": "5️⃣", "coin_reward": 100, "badge_type": "competitive_earned", "rarity": "rare"},
        {"code": "first_blood", "name": "First Blood", "description": "Win your first ranked match", "icon": "🩸", "coin_reward": 25, "badge_type": "competitive_earned", "rarity": "common"},
        {"code": "ranked_wins_10", "name": "Ranked Contender", "description": "Win 10 ranked matches", "icon": "🥊", "coin_reward": 75, "badge_type": "competitive_earned", "rarity": "uncommon"},
        {"code": "ranked_wins_25", "name": "Ranked Warrior", "description": "Win 25 ranked matches", "icon": "⚔️", "coin_reward": 150, "badge_type": "competitive_earned", "rarity": "rare"},
        {"code": "ranked_wins_50", "name": "Ranked Elite", "description": "Win 50 ranked matches", "icon": "🏅", "coin_reward": 300, "badge_type": "competitive_earned", "rarity": "epic"},
        {"code": "ranked_wins_100", "name": "Ranked Legend", "description": "Win 100 ranked matches", "icon": "🏆", "coin_reward": 500, "badge_type": "competitive_earned", "rarity": "legendary"},
        {"code": "win_streak_5", "name": "Ranked Streak 5", "description": "Win 5 ranked matches in a row", "icon": "🔥", "coin_reward": 100, "badge_type": "competitive_earned", "rarity": "rare"},
        {"code": "win_streak_10", "name": "Ranked Streak 10", "description": "Win 10 ranked matches in a row", "icon": "🔥", "coin_reward": 250, "badge_type": "competitive_earned", "rarity": "epic"},
        {"code": "win_rate_60", "name": "Sharp Shooter", "description": "Maintain 60%+ win rate over 100+ ranked matches", "icon": "🎯", "coin_reward": 200, "badge_type": "statistical_earned", "rarity": "epic"},
        {"code": "reach_silver", "name": "Silver Division", "description": "Reach the Silver division", "icon": "🥈", "coin_reward": 50, "badge_type": "event_limited", "rarity": "common"},
        {"code": "reach_gold", "name": "Gold Division", "description": "Reach the Gold division", "icon": "🥇", "coin_reward": 100, "badge_type": "event_limited", "rarity": "uncommon"},
        {"code": "reach_platinum", "name": "Platinum Division", "description": "Reach the Platinum division", "icon": "💠", "coin_reward": 200, "badge_type": "event_limited", "rarity": "rare"},
        {"code": "reach_diamond", "name": "Diamond Division", "description": "Reach the Diamond division", "icon": "💎", "coin_reward": 300, "badge_type": "event_limited", "rarity": "epic"},
        {"code": "reach_master", "name": "Master Division", "description": "Reach the Master division", "icon": "🏆", "coin_reward": 500, "badge_type": "event_limited", "rarity": "epic"},
        {"code": "reach_grandmaster", "name": "Grandmaster Division", "description": "Reach the Grandmaster division", "icon": "👑", "coin_reward": 750, "badge_type": "event_limited", "rarity": "legendary"},
        {"code": "reach_champion", "name": "Champion Division", "description": "Reach the Champion division", "icon": "🏅", "coin_reward": 1000, "badge_type": "event_limited", "rarity": "legendary"},
        {"code": "projection_crusher", "name": "Projection Crusher", "description": "Beat your projected score by 25%+ in a ranked win", "icon": "📈", "coin_reward": 75, "badge_type": "secret_earned", "rarity": "rare", "is_hidden": True},
        {"code": "efficiency_king", "name": "Efficiency King", "description": "Win a ranked match spending under $55,000 salary", "icon": "👑", "coin_reward": 75, "badge_type": "secret_earned", "rarity": "rare", "is_hidden": True},
        {"code": "slate_dominator", "name": "Slate Dominator", "description": "Score 350+ fantasy points in a ranked match", "icon": "🌋", "coin_reward": 100, "badge_type": "secret_earned", "rarity": "epic", "is_hidden": True},
        {"code": "consistency_5", "name": "Mr. Consistent", "description": "Win 5 consecutive ranked matches with positive margins", "icon": "📏", "coin_reward": 100, "badge_type": "secret_earned", "rarity": "rare", "is_hidden": True},
        {"code": "upset_artist", "name": "Upset Artist", "description": "Beat an opponent with 400+ higher MMR", "icon": "🎭", "coin_reward": 150, "badge_type": "secret_earned", "rarity": "epic", "is_hidden": True},
        {"code": "comeback_king", "name": "Comeback King", "description": "Win despite being projected to lose by 30+ points", "icon": "👊", "coin_reward": 150, "badge_type": "secret_earned", "rarity": "epic", "is_hidden": True},
        {"code": "redemption_arc", "name": "Redemption Arc", "description": "Win 5 straight after losing 5 straight in ranked", "icon": "🔄", "coin_reward": 200, "badge_type": "secret_earned", "rarity": "legendary", "is_hidden": True},
        {"code": "promo_clutch", "name": "Promo Clutch", "description": "Win your promotion match to advance a division", "icon": "⬆️", "coin_reward": 100, "badge_type": "secret_earned", "rarity": "rare", "is_hidden": True},
        {"code": "cotd", "name": "Coach of the Day", "description": "Earn Coach of the Day award", "icon": "📰", "coin_reward": 50, "badge_type": "competitive_earned", "rarity": "uncommon"},
        {"code": "cotw", "name": "Coach of the Week", "description": "Earn Coach of the Week award", "icon": "🗞️", "coin_reward": 100, "badge_type": "competitive_earned", "rarity": "rare"},
        {"code": "cotm", "name": "Coach of the Month", "description": "Earn Coach of the Month award", "icon": "📰", "coin_reward": 250, "badge_type": "competitive_earned", "rarity": "epic"},
        {"code": "coty", "name": "Coach of the Year", "description": "Earn Coach of the Year award", "icon": "🏆", "coin_reward": 1000, "badge_type": "competitive_earned", "rarity": "legendary"},
    ]

    existing_codes = {a.code for a in db.query(models.Achievement).all()}

    added = 0
    updated = 0
    for ach in achievements:
        code = ach["code"]
        if code in existing_codes:
            db.query(models.Achievement).filter(models.Achievement.code == code).update({
                "name": ach["name"],
                "description": ach["description"],
                "icon": ach["icon"],
                "coin_reward": ach["coin_reward"],
                "badge_type": ach.get("badge_type", "competitive_earned"),
                "rarity": ach.get("rarity", "common"),
                "is_hidden": ach.get("is_hidden", False),
            })
            updated += 1
        else:
            db.add(models.Achievement(
                code=code,
                name=ach["name"],
                description=ach["description"],
                icon=ach["icon"],
                coin_reward=ach["coin_reward"],
                badge_type=ach.get("badge_type", "competitive_earned"),
                rarity=ach.get("rarity", "common"),
                is_hidden=ach.get("is_hidden", False),
            ))
            added += 1

    if db.query(models.ShopItem).count() == 0:
        shop_items = [
            {"name": "Fire Avatar", "description": "Show you're on fire", "category": "avatar", "price": 100, "item_data": "/static/avatars/fire.png"},
            {"name": "Crown Avatar", "description": "Royal status", "category": "avatar", "price": 250, "item_data": "/static/avatars/crown.png"},
            {"name": "Robot Avatar", "description": "AI-powered picks", "category": "avatar", "price": 150, "item_data": "/static/avatars/robot.png"},
            {"name": "Ocean Theme", "description": "Cool blue vibes", "category": "theme", "price": 200, "item_data": "linear-gradient(135deg, #0077b6, #00b4d8)"},
            {"name": "Sunset Theme", "description": "Warm orange glow", "category": "theme", "price": 200, "item_data": "linear-gradient(135deg, #f72585, #b5179e)"},
            {"name": "Forest Theme", "description": "Natural green", "category": "theme", "price": 200, "item_data": "linear-gradient(135deg, #2d6a4f, #40916c)"},
            {"name": "MVP Badge", "description": "Display your MVP status", "category": "badge", "price": 500, "item_data": "MVP"},
            {"name": "Analyst Badge", "description": "Show your analytical side", "category": "badge", "price": 300, "item_data": "ANALYST"},
            {"name": "Shark Badge", "description": "Fear the shark", "category": "badge", "price": 400, "item_data": "SHARK"},
        ]
        for item in shop_items:
            db.add(models.ShopItem(**item))

    db.commit()
    print(f"Seeded achievements: {added} added, {updated} updated")
    print(f"Total achievements in database: {db.query(models.Achievement).count()}")
    db.close()

if __name__ == "__main__":
    seed_data()
