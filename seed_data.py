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
    
    if db.query(models.Achievement).first():
        print("Data already seeded")
        db.close()
        return
    
    achievements = [
        {"code": "first_entry", "name": "First Steps", "description": "Submit your first lineup", "icon": "1", "coin_reward": 10},
        {"code": "first_win", "name": "Winner!", "description": "Beat the house for the first time", "icon": "W", "coin_reward": 25},
        {"code": "streak_3", "name": "Hot Hand", "description": "Win 3 contests in a row", "icon": "3", "coin_reward": 50},
        {"code": "streak_5", "name": "On Fire", "description": "Win 5 contests in a row", "icon": "5", "coin_reward": 100},
        {"code": "streak_10", "name": "Unstoppable", "description": "Win 10 contests in a row", "icon": "X", "coin_reward": 250},
        {"code": "wins_10", "name": "Double Digits", "description": "Win 10 total contests", "icon": "D", "coin_reward": 50},
        {"code": "wins_50", "name": "Veteran", "description": "Win 50 total contests", "icon": "V", "coin_reward": 200},
        {"code": "wins_100", "name": "Legend", "description": "Win 100 total contests", "icon": "L", "coin_reward": 500},
        {"code": "perfect_slate", "name": "Perfect Slate", "description": "Score 400+ FP in a single contest", "icon": "P", "coin_reward": 100},
        {"code": "value_king", "name": "Value King", "description": "Beat the house with $5000+ salary remaining", "icon": "$", "coin_reward": 75},
        {"code": "cotd", "name": "Coach of the Day", "description": "Earn Coach of the Day award", "icon": "C", "coin_reward": 50},
        {"code": "cotw", "name": "Coach of the Week", "description": "Earn Coach of the Week award", "icon": "C", "coin_reward": 100},
        {"code": "cotm", "name": "Coach of the Month", "description": "Earn Coach of the Month award", "icon": "C", "coin_reward": 250},
        {"code": "coty", "name": "Coach of the Year", "description": "Earn Coach of the Year award", "icon": "C", "coin_reward": 1000},
    ]
    
    for ach in achievements:
        db.add(models.Achievement(**ach))
    
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
    print("Seeded achievements and shop items")
    db.close()

if __name__ == "__main__":
    seed_data()
