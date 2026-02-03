from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, ForeignKey, Text, Date, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from backend.database import Base

class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, index=True, nullable=False)
    email = Column(String(100), unique=True, index=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    display_name = Column(String(100))
    avatar_url = Column(String(255), default="/static/avatars/default.png")
    theme = Column(String(50), default="default")
    coins = Column(Integer, default=100)
    created_at = Column(DateTime, server_default=func.now())
    
    entries = relationship("ContestEntry", back_populates="user")
    achievements = relationship("UserAchievement", back_populates="user")
    currency_transactions = relationship("CurrencyTransaction", back_populates="user")

class Contest(Base):
    __tablename__ = "contests"
    
    id = Column(Integer, primary_key=True, index=True)
    slate_date = Column(Date, nullable=False, index=True)
    lock_time = Column(DateTime, nullable=False)
    status = Column(String(20), default="open")
    house_lineup_score = Column(Float, default=0)
    created_at = Column(DateTime, server_default=func.now())
    
    entries = relationship("ContestEntry", back_populates="contest")
    house_players = relationship("HouseLineupPlayer", back_populates="contest")

class HouseLineupPlayer(Base):
    __tablename__ = "house_lineup_players"
    
    id = Column(Integer, primary_key=True, index=True)
    contest_id = Column(Integer, ForeignKey("contests.id"), nullable=False)
    player_name = Column(String(100), nullable=False)
    position = Column(String(10), nullable=False)
    team = Column(String(10))
    salary = Column(Integer)
    proj_fp = Column(Float)
    actual_fp = Column(Float, default=0)
    
    contest = relationship("Contest", back_populates="house_players")

class ContestEntry(Base):
    __tablename__ = "contest_entries"
    __table_args__ = (UniqueConstraint('user_id', 'contest_id', name='unique_user_contest_entry'),)
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    contest_id = Column(Integer, ForeignKey("contests.id"), nullable=False)
    total_salary = Column(Integer, default=0)
    proj_score = Column(Float, default=0)
    actual_score = Column(Float, default=0)
    beat_house = Column(Boolean, default=False)
    rank = Column(Integer, default=0)
    coins_earned = Column(Integer, default=0)
    created_at = Column(DateTime, server_default=func.now())
    
    user = relationship("User", back_populates="entries")
    contest = relationship("Contest", back_populates="entries")
    players = relationship("EntryPlayer", back_populates="entry")

class EntryPlayer(Base):
    __tablename__ = "entry_players"
    
    id = Column(Integer, primary_key=True, index=True)
    entry_id = Column(Integer, ForeignKey("contest_entries.id"), nullable=False)
    player_name = Column(String(100), nullable=False)
    position = Column(String(10), nullable=False)
    team = Column(String(10))
    salary = Column(Integer)
    proj_fp = Column(Float)
    actual_fp = Column(Float, default=0)
    
    entry = relationship("ContestEntry", back_populates="players")

class UserAchievement(Base):
    __tablename__ = "user_achievements"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    achievement_code = Column(String(50), nullable=False)
    achieved_at = Column(DateTime, server_default=func.now())
    
    user = relationship("User", back_populates="achievements")

class Achievement(Base):
    __tablename__ = "achievements"
    
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(50), unique=True, nullable=False)
    name = Column(String(100), nullable=False)
    description = Column(Text)
    icon = Column(String(50), default="trophy")
    coin_reward = Column(Integer, default=0)

class CurrencyTransaction(Base):
    __tablename__ = "currency_transactions"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    amount = Column(Integer, nullable=False)
    transaction_type = Column(String(50), nullable=False)
    description = Column(String(255))
    created_at = Column(DateTime, server_default=func.now())
    
    user = relationship("User", back_populates="currency_transactions")

class ShopItem(Base):
    __tablename__ = "shop_items"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    description = Column(Text)
    category = Column(String(50), nullable=False)
    price = Column(Integer, nullable=False)
    item_data = Column(Text)
    is_active = Column(Boolean, default=True)

class UserItem(Base):
    __tablename__ = "user_items"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    item_id = Column(Integer, ForeignKey("shop_items.id"), nullable=False)
    purchased_at = Column(DateTime, server_default=func.now())

class LeaderboardCache(Base):
    __tablename__ = "leaderboard_cache"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    period = Column(String(20), nullable=False)
    period_key = Column(String(20), nullable=False)
    wins = Column(Integer, default=0)
    entries = Column(Integer, default=0)
    winrate = Column(Float, default=0)
    total_score = Column(Float, default=0)
    rank = Column(Integer, default=0)

class ProjectionSnapshot(Base):
    """Store historical player projections for ML training."""
    __tablename__ = "projection_snapshots"
    
    id = Column(Integer, primary_key=True, index=True)
    contest_id = Column(Integer, ForeignKey("contests.id"), nullable=False)
    player_name = Column(String(100), nullable=False, index=True)
    team = Column(String(10))
    position = Column(String(20))
    salary = Column(Integer)
    proj_min = Column(Float)
    proj_fp = Column(Float)
    actual_fp = Column(Float)
    fp_sd = Column(Float)
    usg_pct = Column(Float)
    dvp_weight = Column(Float)
    ref_weight = Column(Float)
    line_weight = Column(Float)
    omega = Column(Float)
    prediction_error = Column(Float)
    created_at = Column(DateTime, server_default=func.now())
    
    contest = relationship("Contest")

class PlayerAdjustmentFactor(Base):
    """Store learned adjustment factors per player based on historical performance."""
    __tablename__ = "player_adjustment_factors"
    
    id = Column(Integer, primary_key=True, index=True)
    player_name = Column(String(100), unique=True, nullable=False, index=True)
    sample_size = Column(Integer, default=0)
    avg_prediction_error = Column(Float, default=0)
    adjustment_factor = Column(Float, default=1.0)
    consistency_score = Column(Float, default=0)
    last_updated = Column(DateTime, server_default=func.now(), onupdate=func.now())
