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
    coach_cash = Column(Integer, default=0)
    mmr = Column(Integer, default=1000)
    division = Column(String(20), default="Bronze")
    division_tier = Column(Integer, default=3)  # 3=III, 2=II, 1=I
    season_high_division = Column(String(20), default="Bronze")
    season_high_tier = Column(Integer, default=3)
    promotion_wins = Column(Integer, default=0)
    promotion_losses = Column(Integer, default=0)
    in_promotion = Column(Boolean, default=False)
    ranked_wins = Column(Integer, default=0)
    ranked_losses = Column(Integer, default=0)
    ranked_streak = Column(Integer, default=0)  # positive = win streak, negative = loss streak
    created_at = Column(DateTime, server_default=func.now())
    
    entries = relationship("ContestEntry", back_populates="user")
    achievements = relationship("UserAchievement", back_populates="user")
    currency_transactions = relationship("CurrencyTransaction", back_populates="user")
    cash_transactions = relationship("CashTransaction", back_populates="user")
    h2h_challenges_created = relationship("H2HChallenge", foreign_keys="H2HChallenge.challenger_id", back_populates="challenger")

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
    house_proj_score = Column(Float, default=0)
    house_actual_score = Column(Float, default=0)
    house_lineup_snapshot = Column(Text, default="")
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
    __table_args__ = (
        UniqueConstraint("user_id", "achievement_code", name="uq_user_achievement"),
    )
    
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
    category = Column(String(50), default="competitive")

class CurrencyTransaction(Base):
    __tablename__ = "currency_transactions"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    amount = Column(Integer, nullable=False)
    transaction_type = Column(String(50), nullable=False)
    description = Column(String(255))
    created_at = Column(DateTime, server_default=func.now())
    
    user = relationship("User", back_populates="currency_transactions")

class CashTransaction(Base):
    __tablename__ = "cash_transactions"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    amount = Column(Integer, nullable=False)
    transaction_type = Column(String(50), nullable=False)
    description = Column(String(255))
    created_at = Column(DateTime, server_default=func.now())
    
    user = relationship("User", back_populates="cash_transactions")

class ShopItem(Base):
    __tablename__ = "shop_items"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    description = Column(Text)
    pillar = Column(String(50), nullable=False, default="identity")
    category = Column(String(50), nullable=False)
    price = Column(Integer, nullable=False)
    item_data = Column(Text)
    rarity = Column(String(20), default="common")
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
    __table_args__ = (UniqueConstraint('contest_id', 'player_name_normalized', name='unique_contest_player_snapshot'),)
    
    id = Column(Integer, primary_key=True, index=True)
    contest_id = Column(Integer, ForeignKey("contests.id"), nullable=False)
    player_name = Column(String(100), nullable=False, index=True)
    player_name_normalized = Column(String(100), nullable=False, index=True)
    team = Column(String(10))
    position = Column(String(20))
    salary = Column(Integer)
    proj_min = Column(Float)
    proj_fp = Column(Float)
    actual_fp = Column(Float)
    actual_min = Column(Float)
    minutes_error = Column(Float)
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
    player_name = Column(String(100), nullable=False)
    player_name_normalized = Column(String(100), unique=True, nullable=False, index=True)
    sample_size = Column(Integer, default=0)
    avg_prediction_error = Column(Float, default=0)
    adjustment_factor = Column(Float, default=1.0)
    consistency_score = Column(Float, default=0)
    prediction_variance = Column(Float, default=0)
    variance_dampening = Column(Float, default=1.0)
    avg_actual_fp = Column(Float, default=0)
    minutes_sample_size = Column(Integer, default=0)
    avg_minutes_error = Column(Float, default=0)
    minutes_adjustment_factor = Column(Float, default=1.0)
    minutes_consistency = Column(Float, default=0)
    last_updated = Column(DateTime, server_default=func.now(), onupdate=func.now())

class H2HChallenge(Base):
    __tablename__ = "h2h_challenges"

    id = Column(Integer, primary_key=True, index=True)
    contest_id = Column(Integer, ForeignKey("contests.id"), nullable=False)
    challenger_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    opponent_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    wager = Column(Integer, default=10)
    currency_mode = Column(String(10), default="coin")
    match_type = Column(String(20), default="casual")  # "casual", "ranked", "match_night"
    mmr_change_challenger = Column(Integer, default=0)
    mmr_change_opponent = Column(Integer, default=0)
    status = Column(String(20), default="open")
    winner_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    challenger_score = Column(Float, default=0)
    opponent_score = Column(Float, default=0)
    challenger_lineup_submitted = Column(Boolean, default=False)
    opponent_lineup_submitted = Column(Boolean, default=False)
    created_at = Column(DateTime, server_default=func.now())

    challenger = relationship("User", foreign_keys=[challenger_id], back_populates="h2h_challenges_created")
    opponent = relationship("User", foreign_keys=[opponent_id])
    winner = relationship("User", foreign_keys=[winner_id])
    contest = relationship("Contest")
    players = relationship("H2HLineupPlayer", back_populates="challenge")

class H2HLineupPlayer(Base):
    __tablename__ = "h2h_lineup_players"

    id = Column(Integer, primary_key=True, index=True)
    challenge_id = Column(Integer, ForeignKey("h2h_challenges.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    player_name = Column(String(100), nullable=False)
    position = Column(String(10), nullable=False)
    team = Column(String(10))
    salary = Column(Integer)
    proj_fp = Column(Float)
    actual_fp = Column(Float, default=0)

    challenge = relationship("H2HChallenge", back_populates="players")
