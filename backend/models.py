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
    active_theme = Column(String(50), default=None, nullable=True)
    active_frame = Column(String(50), default=None, nullable=True)
    equipped_badges = Column(Text, default=None, nullable=True)
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
    is_banned = Column(Boolean, default=False)
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
    badge_type = Column(String(30), default="competitive_earned")
    rarity = Column(String(20), default="common")
    is_hidden = Column(Boolean, default=False)

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
    code = Column(String(50), unique=True, nullable=True, index=True)
    name = Column(String(100), nullable=False)
    description = Column(Text)
    pillar = Column(String(50), nullable=False, default="identity")
    category = Column(String(50), nullable=False)
    price = Column(Integer, nullable=False)
    item_data = Column(Text)
    rarity = Column(String(20), default="common")
    is_active = Column(Boolean, default=True)
    is_seasonal = Column(Boolean, default=False)
    season_id = Column(String(20), nullable=True)
    available_from = Column(DateTime, nullable=True)
    available_until = Column(DateTime, nullable=True)
    is_returnable = Column(Boolean, default=True)

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


class DFSPlayerLive(Base):
    __tablename__ = "dfs_players_live"

    id = Column(Integer, primary_key=True, index=True)
    player_name = Column(String(100), nullable=False, index=True)
    fd_position = Column(String(10))
    true_position = Column(String(20))
    projected_min = Column(Float)
    salary = Column(Integer)
    team = Column(String(10))
    opponent = Column(String(10))
    location = Column(String(10))
    implied_total = Column(Float)
    fp_pg = Column(Float)
    fp_per_min = Column(Float)
    usg_pct = Column(Float)
    usg_boost = Column(Float)
    fppm_adj = Column(Float)
    ref_weight = Column(Float)
    dvp_weight = Column(Float)
    line_weight = Column(Float)
    games_pct = Column(Float)
    gp_weight = Column(Float)
    low_gp_flag = Column(Integer)
    min_sd = Column(Float)
    omega = Column(Float)
    omega_weight = Column(Float)
    proj_fp = Column(Float)
    fp_sd = Column(Float)
    ceiling = Column(Float)
    floor = Column(Float)
    fp_range = Column(Float)
    upside_ratio = Column(Float)
    hist_max_fp = Column(Float)
    hist_min_fp = Column(Float)
    tier = Column(String(20))
    raw_fp_sd = Column(Float)
    tier_cv = Column(Float)
    tier_expected_sd = Column(Float)
    value_ratio = Column(Float)
    value_vs_tier = Column(Float)
    archetype = Column(String(50))
    value = Column(Float)
    ceiling_value = Column(Float)
    floor_value = Column(Float)
    upside_per_k = Column(Float)
    value_rank = Column(Integer)
    salary_tier = Column(String(20))
    updated_at = Column(DateTime, server_default=func.now())


class PropRecommendationLive(Base):
    __tablename__ = "prop_recommendations_live"

    id = Column(Integer, primary_key=True, index=True)
    player = Column(String(100), nullable=False, index=True)
    team = Column(String(10))
    opponent = Column(String(10))
    salary = Column(Integer)
    value = Column(Float)
    stat = Column(String(20))
    player_avg = Column(Float)
    adjusted_avg = Column(Float)
    extra_fp = Column(Float)
    edge_pct = Column(Float)
    recommendation = Column(String(20))
    book_line = Column(Float)
    book_over = Column(Float)
    book_under = Column(Float)
    vs_book_edge = Column(Float)
    archetype = Column(String(50))
    dva_edge = Column(Float)
    dvp_edge = Column(Float)
    blend = Column(Float)
    updated_at = Column(DateTime, server_default=func.now())


class TargetedPlayLive(Base):
    __tablename__ = "targeted_plays_live"

    id = Column(Integer, primary_key=True, index=True)
    player_name = Column(String(100), nullable=False, index=True)
    team = Column(String(10))
    opponent = Column(String(10))
    position = Column(String(20))
    salary = Column(Integer)
    stat = Column(String(20))
    player_avg = Column(Float)
    opp_allows = Column(Float)
    league_avg = Column(Float)
    extra_fp = Column(Float)
    edge_pct = Column(Float)
    recommendation = Column(String(20))
    updated_at = Column(DateTime, server_default=func.now())


class OwnershipProjectionLive(Base):
    __tablename__ = "ownership_projections_live"

    id = Column(Integer, primary_key=True, index=True)
    player_name = Column(String(100), nullable=False, index=True)
    team = Column(String(10))
    salary = Column(Integer)
    proj_fp = Column(Float)
    fd_position = Column(String(10))
    appearances = Column(Integer)
    pown_pct = Column(Float)
    salary_tier = Column(String(20))
    raw_pown = Column(Float)
    ownership_tier = Column(String(20))
    value = Column(Float)
    updated_at = Column(DateTime, server_default=func.now())


class PlayerSalaryLive(Base):
    __tablename__ = "player_salaries_live"

    id = Column(Integer, primary_key=True, index=True)
    player_name = Column(String(100), nullable=False, index=True)
    team = Column(String(10))
    position = Column(String(10))
    salary = Column(String(20))
    status = Column(String(50))
    roster_order = Column(Float)
    game = Column(String(50))
    game_time = Column(String(20))
    scraped_at = Column(String(50))
    updated_at = Column(DateTime, server_default=func.now())


class InjuryAlertLive(Base):
    __tablename__ = "injury_alerts_live"

    id = Column(Integer, primary_key=True, index=True)
    player_name = Column(String(100), nullable=False, index=True)
    status = Column(String(20))
    reason = Column(Text)
    alert_title = Column(Text)
    scraped_at = Column(String(50))
    updated_at = Column(DateTime, server_default=func.now())


class PlayerArchetypeLive(Base):
    __tablename__ = "player_archetypes_live"

    id = Column(Integer, primary_key=True, index=True)
    player_name = Column(String(100), nullable=False, index=True)
    team = Column(String(10))
    true_position = Column(String(20))
    archetype = Column(String(50))
    base_archetype = Column(String(50))
    cluster = Column(Integer)
    computed_at = Column(String(50))
    updated_at = Column(DateTime, server_default=func.now())


class PlayerPer100Live(Base):
    __tablename__ = "player_per100_live"

    id = Column(Integer, primary_key=True, index=True)
    player_name = Column(String(100), nullable=False, index=True)
    team = Column(String(10))
    games_played = Column(Float)
    total_minutes = Column(Float)
    pts_per100 = Column(Float)
    reb_per100 = Column(Float)
    ast_per100 = Column(Float)
    stl_per100 = Column(Float)
    blk_per100 = Column(Float)
    tov_per100 = Column(Float)
    mpg = Column(Float)
    fp_per100 = Column(Float)
    updated_at = Column(DateTime, server_default=func.now())


class PlayerPositionLive(Base):
    __tablename__ = "player_positions_live"

    id = Column(Integer, primary_key=True, index=True)
    player_name = Column(String(100), nullable=False, index=True)
    team = Column(String(10))
    true_position = Column(String(20))
    pg_pct = Column(Float)
    sg_pct = Column(Float)
    sf_pct = Column(Float)
    pf_pct = Column(Float)
    c_pct = Column(Float)
    scraped_at = Column(String(50))
    updated_at = Column(DateTime, server_default=func.now())


class PlayerStatLive(Base):
    __tablename__ = "player_stats_live"

    id = Column(Integer, primary_key=True, index=True)
    player_name = Column(String(100), nullable=False, index=True)
    team = Column(String(10))
    games_played = Column(Float)
    mpg = Column(Float)
    pts_pg = Column(Float)
    reb_pg = Column(Float)
    ast_pg = Column(Float)
    stl_pg = Column(Float)
    blk_pg = Column(Float)
    tov_pg = Column(Float)
    fp_pg = Column(Float)
    fp_per_min = Column(Float)
    usg_pct = Column(Float)
    updated_at = Column(DateTime, server_default=func.now())


class PlayerGameLogLive(Base):
    __tablename__ = "player_game_logs_live"

    id = Column(Integer, primary_key=True, index=True)
    player_name = Column(String(100), nullable=False, index=True)
    game_date = Column(String(20))
    matchup = Column(String(50))
    min = Column(Integer)
    pts = Column(Integer)
    reb = Column(Integer)
    ast = Column(Integer)
    stl = Column(Integer)
    blk = Column(Integer)
    tov = Column(Integer)
    fp = Column(Float)
    fg3m = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now())


class PlayerShotZoneLive(Base):
    __tablename__ = "player_shot_zones_live"

    id = Column(Integer, primary_key=True, index=True)
    player_name = Column(String(100), nullable=False, index=True)
    player_id = Column(Integer)
    team = Column(String(10))
    total_fga = Column(Integer)
    ra_fga = Column(Integer)
    ra_fgm = Column(Integer)
    paint_fga = Column(Integer)
    paint_fgm = Column(Integer)
    mid_fga = Column(Integer)
    mid_fgm = Column(Integer)
    three_fga = Column(Integer)
    three_fgm = Column(Integer)
    corner3_fga = Column(Integer)
    atb3_fga = Column(Integer)
    ra_pct = Column(Float)
    paint_pct = Column(Float)
    rim_paint_pct = Column(Float)
    mid_pct = Column(Float)
    three_pct = Column(Float)
    scraped_at = Column(String(50))
    updated_at = Column(DateTime, server_default=func.now())


class PlayerShotCreationLive(Base):
    __tablename__ = "player_shot_creation_live"

    id = Column(Integer, primary_key=True, index=True)
    player_name = Column(String(100), nullable=False, index=True)
    player_id = Column(Integer)
    gp = Column(Integer)
    total_fga = Column(Integer)
    cs_fga = Column(Integer)
    cs_fgm = Column(Integer)
    cs_3fga = Column(Integer)
    cs_3fgm = Column(Integer)
    pu_fga = Column(Integer)
    pu_fgm = Column(Integer)
    pu_3fga = Column(Integer)
    pu_3fgm = Column(Integer)
    paint_fga = Column(Integer)
    paint_fgm = Column(Integer)
    cs_pct = Column(Float)
    pu_pct = Column(Float)
    paint_pct = Column(Float)
    cs_3_share = Column(Float)
    pu_3_share = Column(Float)
    scraped_at = Column(String(50))
    updated_at = Column(DateTime, server_default=func.now())


class PlayerHustleStatLive(Base):
    __tablename__ = "player_hustle_stats_live"

    id = Column(Integer, primary_key=True, index=True)
    player_name = Column(String(100), nullable=False, index=True)
    player_id = Column(Integer)
    team = Column(String(10))
    gp = Column(Integer)
    minutes = Column(Float)
    contested_shots = Column(Integer)
    contested_2pt = Column(Integer)
    contested_3pt = Column(Integer)
    deflections = Column(Integer)
    charges_drawn = Column(Integer)
    screen_assists = Column(Integer)
    loose_balls_off = Column(Integer)
    loose_balls_def = Column(Integer)
    loose_balls_total = Column(Integer)
    box_outs = Column(Integer)
    deflections_per48 = Column(Float)
    contested_per48 = Column(Float)
    loose_per48 = Column(Float)
    charges_per48 = Column(Float)
    screen_ast_per48 = Column(Float)
    box_outs_per48 = Column(Float)
    scraped_at = Column(String(50))
    updated_at = Column(DateTime, server_default=func.now())


class DVAStatLive(Base):
    __tablename__ = "dva_stats_live"

    id = Column(Integer, primary_key=True, index=True)
    opp_team = Column(String(10), nullable=False, index=True)
    archetype = Column(String(50), nullable=False, index=True)
    fp_pm = Column(Float)
    fp_pm_diff = Column(Float)
    sample_n = Column(Integer)
    recent_n = Column(Integer)
    pts_pm = Column(Float)
    pts_pm_diff = Column(Float)
    reb_pm = Column(Float)
    reb_pm_diff = Column(Float)
    ast_pm = Column(Float)
    ast_pm_diff = Column(Float)
    stl_pm = Column(Float)
    stl_pm_diff = Column(Float)
    blk_pm = Column(Float)
    blk_pm_diff = Column(Float)
    fg3m_pm = Column(Float)
    fg3m_pm_diff = Column(Float)
    tov_pm = Column(Float)
    tov_pm_diff = Column(Float)
    dvs_multiplier = Column(Float)
    dvs_raw = Column(Float)
    sample_n_used = Column(Integer)
    pts_component = Column(Float)
    reb_component = Column(Float)
    ast_component = Column(Float)
    stl_component = Column(Float)
    blk_component = Column(Float)
    fg3m_component = Column(Float)
    tov_component = Column(Float)
    updated_at = Column(DateTime, server_default=func.now())


class ArchetypeProfileLive(Base):
    __tablename__ = "archetype_profiles_live"

    id = Column(Integer, primary_key=True, index=True)
    archetype = Column(String(50), nullable=False, unique=True)
    pts_pct = Column(Float)
    reb_pct = Column(Float)
    ast_pct = Column(Float)
    stl_pct = Column(Float)
    blk_pct = Column(Float)
    fg3m_pct = Column(Float)
    tov_pct = Column(Float)
    updated_at = Column(DateTime, server_default=func.now())


class TeamDefenseShotZoneLive(Base):
    __tablename__ = "team_defense_shot_zones_live"

    id = Column(Integer, primary_key=True, index=True)
    team = Column(String(10), nullable=False, index=True)
    team_name = Column(String(50))
    total_fga = Column(Integer)
    ra_fga = Column(Integer)
    ra_fgm = Column(Integer)
    paint_fga = Column(Integer)
    paint_fgm = Column(Integer)
    mid_fga = Column(Integer)
    mid_fgm = Column(Integer)
    corner3_fga = Column(Integer)
    corner3_fgm = Column(Integer)
    atb3_fga = Column(Integer)
    atb3_fgm = Column(Integer)
    ra_freq = Column(Float)
    paint_freq = Column(Float)
    mid_freq = Column(Float)
    corner3_freq = Column(Float)
    atb3_freq = Column(Float)
    ra_fg_pct = Column(Float)
    paint_fg_pct = Column(Float)
    mid_fg_pct = Column(Float)
    corner3_fg_pct = Column(Float)
    atb3_fg_pct = Column(Float)
    updated_at = Column(DateTime, server_default=func.now())


class TeamPlayTypeLive(Base):
    __tablename__ = "team_play_types_live"

    id = Column(Integer, primary_key=True, index=True)
    team = Column(String(10), nullable=False, index=True)
    type_grouping = Column(String(20))
    play_type = Column(String(50))
    play_type_label = Column(String(100))
    poss_pct = Column(Float)
    ppp = Column(Float)
    fg_pct = Column(Float)
    tov_poss_pct = Column(Float)
    score_poss_pct = Column(Float)
    efg_pct = Column(Float)
    poss = Column(Integer)
    pts = Column(Float)
    fgm = Column(Float)
    fga = Column(Float)
    percentile = Column(Float)
    scraped_at = Column(String(50))
    updated_at = Column(DateTime, server_default=func.now())


class PlayerHeadshotLive(Base):
    __tablename__ = "player_headshots_live"

    id = Column(Integer, primary_key=True, index=True)
    player_name = Column(String(100), nullable=False, index=True)
    headshot_url = Column(String(500))
    updated_at = Column(DateTime, server_default=func.now())


class GameOddsLive(Base):
    __tablename__ = "game_odds_live"

    id = Column(Integer, primary_key=True, index=True)
    away_team = Column(String(10), nullable=False)
    home_team = Column(String(10), nullable=False)
    spread = Column(Float)
    total = Column(Float)
    scraped_at = Column(String(50))
    updated_at = Column(DateTime, server_default=func.now())
