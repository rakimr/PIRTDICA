"""
PIRTDICA Ranking System
Modified ELO for DFS esports competition.
"""

DIVISIONS = [
    "Bronze", "Silver", "Gold", "Platinum", "Diamond", "Master", "Grandmaster", "Champion"
]

DIVISION_MMR_THRESHOLDS = {
    "Bronze": 0,
    "Silver": 1100,
    "Gold": 1250,
    "Platinum": 1400,
    "Diamond": 1600,
    "Master": 1800,
    "Grandmaster": 2000,
    "Champion": 2200,
}

DIVISION_COLORS = {
    "Bronze": "#CD7F32",
    "Silver": "#A8A9AD",
    "Gold": "#D4AF37",
    "Platinum": "#E5E4E2",
    "Diamond": "#00BFFF",
    "Master": "#6D28D9",
    "Grandmaster": "#D72638",
    "Champion": "#D4AF37",
}


def calculate_mmr_change(winner_mmr, loser_mmr, winner_score, loser_score,
                         winner_proj, loser_proj, match_type="ranked"):
    """
    Calculate MMR changes after a ranked match.
    
    Returns: (winner_change, loser_change) tuple
    """
    base_k = 20
    
    if match_type == "match_night":
        base_k = 30  # 1.5x multiplier for match nights
    elif match_type == "casual":
        base_k = 8  # Lower impact for casual
    
    # Expected scores (ELO formula)
    expected_winner = 1.0 / (1.0 + 10 ** ((loser_mmr - winner_mmr) / 400.0))
    expected_loser = 1.0 - expected_winner
    
    # Base change
    winner_change = base_k * (1 - expected_winner)
    loser_change = base_k * (0 - expected_loser)
    
    # Performance modifier: reward exceeding projection
    if winner_proj and winner_proj > 0:
        proj_ratio = winner_score / winner_proj
        if proj_ratio > 1.15:
            winner_change += 5
        elif proj_ratio > 1.05:
            winner_change += 3
    
    # Score margin modifier
    if winner_score > 0 and loser_score > 0:
        margin = winner_score - loser_score
        if margin > 50:
            winner_change += 3
        elif margin > 30:
            winner_change += 1
    
    # Underperforming winner penalty
    if winner_proj and winner_proj > 0:
        if winner_score < winner_proj * 0.85:
            winner_change -= 3
    
    # Opponent strength bonus
    mmr_diff = loser_mmr - winner_mmr
    if mmr_diff > 300:
        winner_change += 10
    elif mmr_diff > 150:
        winner_change += 5
    elif mmr_diff < -300:
        loser_change -= 3  # Extra penalty for losing to much weaker
    
    # Anti-smurf: accelerate for streaks
    winner_change = max(5, round(winner_change))
    loser_change = min(-5, round(loser_change))
    
    return winner_change, loser_change


def get_division_for_mmr(mmr):
    """Determine division based on MMR."""
    result_div = "Bronze"
    for div, threshold in DIVISION_MMR_THRESHOLDS.items():
        if mmr >= threshold:
            result_div = div
    return result_div


def get_tier_for_mmr(mmr, division):
    """Determine sub-tier (III, II, I) within a division."""
    threshold = DIVISION_MMR_THRESHOLDS.get(division, 0)
    next_div_idx = DIVISIONS.index(division) + 1
    if next_div_idx < len(DIVISIONS):
        next_threshold = DIVISION_MMR_THRESHOLDS[DIVISIONS[next_div_idx]]
    else:
        return 1  # Top division, always tier I
    
    range_size = next_threshold - threshold
    progress = mmr - threshold
    
    if range_size <= 0:
        return 1
    
    ratio = progress / range_size
    if ratio >= 0.67:
        return 1  # Tier I (highest in division)
    elif ratio >= 0.33:
        return 2  # Tier II
    else:
        return 3  # Tier III


def format_division(division, tier):
    """Format division string like 'Gold II'."""
    tier_map = {1: "I", 2: "II", 3: "III"}
    if division in ("Master", "Grandmaster", "Champion"):
        return division
    return f"{division} {tier_map.get(tier, 'III')}"


def check_promotion(user_mmr, current_division, current_tier):
    """Check if user should enter promotion series."""
    target_division = get_division_for_mmr(user_mmr)
    target_tier = get_tier_for_mmr(user_mmr, target_division)
    
    div_idx = DIVISIONS.index(current_division)
    target_idx = DIVISIONS.index(target_division)
    
    if target_idx > div_idx:
        return True, target_division
    if target_idx == div_idx and target_tier < current_tier:
        return True, current_division
    return False, None


def should_demote(user_mmr, current_division, current_tier):
    """Check if user should be demoted."""
    threshold = DIVISION_MMR_THRESHOLDS.get(current_division, 0)
    if current_division == "Bronze":
        return False
    if user_mmr < threshold - 30:  # Small buffer to prevent yo-yo
        return True
    return False


def update_user_ranking(user, winner_id, mmr_change):
    """
    Update a user's MMR, division, and promotion state after a match.
    Returns dict with changes made.
    """
    old_mmr = user.mmr or 1000
    old_division = user.division or "Bronze"
    old_tier = user.division_tier or 3
    
    new_mmr = max(0, old_mmr + mmr_change)
    user.mmr = new_mmr
    
    is_winner = (user.id == winner_id)
    
    if is_winner:
        user.ranked_wins = (user.ranked_wins or 0) + 1
        user.ranked_streak = max(1, (user.ranked_streak or 0) + 1) if (user.ranked_streak or 0) >= 0 else 1
    else:
        user.ranked_losses = (user.ranked_losses or 0) + 1
        user.ranked_streak = min(-1, (user.ranked_streak or 0) - 1) if (user.ranked_streak or 0) <= 0 else -1
    
    # Handle promotion series
    if user.in_promotion:
        if is_winner:
            user.promotion_wins = (user.promotion_wins or 0) + 1
        else:
            user.promotion_losses = (user.promotion_losses or 0) + 1
        
        # Check if promotion decided (best of 3)
        if user.promotion_wins >= 2:
            # Promoted!
            new_div = get_division_for_mmr(new_mmr)
            new_tier = get_tier_for_mmr(new_mmr, new_div)
            user.division = new_div
            user.division_tier = new_tier
            user.in_promotion = False
            user.promotion_wins = 0
            user.promotion_losses = 0
        elif user.promotion_losses >= 2:
            # Failed promotion
            user.in_promotion = False
            user.promotion_wins = 0
            user.promotion_losses = 0
            user.mmr = max(user.mmr - 10, DIVISION_MMR_THRESHOLDS.get(old_division, 0))
    else:
        # Check for new promotion trigger
        should_promote, target_div = check_promotion(new_mmr, old_division, old_tier)
        if should_promote and is_winner:
            # For minor tier changes, auto-promote
            if target_div == old_division:
                new_tier = get_tier_for_mmr(new_mmr, old_division)
                user.division_tier = new_tier
            else:
                # Major division change: enter promotion series
                user.in_promotion = True
                user.promotion_wins = 1  # This win counts
                user.promotion_losses = 0
        elif not should_promote:
            # Update tier within current division
            user.division_tier = get_tier_for_mmr(new_mmr, user.division)
        
        # Check for demotion
        if should_demote(new_mmr, old_division, old_tier) and not is_winner:
            div_idx = DIVISIONS.index(old_division)
            if div_idx > 0:
                user.division = DIVISIONS[div_idx - 1]
                user.division_tier = 1
    
    # Track season high
    div_order = {d: i for i, d in enumerate(DIVISIONS)}
    current_rank = div_order.get(user.division, 0) * 10 + (4 - (user.division_tier or 3))
    season_high_rank = div_order.get(user.season_high_division or "Bronze", 0) * 10 + (4 - (user.season_high_tier or 3))
    if current_rank > season_high_rank:
        user.season_high_division = user.division
        user.season_high_tier = user.division_tier
    
    return {
        "old_mmr": old_mmr,
        "new_mmr": new_mmr,
        "mmr_change": mmr_change,
        "old_division": format_division(old_division, old_tier),
        "new_division": format_division(user.division, user.division_tier),
        "promoted": user.division != old_division and DIVISIONS.index(user.division) > DIVISIONS.index(old_division),
        "demoted": user.division != old_division and DIVISIONS.index(user.division) < DIVISIONS.index(old_division),
        "in_promotion": user.in_promotion,
    }


def get_matchmaking_range(mmr):
    """Get acceptable MMR range for matchmaking."""
    band = 200
    if mmr < 800:
        band = 300  # Wider band for newcomers
    elif mmr > 1600:
        band = 250  # Slightly wider at top for faster matching
    return (max(0, mmr - band), mmr + band)
