"""
Baseline Minutes by Inferred Depth Rank

This module provides empirical baseline minutes data derived from 2012-2018 NBA 
box scores. For each game-team-position group, players were ranked by minutes 
played (descending) to infer depth (PG1, PG2, etc.), then averaged across all games.

Usage:
    from baseline_minutes import get_baseline_minutes, BASELINE_MINUTES
    
    minutes = get_baseline_minutes("PG1")  # Returns 32.64
    minutes = get_baseline_minutes("SF2")  # Returns 20.16
"""

BASELINE_MINUTES = {
    "PG1": 32.64, "PG2": 20.46, "PG3": 12.58, "PG4": 9.46, "PG5": 5.09,
    "SG1": 31.39, "SG2": 20.55, "SG3": 14.01, "SG4": 9.21, "SG5": 6.67, "SG6": 5.69,
    "SF1": 30.99, "SF2": 20.16, "SF3": 12.63, "SF4": 8.75, "SF5": 5.96, "SF6": 5.80,
    "PF1": 28.94, "PF2": 18.74, "PF3": 12.72, "PF4": 8.94, "PF5": 5.64, "PF6": 4.00,
    "C1": 27.71, "C2": 15.32, "C3": 8.38, "C4": 5.57,
}

G_BASELINE = {
    "G1": (BASELINE_MINUTES["PG1"] + BASELINE_MINUTES["SG1"]) / 2,
    "G2": (BASELINE_MINUTES["PG2"] + BASELINE_MINUTES["SG2"]) / 2,
    "G3": (BASELINE_MINUTES["PG3"] + BASELINE_MINUTES["SG3"]) / 2,
    "G4": (BASELINE_MINUTES["PG4"] + BASELINE_MINUTES["SG4"]) / 2,
    "G5": (BASELINE_MINUTES["PG5"] + BASELINE_MINUTES["SG5"]) / 2,
}

F_BASELINE = {
    "F1": (BASELINE_MINUTES["SF1"] + BASELINE_MINUTES["PF1"]) / 2,
    "F2": (BASELINE_MINUTES["SF2"] + BASELINE_MINUTES["PF2"]) / 2,
    "F3": (BASELINE_MINUTES["SF3"] + BASELINE_MINUTES["PF3"]) / 2,
    "F4": (BASELINE_MINUTES["SF4"] + BASELINE_MINUTES["PF4"]) / 2,
    "F5": (BASELINE_MINUTES["SF5"] + BASELINE_MINUTES["PF5"]) / 2,
}

BASELINE_MINUTES.update(G_BASELINE)
BASELINE_MINUTES.update(F_BASELINE)

SAMPLE_COUNTS = {
    "PG1": 14665, "PG2": 12572, "PG3": 4907, "PG4": 822, "PG5": 33,
    "SG1": 14337, "SG2": 11210, "SG3": 5950, "SG4": 1823, "SG5": 302, "SG6": 16,
    "SF1": 14295, "SF2": 10366, "SF3": 4289, "SF4": 809, "SF5": 120, "SF6": 25,
    "PF1": 14362, "PF2": 11572, "PF3": 5603, "PF4": 1643, "PF5": 192, "PF6": 7,
    "C1": 14103, "C2": 8707, "C3": 2075, "C4": 120,
    "G1": 169, "G2": 17, "G3": 0, "G4": 0, "G5": 0,
    "F1": 169, "F2": 17, "F3": 0, "F4": 0, "F5": 0,
}

def get_baseline_minutes(inferred_rank: str, default: float = 0.0) -> float:
    """
    Get baseline minutes for an inferred depth rank.
    
    Args:
        inferred_rank: Position + depth number (e.g., "PG1", "SF2", "G1", "F3")
        default: Value to return if rank not found
        
    Returns:
        Average minutes for that depth rank based on historical data
    """
    return BASELINE_MINUTES.get(inferred_rank.upper(), default)

def get_all_position_baselines(position: str) -> dict:
    """
    Get all baseline minutes for a position.
    
    Args:
        position: Position code (PG, SG, SF, PF, C, G, F)
        
    Returns:
        Dict mapping depth ranks to baseline minutes for that position
    """
    position = position.upper()
    return {k: v for k, v in BASELINE_MINUTES.items() if k.startswith(position)}

def estimate_minutes_by_depth(position: str, depth: int) -> float:
    """
    Estimate minutes given position and depth number.
    
    Args:
        position: Position code (PG, SG, SF, PF, C, G, F)
        depth: Depth chart position (1=starter, 2=backup, etc.)
        
    Returns:
        Estimated minutes based on historical averages
    """
    rank = f"{position.upper()}{depth}"
    return get_baseline_minutes(rank)


STARTER_BUMP = 10.0
CLOSE_GAME_BUMP = 2.0
BLOWOUT_PENALTY = -2.0

CLOSE_GAME_THRESHOLD = 5.0
BLOWOUT_THRESHOLD = 10.0

MINUTES_FLOOR_PCT = 0.65
MINUTES_CEILING_PCT = 1.35
MINUTES_HARD_CAP = 40.0
MINUTES_HARD_FLOOR = 0.0

def get_minutes_bounds(position_slot: str) -> tuple:
    """
    Get role-based min/max minutes bounds for a position slot.
    
    Args:
        position_slot: Depth chart slot (e.g., "PG1", "SF2")
        
    Returns:
        Tuple of (min_minutes, max_minutes)
    """
    baseline = get_baseline_minutes(position_slot)
    
    if baseline <= 0:
        return (MINUTES_HARD_FLOOR, MINUTES_HARD_CAP)
    
    floor = max(MINUTES_HARD_FLOOR, baseline * MINUTES_FLOOR_PCT)
    ceiling = min(MINUTES_HARD_CAP, baseline * MINUTES_CEILING_PCT)
    
    return (round(floor, 2), round(ceiling, 2))

def clip_minutes(minutes: float, position_slot: str) -> float:
    """
    Clip projected minutes to role-based bounds.
    
    Args:
        minutes: Raw projected minutes
        position_slot: Depth chart slot for bounds lookup
        
    Returns:
        Minutes clipped to [M_min, M_max] for that role
    """
    floor, ceiling = get_minutes_bounds(position_slot)
    return round(max(floor, min(ceiling, minutes)), 2)


def project_minutes(
    position_slot: str,
    is_bench_to_starter: bool = False,
    spread: float = None,
) -> dict:
    """
    Project minutes with dynamic adjustments.
    
    Args:
        position_slot: Depth chart slot (e.g., "PG1", "SF2")
        is_bench_to_starter: True if bench player moved to starter role
        spread: Vegas spread (absolute value used, negative = favorite)
        
    Returns:
        Dict with baseline, adjustments, and projected minutes
    """
    baseline = get_baseline_minutes(position_slot)
    
    adjustments = {
        "starter_bump": 0.0,
        "game_context": 0.0,
    }
    
    if is_bench_to_starter:
        adjustments["starter_bump"] = STARTER_BUMP
    
    if spread is not None:
        abs_spread = abs(spread)
        if abs_spread < CLOSE_GAME_THRESHOLD:
            adjustments["game_context"] = CLOSE_GAME_BUMP
        elif abs_spread >= BLOWOUT_THRESHOLD:
            adjustments["game_context"] = BLOWOUT_PENALTY
    
    total_adjustment = sum(adjustments.values())
    projected = max(0, baseline + total_adjustment)
    
    return {
        "position_slot": position_slot,
        "baseline_min": baseline,
        "starter_bump": adjustments["starter_bump"],
        "game_context": adjustments["game_context"],
        "projected_min": round(projected, 2),
    }


def get_game_context_label(spread: float) -> str:
    """Return label for game context based on spread."""
    if spread is None:
        return "Unknown"
    abs_spread = abs(spread)
    if abs_spread < CLOSE_GAME_THRESHOLD:
        return "Close"
    elif abs_spread >= BLOWOUT_THRESHOLD:
        return "Blowout"
    return "Normal"


if __name__ == "__main__":
    print("=== Baseline Minutes by Inferred Depth Rank ===\n")
    
    for pos in ["PG", "SG", "SF", "PF", "C", "G", "F"]:
        print(f"{pos}:")
        baselines = get_all_position_baselines(pos)
        for rank, mins in sorted(baselines.items()):
            count = SAMPLE_COUNTS.get(rank, 0)
            print(f"  {rank}: {mins:.2f} min (n={count:,})")
        print()
    
    print("\n=== Example Minute Projections ===\n")
    
    print("PG1 starter, normal game (spread -6):")
    print(project_minutes("PG1", spread=-6))
    
    print("\nSF2 bench player promoted to starter, close game (spread -2):")
    print(project_minutes("SF2", is_bench_to_starter=True, spread=-2))
    
    print("\nPF1 starter in blowout risk game (spread -12):")
    print(project_minutes("PF1", spread=-12))
