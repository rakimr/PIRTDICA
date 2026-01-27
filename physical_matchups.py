"""
Physical Matchup Lookup Table

This module defines which players are considered "physical" at each position.
Physical players cause more foul trouble for their matchups.

Matchup modifier values:
- Elite physical (1.5): Players known for drawing fouls, post-ups, physical play
- Physical (0.75): Above-average physicality
- Normal (0.0): Standard matchup

Last updated: 2026-01-27
"""

PHYSICAL_CENTERS = {
    "Joel Embiid": 1.5,
    "Domantas Sabonis": 1.5,
    "Anthony Davis": 1.5,
    "Nikola Jokic": 1.5,
    "Bam Adebayo": 1.25,
    "Giannis Antetokounmpo": 1.5,
    "Karl-Anthony Towns": 1.0,
    "Alperen Sengun": 1.25,
    "Jonas Valanciunas": 1.0,
    "Ivica Zubac": 1.0,
    "Clint Capela": 0.75,
    "Mitchell Robinson": 0.75,
    "Jarrett Allen": 0.75,
    "Chet Holmgren": 0.5,
    "Victor Wembanyama": 0.75,
    "Brook Lopez": 0.5,
    "Rudy Gobert": 1.0,
    "Jusuf Nurkic": 1.0,
    "DeAndre Ayton": 0.75,
    "Jaren Jackson Jr": 0.75,
}

PHYSICAL_POWER_FORWARDS = {
    "Giannis Antetokounmpo": 1.5,
    "Anthony Davis": 1.5,
    "Zion Williamson": 1.5,
    "Julius Randle": 1.25,
    "Pascal Siakam": 1.0,
    "Evan Mobley": 0.75,
    "Paolo Banchero": 1.0,
    "Jabari Smith Jr": 0.75,
    "Scottie Barnes": 1.0,
    "Draymond Green": 1.0,
    "John Collins": 0.75,
}

PHYSICAL_SMALL_FORWARDS = {
    "LeBron James": 1.0,
    "Kawhi Leonard": 0.75,
    "Jimmy Butler": 1.25,
    "OG Anunoby": 0.75,
    "Mikal Bridges": 0.5,
}

PHYSICAL_GUARDS = {
    "Luka Doncic": 1.0,
    "James Harden": 1.25,
    "Shai Gilgeous-Alexander": 1.0,
    "Ja Morant": 0.75,
    "Tyrese Haliburton": 0.5,
}

POSITION_MAP = {
    "C": PHYSICAL_CENTERS,
    "PF": PHYSICAL_POWER_FORWARDS,
    "SF": PHYSICAL_SMALL_FORWARDS,
    "SG": PHYSICAL_GUARDS,
    "PG": PHYSICAL_GUARDS,
}

def get_matchup_modifier(opponent_name, position):
    """
    Get the foul trouble matchup modifier for facing a specific opponent.
    
    Args:
        opponent_name: Name of the opposing player
        position: Position being defended (C, PF, SF, SG, PG)
    
    Returns:
        float: Matchup modifier (0.0 to 1.5)
    """
    pos = position.upper().replace("1", "").replace("2", "").replace("3", "")
    
    if pos in POSITION_MAP:
        players = POSITION_MAP[pos]
        for player, modifier in players.items():
            if player.lower() in opponent_name.lower() or opponent_name.lower() in player.lower():
                return modifier
    
    for pos_players in POSITION_MAP.values():
        for player, modifier in pos_players.items():
            if player.lower() in opponent_name.lower() or opponent_name.lower() in player.lower():
                return modifier
    
    return 0.0

def get_team_physical_players(team_abbrev, position=None):
    """
    Get all physical players on a team.
    
    Args:
        team_abbrev: Team abbreviation (e.g., "PHI", "SAC")
        position: Optional position filter
    
    Returns:
        dict: {player_name: modifier}
    """
    pass

def is_physical_matchup(opponent_team, position):
    """
    Check if a team has a physical player at a position.
    Returns the most physical player and their modifier.
    """
    pass


TEAM_CENTERS = {
    "PHI": ("Joel Embiid", 1.5),
    "SAC": ("Domantas Sabonis", 1.5),
    "LAL": ("Anthony Davis", 1.5),
    "DEN": ("Nikola Jokic", 1.5),
    "MIL": ("Giannis Antetokounmpo", 1.5),
    "MIA": ("Bam Adebayo", 1.25),
    "HOU": ("Alperen Sengun", 1.25),
    "WAS": ("Jonas Valanciunas", 1.0),
    "LAC": ("Ivica Zubac", 1.0),
    "MIN": ("Rudy Gobert", 1.0),
    "POR": ("Jusuf Nurkic", 1.0),
    "CLE": ("Jarrett Allen", 0.75),
    "ATL": ("Clint Capela", 0.75),
    "PHX": ("DeAndre Ayton", 0.75),
    "MEM": ("Jaren Jackson Jr", 0.75),
    "OKC": ("Chet Holmgren", 0.5),
    "SAS": ("Victor Wembanyama", 0.75),
}

def get_opposing_center_modifier(opponent_team):
    """
    Get the physical modifier for the opposing team's center.
    
    Args:
        opponent_team: Team abbreviation (e.g., "SAC")
    
    Returns:
        tuple: (player_name, modifier) or (None, 0.0)
    """
    if opponent_team in TEAM_CENTERS:
        return TEAM_CENTERS[opponent_team]
    return (None, 0.0)


if __name__ == "__main__":
    print("=== Physical Matchup Lookup ===\n")
    
    print("Testing matchup modifiers:")
    test_cases = [
        ("Joel Embiid", "C"),
        ("Domantas Sabonis", "C"),
        ("Anthony Davis", "PF"),
        ("Random Player", "C"),
    ]
    
    for player, pos in test_cases:
        mod = get_matchup_modifier(player, pos)
        print(f"  {player} ({pos}): {mod}")
    
    print("\nTeam center lookup:")
    for team in ["PHI", "SAC", "NY", "BKN"]:
        player, mod = get_opposing_center_modifier(team)
        print(f"  {team}: {player} ({mod})")
