NBA_TEAM_ABBREV = {
    "Atlanta": "ATL", "Boston": "BOS", "Brooklyn": "BKN", "Charlotte": "CHA",
    "Chicago": "CHI", "Cleveland": "CLE", "Dallas": "DAL", "Denver": "DEN",
    "Detroit": "DET", "Golden State": "GSW", "Houston": "HOU", "Indiana": "IND",
    "LA Clippers": "LAC", "LA Lakers": "LAL", "Los Angeles Clippers": "LAC",
    "Los Angeles Lakers": "LAL", "Memphis": "MEM", "Miami": "MIA",
    "Milwaukee": "MIL", "Minnesota": "MIN", "New Orleans": "NOP",
    "New York": "NYK", "Oklahoma City": "OKC", "Orlando": "ORL",
    "Philadelphia": "PHI", "Phoenix": "PHX", "Portland": "POR",
    "Sacramento": "SAC", "San Antonio": "SAS", "Toronto": "TOR",
    "Utah": "UTA", "Washington": "WAS"
}

def to_abbrev(team_name):
    if team_name is None:
        return None
    return NBA_TEAM_ABBREV.get(team_name, team_name)
