NBA_TEAM_ABBREV = {
    "Atlanta": "ATL", "Boston": "BOS", "Brooklyn": "BKN", "Charlotte": "CHA",
    "Chicago": "CHI", "Cleveland": "CLE", "Dallas": "DAL", "Denver": "DEN",
    "Detroit": "DET", "Golden State": "GS", "Houston": "HOU", "Indiana": "IND",
    "LA Clippers": "LAC", "LA Lakers": "LAL", "L.A. Clippers": "LAC",
    "L.A. Lakers": "LAL", "Los Angeles Clippers": "LAC",
    "Los Angeles Lakers": "LAL", "Memphis": "MEM", "Miami": "MIA",
    "Milwaukee": "MIL", "Minnesota": "MIN", "New Orleans": "NO",
    "New York": "NY", "Oklahoma City": "OKC", "Orlando": "ORL",
    "Philadelphia": "PHI", "Phoenix": "PHO", "Portland": "POR",
    "Sacramento": "SAC", "San Antonio": "SA", "Toronto": "TOR",
    "Utah": "UTA", "Washington": "WAS",
    "GSW": "GS", "NOP": "NO", "NYK": "NY", "SAS": "SA", "PHX": "PHO",
    "OKL": "OKC", "Okla City": "OKC",
    "Hawks": "ATL", "Celtics": "BOS", "Nets": "BKN", "Hornets": "CHA",
    "Bulls": "CHI", "Cavaliers": "CLE", "Mavericks": "DAL", "Nuggets": "DEN",
    "Pistons": "DET", "Warriors": "GS", "Rockets": "HOU", "Pacers": "IND",
    "Clippers": "LAC", "Lakers": "LAL", "Grizzlies": "MEM", "Heat": "MIA",
    "Bucks": "MIL", "Timberwolves": "MIN", "Pelicans": "NO", "Knicks": "NY",
    "Thunder": "OKC", "Magic": "ORL", "Seventysixers": "PHI", "76ers": "PHI",
    "Suns": "PHO", "Trailblazers": "POR", "Trail Blazers": "POR",
    "Kings": "SAC", "Spurs": "SA", "Raptors": "TOR", "Jazz": "UTA", "Wizards": "WAS"
}

def to_abbrev(team_name):
    if team_name is None:
        return None
    return NBA_TEAM_ABBREV.get(team_name, team_name)

TEAM_MAP = NBA_TEAM_ABBREV
