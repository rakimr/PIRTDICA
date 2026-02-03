"""Shared player name normalization for consistent matching across the system."""
import unicodedata
import re

def normalize_player_name(name: str) -> str:
    """Normalize player name for consistent matching across data sources."""
    if not name or not isinstance(name, str):
        return ""
    
    name = unicodedata.normalize('NFKD', name)
    name = name.encode('ASCII', 'ignore').decode('ASCII')
    name = name.lower().strip()
    
    suffixes = [' jr', ' jr.', ' sr', ' sr.', ' ii', ' iii', ' iv', ' v']
    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[:-len(suffix)].strip()
    
    name = re.sub(r'\s+', ' ', name)
    
    return name
