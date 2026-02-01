from datetime import datetime, date
from zoneinfo import ZoneInfo

EASTERN = ZoneInfo("America/New_York")

def get_eastern_now() -> datetime:
    """Get current datetime in Eastern timezone."""
    return datetime.now(EASTERN)

def get_eastern_today() -> date:
    """Get today's date in Eastern timezone."""
    return datetime.now(EASTERN).date()

def get_eastern_date_str() -> str:
    """Get today's date as YYYY-MM-DD string in Eastern timezone."""
    return get_eastern_today().strftime("%Y-%m-%d")
