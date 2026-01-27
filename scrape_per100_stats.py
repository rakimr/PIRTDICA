import requests
import pandas as pd
import sqlite3
import time

conn = sqlite3.connect("dfs_nba.db")

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

print("Scraping per-100 possession stats...")
url = "https://www.basketball-reference.com/leagues/NBA_2026_per_poss.html"
resp = requests.get(url, headers=headers)
with open("/tmp/per100.html", "w") as f:
    f.write(resp.text)
tables = pd.read_html("/tmp/per100.html")
df = tables[0]

df = df[df["Player"] != "Player"]
df = df[df["Player"].notna()]

keep_cols = ["Player", "Team", "G", "MP", "PTS", "TRB", "AST", "STL", "BLK", "TOV"]
available_cols = [c for c in keep_cols if c in df.columns]
df = df[available_cols].copy()

for col in ["G", "MP", "PTS", "TRB", "AST", "STL", "BLK", "TOV"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

if "G" in df.columns and "MP" in df.columns:
    df["mpg"] = (df["MP"] / df["G"]).round(1)
else:
    df["mpg"] = 0.0

df = df.rename(columns={
    "Player": "player_name",
    "Team": "team",
    "G": "games_played",
    "MP": "total_minutes",
    "TRB": "reb_per100"
})
df = df.rename(columns={
    "PTS": "pts_per100",
    "AST": "ast_per100",
    "STL": "stl_per100",
    "BLK": "blk_per100",
    "TOV": "tov_per100"
})

df["fp_per100"] = (
    df["pts_per100"] * 1.0 +
    df["reb_per100"] * 1.2 +
    df["ast_per100"] * 1.5 +
    df["stl_per100"] * 3.0 +
    df["blk_per100"] * 3.0 +
    df["tov_per100"] * -1.0
)

df = df.drop_duplicates(subset=["player_name"], keep="first")

df.to_sql("player_per100", conn, if_exists="replace", index=False)
print(f"Saved {len(df)} players with per-100 stats")

time.sleep(3)

print("\nScraping team pace stats...")
pace_url = "https://www.basketball-reference.com/leagues/NBA_2026.html"
resp = requests.get(pace_url, headers=headers)
with open("/tmp/pace.html", "w") as f:
    f.write(resp.text)
tables = pd.read_html("/tmp/pace.html")
pace_df = tables[10]

if isinstance(pace_df.columns, pd.MultiIndex):
    pace_df.columns = [col[-1] if col[-1] != "" else col[0] for col in pace_df.columns]

pace_df = pace_df[pace_df["Team"].notna()]
pace_df = pace_df[~pace_df["Team"].str.contains("League Average", na=False)]
pace_df = pace_df[["Team", "Pace"]].copy()
pace_df["Pace"] = pd.to_numeric(pace_df["Pace"], errors="coerce")

team_abbrevs = {
    "Atlanta Hawks": "ATL", "Boston Celtics": "BOS", "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA", "Chicago Bulls": "CHI", "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL", "Denver Nuggets": "DEN", "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW", "Houston Rockets": "HOU", "Indiana Pacers": "IND",
    "Los Angeles Clippers": "LAC", "Los Angeles Lakers": "LAL", "Memphis Grizzlies": "MEM",
    "Miami Heat": "MIA", "Milwaukee Bucks": "MIL", "Minnesota Timberwolves": "MIN",
    "New Orleans Pelicans": "NOP", "New York Knicks": "NYK", "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL", "Philadelphia 76ers": "PHI", "Phoenix Suns": "PHO",
    "Portland Trail Blazers": "POR", "Sacramento Kings": "SAC", "San Antonio Spurs": "SAS",
    "Toronto Raptors": "TOR", "Utah Jazz": "UTA", "Washington Wizards": "WAS"
}

pace_df["team"] = pace_df["Team"].map(team_abbrevs)
pace_df = pace_df[["team", "Pace"]].rename(columns={"Pace": "pace"})
pace_df = pace_df.dropna()

pace_df.to_sql("team_pace", conn, if_exists="replace", index=False)
print(f"Saved {len(pace_df)} teams with pace data")

print("\nTop 10 FP/100 players:")
top = df.nlargest(10, "fp_per100")[["player_name", "team", "fp_per100"]]
print(top.to_string(index=False))

conn.close()
