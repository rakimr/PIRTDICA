# PIRTDICA SPORTS CO. - NBA DFS Platform

## Overview
PIRTDICA SPORTS CO. operates an NBA Daily Fantasy Sports (DFS) platform providing player projections and a unique "Beat the House" game. The platform leverages advanced analytics, including sophisticated player archetype classification and salary-tier volatility modeling, to offer a competitive and engaging DFS experience. It aims to generate accurate projections and facilitate a game where users compete against an AI-generated lineup using Monte Carlo simulation. The platform also features a dual-currency economy (Coach Coin for engagement, Coach Cash for competition) designed to drive long-term retention and foster a vibrant community.

## User Preferences
Preferred communication style: Simple, everyday language.
Avatar art style: "Gravity Falls meets Emoji" — chunky cartoon characters with big expressive eyes, thick black outlines, slightly asymmetric features, round heads, quirky charm, flat illustration, muted color palettes.

## System Architecture
The system uses an ETL pattern with SQLite for data storage, orchestrated by `run_daily_update.py`. Core projection models include minutes projection, usage-based FPPM adjustment, and robust name normalization. Advanced models like Phillips Archetype Classification (K-means clustering for secondary positions) and Salary-Tier Volatility Model (regularizing fantasy point standard deviation) enhance projection accuracy. The Ceiling/Floor Model transforms point projections into full distributions for strategic decision-making. The Blended DVP and Defense vs Archetype (DVA) systems provide reactive matchup ratings, adapting throughout the season. A Team Incentive Score adjusts volatility based on team standings, and a Prop Trend Analysis Modal offers OVER/UNDER calls based on blended DVP/DVA edges.

Lineup optimization is handled by a PuLP linear programming optimizer and a Monte Carlo optimizer for win probability. The "Beat the House" game features daily contests against an AI. The "Coach vs Coach" (H2H) system allows users to challenge each other with their lineups, featuring a lobby, coin escrow, and live scoring.

The web platform uses FastAPI (Python) with SQLAlchemy and PostgreSQL for the backend, and Jinja2 templates with custom CSS for the frontend. It includes live scoring, contest history, and an admin control panel. The dual-currency economy (Coach Coin for cosmetics/access, Coach Cash for competitive play) supports a Play-to-Earn (P2E) model focused on Identity, Prestige, Access, and Analytics, without compromising competitive integrity. Coach Cash can be converted to Coach Coin (1:5 rate), but not vice-versa, to prevent farming exploits. Ranked modes include a free Coin Mode and a competitive Cash Mode.

## External Dependencies

### Web Scraping Targets
-   **ESPN:** Depth charts
-   **RotoGrinders:** Player lineups, injury alerts, FanDuel salaries
-   **FantasyPros:** FanDuel salary fallback for missing data
-   **TeamRankings:** Game odds/spreads
-   **HashtagBasketball:** Defense vs Position stats
-   **Basketball Reference:** Per-100 possession stats, player positions, foul rates
-   **NBA.com Stats API:** Player game logs, minutes volatility, referee assignments
-   **NBAStuffer:** Historical referee statistics
-   **SportsDatabase:** Historic betting lines
-   **FantasyTeamAdvice.com:** FanDuel NBA ownership data

### APIs
-   **The Odds API:** Player prop lines (FanDuel)
-   **plaintextsports.com/nba:** Live scoring data

### Databases
-   **SQLite:** Primary data store (`dfs_nba.db`) for scraped and processed data.
-   **PostgreSQL:** Stores user accounts, contest data, entries, achievements, shop items, and dual-currency ledgers for the web platform.

### Python Libraries
-   `requests`, `BeautifulSoup`: Web scraping
-   `pandas`: Data manipulation
-   `sqlite3`: SQLite database operations
-   `PuLP`: Linear programming for optimization
-   `scikit-learn`: K-means clustering
-   `Chart.js`: Frontend charting