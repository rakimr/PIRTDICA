# PIRTDICA SPORTS CO. - NBA DFS Platform

## Overview

PIRTDICA SPORTS CO. operates an NBA Daily Fantasy Sports (DFS) platform focused on providing projections and a unique "Beat the House" game. The core system processes basketball data to generate player projections, while the game allows users to compete against an AI-generated lineup using Monte Carlo simulation. The platform aims to offer a competitive and engaging DFS experience, leveraging advanced analytics for projection accuracy and game mechanics. Key capabilities include sophisticated player archetype classification, salary-tier volatility modeling, and real-time ownership projections.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Data Pipeline and Orchestration
The system employs an ETL (Extract-Transform-Load) pattern using a SQLite database as the central data store. Scripts are categorized into scrapers, ETL processors, and analysis scripts, with `run_daily_update.py` orchestrating their execution in dependency order. This ensures data integrity and timely projection generation.

### Core Projection Models and Features
-   **Minutes Projection Model:** Utilizes empirical baseline minutes, depth charts, injury impacts, and game context to project player minutes.
-   **Usage-Based FPPM Adjustment:** Dynamically redistributes injured players' usage to teammates, adjusting their fantasy points per minute (FPPM) to reflect increased opportunity.
-   **Position Handling:** Tracks five core positions plus hybrids, deriving data from Basketball Reference.
-   **Name Normalization:** Robust system for handling player name inconsistencies across various data sources.
-   **Physical Matchup Modifiers:** Adjusts projected minutes based on player-specific foul tendencies for opponents.
-   **FanDuel Roster Order Integration:** Incorporates FanDuel's `roster_order` to refine depth chart interpretations.
-   **Phillips Archetype Classification (Model 6):** Uses K-means clustering to assign secondary position labels (e.g., Combo Guard, Playmaker) based on advanced per-100 stats and usage. Interactive PCA scatter chart on Trends page (`/api/archetype-clusters` endpoint) with clickable legend for toggling archetypes.
-   **Salary-Tier Volatility Model (Model 5):** Regularizes player fantasy point standard deviation (fp_sd) using empirical salary-tier profiles, blending individual and tier-expected volatility. Includes tail capping at tier-specific P5/P95 and a `value_vs_tier` metric.
-   **Ceiling/Floor Model:** Transforms point projections into full distributions to aid in cash game (consistent, high floor) vs. GPP (high upside) decision-making.
-   **ML Minutes Correction (Model 3):** Learns and applies multipliers to correct systematic minutes over/under-projection.
-   **Blended DVP System:** Adaptively weights full-season and 30-day Defense vs. Position (DvP) data.
-   **Defense vs Archetype (DVA) System:** Three-phase system: (1) DVA table computing FP/min and stat splits per minute for each team vs each archetype, (2) archetype stat-profile vectors showing % of FP from each stat category, (3) DVS multiplier combining archetype stat weights × defensive leak rates. Features: 30-day rolling window blended 50/50 with full-season data for reactive matchup ratings; sample-size shrinkage (linear toward zero for n<50); dynamic DVP/DVS blend weights shifting from 70/30 DVP-heavy early season to 30/70 DVS-heavy late season; ceiling asymmetry applying favorable DVS to boost upside (fp_sd) rather than just median projection; DVP vs DVS correlation diagnostic logged each run. Interactive heatmap table on Trends page (`/api/dva` endpoint) with toggle between DVS multiplier and FP/min differential views, hover tooltips showing full stat breakdown. Script: `build_dva.py`.
-   **Team Incentive Score:** Adjusts player volatility based on team standings (e.g., must-win vs. tanking teams).
-   **Prop Trend Analysis Modal:** Visualizes a player's last 10 games for a specific stat against book lines, providing OVER/UNDER calls.

### Lineup Optimization and Gaming
-   **Lineup Optimizer:** Uses PuLP linear programming to find optimal 9-player FanDuel lineups within salary caps and position requirements, with optional reliability adjustments.
-   **Monte Carlo Optimizer:** Stochastic simulation finds lineups with the highest win probability.
-   **Beat the House Game:** Daily contests where users compete against an AI-generated house lineup; features include user accounts, leaderboards, in-game currency, and live scoring.
-   **Coach vs Coach (H2H):** Head-to-head challenge system where users wager coins and compete with their own lineups against each other. Features: lobby for creating/accepting challenges, coin escrow system with 10% house cut, same salary cap and player pool as main contest, game lock enforcement, live scoring with 30-second refresh, side-by-side match view, settlement on contest completion (auto-triggered via live scoring API and score_contest.py), tie handling with full refund, H2H record tracking on user profiles.

### Web Platform
-   **Backend:** FastAPI (Python) with SQLAlchemy ORM, PostgreSQL for user and contest data.
-   **Frontend:** Jinja2 templates with custom CSS.
-   **Live Scoring:** Real-time fantasy point tracking with 30-second auto-refresh.
-   **Contest History Page:** Displays past contest entries, win/loss records, and earnings.
-   **Admin Control Panel:** Web-based interface for data refresh and injury overrides.

## External Dependencies

### Web Scraping Targets
-   **ESPN:** Depth charts
-   **RotoGrinders:** Player salaries, injury alerts
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
-   **PostgreSQL:** Stores user accounts, contest data, entries, achievements, and shop items for the web platform.

### Python Libraries
-   `requests`, `BeautifulSoup`: Web scraping
-   `pandas`: Data manipulation
-   `sqlite3`: SQLite database operations
-   `PuLP`: Linear programming for optimization
-   `scikit-learn`: K-means clustering for player archetype classification
-   `Chart.js`: Frontend charting