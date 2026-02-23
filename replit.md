# PIRTDICA SPORTS CO. - NBA DFS Platform

## Overview
PIRTDICA SPORTS CO. offers an NBA Daily Fantasy Sports (DFS) platform focused on accurate player projections through advanced analytics like sophisticated player archetype classification and salary-tier volatility modeling. Its core purpose is to provide a skill-based esports competition platform, moving beyond traditional gambling. Key features include a "Beat the House" game using Monte Carlo simulation, competitive play against AI, and a dual-currency economy (Coach Coin for engagement, Coach Cash for competition) designed to boost user retention and build community. The platform aims to establish a new standard for competitive fantasy sports.

## User Preferences
Preferred communication style: Simple, everyday language.
Auto-push to GitHub: Always push changes to GitHub at the end of every task using Replit's GitHub connector OAuth token.

### Avatar & Identity Design Direction
**Style:** Strategic Minimalism meets Editorial Sports Design.
**Vibe:** Nike campaign meets tech startup meets esports broadcast.

**Design Rules:**
- White background, black primary vector, 3-4px line weight
- Single accent color max (unless legendary tier)
- Circular 1:1 badge format (works at 32px and 128px)
- No gradients, no dark glowing cyber stuff
- Accent palette: Crimson (#D72638), Royal Blue (#1E3A8A), Emerald (#0F9D58), Gold (#D4AF37), Violet (#6D28D9)

### Coach Shop - Identity Cosmetics Framework

**Core Rule:** Every cosmetic must communicate Skill, Status, or Story. If it doesn't, it won't sell.

**Critical Integrity Rules:**
- Purchasable cosmetics must NEVER mimic rank colors, division badge shapes, or Champion aesthetics
- Purchased badges must be decorative (animated borders, metallic trims, signatures) — never mimic earned achievement badges
- Skill must remain sacred. No one should ever think "they won because they bought something."
- Shop positioning = "Identity customization", NOT "Power boost store"

## Player Archetype System
10 total archetypes: 5 specialist + 5 hybrid branch categories.

**Specialist Archetypes (5):** Playmaker, 3-and-D Wing, Scoring Wing, Versatile Big, Traditional Big

**Hybrid Branch Archetypes (5):** Combo Guard, Stretch 4, Stretch 5, Point Center, Point Forward

- **Combo Guard:** Blends PG/SG roles. Transcendent guards (PTS/100 >= 30, AST >= 7, USG >= 26). Examples: SGA, Curry, Harden, Brunson.
- **Stretch 4:** PF who shoots like a guard, C% < 50. Examples: Lauri Markkanen, Jabari Smith.
- **Stretch 5:** Center who shoots like a guard, C% >= 50. Examples: Wembanyama, KAT, Myles Turner.
- **Point Center:** Center who facilitates, AST/100 >= 5.0, PTS/100 >= 24.0, C% >= 50%. Examples: Jokic, Embiid, Sabonis.
- **Point Forward:** Forward who runs offense, AST/100 >= 5.0, C% < 50%. Examples: Giannis, LeBron, Banchero.

**Classification pipeline:** K-means clustering (k=6, 18+corner3 features) -> shot-zone reclassification -> wing escape -> facilitating big detection (with ball initiation gate) -> height-based correction -> hybrid branch routing -> playmaker reclassification -> Stretch 4/5 split

## Matchup Interaction Layer (Implemented Feb 23, 2026)

### Origin: The Clingan vs Williams Law
On Feb 22, 2026, Donovan Clingan (7'2", ~280 lbs, second year player drafted 2024) physically dominated Mark Williams (7'0", ~245 lbs) despite our projection system favoring Williams. The base model missed the physical mismatch. Named "The Clingan vs Williams Law" internally.

### Architecture (4 Active Components + Data Layer)

**1. Matchup Familiarity Score** (`matchup_engine.py:build_matchup_familiarity`)
- Player-vs-team FPPM differentials from current season game logs
- Bayesian shrinkage: log1p(games_vs) / log1p(max(max_games, 10)) prevents early-season overfitting
- Minimum 10-game floor for shrinkage normalization

**2. Archetype Matchup Profiles** (`matchup_engine.py:build_archetype_matchup_profiles`)
- Archetype-vs-archetype performance patterns derived from game logs
- Matchup groups: bigs, forwards, wings, guards with adjacency rules
- Confidence weighting: sample_size / 100, capped at 1.0

**3. Size Advantage Metric** (`matchup_engine.py:compute_size_advantage`)
- Formula: S = (Height_diff) + 0.5*(Weight_diff) + 0.3*(Wingspan_diff)
- Interior weight: 1.0 for bigs/stretch, 0.3 for perimeter players
- Normalized by dividing by 30, clamped to [-1, 1]

**4. Durability Modifier** (`matchup_engine.py:compute_durability_modifier`)
- Opponent minutes stability from player_volatility (min_sd / 10)
- Low-games penalty: stability *= 0.7 for < 20 games
- Weakest matchable opponent determines modifier

**Projection Integration:** Model 6 in `dfs_players.py`, applied after ML bias correction (Model 1/2) and before Tier Volatility (Model 5). Alpha weights: familiarity=0.35, archetype=0.25, size=0.25, durability=0.15. Max adjustment capped at +/- 3.0 FP.

**Results:** 249 players adjusted in Feb 22 slate run. Typical adjustments: +1.0 to +3.0 FP for strong matchups.

### Data Sources
- **Physical Measurements:** `scrape_measurements.py` scrapes all 30 NBA team roster pages from Basketball Reference for height/weight. 56 key players have measured wingspan; rest estimated from height ratios (1.04-1.07 depending on position).
- **Historical Season Data:** `scrape_historical_gamelogs.py` pulls 3 seasons (2022-23, 2023-24, 2024-25) of totals from Basketball Reference. 771 unique players, 426 overlap with current season.
- **Current Game Logs:** 17,709 records from Oct 2025 to Feb 2026 via NBA.com API.

### Database Tables
- `player_measurements` (SQLite) -> `player_measurements_live` (Postgres)
- `matchup_history` (SQLite) -> `matchup_history_live` (Postgres)
- `archetype_matchup_profiles` (SQLite) -> `archetype_matchup_profiles_live` (Postgres)
- `historical_season_totals`, `historical_player_seasons` (SQLite only, reference data)

### Future Improvements
- Scrape per-game opponent-level logs for true matchup familiarity (not just team-level)
- Calibrate alpha weights via backtesting against historical actuals
- Add Draft Combine wingspan data (blocked by NBA.com cloud rate limiting)

## System Architecture
The system utilizes an ETL pattern, staging data in SQLite and storing operational data in PostgreSQL. Core projection models include minutes projection and usage-based FPPM adjustment. Advanced analytics are driven by Phillips Archetype Classification (K-means clustering with 18 features) and a Salary-Tier Volatility Model. A Ceiling/Floor Model generates projection distributions, while Blended DVP and DVA systems provide dynamic matchup ratings. A Team Incentive Score adjusts volatility, and a Prop Trend Analysis Modal offers OVER/UNDER calls.

The Matchup Interaction Layer dynamically adjusts projections based on physical mismatch (size, weight, wingspan), matchup familiarity, and opponent durability. This layer incorporates historical game logs and physical measurements to refine predictions beyond traditional DVP.

Lineup optimization is achieved using PuLP for linear programming and a Monte Carlo optimizer. The platform features a "Beat the House" game against AI, and "Coach vs Coach" (H2H) competitive play with a lobby, coin escrow, and live scoring.

The web platform is built with FastAPI (Python) for the backend, SQLAlchemy for ORM, and Jinja2 templates with custom CSS for the frontend, featuring live scoring, contest history, and admin controls. The dual-currency system (Coach Coin, Coach Cash) supports a Play-to-Earn (P2E) model focused on Identity, Prestige, Access, and Analytics, strictly avoiding pay-to-win mechanics. Ranked modes include free Coin Mode and competitive Cash Mode, structured with a tiered division system, hidden MMR, and seasonal resets. Monetization relies on a small rake on Coach Cash competitions, cosmetic sales, and future subscriptions.

### Chart Screenshot Infrastructure
- Temporary route `/chart-screenshot/{chart_type}/{target}` renders PIRTDICA charts for capture
- Uses synchronous XHR + raw Canvas API (no Chart.js dependency) for immediate rendering
- Playwright + nix Chromium (`capture_charts.py`) automates batch screenshot capture at 2x resolution

## External Dependencies

### Web Scraping Targets
-   **ESPN:** Depth charts
-   **RotoGrinders, FantasyPros:** Player lineups, injury alerts, FanDuel salaries
-   **TeamRankings:** Game odds/spreads
-   **HashtagBasketball:** Defense vs Position stats
-   **Basketball Reference:** Per-100 possession stats, player positions, foul rates, player physical measurements (height, weight from team roster pages)
-   **NBA.com Stats API:** Player game logs, minutes volatility, referee assignments, shot zone distribution, shot creation types, hustle stats. **Note:** NBA.com rate-limits cloud server IPs aggressively. All scrapers include browser-like headers, 60s request timeouts, 2 retries with 5/15s delays, and graceful fallback to cached SQLite data.
-   **NBAStuffer:** Historical referee statistics
-   **SportsDatabase:** Historic betting lines
-   **FantasyTeamAdvice.com:** FanDuel NBA ownership data

### APIs
-   **The Odds API:** Player prop lines (FanDuel)
-   **plaintextsports.com/nba:** Live scoring data

### Databases
-   **SQLite:** Staging database (`dfs_nba.db`) for local pipeline scraping.
-   **PostgreSQL:** Production database for web platform and pipeline output.
-   **Data Sync:** `sync_to_postgres.py` syncs scraped data from SQLite/CSV into PostgreSQL `*_live` tables, and platform tables from local PG to Supabase.
-   **Data Access Layer:** `backend/data_access.py` provides unified data access that checks PostgreSQL first, falls back to CSV/SQLite.
-   **Pipeline Tables:** `dfs_players_live`, `prop_recommendations_live`, `targeted_plays_live`, `ownership_projections_live`, `player_salaries_live`, `injury_alerts_live`, `player_archetypes_live`, `player_per100_live`, `player_positions_live`, `player_stats_live`, `player_game_logs_live`, `player_shot_zones_live`, `player_shot_creation_live`, `player_hustle_stats_live`, `dva_stats_live`, `archetype_profiles_live`, `team_defense_shot_zones_live`, `team_play_types_live`, `player_headshots_live`, `player_measurements_live`, `matchup_history_live`, `archetype_matchup_profiles_live`

### Username Moderation
-   `backend/profanity_filter.py`: Profanity/slur filter with leet-speak detection
-   Registration blocks offensive usernames, login blocks banned accounts
-   Admin panel has "Username Moderation" section with force-rename and ban actions

### Content & Article Writing Guidelines (for X / Social Media)

**Social Accounts:** X: @pirtdicaco | Instagram: @pirtdicaco | Email: pirtdicaco@gmail.com

**Article Framing:** Articles are written for real basketball performance and sportsbook bettors, NOT for DFS/fantasy lineups. Focus on which specific stats a player will exceed tonight and why.

**Formatting:** No hyphens or dashes in articles. Use commas, periods, and sentence restructuring instead.

**Article Header Art Direction (Dan Koe-inspired):**
- Black and white woodcut/engraving style, Gustave Dore meets sci-fi surrealism
- Wide panoramic format (approximately 2.5:1 ratio, 1920x768)
- No text, no logos, no color. Pure black and white illustration

### Python Libraries
-   `requests`, `BeautifulSoup`: Web scraping
-   `pandas`, `numpy`: Data manipulation
-   `sqlite3`: SQLite interaction
-   `PuLP`: Linear programming
-   `scikit-learn`: K-means clustering
-   `nba_api`: NBA.com stats API wrapper
-   `playwright`: Headless browser for chart screenshot capture

## Daily Pipeline Order (`run_daily_update.py`)
1. Player Salaries, Depth Charts, Game Odds, DVP Stats
2. Per-100 Stats, Referee Stats/Assignments, Injury Alerts
3. NBA Game Logs, Standings, Rotation Detection
4. Shot Zones, Team Defense Zones, Play Types
5. **Player Physical Measurements** (NEW)
6. Player Archetype Classification, Defense vs Archetype (DVA)
7. **Matchup Interaction Layer** (NEW)
8. DFS Player Projections (includes Model 6 matchup adjustments)
9. Player Props, Ownership Estimation, House Lineup, Contest Scoring
10. Sync to PostgreSQL

## Recent Changes (Feb 2026)
- **Feb 23:** IMPLEMENTED Matchup Interaction Layer: scrape_measurements.py (523 players, 56 measured wingspans), scrape_historical_gamelogs.py (3 seasons, 771 players), matchup_engine.py (familiarity + archetype + size + durability), integrated as Model 6 in dfs_players.py (249 players adjusted, capped at +/- 3.0 FP).
- **Feb 23:** Captured 10 high-res PIRTDICA chart screenshots using Playwright + nix Chromium.
- **Feb 23:** Fixed defensive scheme chart rendering (PLAY_TYPE_ORDER variable hoisting bug).
- **Feb 22:** Published first X article with 4 data-backed player picks.
- **Feb 22:** Ran daily pipeline: 389 players, 359 archetypes, 327 DFS projections, 33 prop recommendations.
