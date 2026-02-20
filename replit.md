# PIRTDICA SPORTS CO. - NBA DFS Platform

## Overview
PIRTDICA SPORTS CO. offers an NBA Daily Fantasy Sports (DFS) platform featuring player projections, advanced analytics, and a unique "Beat the House" game. The platform aims to provide accurate projections through sophisticated player archetype classification and salary-tier volatility modeling. It facilitates competitive play against an AI-generated lineup using Monte Carlo simulation and a dual-currency economy (Coach Coin for engagement, Coach Cash for competition) to enhance user retention and community building. The project vision is to establish a skill-based esports competition platform, moving beyond traditional gambling models.

## User Preferences
Preferred communication style: Simple, everyday language.

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
These are players who break traditional position boundaries by blending skills from multiple roles, informed by the evolution toward positionless basketball.

- **Combo Guard:** Blends PG/SG roles — elite scorers who also facilitate. Transcendent guards (PTS/100 >= 30, AST >= 7, USG >= 26) route here. Examples: SGA, Curry, Harden, Brunson, Donovan Mitchell.
- **Stretch 4:** PF who shoots like a guard — high 3PT% with catch-and-shoot emphasis, C% < 50. Examples: Lauri Markkanen, Jabari Smith, Jaren Jackson Jr.
- **Stretch 5:** Center who shoots like a guard — high 3PT% with catch-and-shoot emphasis, C% >= 50. Examples: Wembanyama, KAT, Myles Turner, Brook Lopez, Chet Holmgren.
- **Point Center:** Center who facilitates like a PG — requires AST/100 >= 5.0, PTS/100 >= 24.0, C% >= 50%. Examples: Jokic, Embiid, Sabonis, Sengun.
- **Point Forward:** Forward who runs the offense — requires AST/100 >= 5.0 (or 7.5 for transcendent forwards), C% < 50%. Examples: Giannis, LeBron, Randle, Banchero, Wagner.

**Wing escape rule:** Players with C% < 10 + pull-up% >= 20 + REB < 8 stay in their wing/guard archetype even if PF% is high (prevents Tobias Harris, Dillon Brooks misclassification).

**Height threshold for big classification:** 6'10" (82 inches) — players must be 6'10"+ with significant frontcourt minutes (C%+PF% >= 40%) to be reclassified as bigs.

**Ball initiation gate:** Point Forward and Point Center require touches_per_min >= 2.0 (NBA tracking data). This prevents non-initiators like Miles Bridges from being misclassified as playmakers. Data sourced from NBA.com leaguedashptstats (Possessions) endpoint.

**Classification pipeline:** K-means clustering (k=6, 18+corner3 features) → shot-zone reclassification → wing escape → facilitating big detection (with ball initiation gate) → height-based correction → hybrid branch routing → playmaker reclassification → Stretch 4/5 split

## System Architecture
The system employs an ETL pattern, primarily using SQLite for data staging and PostgreSQL for the web platform's operational data. Core projection models include minutes projection and usage-based FPPM adjustment, supported by advanced analytics such as Phillips Archetype Classification (K-means clustering with 18 features: per-100 stats, position %, shot zones, shot creation, and defensive hustle stats) and a Salary-Tier Volatility Model. A Ceiling/Floor Model converts point projections into full distributions, and Blended DVP (Defense vs. Position) and Defense vs. Archetype (DVA) systems provide dynamic matchup ratings. A Team Incentive Score adjusts volatility based on team standings, and a Prop Trend Analysis Modal offers OVER/UNDER calls.

Lineup optimization is achieved via a PuLP linear programming optimizer and a Monte Carlo optimizer. The platform supports a "Beat the House" game against AI and "Coach vs Coach" (H2H) competitive play with a lobby, coin escrow, and live scoring.

The web platform is built with FastAPI (Python) for the backend, SQLAlchemy for ORM, and Jinja2 templates with custom CSS for the frontend. It features live scoring, contest history, and an admin control panel. The dual-currency system (Coach Coin for cosmetics/access, Coach Cash for competitive play) supports a Play-to-Earn (P2E) model focused on Identity, Prestige, Access, and Analytics, without enabling pay-to-win mechanics. Coach Cash can be converted to Coach Coin (1:5), but not vice-versa. Ranked modes include a free Coin Mode and a competitive Cash Mode. The esports competitive architecture emphasizes skill-based competition, utilizing a tiered division system with hidden MMR (Modified ELO) and seasonal resets. Match formats include async H2H, scheduled match nights, and a daily casual slate mode. Monetization focuses on a small rake on Coach Cash competitions, cosmetic sales, and future subscriptions/battle passes.

## External Dependencies

### Web Scraping Targets
-   **ESPN:** Depth charts
-   **RotoGrinders:** Player lineups, injury alerts, FanDuel salaries
-   **FantasyPros:** FanDuel salary fallback
-   **TeamRankings:** Game odds/spreads
-   **HashtagBasketball:** Defense vs Position stats
-   **Basketball Reference:** Per-100 possession stats, player positions, foul rates
-   **NBA.com Stats API:** Player game logs, minutes volatility, referee assignments, shot zone distribution, shot creation types, hustle stats (deflections, contested shots, loose balls)
-   **NBAStuffer:** Historical referee statistics
-   **SportsDatabase:** Historic betting lines
-   **FantasyTeamAdvice.com:** FanDuel NBA ownership data

### APIs
-   **The Odds API:** Player prop lines (FanDuel)
-   **plaintextsports.com/nba:** Live scoring data

### Databases
-   **SQLite:** Staging database (`dfs_nba.db`) — used locally during pipeline scraping
-   **PostgreSQL:** Production database for web platform data AND pipeline output
-   **Data Sync:** `sync_to_postgres.py` runs at the end of the daily pipeline to copy all scraped data from SQLite/CSV into PostgreSQL `*_live` tables. This enables stateless deployments on Render.
-   **Data Access Layer:** `backend/data_access.py` provides unified data access that checks PostgreSQL first, falls back to CSV/SQLite (for local dev). All endpoints in `main.py` use this layer.
-   **Pipeline Tables:** `dfs_players_live`, `prop_recommendations_live`, `targeted_plays_live`, `ownership_projections_live`, `player_salaries_live`, `injury_alerts_live`, `player_archetypes_live`, `player_per100_live`, `player_positions_live`, `player_stats_live`, `player_game_logs_live`, `player_shot_zones_live`, `player_shot_creation_live`, `player_hustle_stats_live`, `dva_stats_live`, `archetype_profiles_live`, `team_defense_shot_zones_live`, `team_play_types_live`, `player_headshots_live`

### Username Moderation
-   `backend/profanity_filter.py`: Profanity/slur filter with leet-speak detection (0→o, 1→i, 3→e, @→a, etc.)
-   Registration blocks offensive usernames with clear error messages
-   Login blocks banned accounts (`is_banned` column on users table)
-   Admin panel has "Username Moderation" section: scans all usernames + display names, offers force-rename and ban actions
-   No display name update endpoint exists yet — when added, must integrate `check_username()` from profanity_filter

### Python Libraries
-   `requests`, `BeautifulSoup`: Web scraping
-   `pandas`: Data manipulation
-   `sqlite3`: SQLite interaction
-   `PuLP`: Linear programming
-   `scikit-learn`: K-means clustering
-   `Chart.js`: Frontend charting