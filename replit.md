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

## System Architecture
The system employs an ETL pattern, primarily using SQLite for data staging and PostgreSQL for the web platform's operational data. Core projection models include minutes projection and usage-based FPPM adjustment, supported by advanced analytics such as Phillips Archetype Classification (K-means clustering) and a Salary-Tier Volatility Model. A Ceiling/Floor Model converts point projections into full distributions, and Blended DVP (Defense vs. Position) and Defense vs. Archetype (DVA) systems provide dynamic matchup ratings. A Team Incentive Score adjusts volatility based on team standings, and a Prop Trend Analysis Modal offers OVER/UNDER calls.

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
-   **NBA.com Stats API:** Player game logs, minutes volatility, referee assignments
-   **NBAStuffer:** Historical referee statistics
-   **SportsDatabase:** Historic betting lines
-   **FantasyTeamAdvice.com:** FanDuel NBA ownership data

### APIs
-   **The Odds API:** Player prop lines (FanDuel)
-   **plaintextsports.com/nba:** Live scoring data

### Databases
-   **SQLite:** Staging database (`dfs_nba.db`)
-   **PostgreSQL:** Production database for web platform data

### Python Libraries
-   `requests`, `BeautifulSoup`: Web scraping
-   `pandas`: Data manipulation
-   `sqlite3`: SQLite interaction
-   `PuLP`: Linear programming
-   `scikit-learn`: K-means clustering
-   `Chart.js`: Frontend charting