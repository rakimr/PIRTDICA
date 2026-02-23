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

## System Architecture
The system utilizes an ETL pattern, staging data in SQLite and storing operational data in PostgreSQL. Core projection models include minutes projection and usage-based FPPM adjustment. Advanced analytics are driven by Phillips Archetype Classification (K-means clustering with 18 features) and a Salary-Tier Volatility Model. A Ceiling/Floor Model generates projection distributions, while Blended DVP and DVA systems provide dynamic matchup ratings. A Team Incentive Score adjusts volatility, and a Prop Trend Analysis Modal offers OVER/UNDER calls.

A key enhancement is the Matchup Interaction Layer, which dynamically adjusts projections based on physical mismatch (size, weight, wingspan), matchup familiarity, and opponent durability, learned via regression against historical actuals. This layer incorporates historical game logs and physical measurements to refine predictions beyond traditional DVP.

Lineup optimization is achieved using PuLP for linear programming and a Monte Carlo optimizer. The platform features a "Beat the House" game against AI, and "Coach vs Coach" (H2H) competitive play with a lobby, coin escrow, and live scoring.

The web platform is built with FastAPI (Python) for the backend, SQLAlchemy for ORM, and Jinja2 templates with custom CSS for the frontend, featuring live scoring, contest history, and admin controls. The dual-currency system (Coach Coin, Coach Cash) supports a Play-to-Earn (P2E) model focused on Identity, Prestige, Access, and Analytics, strictly avoiding pay-to-win mechanics. Ranked modes include free Coin Mode and competitive Cash Mode, structured with a tiered division system, hidden MMR, and seasonal resets. Monetization relies on a small rake on Coach Cash competitions, cosmetic sales, and future subscriptions.

A chart screenshot infrastructure renders PIRTDICA charts via a temporary route (`/chart-screenshot/{chart_type}/{target}`) using synchronous XHR and raw Canvas API. Playwright with Nix Chromium automates batch screenshot capture for embedding in articles.

## External Dependencies

### Web Scraping Targets
-   **ESPN:** Depth charts
-   **RotoGrinders, FantasyPros:** Player lineups, injury alerts, FanDuel salaries
-   **TeamRankings:** Game odds/spreads
-   **HashtagBasketball:** Defense vs Position stats
-   **Basketball Reference:** Per-100 possession stats, player positions, foul rates, player physical measurements
-   **NBA.com Stats API:** Player game logs, minutes volatility, referee assignments, shot zone distribution, shot creation types, hustle stats, historical game logs (multi-season), Draft Combine for wingspan.
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

### Python Libraries
-   `requests`, `BeautifulSoup`: Web scraping
-   `pandas`: Data manipulation
-   `sqlite3`: SQLite interaction
-   `PuLP`: Linear programming
-   `scikit-learn`: K-means clustering
-   `playwright`: Headless browser for chart screenshot capture