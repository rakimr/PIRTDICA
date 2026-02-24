# PIRTDICA SPORTS CO. - NBA DFS Platform

## Overview
PIRTDICA SPORTS CO. offers an NBA Daily Fantasy Sports (DFS) platform focused on accurate player projections through advanced analytics like sophisticated player archetype classification and salary-tier volatility modeling. Its core purpose is to provide a skill-based esports competition platform, moving beyond traditional gambling. Key features include a "Beat the House" game using Monte Carlo simulation, competitive play against AI, and a dual-currency economy (Coach Coin for engagement, Coach Cash for competition) designed to boost user retention and build community. The platform aims to establish a new standard for competitive fantasy sports with a business vision to capture market share in competitive fantasy sports and esports.

## User Preferences
Preferred communication style: Simple, everyday language.
Auto-push to GitHub: Always push changes to GitHub at the end of every task using Replit's GitHub connector OAuth token.

### Article Writing Guidelines (PIRTDICA Daily Picks)
- **NO fantasy salary** ($7,300, etc.), **NO fantasy points projections** (proj_fp, ceiling, floor), **NO value ratios**, **NO FanDuel/DraftKings lineup talk**.
- Focus on **prop bets**: points over/under, rebounds, assists, steals, blocks, threes. Reference actual book lines when available.
- Use **per game averages** and **per 100 possession rates** to establish baseline production.
- Use **shot chart data** (rim/paint %, three %, pull-up vs catch-and-shoot) to explain HOW a player scores.
- Use **DVP/DVA archetype matchup data** to explain WHY this specific matchup is exploitable (e.g., "+0.088 points per minute to Traditional Bigs").
- Include **teammate context** (who creates shots, who spaces the floor, how the offense functions around this player).
- Each player section ends with **"The stat to watch:"** giving a specific, actionable prop recommendation.
- Close with **"THE BIGGER PICTURE"** section tying the picks together thematically.
- Format: `PLAYER NAME: Stat Edge vs Team` header, `TEAM vs TEAM | Position | Archetype` subheader.
- Tone: Analytical, direct, no hype. Data-driven but readable. Written like you are explaining an edge to a sharp bettor.
- Article header images: Gustave Doré engraving style mixed with Dan Koe minimalist editorial. Monochromatic black/white with subtle gold accents. 16:9 aspect ratio. Each day should have a unique visual theme (no repeats).

## System Architecture
The system utilizes an ETL pattern, staging data in SQLite and storing operational data in PostgreSQL. Core projection models include minutes projection and usage-based FPPM adjustment. Advanced analytics are driven by Phillips Archetype Classification (K-means clustering with 18 features) and a Salary-Tier Volatility Model. A Ceiling/Floor Model generates projection distributions, while Blended DVP and DVA systems provide dynamic matchup ratings. A Team Incentive Score adjusts volatility, and a Prop Trend Analysis Modal offers OVER/UNDER calls.

The Context Engine v2 (Matchup Interaction Layer) dynamically adjusts projections using interaction-probability-weighted physical mismatch (size, weight, wingspan), matchup familiarity, archetype effects, and opponent durability. This layer models possession-level physical confrontation probability through statistical structure, addressing issues like "The Clingan vs Williams Law" by weighting interactions rather than averaging roster-wide effects. The Context Engine v2 uses a 5-layer architecture:
1. **Interaction Probability Matrix**: Computes weights based on position, minutes overlap, and role interaction.
2. **Interaction-Weighted Size Impact**: Adjusts for size differences (height, weight, wingspan) gated by interior usage.
3. **Interaction-Weighted Archetype Matchup**: Applies archetype-vs-archetype FPPM differentials.
4. **Familiarity Effect**: Incorporates player-vs-team FPPM differentials.
5. **Bidirectional Durability**: Adjusts for opponent stability.

Lineup optimization is achieved using PuLP for linear programming and a Monte Carlo optimizer. The platform features a "Beat the House" game against AI, and "Coach vs Coach" (H2H) competitive play with a lobby, coin escrow, and live scoring.

The web platform is built with FastAPI (Python) for the backend, SQLAlchemy for ORM, and Jinja2 templates with custom CSS for the frontend, featuring live scoring, contest history, and admin controls. The dual-currency system (Coach Coin, Coach Cash) supports a Play-to-Earn (P2E) model focused on Identity, Prestige, Access, and Analytics, strictly avoiding pay-to-win mechanics. Ranked modes include free Coin Mode and competitive Cash Mode, structured with a tiered division system, hidden MMR, and seasonal resets. Monetization relies on a small rake on Coach Cash competitions, cosmetic sales, and future subscriptions.

The player archetype system includes 10 total archetypes: 5 specialist (Playmaker, 3-and-D Wing, Scoring Wing, Versatile Big, Traditional Big) and 5 hybrid branch categories (Combo Guard, Stretch 4, Stretch 5, Point Center, Point Forward). Classification involves K-means clustering, shot-zone reclassification, and specific detection for hybrid roles.

The projection philosophy, "Minimal Viable Elite (MVE)", focuses on capturing 85-90% of predictive signal with 30-40% of the complexity. It uses a Three-Layer Rule for feature inclusion: Layer A (Game Environment), Layer B (Player Role Engine), and Layer C (Interaction Context). New features must improve cross-validated RMSE, backtested ROI, and not excessively increase variance.

A chart screenshot infrastructure exists via `/chart-screenshot/{chart_type}/{target}` using synchronous XHR + raw Canvas API for immediate rendering, with Playwright + nix Chromium automating batch screenshot capture.

Avatar & Identity Design Direction follows a "Strategic Minimalism meets Editorial Sports Design" style with a "Nike campaign meets tech startup meets esports broadcast" vibe. Design rules include a white background, black primary vector, 3-4px line weight, single accent color (unless legendary tier), circular 1:1 badge format, no gradients, and a specific accent palette. The Coach Shop framework dictates that cosmetics must communicate Skill, Status, or Story, and critically, must never mimic rank colors, division badge shapes, or champion aesthetics to maintain the integrity of skill-based competition.

## External Dependencies

### Web Scraping Targets
- **ESPN:** Depth charts
- **RotoGrinders, FantasyPros:** Player lineups, injury alerts, FanDuel salaries
- **TeamRankings:** Game odds/spreads
- **HashtagBasketball:** Defense vs Position stats
- **Basketball Reference:** Per-100 possession stats, player positions, foul rates, player physical measurements, historical game logs.
- **NBA.com Stats API:** Player game logs, minutes volatility, referee assignments, shot zone distribution, shot creation types, hustle stats.
- **NBAStuffer:** Historical referee statistics
- **SportsDatabase:** Historic betting lines
- **FantasyTeamAdvice.com:** FanDuel NBA ownership data

### APIs
- **The Odds API:** Player prop lines (FanDuel)
- **plaintextsports.com/nba:** Live scoring data

### Databases
- **SQLite:** Staging database (`dfs_nba.db`) for local pipeline scraping.
- **PostgreSQL:** Production database for web platform and pipeline output, including `player_measurements_live`, `matchup_history_live`, `archetype_matchup_profiles_live`, and various other `*_live` tables.
- **Supabase:** Used for syncing platform tables from local PostgreSQL.

### Python Libraries
- `requests`, `BeautifulSoup`: Web scraping
- `pandas`, `numpy`: Data manipulation
- `sqlite3`: SQLite interaction
- `PuLP`: Linear programming
- `scikit-learn`: K-means clustering
- `nba_api`: NBA.com stats API wrapper
- `playwright`: Headless browser for chart screenshot capture