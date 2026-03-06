# PIRTDICA SPORTS CO. - NBA DFS Platform

## Overview
PIRTDICA SPORTS CO. offers an NBA Daily Fantasy Sports (DFS) platform focused on accurate player projections through advanced analytics like sophisticated player archetype classification and salary-tier volatility modeling. Its core purpose is to provide a skill-based esports competition platform, moving beyond traditional gambling. Key features include a "Beat the House" game using Monte Carlo simulation, competitive play against AI, and a dual-currency economy (Coach Coin for engagement, Coach Cash for competition) designed to boost user retention and build community. The platform aims to establish a new standard for competitive fantasy sports with a business vision to capture market share in competitive fantasy sports and esports.

## User Preferences
Preferred communication style: Simple, everyday language.
Auto-push to GitHub: Always push changes to GitHub at the end of every task using Replit's GitHub connector OAuth token.
**Daily auto-push**: `run_daily_update.py` automatically commits and pushes pipeline data (CSVs, chart images) to GitHub after each run, keeping the live site current. Standalone `push_to_github.py` can also be run manually.
**Do NOT push to GitHub**: `articles/` directory and conversation logs. These are local-only and must never be committed or pushed.

## System Architecture
The system utilizes an ETL pattern, staging data in SQLite and storing operational data in PostgreSQL. Core projection models include minutes projection and usage-based FPPM adjustment. Advanced analytics are driven by Phillips Archetype Classification v2 (8 composite indices, minutes-weighted K-Means, soft clustering) and a Salary-Tier Volatility Model. A Ceiling/Floor Model generates projection distributions, while Blended DVP and DVA systems provide dynamic matchup ratings. A Team Incentive Score adjusts volatility, and a Prop Trend Analysis Modal offers OVER/UNDER calls.

The Context Engine v2 (Matchup Interaction Layer) dynamically adjusts projections using interaction-probability-weighted physical mismatch, matchup familiarity, archetype effects, and opponent durability. This layer models possession-level physical confrontation probability through statistical structure.

NBA.com API resilience uses a **circuit breaker pattern** (`utils/nba_api_helpers.py`) with cached data fallback to reduce outage time. **Basketball Reference fallback** (`scrape_bref_gamelogs.py`) is implemented when NBA.com game logs fail, incrementally scraping box scores. **ESPN injury indicators** (`scrape_depth_charts.py`, `scrape_espn_injuries.py`) capture `(IL)` markers from depth charts plus OUT/QUESTIONABLE/DAY-TO-DAY statuses from the injuries page, merging both sources into `injury_alerts`. Rotation detection (`detect_rotation_changes.py`) also reads depth chart `injury_indicator` column directly as a fallback.

Lineup optimization is achieved using PuLP for linear programming and a Monte Carlo optimizer. The platform features a "Beat the House" game against AI, and "Coach vs Coach" (H2H) competitive play with a lobby, coin escrow, and live scoring.

The web platform is built with FastAPI (Python) for the backend, SQLAlchemy for ORM, and Jinja2 templates with custom CSS for the frontend, featuring live scoring, contest history, and admin controls. The dual-currency system (Coach Coin, Coach Cash) supports a Play-to-Earn (P2E) model focused on Identity, Prestige, Access, and Analytics, strictly avoiding pay-to-win mechanics. Ranked modes include free Coin Mode and competitive Cash Mode, structured with a tiered division system, hidden MMR, and seasonal resets. Monetization relies on a small rake on Coach Cash competitions, cosmetic sales, and future subscriptions.

The notification system (`backend/notifications.py`, `backend/events.py`) provides in-app notifications with 4 priority levels, category filtering (competitive/financial/system), and a bell icon dropdown in the navbar. Event emitters are wired into contest scoring (`score_contest.py`), H2H settlement (`backend/main.py`), and registration. The email system (`backend/email_service.py`, `backend/email_templates.py`) uses Resend API with a queue, rate limiting (max 2/day), and inactive-user-only delivery. Tables: `notifications` (with dedup constraint, user+read index), `email_queue`. RLS enabled on Supabase (no anon access). API endpoints: `GET /api/notifications`, `GET /api/notifications/unread-count`, `POST /api/notifications/{id}/read`, `POST /api/notifications/read-all`.

The player archetype system includes 11 total archetypes, classified using the Phillips Archetype Classification v2 system. This system compresses raw player stats into 8 orthogonal composite indices (Creation, Playmaking, Interior, Perimeter, Off-Ball, Rebound, Defense, Size), then clusters on those axes using minutes-weighted K-Means with soft probability assignments. Post-clustering domain logic refines these classifications based on shot-zone data, facilitating roles, and position.

The projection philosophy, "Minimal Viable Elite (MVE)", focuses on capturing 85-90% of predictive signal with 30-40% of the complexity, using a Three-Layer Rule for feature inclusion. A chart screenshot infrastructure exists via `/chart-screenshot/{chart_type}/{target}` using synchronous XHR + raw Canvas API for immediate rendering, with Playwright + nix Chromium automating batch screenshot capture.

The **Stat-Specific Projection Engine** (`analysis/player_value.py`) generates projected values for each stat type (PTS, REB, AST, STL, BLK) using dedicated projection functions that incorporate ALL available data sources: recency-weighted averages (60% last-10 / 40% season), pace factor, game total factor, DVA/DVP blended edges, shot zone vs team defense alignment, shot creation self-sufficiency, physical measurements (height/weight/wingspan mismatches), hustle stats (deflections, box-outs, contested shots), archetype composite indices, opponent play type vulnerabilities, matchup history, teammate injury usage redistribution, and blowout minutes caps. Each projection returns a value and a list of named factors with magnitudes.

The **Composite Score** ranks prop recommendations by combining: edge size (30%), DVA strength (20%), game environment quality (20%), consistency/CV (15%), and trend/hit-rate alignment (15%). Props sort by composite score descending. The **Prop Confidence Filter** still runs as metadata (hit_rate, cv, last5_avg) with HIGH/LOW labels but no longer gates recommendations.

New CSV columns: `projected_value`, `composite_score`, `pace_factor`, `total_factor`, `physical_edge`, `usage_boost`, `projection_factors` (semicolon-separated key factors). The web UI (`templates/trends.html`) displays composite score badges (color-coded high/mid/low), projected values, and projection factor subtitles.

Avatar & Identity Design Direction follows a "Strategic Minimalism meets Editorial Sports Design" style with a "Nike campaign meets tech startup meets esports broadcast" vibe. Design rules include a white background, black primary vector, 3-4px line weight, single accent color, circular 1:1 badge format, no gradients, and a specific accent palette. Cosmetics must communicate Skill, Status, or Story, and critically, must never mimic rank colors, division badge shapes, or champion aesthetics.

## Daily Articles

### Article Format
Articles target sportsbook bettors with prop bet analysis. Format: slate overview paragraph (games, totals, spreads, injury context), then 4 deep-dive player sections, then "THE BIGGER PICTURE" closing section. Each player section follows this structure:
- **Headline**: PLAYER NAME: Descriptive Edge Title
- **Subtitle line**: AWAY @ HOME | Position | Archetype
- **Body paragraphs** (3-5 per player): Per-game averages, per-100 possession rates, shot creation profile (catch-and-shoot %, pull-up %, paint %), shot zone distribution (restricted area, paint, mid, three), DVA matchup data with specific FPPM differentials and sample sizes, hustle stats (deflections/48, contested shots/48, box-outs/48), physical measurements, injury context for teammates and opponents, book lines with juice, game environment (total, spread, line weight).
- **Closing line**: "The stat to watch:" — specific prop recommendation with structural reasoning.
- Sections separated by `---` dividers.
- Tone: analytical, not hype. Let the data speak. No exclamation marks in analysis. Competitive framing throughout.
- These are prop bet targets, NOT fantasy lineup recommendations. State this explicitly in the intro.
- Files saved to `articles/` directory (local-only, never pushed to GitHub).

### Article Header Images
Style: **Gustave Doré-inspired black and white engravings**. Gothic cathedral interiors with soaring arches and vaulted ceilings. Basketball hoops integrated into stone columns like sacred relics. A cloaked scholar figure studying at a wooden desk with scrolls, books, and candlelight. Silhouettes of basketball players moving through the cathedral nave. Dramatic rays of light streaming from rose windows. Detailed crosshatching technique, fine line engraving, black ink on white paper. Renaissance woodcut aesthetic meets basketball arena. No text, no logos, no color. Aspect ratio 16:9. Files saved to `articles/` as `{date}_header.png`.

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
- **Supabase:** Used for syncing platform tables from local PostgreSQL, with RLS enabled on all 40 public tables.

### Python Libraries
- `requests`, `BeautifulSoup`: Web scraping
- `pandas`, `numpy`: Data manipulation
- `sqlite3`: SQLite interaction
- `PuLP`: Linear programming
- `scikit-learn`: K-means clustering
- `nba_api`: NBA.com stats API wrapper
- `playwright`: Headless browser for chart screenshot capture