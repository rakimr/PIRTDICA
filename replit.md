# PIRTDICA SPORTS CO. - NBA DFS Platform

## Overview
PIRTDICA SPORTS CO. offers an NBA Daily Fantasy Sports (DFS) platform focused on accurate player projections through advanced analytics like sophisticated player archetype classification and and salary-tier volatility modeling. Its core purpose is to provide a skill-based esports competition platform, moving beyond traditional gambling. Key features include a "Beat the House" game using Monte Carlo simulation, competitive play against AI, and a dual-currency economy (Coach Coin for engagement, Coach Cash for competition) designed to boost user retention and build community. The platform aims to establish a new standard for competitive fantasy sports with a business vision to capture market share in competitive fantasy sports and esports.

## User Preferences
Preferred communication style: Simple, everyday language.
Auto-push to GitHub: Always push changes to GitHub at the end of every task using Replit's GitHub connector OAuth token.
**Daily auto-push**: `run_daily_update.py` automatically commits and pushes pipeline data (CSVs, chart images) to GitHub after each run, keeping the live site current. Standalone `push_to_github.py` can also be run manually.
**Do NOT push to GitHub**: `articles/` directory and conversation logs. These are local-only and must never be committed or pushed.

## System Architecture
The system utilizes an ETL pattern, staging data in SQLite and storing operational data in PostgreSQL. Core projection models include minutes projection and usage-based FPPM adjustment. Advanced analytics are driven by Phillips Archetype Classification v2 (8 composite indices, minutes-weighted K-Means, soft clustering) and a Salary-Tier Volatility Model. A Ceiling/Floor Model generates projection distributions, while Blended DVP and DVA systems provide dynamic matchup ratings. A Team Incentive Score adjusts volatility, and a Prop Trend Analysis Modal offers OVER/UNDER calls.

The Context Engine v2 (Matchup Interaction Layer) dynamically adjusts projections using interaction-probability-weighted physical mismatch, matchup familiarity, archetype effects, and opponent durability. This layer models possession-level physical confrontation probability through statistical structure.

Team resolution handles "2TM"/"3TM" players from Basketball Reference by mapping them to current teams using ESPN depth chart data and normalized name matching. NBA.com API resilience uses a circuit breaker pattern with cached data fallback. Basketball Reference fallback is implemented for game logs when NBA.com fails. ESPN injury indicators are captured from depth charts and the injuries page, merging both sources. Rotation detection also reads depth chart injury indicators.

Lineup optimization is achieved using PuLP for linear programming and a Monte Carlo optimizer. The platform features a "Beat the House" game against AI, and "Coach vs Coach" (H2H) competitive play with a lobby, coin escrow, and live scoring.

The web platform is built with FastAPI (Python) for the backend, SQLAlchemy for ORM, and Jinja2 templates with custom CSS for the frontend, featuring live scoring, contest history, and admin controls. The dual-currency system (Coach Coin, Coach Cash) supports a Play-to-Earn (P2E) model focused on Identity, Prestige, Access, and Analytics, strictly avoiding pay-to-win mechanics. Ranked modes include free Coin Mode and competitive Cash Mode, structured with a tiered division system, hidden MMR, and seasonal resets. Monetization relies on a small rake on Coach Cash competitions, cosmetic sales, and future subscriptions.

The notification system provides in-app notifications with 4 priority levels, category filtering, and a bell icon dropdown. Event emitters are wired into contest scoring, H2H settlement, and registration. The email system uses Resend API with a queue, rate limiting, and inactive-user-only delivery. RLS is enabled on Supabase.

The player archetype system includes 11 total archetypes, classified using the Phillips Archetype Classification v2 system. This system compresses raw player stats into 8 orthogonal composite indices, then clusters on those axes using minutes-weighted K-Means with soft probability assignments. Post-clustering domain logic refines these classifications based on shot-zone data, facilitating roles, and position.

The projection philosophy, "Minimal Viable Elite (MVE)", focuses on capturing 85-90% of predictive signal with 30-40% of the complexity, using a Three-Layer Rule for feature inclusion. A chart screenshot infrastructure exists via `/chart-screenshot/{chart_type}/{target}` using synchronous XHR + raw Canvas API for immediate rendering, with Playwright + nix Chromium automating batch screenshot capture.

Article header images are generated via `generate_header.py` using Pillow (pure Python, no browser). It reads HIGH confidence picks from `prop_recommendations.csv`, fetches circular ESPN CDN headshots, and renders the white-background centered layout at 800px wide. Righteous font is downloaded to `/tmp/Righteous-Regular.ttf` from GitHub each session. ESPN team IDs must use the confirmed map (LAL=13, MIA=14, NY=18, PHI=20, etc.). Run: `python generate_header.py [YYYY-MM-DD]`.

The Stat-Specific Projection Engine generates projected values for each stat type (PTS, REB, AST, STL, BLK) using dedicated projection functions that incorporate ALL available data sources: recency-weighted averages, pace factor, game total factor, DVA/DVP blended edges, shot zone vs team defense alignment, shot creation self-sufficiency, physical measurements, hustle stats, archetype composite indices, opponent play type vulnerabilities, matchup history, teammate injury usage redistribution, and probabilistic minutes modeling. Each projection returns a value and a list of named factors with magnitudes.

The Probabilistic Minutes Model replaces the old deterministic minutes approach with an empirically-calibrated distribution-aware system. It uses percentile and volatility models based on game log availability, and includes a blowout decay curve.

The Opportunity-Based Rebound Model replaces the old additive rebound projection with a two-stage multiplicative architecture. Stage 1 predicts total game rebound opportunities (Rebound Environment Score RES) using opponent miss rate, 3PA rate, pace, shot distance, and game spread. Stage 2 predicts player rebound share using TRB% proxy, adjusted by frontcourt redistribution from injured teammates, Size Advantage Score, rebound type fit, box-out rate, contested rebound rate, and DVA/DVP matchup blend. The final projection uses a 55/45 anchor blend with the opponent model and season average.

The Composite Score ranks prop recommendations by combining: edge size (30%), DVA strength (20%), game environment quality (20%), consistency/CV (15%), and trend/hit-rate alignment (15%). The Prop Confidence Filter provides metadata (hit_rate, cv, last5_avg) with HIGH/LOW labels.

The web UI displays composite score badges, projected values, and projection factor subtitles.

Avatar & Identity Design Direction follows a "Strategic Minimalism meets Editorial Sports Design" style with a "Nike campaign meets tech startup meets esports broadcast" vibe. Design rules include a white background, black primary vector, 3-4px line weight, single accent color, circular 1:1 badge format, no gradients, and a specific accent palette. Cosmetics must communicate Skill, Status, or Story, and must never mimic rank colors, division badge shapes, or champion aesthetics.

## Daily Chat Output Formats

**Grading Report Table** (used after games are final):
`Hit or Miss | Player | Stat | Book | Pirtdica Projection | Actual | ProjMin | ActMin`
Use ✓ for hit, ✗ for miss, — for DNP. Record shown below table as X/Y (Z%).

**HIGH Confidence Picks Table** (pre-game):
`# | Player | Game | Stat | Player Avg | Line | Projected | Edge | Pick`
Edge shown as +X.X% (OVER) or -X.X% (UNDER).

**Props Table** (all props with lines, sorted by edge):
`Player | Stat | Call | Line | Proj | Edge | Conf | Hit% | Score`

**Analysis Output** (pre-game narrative):
Slate overview paragraph → player-by-player breakdown (matchup/archetype/DVA/factors/last 5/stat to watch) → 5-day HIGH trend.

**Grading Report** (post-game full output):
Header: "Key Takeaways & Grading Report (Mar DD) 🏀"
Then the grading table (Hit or Miss | Player | Stat | Book | Pirtdica Projection | Actual | ProjMin | ActMin).
Then narrative paragraphs: lead with the best/cleanest win, group hits together, then address each miss individually with explanation (blowout, shooting collapse, trend miss, etc). End record shown below table as X/Y (Z%).

Daily Articles target sportsbook bettors with prop bet analysis. Article format includes a slate overview, four deep-dive player sections, and a "THE BIGGER PICTURE" closing section. Each player section provides detailed analytical breakdowns. The tone is analytical, data-driven, and competitive, explicitly stating that these are prop bet targets, not fantasy lineup recommendations. Article files are saved locally.

Article Header Images are in a Gustave Doré-inspired black and white engraving style, featuring Gothic cathedral interiors with integrated basketball elements and cloaked scholar figures. No text, no logos, no color, 16:9 aspect ratio.

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
- **Resend API:** Email service

### Databases
- **SQLite:** Staging database (`dfs_nba.db`) for local pipeline scraping.
- **PostgreSQL:** Production database for web platform and pipeline output.
- **Supabase:** Used for syncing platform tables from local PostgreSQL, with RLS enabled.

### Python Libraries
- `requests`, `BeautifulSoup`: Web scraping
- `pandas`, `numpy`: Data manipulation
- `sqlite3`: SQLite interaction
- `PuLP`: Linear programming
- `scikit-learn`: K-means clustering
- `nba_api`: NBA.com stats API wrapper
- `playwright`: Headless browser for chart screenshot capture