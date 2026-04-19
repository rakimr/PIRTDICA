# PIRTDICA SPORTS CO. - NBA DFS Platform

## Overview
PIRTDICA SPORTS CO. offers an NBA Daily Fantasy Sports (DFS) platform focused on accurate player projections through advanced analytics like sophisticated player archetype classification and salary-tier volatility modeling. Its core purpose is to provide a skill-based esports competition platform, moving beyond traditional gambling. Key features include a "Beat the House" game using Monte Carlo simulation, competitive play against AI, and a single-currency economy (Coach Coin) earned through gameplay. The platform aims to establish a new standard for competitive fantasy sports with a business vision to capture market share in competitive fantasy sports and esports.

## User Preferences
Preferred communication style: Simple, everyday language.
Auto-push to GitHub: Always push changes to GitHub at the end of every task using Replit's GitHub connector OAuth token.
Daily auto-push: `run_daily_update.py` automatically commits and pushes pipeline data (CSVs, chart images) to GitHub after each run, keeping the live site current. Standalone `push_to_github.py` can also be run manually.
Render deploy hook: After every GitHub push, `push_to_github.py` triggers a Render deploy hook (`RENDER_DEPLOY_HOOK_URL` env var) to automatically redeploy the production site.
Pre-game refresh: `scheduler_pregame.py` runs as a persistent workflow, triggering `run_pregame_refresh.py` 60 minutes before the first NBA tipoff today. The earliest game time is read from SQLite `player_salaries.game_time` (same source as the home page countdown timer), parsed in ET, and the trigger is set to `first_game − 60min`. If no game times are available, falls back to 6:20 PM ET. A 180-minute grace window covers cases where the scheduler starts late (still triggers if within 3 hours of target). After running, sleeps until 8 AM ET the next day before reassessing — fires once per slate. The refresh re-scrapes injuries, depth charts, odds, props, rebuilds archetypes/projections, regenerates the article, and pushes to GitHub — ensuring data is fresh in time for early afternoon games (e.g., 1 PM ET tipoffs trigger at 12 PM).
Post-game pipeline: `scheduler_postgame.py` runs as a persistent workflow, triggering `run_daily_update.py` at 1:00 AM ET daily. This ensures next-day data (salaries, odds, projections, article) is ready by morning after late West Coast games end (~12:30 AM ET). Has a 2-hour timeout.
Do NOT push to GitHub: `articles/` directory and conversation logs. These are local-only and must never be committed or pushed.

## System Architecture
The system utilizes an ETL pattern, staging data in SQLite and storing operational data in PostgreSQL. Core projection models include minutes projection and usage-based FPPM adjustment. Advanced analytics are driven by Phillips Archetype Classification v2 (8 composite indices, minutes-weighted K-Means, soft clustering) and a Salary-Tier Volatility Model. A Ceiling/Floor Model generates projection distributions, while Blended DVP and DVA systems provide dynamic matchup ratings. A Team Incentive Score adjusts volatility, and a Prop Trend Analysis Modal offers OVER/UNDER calls. The Context Engine v2 dynamically adjusts projections using interaction-probability-weighted physical mismatch, matchup familiarity, archetype effects, and opponent durability.

Game date filtering uses The Odds API events to determine which games are actually today, cross-referencing TeamRankings odds. Team resolution handles "2TM"/"3TM" players by mapping them to current teams using ESPN depth chart data. NBA.com API resilience uses a circuit breaker pattern with cached data fallback, and Basketball Reference fallback is implemented for game logs. ESPN injury indicators are captured from depth charts and the injuries page. Injury status reconciliation uses ESPN as the baseline, with fresh same-day RotoGrinders alerts (via `alert-timestamp` span) overriding ESPN in either direction. Players marked "available" or "probable" by fresh RotoGrinders alerts suppress ESPN's OUT status. The most recent alert per player wins. All statuses reset at the start of each pipeline run. FanDuel slate cross-reference: if ESPN marks a player OUT but FanDuel lists them as Starter/Active on today's slate, the ESPN OUT is suppressed as a stale IL marker (FD-SUPPRESSED).

Lineup optimization is achieved using PuLP for linear programming and a Monte Carlo optimizer. The platform features a "Beat the House" game against AI, and "Coach vs Coach" (H2H) competitive play with a lobby, coin escrow, and live scoring.

The web platform is built with FastAPI (Python) for the backend, SQLAlchemy for ORM, and Jinja2 templates with custom CSS for the frontend. The single-currency system (Coach Coin) supports a Play-to-Earn (P2E) model focused on Identity, Prestige, Access, and Analytics, strictly avoiding pay-to-win mechanics. Monetization relies on a small rake on H2H competitions, cosmetic sales, and Stripe subscriptions.

Subscription tiers via Stripe include PIRTDICA Picks, PIRTDICA Stat Pack, and PIRTDICA Bundle. A multi-subscription architecture is in place with a `user_subscriptions` table. Stripe integration handles checkout, webhooks, and customer portals. Subscription activation triggers in-app notifications and emails via Resend.

The player archetype system includes 11 total archetypes, classified using the Phillips Archetype Classification v2, which compresses raw player stats into 8 orthogonal composite indices and clusters on those axes. The projection philosophy, "Minimal Viable Elite (MVE)", focuses on capturing 85-90% of predictive signal with 30-40% of the complexity.

A chart screenshot infrastructure exists via `/chart-screenshot/{chart_type}/{target}` using Playwright for batch capture. Article header images are generated via `generate_header.py` using Pillow. The Articles page (`/articles`) displays daily HIGH Confidence Props analysis, including a header image, picks table, and per-player analysis sections.

Article generation uses a three-tier system in `generate_article.py`:
1. **Claude Analyst mode (primary)**: Sends Claude the FULL slate data — every player prop line with projections, DVP/DVA edges, usage boosts, injury context, game environment, recent form, and archetypes. Claude independently analyzes the entire slate and selects 4-8 HIGH confidence picks, applying the March 12 gold-standard analytical framework (usage redistribution, DVP/DVA double alignment, game totals, last-5 clearing book lines, composite scores). Claude's picks are validated against the actual slate data to prevent hallucination — numeric fields (book_line, projected, edge, composite_score) are overwritten from source truth. Duplicates and picks not found in the slate are rejected. The `DailyArticle` model has a `claude_selected` boolean column to track this.
2. **Statistical model + Claude narrator (fallback)**: If Claude Analyst fails, falls back to the statistical model's HIGH confidence picks with Claude writing narratives for pre-selected picks.
3. **Template engine (final fallback)**: If Claude API is unavailable entirely, uses rule-based template engine for analysis text.

A Pick Grading Report (`grade_picks.py`) runs daily after `score_contest.py` in the post-game pipeline. It loads yesterday's article picks from `daily_articles.picks_json`, looks up actual stat values from SQLite `player_game_logs`, determines HIT/MISS/PUSH for each pick, and sends results to Claude for brief contextual analysis. Grades are stored in `daily_pick_grades` (PostgreSQL). The `/articles` page shows "PIRTDICA'S GRADING REPORT" below today's picks (subscribers only) with a running season W-L record (pushes excluded from hit rate). The `DailyPickGrade` model tracks: slate_date, player, stat, book_line, direction, projected, actual, hit (nullable boolean — null=PUSH), composite_score, and claude_analysis.

When no HIGH confidence picks exist, the article generator falls back to "best available" — selecting up to 6 LOW picks closest to HIGH (fewest gate failures, highest composite score). These are labeled "TOP PICKS OF THE DAY" in the header and article page. The `DailyArticle` model has a `best_available` boolean column to track this. Stale headers are cleaned up when picks change between pipeline runs.

The Stat-Specific Projection Engine generates projected values for each stat type (PTS, REB, AST, STL, BLK) using dedicated projection functions. A Role Change Detection system (`_detect_role_change`) identifies regime shifts, adjusting weighting for recent production (e.g., 80/20 last-5/season for role upgrades). The Probabilistic Minutes Model replaces deterministic approaches. The Opportunity-Based Rebound Model uses a two-stage multiplicative architecture. The Composite Score ranks prop recommendations by combining edge size, matchup alignment, game environment, consistency, and trend alignment.

The Usage Redistribution Model v2 replaces flat injury boosts with hierarchical, archetype-weighted redistribution. It tracks vacated usage, MPG, and archetypes of OUT players. Cascade tiers activate based on total vacated usage, and archetype similarity determines absorption. Minutes escalation distributes vacated minutes to remaining starters. An Opportunity Index calculates compounded impact, and an "Opportunity Spike" flag indicates significant role increases combined with positive matchups. The model prevents double-counting by disabling separate minute ratio multipliers when Opportunity Index is active.

Prop recommendations require an actual sportsbook book line from The Odds API — players without Odds API coverage are filtered out before article generation and Claude analysis. The Pre-Game Refresh at 6:20 PM ET force-refreshes props (`--force`) to capture lines posted later in the day (books typically post full props by 2-4 PM ET).

HIGH Confidence Prop Classification requires ALL gates to pass: hit rate >= 58%, CV <= 0.30 (tightened from 0.45), last-5 average must clear the book line in the pick direction, at least one of DVA/DVP must actively support the pick direction (>0.5 magnitude — neutral matchups are now blocked), implied team total must support direction (OVER blocked if team implied < 105 for PTS/AST, UNDER blocked if > 115), and UNDER picks with usage boost > 3.0 are blocked. Confidence reasons are always populated. All 5 stat projection functions (PTS, REB, AST, STL, BLK) enforce a 20% inflation cap — no projection can exceed base × 1.20 regardless of stacked adjustments.

Avatar & Identity Design Direction follows a "Strategic Minimalism meets Editorial Sports Design" style with specific design rules and an accent palette. Cosmetics communicate Skill, Status, or Story.

Cookie consent and analytics tracking is implemented via `PageView` and `CookieConsent` models in PostgreSQL. A fixed cookie banner appears for first-time visitors (before any `analytics_consent` cookie is set). Users can accept or decline analytics cookies. Only when accepted does the frontend send page view data to `/api/track`. IP addresses are hashed (SHA-256, truncated to 16 chars) for privacy. A `/cookie-settings` page lets users toggle analytics on/off at any time. The footer links to Cookie Settings on every page.

## Session Memory
Source of truth for historical narrative detail and session logs: `session_memory.md`. Always read both `replit.md` and `session_memory.md` for full context.
- March 12, 2026: Gold-standard 80% hit rate slate (6 book-line plays). Full narrative analysis and winning patterns in `session_memory.md`.
- March 15, 2026: 0-7 root cause analysis drove Task #5 (confidence tightening) and Task #6 (Usage Redistribution v2).
- Winning pattern keys: usage redistribution from injured stars, DVP/DVA double alignment, game total/pace environments, last-5 avg clearing book lines, high composite scores.
- Unresolved bugs: `_detect_role_change` Series truth value error; article deduplication (player with 2 HIGH picks only shows first).

## External Dependencies

### Web Scraping Targets
- ESPN: Depth charts
- RotoGrinders, FantasyPros: Player lineups, injury alerts, FanDuel salaries
- TeamRankings: Game odds/spreads
- HashtagBasketball: Defense vs Position stats
- Basketball Reference: Player stats, historical game logs
- NBA.com Stats API: Player game logs, stats, hustle stats
- NBAStuffer: Historical referee statistics
- SportsDatabase: Historic betting lines
- FantasyTeamAdvice.com: FanDuel NBA ownership data

### APIs
- The Odds API: Player prop lines (FanDuel)
- plaintextsports.com/nba: Live scoring data
- Resend API: Email service

### Databases
- SQLite: Staging database (dfs_nba.db)
- PostgreSQL: Production database
- Supabase: Used for syncing platform tables

### Python Libraries
- requests, BeautifulSoup: Web scraping
- pandas, numpy: Data manipulation
- sqlite3: SQLite interaction
- PuLP: Linear programming
- scikit-learn: K-means clustering
- nba_api: NBA.com stats API wrapper
- playwright: Headless browser for chart screenshot capture
- stripe: Stripe payment processing
- anthropic: Claude AI for article narrative generation (via Replit AI Integrations)