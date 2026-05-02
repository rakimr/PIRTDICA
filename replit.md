# PIRTDICA SPORTS CO. - NBA DFS Platform

## Overview
PIRTDICA SPORTS CO. offers an NBA Daily Fantasy Sports (DFS) platform focused on accurate player projections through advanced analytics like sophisticated player archetype classification and salary-tier volatility modeling. Its core purpose is to provide a skill-based esports competition platform, moving beyond traditional gambling. Key features include a "Beat the House" game using Monte Carlo simulation, competitive play against AI, and a single-currency economy (Coach Coin) earned through gameplay. The platform aims to establish a new standard for competitive fantasy sports with a business vision to capture market share in competitive fantasy sports and esports.

## User Preferences
Preferred communication style: Simple, everyday language.
Auto-push to GitHub: Always push changes to GitHub at the end of every task using Replit's GitHub connector OAuth token.
Daily auto-push: `run_daily_update.py` automatically commits and pushes pipeline data (CSVs, chart images) to GitHub after each run, keeping the live site current. Standalone `push_to_github.py` can also be run manually.
Production hosting: Replit Reserved VM serves both the FastAPI web tier (uvicorn :5000) and the four schedulers via `start_production.sh` // custom domain `pirtdica.com` points at the Reserved VM // pushes to GitHub no longer trigger a separate deploy hook (Render was retired April 2026; the Reserved VM is the single source of truth for production).
Scheduler env gate: All four schedulers (`scheduler_pregame.py`, `scheduler_charts.py`, `scheduler_postgame.py`, `scheduler_props.py`) check `SCHEDULERS_ENABLED=1` at startup and exit cleanly otherwise. The Reserved VM launcher (`start_production.sh`) sets the gate and is the single source of truth for live runs.
Scheduler visibility: `start_production.sh` tees each scheduler's stdout/stderr to BOTH the local file (`/tmp/pirtdica_logs/<name>.log`) AND the deployment's stdout (so `fetch_deployment_logs` shows live activity prefixed with `[scheduler_name]`). `set -m` puts each backgrounded subshell in its own process group so SIGTERM cleanup reaches python+awk+tee on graceful shutdown. Admin-only `/admin/scheduler-status` endpoint + the "Scheduler Status (Reserved VM)" card on `/admin` show per-scheduler log freshness, state JSON, and a tail of the last 40 log lines, with a 15s auto-refresh toggle.
Do NOT push to GitHub: `articles/` directory and conversation logs. These are local-only and must never be committed or pushed.

## System Architecture
The system utilizes an ETL pattern, staging data in SQLite and storing operational data in PostgreSQL. Core projection models include minutes projection and usage-based FPPM adjustment. Advanced analytics are driven by Phillips Archetype Classification v2 and a Salary-Tier Volatility Model. A Ceiling/Floor Model generates projection distributions, while Blended DVP and DVA systems provide dynamic matchup ratings. A Team Incentive Score adjusts volatility, and a Prop Trend Analysis Modal offers OVER/UNDER calls. The Context Engine v2 dynamically adjusts projections using various factors.

Data sourcing includes game date filtering via The Odds API, team resolution using ESPN depth charts, and API resilience with circuit breaker patterns and cached data fallbacks for NBA.com and Basketball Reference. Injury status reconciliation prioritizes ESPN with RotoGrinders overrides and FanDuel slate cross-referencing.

Lineup optimization uses PuLP for linear programming and a Monte Carlo optimizer. The platform features "Beat the House" and "Coach vs Coach" competitive play with a lobby, coin escrow, and live scoring.

The web platform is built with FastAPI (Python) for the backend, SQLAlchemy for ORM, and Jinja2 templates with custom CSS for the frontend. The single-currency system (Coach Coin) supports a Play-to-Earn (P2E) model focused on Identity, Prestige, Access, and Analytics, strictly avoiding pay-to-win. Monetization relies on rake on H2H competitions, cosmetic sales, and Stripe subscriptions. Stripe integration manages subscription tiers, webhooks, and customer portals, with Resend for notifications. Manual subscription granting is available via an admin panel. Stripe SDK 15.x defaults to API version 2025-09-30+ which moved `current_period_end` from the Subscription object onto `subscription.items.data[i].current_period_end`; `backend/stripe_billing.get_subscription_period_end(sub)` reads the new location with a fallback to the legacy top-level field, and is used by every activation/renewal/recovery callsite (subscribe_success, /stripe/webhook, /billing/recover, /billing auto-recovery).

The player archetype system includes 11 archetypes classified by Phillips Archetype Classification v2. The projection philosophy, "Minimal Viable Elite (MVE)", balances predictive signal with complexity.

A chart screenshot infrastructure uses Playwright. Article header images are generated via Pillow. The Articles page displays daily HIGH Confidence Props analysis. Article generation uses a three-tier system: Claude Analyst (primary), statistical model + Claude narrator (fallback), and a rule-based template engine (final fallback). A Pick Grading Report runs daily, stores results in PostgreSQL, and displays a season W-L record for subscribers.

The Stat-Specific Projection Engine generates projected values, supported by Role Change Detection, a Probabilistic Minutes Model, and an Opportunity-Based Rebound Model. A Composite Score ranks prop recommendations. Playoff awareness is integrated throughout the system, adjusting data lookups, weighting, and article generation logic based on the season phase.

Pipeline resilience features include robust ESPN depth chart scraping with retries and fallbacks, a synthesized MPG-based fallback rotation, and guarded `nlargest` calls in `player_value.py` to prevent pipeline failures. B2B fatigue features compute per-team rest, integrating into ML adjustments, composite scores, and article generation. The Composite Score includes line-movement modifiers based on total drift, last-hour drift, and move-pattern bonuses. The Usage Redistribution Model v2 uses hierarchical, archetype-weighted redistribution for vacated usage, MPG, and archetypes of OUT players. Prop recommendations require actual sportsbook lines, with HIGH Confidence classification adhering to strict criteria, including hit rate, CV, last-5 average, DVA/DVP support, and implied team total support.

Avatar & Identity Design follows "Strategic Minimalism meets Editorial Sports Design" with specific design rules and an accent palette. Cookie consent and analytics tracking are implemented with `PageView` and `CookieConsent` models, a fixed cookie banner, and a `/cookie-settings` page, with IP addresses hashed for privacy.

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
- Anthropic: Claude AI (via Replit AI Integrations)

### Databases
- SQLite: Staging database (dfs_nba.db)
- PostgreSQL: Production database
- Supabase: Used for syncing platform tables