# PIRTDICA SPORTS CO. - NBA DFS Platform

## Overview
PIRTDICA SPORTS CO. offers an NBA Daily Fantasy Sports (DFS) platform focused on accurate player projections through advanced analytics like sophisticated player archetype classification and salary-tier volatility modeling. Its core purpose is to provide a skill-based esports competition platform, moving beyond traditional gambling. Key features include a "Beat the House" game using Monte Carlo simulation, competitive play against AI, and a single-currency economy (Coach Coin) earned through gameplay. The platform aims to establish a new standard for competitive fantasy sports with a business vision to capture market share in competitive fantasy sports and esports.

## User Preferences
Preferred communication style: Simple, everyday language.
Auto-push to GitHub: Always push changes to GitHub at the end of every task using Replit's GitHub connector OAuth token.
Daily auto-push: `run_daily_update.py` automatically commits and pushes pipeline data (CSVs, chart images) to GitHub after each run, keeping the live site current. Standalone `push_to_github.py` can also be run manually.
Render deploy hook: After every GitHub push, `push_to_github.py` triggers a Render deploy hook (`RENDER_DEPLOY_HOOK_URL` env var) to automatically redeploy the production site.
Scheduler env gate: All four schedulers (`scheduler_pregame.py`, `scheduler_charts.py`, `scheduler_postgame.py`, `scheduler_props.py`) check `SCHEDULERS_ENABLED=1` at startup and exit cleanly otherwise. The Reserved VM launcher (`start_production.sh`) sets the gate and is the single source of truth for live runs.
Scheduler visibility: `start_production.sh` tees each scheduler's stdout/stderr to BOTH the local file (`/tmp/pirtdica_logs/<name>.log`) AND the deployment's stdout (so `fetch_deployment_logs` shows live activity prefixed with `[scheduler_name]`). `set -m` puts each backgrounded subshell in its own process group so SIGTERM cleanup reaches python+awk+tee on graceful shutdown. Admin-only `/admin/scheduler-status` endpoint + the "Scheduler Status (Reserved VM)" card on `/admin` show per-scheduler log freshness, state JSON, and a tail of the last 40 log lines, with a 15s auto-refresh toggle.
Do NOT push to GitHub: `articles/` directory and conversation logs. These are local-only and must never be committed or pushed.

## System Architecture
The system utilizes an ETL pattern, staging data in SQLite and storing operational data in PostgreSQL. Core projection models include minutes projection and usage-based FPPM adjustment. Advanced analytics are driven by Phillips Archetype Classification v2 (8 composite indices, minutes-weighted K-Means, soft clustering) and a Salary-Tier Volatility Model. A Ceiling/Floor Model generates projection distributions, while Blended DVP and DVA systems provide dynamic matchup ratings. A Team Incentive Score adjusts volatility, and a Prop Trend Analysis Modal offers OVER/UNDER calls. The Context Engine v2 dynamically adjusts projections using interaction-probability-weighted physical mismatch, matchup familiarity, archetype effects, and opponent durability.

Game date filtering uses The Odds API events. Team resolution handles "2TM"/"3TM" players using ESPN depth chart data. NBA.com API resilience uses a circuit breaker pattern with cached data fallback, and Basketball Reference fallback is implemented for game logs. Injury status reconciliation uses ESPN as the baseline, with fresh same-day RotoGrinders alerts overriding ESPN. FanDuel slate cross-reference suppresses stale ESPN OUT markers if the player is active on FanDuel.

Lineup optimization is achieved using PuLP for linear programming and a Monte Carlo optimizer. The platform features a "Beat the House" game against AI, and "Coach vs Coach" (H2H) competitive play with a lobby, coin escrow, and live scoring.

The web platform is built with FastAPI (Python) for the backend, SQLAlchemy for ORM, and Jinja2 templates with custom CSS for the frontend. The single-currency system (Coach Coin) supports a Play-to-Earn (P2E) model focused on Identity, Prestige, Access, and Analytics, strictly avoiding pay-to-win mechanics. Monetization relies on a small rake on H2H competitions, cosmetic sales, and Stripe subscriptions.

Subscription tiers via Stripe include PIRTDICA Picks, PIRTDICA Stat Pack, and PIRTDICA Bundle. A multi-subscription architecture is in place with a `user_subscriptions` table. Stripe integration handles checkout, webhooks, and customer portals. Subscription activation triggers in-app notifications and emails via Resend.

Admins can manually grant subscription access via the `/admin` panel ("Manual Subscription Access" card). Endpoints: `GET /admin/lookup-user`, `POST /admin/grant-subscription` (body: username, plan, days — blank days = lifetime), `POST /admin/revoke-subscription`. Manually granted rows use `stripe_subscription_id = "manual_<plan>_<userid>_<timestamp>"` to satisfy the unique-non-null constraint without colliding with real `sub_*` Stripe IDs. The billing template uses the `sub_` prefix to gate the "Change Plan / Cancel Subscription" buttons, so manually granted users never trigger a Stripe portal call (no 500 errors). When granting to a user who already has an active Stripe-managed subscription, the legacy `user.subscription_plan` field is preserved to avoid downgrading the displayed primary plan. Re-granting an existing active manual sub of the same plan extends it instead of duplicating. `IntegrityError` on the unique constraint is caught and returned as a friendly retry message.

The player archetype system includes 11 total archetypes, classified using the Phillips Archetype Classification v2, which compresses raw player stats into 8 orthogonal composite indices and clusters on those axes. The projection philosophy, "Minimal Viable Elite (MVE)", focuses on capturing 85-90% of predictive signal with 30-40% of the complexity.

A chart screenshot infrastructure exists via `/chart-screenshot/{chart_type}/{target}` using Playwright for batch capture. Article header images are generated via `generate_header.py` using Pillow. The Articles page (`/articles`) displays daily HIGH Confidence Props analysis, including a header image, picks table, and per-player analysis sections.

Article generation uses a three-tier system in `generate_article.py`:
1. **Claude Analyst mode (primary)**: Claude analyzes full slate data and selects 4-8 HIGH confidence picks, validated against actual slate data to prevent hallucination.
2. **Statistical model + Claude narrator (fallback)**: If Claude Analyst fails, falls back to the statistical model's HIGH confidence picks with Claude writing narratives.
3. **Template engine (final fallback)**: If Claude API is unavailable, uses rule-based template engine.

A Pick Grading Report (`grade_picks.py`) runs daily, loading yesterday's article picks, looking up actual stat values, determining HIT/MISS/PUSH, and sending results to Claude for brief contextual analysis. Grades are stored in `daily_pick_grades` (PostgreSQL). The `/articles` page shows "PIRTDICA'S GRADING REPORT" (subscribers only) with a running season W-L record.

The Stat-Specific Projection Engine generates projected values for each stat type. A Role Change Detection system identifies regime shifts. The Probabilistic Minutes Model replaces deterministic approaches. The Opportunity-Based Rebound Model uses a two-stage multiplicative architecture. The Composite Score ranks prop recommendations by combining edge size, matchup alignment, game environment, consistency, and trend alignment.

B2B fatigue features compute per-team `days_rest`, `is_b2b`, and `is_3in4`. The fatigue block runs after ML minutes adjustment and before `base_fp` so the drag propagates everywhere downstream. The composite score deducts -3 for HIGH OVER picks where the player is on a B2B side AND the matchup isn't already terrible. The article generator surfaces a "REST WATCH" section and a `b2b_signal` field per pick context. Both Claude system prompts instruct the model to treat B2B as a yellow flag for OVERs and supporting context for UNDERs. The template fallback injects different B2B caveat copy.

Composite Score line-movement modifier is split across three signals: a total-drift component (open-vs-current, slope 1.0, cap ±2), a sharper last-hour-drift component (slope 2.0, cap ±2.5, threshold 0.25 so a single intra-day half-point tick still scores), and a small move-pattern bonus (sudden_swing aligned with the pick = +0.5, misaligned = −0.5; reversal = −0.25 regardless of direction). Combined cap is ±5. Slopes/caps are conservative priors.

The Usage Redistribution Model v2 replaces flat injury boosts with hierarchical, archetype-weighted redistribution. It tracks vacated usage, MPG, and archetypes of OUT players. Cascade tiers activate based on total vacated usage, and archetype similarity determines absorption. Minutes escalation distributes vacated minutes to remaining starters. An Opportunity Index calculates compounded impact, and an "Opportunity Spike" flag indicates significant role increases combined with positive matchups.

Prop recommendations require an actual sportsbook book line from The Odds API. HIGH Confidence Prop Classification requires ALL gates to pass, including hit rate >= 58%, CV <= 0.30, last-5 average clearing the book line, DVA/DVP support, implied team total support, and specific blocks for UNDER picks with high usage boosts. All 5 stat projection functions enforce a 20% inflation cap.

Avatar & Identity Design Direction follows a "Strategic Minimalism meets Editorial Sports Design" style with specific design rules and an accent palette. Cosmetics communicate Skill, Status, or Story.

Cookie consent and analytics tracking is implemented via `PageView` and `CookieConsent` models in PostgreSQL. A fixed cookie banner appears for first-time visitors. Users can accept or decline analytics cookies. IP addresses are hashed for privacy. A `/cookie-settings` page allows users to toggle analytics on/off at any time, with footer links on every page.

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