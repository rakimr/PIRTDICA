# PIRTDICA SPORTS CO. - NBA DFS Platform

## Overview
PIRTDICA SPORTS CO. operates an NBA Daily Fantasy Sports (DFS) platform providing player projections and a unique "Beat the House" game. The platform leverages advanced analytics, including sophisticated player archetype classification and salary-tier volatility modeling, to offer a competitive and engaging DFS experience. It aims to generate accurate projections and facilitate a game where users compete against an AI-generated lineup using Monte Carlo simulation. The platform also features a dual-currency economy (Coach Coin for engagement, Coach Cash for competition) designed to drive long-term retention and foster a vibrant community.

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

**10 Launch Avatars:**
1. The Architect (Crimson accent, side-profile silhouette) - 75 CC
2. The Analyst (Royal Blue, data glasses) - 75 CC
3. Stretch Big Emblem (Emerald, archetype symbol) - 50 CC
4. Combo Guard Elite Sigil (Violet, lightning bolt) - 50 CC
5. Playmaker Node (Royal Blue, network constellation) - 50 CC
6. The General (Crimson, commander silhouette) - 100 CC
7. Scoring Wing Mark (Gold, V wings + shot arc) - 100 CC
8. The Strategist Clipboard (Emerald, clipboard diagram) - 75 CC
9. Data Mask (Violet, half face/half stats) - 250 CC, Rare
10. Founders Badge (Gold, Season 1 exclusive, Roman numeral I) - 500 CC, Legendary

**Rank Frame System (overlay on any avatar):**
- Bronze (1+ wins), Silver (15+), Gold (35+), Diamond (75+), Master (150+), Grandmaster (300+), Champion (500+)
- Frames stored in /static/avatars/frames/

**Avatar Tier Pricing:**
- Tier 1 (50-100 CC): Basic silhouettes, single accent
- Tier 2 (250-400 CC): Rare, dual accent, thicker lines
- Tier 3 (800-1500 CC): Legendary, animated, division-reactive

### Coach Shop - Identity Cosmetics Framework

**Core Rule:** Every cosmetic must communicate Skill, Status, or Story. If it doesn't, it won't sell.

**Critical Integrity Rules:**
- Purchasable cosmetics must NEVER mimic rank colors, division badge shapes, or Champion aesthetics
- Purchased badges must be decorative (animated borders, metallic trims, signatures) — never mimic earned achievement badges
- Skill must remain sacred. No one should ever think "they won because they bought something."
- Shop positioning = "Identity customization", NOT "Power boost store"

**Profile Themes (Modify header, accent color, profile frame, texture overlays, rank badge pedestal):**
- Dark Competitive: Obsidian Arena (matte black, red glow), Frost Division (white marble, silver), Inferno Circuit (ember particles, orange/gold)
- Prestige (locked behind division OR expensive): Champion's Hall (white marble, gold trim), Grandmaster Aura (navy, purple halo)
- Analytical (PIRTDICA identity): Data Grid (graph grid, green/white), Neural Network (connecting nodes, pulsing dots)
- Seasonal (rotating, creates urgency): Shadow Ascension (gothic crest), Eclipse Season (eclipse animation)

**Badge System (5 Layers — separated at DB level):**
1. competitive_earned: 100 Ranked Wins, 10 Win Streak, 60%+ Win Rate, Promotion Series Clutch
2. statistical_earned: Projection Crusher, Efficiency King, Slate Dominator, Consistency Award
3. event_limited: Division-specific (Gold Conqueror, Diamond Veteran, Master Slayer)
4. secret_earned: Upset Artist (beat 400+ MMR above), Giant Killer, Perfect Slate, Redemption Arc (hidden until unlocked)
5. cosmetic_purchased: Animated border rings, metallic trims, profile dividers, signatures (decorative only)

**Price Tiers (Coach Coin):**
- Low (50-150 CC): Static profile frames, simple header textures, basic decorative badges
- Mid (200-400 CC): Animated header accents, metallic trims, particle effects
- High (500-1000 CC): Prestige animated themes, seasonal limited themes, division aura effects

**Profile Entrance Animation (Phase 2):**
- Opponent sees your name, division, and a subtle animation when matched in H2H
- Deferred until rank prestige + achievements + identity depth all exist

**Limited Item Strategy:** Monthly rotation creates scarcity. Schema supports is_seasonal, season_id, available_from/until, is_returnable. Manual toggle for now, automated rotation later.

### Implementation Roadmap
**Phase 1 — Competitive Foundation (COMPLETE):** Achievement tracking engine, earned badge auto-awarding, badge display hierarchy, profile badge slots (earned vs cosmetic separation), seasonal flag schema. 58 achievements across 4 earned badge types.
**Phase 2 — Cosmetic Layer (COMPLETE):** 9 profile themes (Dark Competitive, Analytical, Seasonal, Prestige tiers), 15 cosmetic badges (frame accents, signatures, dividers, auras across 3 price tiers). Equip/unequip system with max 3 cosmetic badges. Theme applies via CSS variables to profile page. active_frame field ready for Phase 3.
**Phase 3 — Prestige Enhancements:** Entrance animations, animated division auras, Champion-exclusive cosmetics, seasonal animated effects, profile frame equip system
**Order principle:** Skill → Identity → Spectacle (not Spectacle → Monetization → Skill)

## Esports Competitive Architecture

### Core Philosophy
PIRTDICA is a **skill-based esports competition platform**, not a gambling site. Lineup construction is the game. Money functions as entry fees and prize pool fuel, not wagers/bets. All prize pools are peer-funded only (no house risk).

### Language Rules (Critical)
**USE:** Entry Fee, Prize Pool, Competition, Tournament, Match, Arena
**NEVER USE:** Bet, Wager, Gamble, Odds (in competitive context)

### Three-Layer Structure
1. **Ranked Esports Ladder (Free)** - Foundation. ELO/MMR rating, seasonal rankings, public leaderboards. Rewards: Coach Coins, cosmetic unlocks, titles, seasonal prestige frames.
2. **Coach Cash Arena (Esports Prize Pool)** - Peer-funded prize competitions. H2H matches, tournaments, bracket competitions. Small transparent platform rake. Cash does not affect gameplay mechanics.
3. **Status Economy (Coach Coin)** - Cosmetics, analytics access, identity items. No pay-to-win.

### Division System (Tiered + Hidden MMR)
**Visible Divisions:** Bronze, Silver, Gold, Platinum, Diamond, Master, Grandmaster, Champion (Top X only)
- Each division (except top tiers) has 3 sub-tiers (e.g., Gold III, Gold II, Gold I)
- Hidden MMR keeps matchmaking fair; visible rank creates prestige

**MMR Calculation (Modified ELO for DFS):**
- Base: Win = +20, Loss = -20
- Performance Modifier: +5 if exceed projection by X%, +3 for large margin win, -3 for underperforming win
- Opponent Strength Modifier: +10 bonus for beating someone 300+ MMR above, small penalty for losing to much stronger
- Starting MMR: 1000

**Promotion/Demotion:** Best-of-3 promotion series at division boundaries. Lose = drop back slightly.

**Seasonal Structure:** 6-8 week seasons, soft MMR reset (Diamond resets to Platinum I, Gold resets to Gold III). Rewards based on highest division achieved, not ending division.

**Anti-Smurf:** Rapid MMR acceleration for high win rate, performance-based adjustments, Cash Arena locked behind minimum division (Gold+).

**Target Distribution:** Bronze 25%, Silver 25%, Gold 20%, Platinum 15%, Diamond 8%, Master 4%, Grandmaster 2%, Champion <1%

### Hybrid Match Format
**Primary: Async H2H (Always-On Ladder)** - Chess.com-style. Queue anytime, matched instantly by MMR band, ladder updates continuously. Foundation of skill legitimacy.
**Secondary: Scheduled Match Windows (Match Nights)** - 1-2 nights/week. Fixed entry window, structured division pairings, higher MMR impact (1.5x multiplier), broadcast leaderboard updates. Creates hype and spectacle.
**Tertiary: Daily Slate Mode (Casual)** - Lower MMR impact, Coach Coin rewards, entry-level competition, good for onboarding.

### Competitive Integrity (Brand Pillar)
- Strong matchmaking with ELO band restrictions
- Anti-collusion detection
- Lineup validity thresholds
- Withdrawal KYC (future)
- Win-trading treated as cheating

### Monetization (Clean Esports Model)
1. Small rake on Coach Cash competitions (10%)
2. Premium analytics subscriptions (future)
3. Cosmetic sales (Coach Coin)
4. Seasonal battle passes (future)

## Archetype Clustering Notes (K-Means Tuning)
**Current state (Feb 2026):** K=6 clusters + multi-layer player-level reclassification. 359 players, 9 final archetypes.
**Silhouette scores:** k=6: 0.2226 (best). Scores 0.2–0.35 are normal for NBA behavioral data.
**9 Archetypes (K=6 clusters + 3 reclassification layers):**
1. Playmaker — High-assist guards (Curry, Harden, Trae Young, Luka, SGA)
2. Combo Guard — Scoring guards with some creator tendencies
3. 3-and-D Guard — Defensive-minded guards, low usage
4. Scoring Guard — Pure scoring guards (Norman Powell)
5. 3-and-D Wing — Perimeter role players (Mikal Bridges, Draymond Green)
6. Scoring Wing — Elite offensive forwards (KD, LeBron, Kawhi, Jaylen Brown)
7. Stretch Big — Bigs with high 3PM/100 >= 4.0 (Wembanyama, KAT, Myles Turner, Brook Lopez)
8. Point Center — 6'9"+, C/PF position, high AST/100 >= 5.0 (Jokic, Giannis, Embiid, Banchero)
9. Versatile Big — 6'9"+, C/PF position, well-rounded but not primary facilitator (Sabonis, Randle-type without high ast, Lauri Markkanen)
10. Traditional Big — Paint-focused, low perimeter activity (Gobert, AD, Zach Edey)

**Reclassification hierarchy (applied in order after K-Means):**
1. Stretch Big: Traditional Big cluster + 3PM/100 >= 4.0
2. Traditional Big → Point Center/Versatile Big: AST/100 >= 5.0 + PTS/100 >= 18.0
3. Height-based wing correction: 6'9"+ with C%+PF% >= 40% → Point Center (high AST) or Versatile Big
**Height data source:** nba_api LeagueDashPlayerBioStats (bulk endpoint, PLAYER_HEIGHT_INCHES)
**Validation:** 23/23 known players correct (0% error rate)
**Future improvements:**
1. Add role-separating features: % shots at rim, post-up frequency, ORB/DRB rate, screen assists, paint touches
2. Consider Gaussian Mixture Model (GMM) for probabilistic assignment — could weight DVS multipliers by archetype probability
3. Tighten feature scaling once new features added

## System Architecture
The system uses an ETL pattern with SQLite for data storage, orchestrated by `run_daily_update.py`. Core projection models include minutes projection, usage-based FPPM adjustment, and robust name normalization. Advanced models like Phillips Archetype Classification (K-means clustering for secondary positions) and Salary-Tier Volatility Model (regularizing fantasy point standard deviation) enhance projection accuracy. The Ceiling/Floor Model transforms point projections into full distributions for strategic decision-making. The Blended DVP and Defense vs Archetype (DVA) systems provide reactive matchup ratings, adapting throughout the season. A Team Incentive Score adjusts volatility based on team standings, and a Prop Trend Analysis Modal offers OVER/UNDER calls based on blended DVP/DVA edges.

Lineup optimization is handled by a PuLP linear programming optimizer and a Monte Carlo optimizer for win probability. The "Beat the House" game features daily contests against an AI. The "Coach vs Coach" (H2H) system allows users to challenge each other with their lineups, featuring a lobby, coin escrow, and live scoring.

The web platform uses FastAPI (Python) with SQLAlchemy and PostgreSQL for the backend, and Jinja2 templates with custom CSS for the frontend. It includes live scoring, contest history, and an admin control panel. The dual-currency economy (Coach Coin for cosmetics/access, Coach Cash for competitive play) supports a Play-to-Earn (P2E) model focused on Identity, Prestige, Access, and Analytics, without compromising competitive integrity. Coach Cash can be converted to Coach Coin (1:5 rate), but not vice-versa, to prevent farming exploits. Ranked modes include a free Coin Mode and a competitive Cash Mode.

## External Dependencies

### Web Scraping Targets
-   **ESPN:** Depth charts
-   **RotoGrinders:** Player lineups, injury alerts, FanDuel salaries
-   **FantasyPros:** FanDuel salary fallback for missing data
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
-   **SQLite:** Primary data store (`dfs_nba.db`) for scraped and processed data.
-   **PostgreSQL:** Stores user accounts, contest data, entries, achievements, shop items, and dual-currency ledgers for the web platform.

### Python Libraries
-   `requests`, `BeautifulSoup`: Web scraping
-   `pandas`: Data manipulation
-   `sqlite3`: SQLite database operations
-   `PuLP`: Linear programming for optimization
-   `scikit-learn`: K-means clustering
-   `Chart.js`: Frontend charting