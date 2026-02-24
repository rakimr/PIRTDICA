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
The system utilizes an ETL pattern, staging data in SQLite and storing operational data in PostgreSQL. Core projection models include minutes projection and usage-based FPPM adjustment. Advanced analytics are driven by Phillips Archetype Classification v2 (8 composite indices, minutes-weighted K-Means, soft clustering) and a Salary-Tier Volatility Model. A Ceiling/Floor Model generates projection distributions, while Blended DVP and DVA systems provide dynamic matchup ratings. A Team Incentive Score adjusts volatility, and a Prop Trend Analysis Modal offers OVER/UNDER calls.

The Context Engine v2 (Matchup Interaction Layer) dynamically adjusts projections using interaction-probability-weighted physical mismatch (size, weight, wingspan), matchup familiarity, archetype effects, and opponent durability. This layer models possession-level physical confrontation probability through statistical structure, addressing issues like "The Clingan vs Williams Law" by weighting interactions rather than averaging roster-wide effects. The Context Engine v2 uses a 5-layer architecture:
1. **Interaction Probability Matrix**: Computes weights based on position, minutes overlap, and role interaction.
2. **Interaction-Weighted Size Impact**: Adjusts for size differences (height, weight, wingspan) gated by interior usage.
3. **Interaction-Weighted Archetype Matchup**: Applies archetype-vs-archetype FPPM differentials.
4. **Familiarity Effect**: Incorporates player-vs-team FPPM differentials.
5. **Bidirectional Durability**: Adjusts for opponent stability.

Lineup optimization is achieved using PuLP for linear programming and a Monte Carlo optimizer. The platform features a "Beat the House" game against AI, and "Coach vs Coach" (H2H) competitive play with a lobby, coin escrow, and live scoring.

The web platform is built with FastAPI (Python) for the backend, SQLAlchemy for ORM, and Jinja2 templates with custom CSS for the frontend, featuring live scoring, contest history, and admin controls. The dual-currency system (Coach Coin, Coach Cash) supports a Play-to-Earn (P2E) model focused on Identity, Prestige, Access, and Analytics, strictly avoiding pay-to-win mechanics. Ranked modes include free Coin Mode and competitive Cash Mode, structured with a tiered division system, hidden MMR, and seasonal resets. Monetization relies on a small rake on Coach Cash competitions, cosmetic sales, and future subscriptions.

The player archetype system includes 10 total archetypes: 5 specialist (Playmaker, 3-and-D Wing, Scoring Wing, Versatile Big, Traditional Big) and 5 hybrid branch categories (Combo Guard, Stretch 4, Stretch 5, Point Center, Point Forward). Classification uses the Phillips Archetype Classification v2 system described below.

### Phillips Archetype Classification v2
The archetype system compresses raw player stats into 8 orthogonal composite indices, then clusters on those axes using minutes-weighted K-Means with soft probability assignments.

**8 Composite Indices** (each is a sum of z-scored raw features):
1. **Creation Index**: USG% + Pull-Up% + Sec/Touch + Dribbles/Touch — measures self-created offense
2. **Playmaking Index**: AST/100 + Touches/Min — measures ball distribution
3. **Interior Index**: Rim+Paint% + Post Touches + Paint Touches — measures paint presence
4. **Perimeter Index**: 3PT Shot% + Catch-and-Shoot% + Pull-Up 3 Share — measures outside shooting profile
5. **Off-Ball Index**: C&S 3 Share - Time of Possession — measures movement without the ball
6. **Rebound Index**: REB/100 + Box Outs/48 — measures rebounding activity
7. **Defense Index**: STL/100 + BLK/100 + Deflections/48 + Contested/48 — measures defensive engagement
8. **Size Index**: Height + Weight + Wingspan — measures physical dimensions

**Clustering Approach**:
- K=6 clusters trained on high-minute players only (800+ total minutes) to prevent bench noise from distorting centroids
- All players (including low-minute) are assigned to nearest centroid without influencing centroid positions
- Soft clustering stores probability vectors (`cluster_0_prob` through `cluster_5_prob`) for each player, enabling fuzzy archetype boundaries
- Silhouette scores evaluated for k=5 through k=11; k=6 chosen as optimal balance

**Post-Clustering Domain Logic** (sequential reclassification):
1. Cluster auto-labeling via centroid profile scoring
2. Shot-zone big man reclassification (Traditional ↔ Stretch based on rim/paint vs 3PT distribution)
3. Facilitating big detection: Point Center (C% ≥ 50) / Point Forward (C% < 50) for bigs with AST/100 ≥ 5.0 and PTS/100 ≥ 24.0, gated by ball initiation threshold (touches/min ≥ 2.0)
4. Position-based frontcourt reclassification for players in guard/wing clusters with C%+PF% ≥ 50, with wing escape valve (high pull-up%, low rebounding)
5. Versatile Big shot-zone refinement (extreme shooters → Stretch, extreme rim players → Traditional)
6. Hybrid branch routing for transcendent players (PTS/100 ≥ 30, AST/100 ≥ 7, USG ≥ 26)
7. Guard playmaker reclassification (facilitator-first guards with PG% ≥ 70 and AST/100 ≥ 6)
8. Stretch Big split into Stretch 4 / Stretch 5 by C% threshold

**Correlation Notes**: Interior↔Perimeter (r=-0.81), Creation↔Off-Ball (r=-0.82), and Rebound↔Size (r=0.80) are the highest correlated pairs. These reflect real basketball dimensions (paint vs perimeter, ball-dominant vs off-ball, big vs small) and do not degrade clustering quality — K-Means still produces meaningfully distinct clusters with clear archetype separation.

**Validation**: 28/32 known player classifications correct (88% accuracy). Remaining 4 are defensible edge cases (e.g., Kawhi Leonard as Playmaker due to elite creation metrics, Klay Thompson as 3-and-D Wing (Role) reflecting his current off-ball role).

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