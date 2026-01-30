# NBA DFS Projection System

## Overview

This is an NBA Daily Fantasy Sports (DFS) projection system that scrapes, processes, and analyzes basketball data to generate player projections for fantasy contests. The system collects data from multiple sources including player salaries, depth charts, game odds, referee statistics, injury reports, and advanced per-possession stats, then combines them to project player minutes and fantasy points.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Data Pipeline Design
The system follows an ETL (Extract-Transform-Load) pattern with a SQLite database as the central data store. Scripts are organized into three categories:

1. **Scrapers** (`scrape_*.py`) - Fetch raw data from external websites
2. **ETL Scripts** (`etl_*.py`) - Transform and aggregate scraped data
3. **Analysis Scripts** (`dfs_players.py`, `detect_rotation_changes.py`) - Generate final projections

### Orchestration
`run_daily_update.py` serves as the main orchestrator, running all scripts in dependency order. This ensures scraped data is available before ETL scripts run, and ETL outputs are ready before projection generation.

### Minutes Projection Model
The core projection logic uses empirical baseline minutes from 2012-2018 NBA box scores (`baseline_minutes.py`). Players are assigned minutes based on:
- Depth chart position (PG1, SG2, etc.)
- Injury-driven promotions
- Historical minutes per game (MPG)
- Game context (pace, spread, total)

### Usage-Based FPPM Adjustment
When teammates are injured (OUT), their usage is redistributed to remaining players, boosting per-minute production:

**Formula:**
```
FPPM_adj = FPPM_base × (1 + β × (Usage_adj - Usage_base) / Usage_base)
```

Where:
- **β = 0.7** (diminishing returns factor)
- **Usage_adj = Usage_base + (Injured Usage × Player's Usage Share × 0.6)**
- Usage rates come from Basketball Reference advanced stats (USG%)

This captures how players like RJ Barrett get more touches/shots when a star teammate is out.

### Position Handling
The system tracks five positions (PG, SG, SF, PF, C) plus hybrid positions (G, F). Position data comes from Basketball Reference play-by-play data showing percentage of time at each position.

### Name Normalization
Player names are aggressively normalized to handle inconsistencies across data sources:
- Unicode normalization (accents, special characters)
- Suffix removal (Jr, Sr, II, III)
- Alias mapping for common variations

### Physical Matchup Modifiers
`physical_matchups.py` contains lookup tables for players who cause above-average foul trouble for opponents. This affects projected minutes for opposing centers/forwards.

### FanDuel Roster Order
The system captures FanDuel's `roster_order` (1-5 = starters, 6+ = bench) to override ESPN depth charts when conflicts arise. This prevents false promotions when ESPN starters aren't DFS-eligible.

### Lineup Optimizer
`optimize_fanduel.py` uses PuLP linear programming to find the optimal 9-player lineup:
- **Constraints:** $60,000 cap, 2 PG, 2 SG, 2 SF, 2 PF, 1 C
- **Dual Eligibility:** Correctly handles players like C/PF, SG/PG who can fill either slot
- **Filters:** Excludes players with <20 projected minutes
- **Star Weight (ω):** Optional reliability adjustment based on games played percentage and salary tier

Usage:
```bash
python optimize_fanduel.py                    # Raw projections
python optimize_fanduel.py --reliability      # With reliability adjustment
python optimize_fanduel.py --min-minutes 25   # Higher minutes threshold
```

### Volatility Model
`scrape_nba_gamelogs.py` pulls all player game logs from NBA.com's stats API to calculate:
- **min_sd** - Player's minutes standard deviation (lower = more consistent)
- **omega (ω)** - Combined reliability score (0.10-0.90)
- **fp_sd** - Fantasy point standard deviation (historical volatility)
- **avg_fp** - Historical average fantasy points per game
- **max_fp / min_fp** - Season high/low fantasy point games

**Omega Formula:**
```
ω = (games_pct × 0.5) + (sd_factor × 0.5)
sd_factor = clip(1 - (SD - 3) / 7, 0, 1)
```

Where:
- **games_pct** = games played / ~50 games (availability)
- **sd_factor** = penalizes high SD (volatility)
- SD of 3 = perfect stability, SD of 10+ = high volatility

Low ω players (e.g., Sengun at 0.60) have higher projection risk than high ω players (e.g., Bam at 0.74).

### Ceiling/Floor Model
Turns point projections into full distributions for GPP/cash decision-making:

**Formulas:**
```
ceiling = proj_fp + (1.5 × fp_sd)
floor = proj_fp - (1.0 × fp_sd)
upside_ratio = (ceiling - proj_fp) / proj_fp
```

**Usage:**
- **Cash games**: Prefer low fp_sd players (consistent producers, high floor)
- **GPPs**: Target high upside_ratio players (boom-or-bust leverage)
- **Bust probability**: Use floor to assess downside risk vs salary

**FanDuel FP Calculation:**
```
FP = PTS + (REB × 1.2) + (AST × 1.5) + (STL × 3) + (BLK × 3) - TO
```

## External Dependencies

### Web Scraping Targets
- **ESPN** - Depth charts (`espn.com/nba/depth`)
- **RotoGrinders** - Player salaries, injury alerts (`rotogrinders.com`)
- **TeamRankings** - Game odds/spreads (`teamrankings.com/nba/odds`)
- **HashtagBasketball** - Defense vs Position stats (`hashtagbasketball.com`)
- **Basketball Reference** - Per-100 possession stats, player positions, foul rates
- **NBA.com Stats API** - Player game logs, minutes volatility (`stats.nba.com`)
- **NBA Official** - Referee assignments (`official.nba.com`)
- **NBAStuffer** - Historical referee statistics
- **SportsDatabase** - Historic betting lines

### Database
SQLite database (`dfs_nba.db`) stores all scraped and processed data. Tables are recreated on each run to ensure fresh data.

### Python Libraries
- `requests` + `BeautifulSoup` - Web scraping
- `pandas` - Data manipulation
- `sqlite3` - Database operations

### Data Sources Summary
| Data Type | Source | Table |
|-----------|--------|-------|
| Player Salaries | RotoGrinders | `player_salaries` |
| Depth Charts | ESPN | `depth_charts` |
| Game Odds | TeamRankings | `game_odds` |
| DvP Stats | HashtagBasketball | `dvp_stats` |
| Per-100 Stats | Basketball Reference | `player_per100` |
| Referee Stats | NBAStuffer | `referee_stats` |
| Referee Assignments | NBA.com | `referee_assignments` |
| Injury Alerts | RotoGrinders | `injury_alerts` |
| Player Positions | Basketball Reference | `player_positions` |