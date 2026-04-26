# PIRTDICA Session Memory & Narrative Reference

## Narrative Analysis Reference - March 12, 2026 (80% Hit Rate)

Gold-standard slate: 9-game night, 8 HIGH confidence plays, 6 with book lines, ~80% hit rate. This is the benchmark for article narrative quality and winning pick patterns.

### Picks Table

| # | Player | Game | Stat | Avg | Line | Projected | Edge | Pick |
|---|--------|------|------|-----|------|-----------|------|------|
| 1 | Rui Hachimura | CHI @ LAL | PTS | 11.6 | 9.5 | 16.3 | +8.7% | OVER |
| 2 | Tre Johnson | WAS @ ORL | PTS | 12.7 | 11.5 | 16.0 | +39.1% | OVER |
| 3 | Tre Jones | CHI @ LAL | AST | 5.5 | 4.5 | 6.8 | +51.1% | OVER |
| 4 | Dylan Harper | DEN @ SA | PTS | 11.1 | 10.5 | 14.5 | +38.1% | OVER |
| 5 | De'Aaron Fox | DEN @ SA | PTS | 19.0 | 17.5 | 19.7 | +12.6% | OVER |
| 6 | Cam Spencer | DAL @ MEM | PTS | 11.3 | 13.5 | 13.1 | — | UNDER |

### Slate Context
- Tatum OUT, Siakam OUT — two of the biggest usage players off the board. Redistribution beneficiaries (Nembhard, Jalen Johnson) projected to see bumps.
- Rui Hachimura top composite score (72.9) — CHI is the best PF matchup in the league (+3.8 DVP edge).
- Dylan Harper + De'Aaron Fox both in DEN @ SA with 239.5 total — second-highest total on the slate, elite pace environment.
- Dyson Daniels OUT (toe) — removes ATL key playmaker. Jalen Johnson and Onyeka Okongwu projected to absorb usage vs BKN.
- Cam Spencer UNDER — only contrarian pick, low projection vs inflated 13.5 line.

### Per-Player Analysis (Reference Narrative Style)

**1. RUI HACHIMURA — PTS OVER 9.5 (CHI @ LAL)**
The line is almost insultingly low. Hachimura averages 11.6 on the season and the model has him at 16.3 in 35 projected minutes. Chicago is the best power forward matchup in the league right now — +3.8 DVP edge — and LAL is missing Hayes, Smart, and Kleber, pushing Rui into a heavier offensive role (+4.2 usage boost). He's hit this line in back-to-back meetings with CHI and the 240 game total creates an ideal scoring environment. Top composite score on the slate at 72.9.

**2. TRE JOHNSON — PTS OVER 11.5 (WAS @ ORL)**
Washington is decimated — D'Angelo Russell OUT, Cam Whitmore OUT — pushing Johnson into a primary ball-handler role with a +15.0 usage boost baked in. His last 5 average of 13.2 already clears the 11.5 line with room to spare, and his shot profile aligns favorably against Orlando's defensive weak zones (+0.4 shot zone edge). Hit rate of 56.9% is modest, but the usage context and recent form make this one of the cleaner value spots on the board.

**3. TRE JONES — AST OVER 4.5 (CHI @ LAL)**
Jones runs this Bulls offense and averages 5.5 assists — a full assist above the book line. The model projects 6.8 in 31 minutes, with Ayo Dosunmu OUT redistributing 0.5 extra assists his way. His playmaking composite index of 3.1 is strong, both DVA (+0.73) and DVP (+0.79) are positive, and the 240 game total means plenty of possessions. Hit rate of 64.6% is the second-highest among all HIGH picks today. The one caution: last 5 average is 4.6, slightly below his season mark — but the matchup and usage context override that dip.

**4. DYLAN HARPER — PTS OVER 10.5 (DEN @ SA)**
Harper's been heating up — last 5 average of 13.0 already clears the line by 2.5 points. He's scored in a DEN matchup before (+2.4 H2H edge) and the game environment is elite: second-highest total on the slate at 240, fast pace on both sides. DVA +0.6 confirms his archetype produces well against Denver's coverage scheme. The line at 10.5 hasn't caught up to his recent form, which is exactly the inefficiency we're targeting.

**5. DE'AARON FOX — PTS OVER 17.5 (DEN @ SA)**
Fox averages 19.0 with a 17.5 line — natural cushion. Same DEN @ SA environment (240 total, pace-up). Model projects 19.7 with positive matchup alignment. Consistency and volume make this a high-floor play.

**6. CAM SPENCER — PTS UNDER 13.5 (DAL @ MEM)**
Only contrarian pick on the slate. Spencer's model projection of 13.1 sits below the 13.5 line. Low-projection vs inflated line = clean UNDER spot.

### Winning Patterns (What Made This Slate Hit)
1. **Usage redistribution from injured stars** — Multiple star absences (Tatum, Siakam, D'Angelo Russell, Whitmore, Dosunmu, Daniels) pushed secondary players into expanded roles. The model correctly identified usage absorption beneficiaries.
2. **DVP/DVA double alignment** — Top picks had BOTH DVP and DVA supporting the direction (Hachimura +3.8 DVP, Jones +0.73 DVA / +0.79 DVP, Harper DVA +0.6).
3. **Game total/pace environments** — Multiple picks targeted games with 239-240 totals, maximizing scoring opportunity.
4. **Last-5 avg clearing book line** — Johnson (13.2 vs 11.5), Harper (13.0 vs 10.5), Fox (19.0 vs 17.5) all had recent production well above the line.
5. **High composite scores** — Top pick (Hachimura) had the highest composite on the slate (72.9), validating multi-factor quality ranking.
6. **Single contrarian UNDER** — Only one UNDER pick, with clear projection-vs-line mismatch. Discipline in pick direction.

## Session History

### Task #5 — HIGH Confidence Criteria Tightening
- **Problem**: March 15 went 0-7 on HIGH confidence picks. Root cause: criteria too loose.
- **Changes**: Hit rate minimum 55% → 58%, CV threshold 0.55 → 0.45, added last-5 vs book line directional gate, DVA/DVP matchup alignment gate (both opposing = fail), UNDER+usage boost > 3.0 blocked, composite score now directional for DVA/DVP, confidence_reasons always populated (no more "nan").
- **Root cause analysis**: (1) Flat redistribution ignored multi-star absences, (2) DVA/DVP both opposing picks still labeled HIGH, (3) UNDER calls with +15.1 usage boost not blocked, (4) last-5 avg never checked against book line.

### Task #6 — Usage Redistribution Model v2
- **Problem**: Old flat formula (`player_avg × sum(usg × 0.15) / 100`) badly underestimated multi-star absences. On March 15, with Siakam (29.6% USG) and Nembhard (24.0% USG) both OUT, Nesmith was boosted only +2.0 pts (13.1 → 13.8). He scored 32.
- **Changes**: Hierarchical cascade tiers (0/1/2 based on vacated usage), archetype cosine similarity for absorption weighting, minutes escalation for remaining starters (capped at 38), Opportunity Index = (new_min × new_usg × pace) / (baseline_min × baseline_usg × pace), Opportunity Spike flag when opp_index >= 1.3 + positive matchup, double-counting prevention (Opp Index active → skip min_ratio).
- **Validation**: Nesmith simulation: old model +2.0 pts → new model +6.1 pts (proj 19.2 vs 13.8), correctly flipping to OVER.

### March 15, 2026 — 0-7 Root Cause Analysis
- All 7 HIGH picks missed. Deep dive identified 4 systemic failures:
  1. Flat redistribution: Ignored that 53.6% combined vacated usage (Siakam+Nembhard) should have triggered massive redistribution.
  2. DVA/DVP gate missing: Picks with BOTH DVA and DVP opposing the direction were still labeled HIGH.
  3. UNDER+usage contradiction: Brogdon called UNDER with +15.1 usage boost — logically contradictory.
  4. Last-5 vs line: Never checked whether recent production actually supported the pick direction vs the book line.
- This analysis directly drove Task #5 and Task #6 changes.

### March 12, 2026 — 80% Hit Rate Slate (Reference)
- 9-game slate, 8 HIGH confidence plays, 6 with book lines, ~80% hit rate.
- Full narrative analysis preserved above as gold-standard reference.
- Key success factors: usage redistribution accuracy, DVP/DVA double alignment, high game totals, last-5 averages clearing lines, high composite scores.

### Known Unresolved Issues
- **`_detect_role_change` bug**: `pd.notna(pstats.get('mpg'))` can fail with Series truth value error — still unresolved.
- **Article deduplication bug**: `generate_article.py` deduplicates by player; a player with 2 HIGH picks only shows the first. Still unresolved.

## April 25, 2026 — External Technical Guide Review (Team-Level Win/Loss System)

User pasted a comprehensive external guide titled *"Complete Technical Guide: From Data Collection to a Working Model"* covering an end-to-end NBA prediction system. Reviewed in full and graded against PIRTDICA's existing architecture. Reference paste: `attached_assets/Pasted-Complete-Technical-Guide-From-Data-Collection-to-a-Work_1777142224693.txt`.

### Guide scope
The guide builds a **team-level binary Win/Loss classifier** (Logistic Regression + Random Forest + XGBoost ensemble, soft-voted) using nba_api game logs, Four Factors, ELO ratings, fatigue/B2B features, head-to-head, sportsbook spread/moneyline conversion, and a Polymarket prediction-market overlay. Adds a "triple layer" divergence engine (Sportsbook vs Polymarket vs ML) with KL-divergence features and Claude as an interpretation layer. Includes walk-forward backtest, calibration plots, ablation study, and a hybrid `NBAHybridPredictor` that adaptively weights sources by Polymarket liquidity.

### What PIRTDICA already covers as well or better
- **Four Factors / pace / possessions**: Already baked into projection inputs (USG, TS%, implied team totals from The Odds API).
- **Sportsbook line**: Source of truth for HIGH-confidence gating via The Odds API.
- **DVP/DVA**: Our DVA is defense-vs-archetype with Bayesian shrinkage — more sophisticated than the guide's per-position DVP.
- **Usage redistribution from injuries**: Our v2 hierarchical archetype-weighted cascade is significantly more advanced than the guide's flat redistribution.
- **Player archetype clustering**: Phillips Archetype v2 (8 composite indices, minutes-weighted K-Means, soft clustering) is far ahead of anything in the guide.
- **ML adjustment per player**: `player_adjustment_factors` already feeds `ml_bias` into projections.

### Four ranked adoption candidates (highest ROI first)
1. **Line movement tracking (opening vs closing line)** — Highest ROI, lowest effort. We currently only capture the line at refresh time. Adding a `prop_line_history` table (timestamp, player, stat, book_line, source) with `line_drift = current - opening` would surface sharp money moves and feed both the composite score and Claude's article ("Vegas opened 21.5, moved to 22.5 in 3 hours — sharp money on OVER, aligning with our projection").
2. **Calibration report + walk-forward backtest as subscriber-facing dashboard** — All raw data already exists in `daily_pick_grades`. Build a `/model-performance` page with calibration curve (predicted hit rate vs actual), rolling 30-day accuracy chart, and ablation slices by archetype/DVA edge/usage boost. Doubles as a trust marketing asset and a debugging tool. Direct port of guide lines 1810-1839.
3. **Explicit B2B / rest features in projections** — `dfs_players.py` lacks explicit fatigue features. Port the guide's logic (lines 820-867): `home_b2b`, `away_b2b`, `rest_advantage`, 3-in-4 indicator. Wire into minutes projection (B2B downward bias for 30+ stars, load-management risk) and Claude's article context.
4. **Claude divergence-prompt enrichment** — Prompt-only change. Enrich Claude's article prompt with multiple reference probabilities (current FanDuel line, opening line from #1, our model projection, optional Polymarket). Then ask Claude to interpret consensus vs divergence the way a sharp bettor would.

### Skip list (explicit "do not build")
- **Full Win/Loss XGBoost classifier** — Wrong product. PIRTDICA is a player-prop platform, not a moneyline picker.
- **Polymarket integration for player props** — Polymarket has thin/no coverage of granular NBA prop markets. Liquidity isn't there for "LeBron OVER 25.5 tonight." Save the integration cost.
- **Guide's matplotlib visualization stack** — Our chart infrastructure (Playwright batch capture, four-chart gallery, ref foul charts) is already more polished.
- **Telegram bot output layer** — Already covered by web app + email + Stripe-gated subscriber pages.
- **Team-level ELO** — Lower priority. Vegas implied totals already do most of this work.

### YOLO/OpenCV computer vision system (mentioned but not yet received)
User mentioned a separate computer vision system built on YOLO + OpenCV + Python intended for player-possession analysis. Despite multiple paste attempts, only the team-level DFS guide came through (different file). When the actual CV code arrives, the framing for review should be: most "watch broadcast video and count points" CV systems just reproduce data NBA.com already exposes (touches, time of possession, contested shots, defensive matchup minutes — all in `player_tracking_stats` and `player_hustle_stats`). The only CV applications that would genuinely move the needle are:
- **Possession-level usage attribution** the NBA aggregates away (per-possession on/off splits with specific teammates absent).
- **Defender identity on each shot** ("who guarded Tatum on each contested 3?"), feeding refined DVA against specific defender archetypes.
- **Real-time shot quality** (open vs contested at the moment of release, not just aggregates).

If the system addresses any of those three, it's worth integrating. Otherwise it's likely overkill for our needs.

### Implementation status
- **All four adoption candidates remain unbuilt as of this entry.** No projection-engine, schema, or article-generator code was changed during this review. The user wanted feedback first before greenlighting individual builds. Each candidate should be its own task when approved.

## April 25, 2026 — Move-Pattern Backtest (Task #26)

Built `analysis/move_pattern_backtest.py` to validate whether picks aligned with a `sudden_swing` actually beat picks aligned with a `gradual_drift` of the same magnitude. The script joins `daily_pick_grades` (Postgres) and the `march*_grades.csv` slates against per-snapshot lines in `player_props_history` (SQLite), mirrors `_load_line_movement` (single-snapshot props classify as `flat`, drift=0), reruns `_classify_move_pattern` on each prop, and reports HIT% by bucket plus a `largest_swing_share` cutoff sensitivity sweep.

Findings (`analysis/move_pattern_backtest.md`):
- **Flat baseline:** 47.2% HIT (17/36 picks) — every matched pick collapses to `flat` because the props_history table has zero multi-snapshot props on any slate that overlaps a graded pick.
- **gradual_drift / sudden_swing / reversal:** n=0. Cannot validate the 70% / 0.5 cutoffs yet.
- Root cause: the `Props Refresh` workflow that drives `scheduler_props.py` is not running, so only one snapshot per (player, stat) per `game_date` exists in the historical record (one slate, 2026-03-03, has two minutes — but no graded picks on that date).

Recommendation: keep the 70% / 0.5 thresholds at their current documented defaults, do NOT add a separate composite-score bump for sudden_swing, and re-run the backtest after the `Props Refresh` workflow has accumulated 3-4 weeks of intra-day snapshots (target n>=15 per bucket). The backtest script itself is built to handle that day-1.
