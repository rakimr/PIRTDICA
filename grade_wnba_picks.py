"""
Grade yesterday's WNBA article picks against actual game results.
Usage: python grade_wnba_picks.py [YYYY-MM-DD]
       Defaults to yesterday's date.
Loads picks from wnba_daily_articles.picks_json, looks up actual stats from
wnba_player_game_logs in SQLite, determines HIT/MISS/PUSH, and optionally uses
Claude to generate brief analysis for each outcome. Stores results in
wnba_daily_pick_grades.

WNBA mirrors the NBA grader but drops archetype/dva/usage fields // WNBA has no
Phillips archetype or tracking-data source (same honesty rule as the articles,
which use position not archetype).
"""
import os
import sys
import json
import sqlite3
from datetime import timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy.orm import sessionmaker
from backend.database import Base, engine
from backend import models
from utils.timezone import get_eastern_today

STAT_COL_MAP = {
    'PTS': 'pts', 'REB': 'reb', 'AST': 'ast',
    'STL': 'stl', 'BLK': 'blk', '3PM': 'fg3m', 'TO': 'tov',
}


def _load_actual_stats(slate_date):
    db_path = os.path.join(os.path.dirname(__file__), "dfs_nba.db")
    if not os.path.exists(db_path):
        print(f"[GRADE-WNBA] SQLite DB not found at {db_path}")
        return {}

    conn = sqlite3.connect(db_path)
    date_str = slate_date.strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT player_name, pts, reb, ast, stl, blk, tov, fg3m, min "
        "FROM wnba_player_game_logs WHERE game_date = ?",
        (date_str,)
    ).fetchall()
    conn.close()

    stats = {}
    for r in rows:
        name = r[0]
        stats[name] = {
            'pts': r[1] or 0, 'reb': r[2] or 0, 'ast': r[3] or 0,
            'stl': r[4] or 0, 'blk': r[5] or 0, 'tov': r[6] or 0,
            'fg3m': r[7] or 0, 'min': r[8] or 0,
        }
    return stats


def _get_actual_value(player_name, stat, actual_stats):
    col = STAT_COL_MAP.get(stat, stat.lower())

    if player_name in actual_stats:
        return actual_stats[player_name].get(col)

    from utils.name_normalize import normalize_player_name
    norm = normalize_player_name(player_name)
    for name, data in actual_stats.items():
        if normalize_player_name(name) == norm:
            return data.get(col)
    return None


def _generate_claude_analyses(graded_picks):
    api_key = os.environ.get("AI_INTEGRATIONS_ANTHROPIC_API_KEY")
    base_url = os.environ.get("AI_INTEGRATIONS_ANTHROPIC_BASE_URL")
    if not api_key or not base_url:
        print("[GRADE-WNBA] Claude API not configured — skipping analysis generation")
        return {}

    picks_summary = []
    for gp in graded_picks:
        diff = gp['actual'] - gp['book_line'] if gp['actual'] is not None else 0
        picks_summary.append({
            'player': gp['player'],
            'stat': gp['stat'],
            'book_line': gp['book_line'],
            'direction': gp['direction'],
            'projected': gp['projected'],
            'actual': gp['actual'],
            'hit': gp['hit'],
            'margin': round(abs(diff), 1),
        })

    prompt = f"""You are a sharp WNBA analyst grading yesterday's prop picks for PIRTDICA SPORTS CO.

For each pick below, write a brief 1-2 sentence analysis explaining WHY the pick hit or missed. Focus on specific game events: blowouts, foul trouble, injuries mid-game, unexpected usage changes, pace of play, or defensive adjustments. Be concise and analytical — no filler.

Yesterday's picks and results:
{json.dumps(picks_summary, indent=2)}

Return ONLY a JSON array where each element has:
- "player": the player name (must match exactly)
- "stat": the stat type
- "analysis": your 1-2 sentence explanation

Example:
[{{"player": "A'ja Wilson", "stat": "PTS", "analysis": "Hit comfortably as Las Vegas leaned on her in the post against a thin Dallas frontcourt. Wilson logged 34 minutes with a heavy usage rate in a competitive game script."}}]"""

    try:
        from anthropic import Anthropic
        client = Anthropic(api_key=api_key, base_url=base_url)
        print(f"[GRADE-WNBA] Calling Claude for {len(picks_summary)} pick analyses...")
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}]
        )
        text = response.content[0].text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

        analyses = json.loads(text)
        result = {}
        for a in analyses:
            key = (a.get('player', ''), a.get('stat', ''))
            result[key] = a.get('analysis', '')
        print(f"[GRADE-WNBA] Claude returned {len(result)} analyses")
        return result
    except Exception as e:
        error_msg = str(e)
        if "budget" in error_msg.lower() or "credit" in error_msg.lower():
            print(f"[GRADE-WNBA] Claude budget exceeded — skipping analysis")
        else:
            print(f"[GRADE-WNBA] Claude error: {e}")
        return {}


def grade_picks(target_date=None):
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    db = Session()

    if target_date is None:
        target_date = get_eastern_today() - timedelta(days=1)

    print(f"\n{'='*60}")
    print(f"GRADING WNBA PICKS FOR {target_date}")
    print(f"{'='*60}")

    existing = db.query(models.WNBADailyPickGrade).filter(
        models.WNBADailyPickGrade.slate_date == target_date
    ).first()
    if existing:
        print(f"[GRADE-WNBA] Grades already exist for {target_date} — skipping")
        db.close()
        return True

    article = db.query(models.WNBADailyArticle).filter(
        models.WNBADailyArticle.slate_date == target_date
    ).first()

    if not article or not article.picks_json:
        print(f"[GRADE-WNBA] No article or picks found for {target_date}")
        db.close()
        return False

    try:
        picks = json.loads(article.picks_json)
    except (json.JSONDecodeError, TypeError):
        print(f"[GRADE-WNBA] Failed to parse picks_json for {target_date}")
        db.close()
        return False

    if not picks:
        print(f"[GRADE-WNBA] No picks in article for {target_date}")
        db.close()
        return False

    print(f"[GRADE-WNBA] Found {len(picks)} picks to grade")

    actual_stats = _load_actual_stats(target_date)
    if not actual_stats:
        print(f"[GRADE-WNBA] No actual stats found for {target_date} — games may not have been played yet")
        db.close()
        return False

    print(f"[GRADE-WNBA] Loaded actual stats for {len(actual_stats)} players")

    graded = []
    for pick in picks:
        player = pick.get('player', '')
        stat = pick.get('stat', '')
        book_line = float(pick.get('line', 0)) if pick.get('line') else 0
        direction = pick.get('pick', 'OVER')
        projected = float(pick.get('projected', 0)) if pick.get('projected') else 0

        actual_val = _get_actual_value(player, stat, actual_stats)
        if actual_val is None:
            print(f"  [GRADE-WNBA] No actual stats found for {player} ({stat}) — skipping")
            continue

        actual_val = float(actual_val)
        if actual_val == book_line:
            hit = None
        elif direction == 'OVER':
            hit = actual_val > book_line
        else:
            hit = actual_val < book_line

        if hit is None:
            verdict = "PUSH"
        elif hit:
            verdict = "HIT"
        else:
            verdict = "MISS"
        print(f"  {verdict}: {player} {stat} {direction} {book_line} — Actual: {actual_val} (Proj: {projected})")

        graded.append({
            'player': player,
            'stat': stat,
            'book_line': book_line,
            'direction': direction,
            'projected': projected,
            'actual': actual_val,
            'hit': hit,
        })

    if not graded:
        print(f"[GRADE-WNBA] No picks could be graded for {target_date}")
        db.close()
        return False

    hits = sum(1 for g in graded if g['hit'] is True)
    misses = sum(1 for g in graded if g['hit'] is False)
    pushes = sum(1 for g in graded if g['hit'] is None)
    decided = hits + misses
    pct = (hits / decided * 100) if decided > 0 else 0
    push_str = f", {pushes} push(es)" if pushes else ""
    print(f"\n[GRADE-WNBA] Results: {hits}-{misses} ({pct:.1f}%){push_str}")

    claude_analyses = _generate_claude_analyses(graded)

    saved = 0
    for g in graded:
        key = (g['player'], g['stat'])
        analysis = claude_analyses.get(key, '')
        if not analysis:
            if g['hit'] is None:
                analysis = f"Landed exactly on the line at {g['actual']} — a push."
            elif g['hit']:
                diff = abs(g['actual'] - g['book_line'])
                analysis = f"Cleared the line by {diff:.1f}, finishing at {g['actual']} against a {g['book_line']} line."
            else:
                diff = abs(g['actual'] - g['book_line'])
                analysis = f"Fell short by {diff:.1f}, finishing at {g['actual']} against a {g['book_line']} line."

        existing_grade = db.query(models.WNBADailyPickGrade).filter(
            models.WNBADailyPickGrade.slate_date == target_date,
            models.WNBADailyPickGrade.player == g['player'],
            models.WNBADailyPickGrade.stat == g['stat'],
        ).first()
        if existing_grade:
            continue

        grade = models.WNBADailyPickGrade(
            slate_date=target_date,
            player=g['player'],
            stat=g['stat'],
            book_line=g['book_line'],
            direction=g['direction'],
            projected=g['projected'],
            actual=g['actual'],
            hit=g['hit'],
            claude_analysis=analysis,
        )
        db.add(grade)
        saved += 1

    try:
        db.commit()
        print(f"[GRADE-WNBA] Saved {saved} grades to database ({len(graded)} total graded)")
    except Exception as e:
        db.rollback()
        print(f"[GRADE-WNBA] Error saving grades: {e}")
        db.close()
        return False

    db.close()
    return True


if __name__ == "__main__":
    from datetime import datetime
    target = None
    if len(sys.argv) > 1:
        try:
            target = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
        except ValueError:
            print(f"Invalid date format: {sys.argv[1]} (use YYYY-MM-DD)")
            sys.exit(1)
    grade_picks(target)
