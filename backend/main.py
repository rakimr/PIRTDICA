from fastapi import FastAPI, Request, Depends, Form, HTTPException, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse, JSONResponse, HTMLResponse
from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from datetime import datetime, date, timedelta
import os
import sys
import time
import json
import threading
import subprocess

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.timezone import get_eastern_today, get_eastern_now, EASTERN

from backend.database import engine, get_db, Base
from backend import models, auth, data_access
from backend.ranking import (
    calculate_mmr_change, update_user_ranking, get_matchmaking_range,
    format_division, DIVISION_COLORS, DIVISIONS
)

Base.metadata.create_all(bind=engine)

app = FastAPI(title="PIRTDICA")

import traceback as _tb

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    print(f"[UNHANDLED ERROR] {request.method} {request.url.path}: {exc}")
    _tb.print_exc()
    return HTMLResponse(
        content=f"<h1>Internal Server Error</h1><p>Something went wrong. Please try again.</p>",
        status_code=500
    )

class NoCacheMiddleware:
    """Pure ASGI middleware (NOT BaseHTTPMiddleware) so it doesn't buffer
    StaticFiles FileResponses in memory. The BaseHTTPMiddleware variant was
    triggering [Errno 5] Input/output error for every CSS/JS/image on the
    Reserved VM by reading the streaming response body through an internal
    memory stream. We also skip /static/ entirely so browsers and Replit's
    proxy can cache assets normally.
    """
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope.get("path", "").startswith("/static/"):
            await self.app(scope, receive, send)
            return

        async def send_with_no_cache(message):
            if message["type"] == "http.response.start":
                headers = [
                    (k, v) for k, v in message.get("headers", [])
                    if k.lower() not in (b"cache-control", b"pragma", b"expires")
                ]
                headers.append((b"cache-control", b"no-cache, no-store, must-revalidate"))
                headers.append((b"pragma", b"no-cache"))
                headers.append((b"expires", b"0"))
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_with_no_cache)

app.add_middleware(NoCacheMiddleware)
from backend.static_handler import CachedStaticFiles
app.mount("/static", CachedStaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

_house_lineup_lock = threading.Lock()

def auto_generate_house_lineup():
    if not _house_lineup_lock.acquire(blocking=False):
        print("[Auto] House lineup generation already in progress, skipping")
        return
    try:
        from sqlalchemy.orm import sessionmaker
        Session = sessionmaker(bind=engine)
        db = Session()
        today = get_eastern_today()
        existing = db.query(models.Contest).filter(models.Contest.slate_date == today).first()
        has_house_players = False
        if existing:
            has_house_players = db.query(models.HouseLineupPlayer).filter(
                models.HouseLineupPlayer.contest_id == existing.id
            ).count() > 0
        db.close()
        
        has_player_data = os.path.exists("dfs_players.csv") or data_access.use_postgres()
        if has_player_data and (not existing or not has_house_players):
            print("[Auto] Generating house lineup...")
            subprocess.run(
                [sys.executable, "generate_house_lineup.py", "--force"],
                timeout=120,
                capture_output=True,
                cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            )
            print("[Auto] House lineup generated successfully")
        else:
            print("[Auto] House lineup already exists for today")
    except Exception as e:
        print(f"[Auto] House lineup generation failed: {e}")
    finally:
        _house_lineup_lock.release()

@app.on_event("startup")
async def startup_event():
    try:
        from migrations.add_season_type import ensure_season_type_schema
        result = ensure_season_type_schema(verbose=False)
        if result.get("player_game_logs", 0) or result.get("matchup_history", 0):
            print(f"[startup] season_type migration: {result}")
    except Exception as e:
        print(f"[startup] season_type migration warning: {e}")

    try:
        from sqlalchemy import inspect as sa_inspect, text as sa_text
        from backend.database import engine as _eng
        insp = sa_inspect(_eng)
        if 'daily_pick_grades' in insp.get_table_names():
            existing_cols = {c['name'] for c in insp.get_columns('daily_pick_grades')}
            adds = []
            if 'archetype' not in existing_cols:
                adds.append("ADD COLUMN IF NOT EXISTS archetype VARCHAR(50)")
            if 'dva_edge' not in existing_cols:
                adds.append("ADD COLUMN IF NOT EXISTS dva_edge DOUBLE PRECISION")
            if 'usage_boost' not in existing_cols:
                adds.append("ADD COLUMN IF NOT EXISTS usage_boost DOUBLE PRECISION")
            if adds:
                with _eng.begin() as conn:
                    conn.execute(sa_text(f"ALTER TABLE daily_pick_grades {', '.join(adds)}"))
                print(f"[STARTUP] Added columns to daily_pick_grades: {adds}")
    except Exception as e:
        print(f"[STARTUP] daily_pick_grades migration skipped: {e}")

    # Task #45: Official Call Snapshot — add columns + backfill past slates so
    # historical W-L cards keep rendering against the locked snapshot. This block
    # re-imports its own `insp`/`_eng` so a failure in an earlier migration block
    # can't cause this one to silently skip.
    try:
        from sqlalchemy import inspect as sa_inspect, text as sa_text
        from backend.database import engine as _eng
        insp = sa_inspect(_eng)
        if 'daily_articles' in insp.get_table_names():
            existing_cols = {c['name'] for c in insp.get_columns('daily_articles')}
            adds = []
            if 'official_picks_json' not in existing_cols:
                adds.append("ADD COLUMN IF NOT EXISTS official_picks_json TEXT")
            if 'official_locked_at' not in existing_cols:
                adds.append("ADD COLUMN IF NOT EXISTS official_locked_at TIMESTAMP WITH TIME ZONE")
            if adds:
                with _eng.begin() as conn:
                    conn.execute(sa_text(f"ALTER TABLE daily_articles {', '.join(adds)}"))
                print(f"[STARTUP] Added columns to daily_articles: {adds}")
            # One-shot backfill: copy picks_json into official_picks_json for every
            # PAST slate (strictly < today's ET date) where the latter is NULL and
            # the former is non-empty. We compute the cutoff in ET (not UTC), so a
            # late-evening ET restart doesn't classify the in-progress slate as past
            # (CURRENT_DATE in Postgres is UTC and would be one day ahead of ET).
            # The locked-at timestamp is set to updated_at so the badge renders
            # sensibly for backfilled rows.
            from datetime import datetime as _dt
            from zoneinfo import ZoneInfo as _ZI
            et_today = _dt.now(_ZI("America/New_York")).date()
            with _eng.begin() as conn:
                result = conn.execute(sa_text("""
                    UPDATE daily_articles
                    SET official_picks_json = picks_json,
                        official_locked_at = COALESCE(updated_at, NOW())
                    WHERE official_picks_json IS NULL
                      AND picks_json IS NOT NULL
                      AND picks_json <> ''
                      AND picks_json <> '[]'
                      AND slate_date < :et_today
                """), {"et_today": et_today})
                if result.rowcount:
                    print(f"[STARTUP] Backfilled official_picks_json for {result.rowcount} past slates (ET cutoff: {et_today})")
    except Exception as e:
        print(f"[STARTUP] daily_articles official-call migration skipped: {e}")

    # Official Call Snapshot for WNBA — mirror the NBA daily_articles block so
    # WNBA picks are graded against a pre-tipoff locked snapshot, not the
    # mutable picks_json. Adds the columns and backfills past slates.
    try:
        from sqlalchemy import inspect as sa_inspect, text as sa_text
        from backend.database import engine as _eng
        insp = sa_inspect(_eng)
        if 'wnba_daily_articles' in insp.get_table_names():
            existing_cols = {c['name'] for c in insp.get_columns('wnba_daily_articles')}
            adds = []
            if 'official_picks_json' not in existing_cols:
                adds.append("ADD COLUMN IF NOT EXISTS official_picks_json TEXT")
            if 'official_locked_at' not in existing_cols:
                adds.append("ADD COLUMN IF NOT EXISTS official_locked_at TIMESTAMP WITH TIME ZONE")
            if adds:
                with _eng.begin() as conn:
                    conn.execute(sa_text(f"ALTER TABLE wnba_daily_articles {', '.join(adds)}"))
                print(f"[STARTUP] Added columns to wnba_daily_articles: {adds}")
            from datetime import datetime as _dt
            from zoneinfo import ZoneInfo as _ZI
            et_today = _dt.now(_ZI("America/New_York")).date()
            with _eng.begin() as conn:
                result = conn.execute(sa_text("""
                    UPDATE wnba_daily_articles
                    SET official_picks_json = picks_json,
                        official_locked_at = COALESCE(updated_at, NOW())
                    WHERE official_picks_json IS NULL
                      AND picks_json IS NOT NULL
                      AND picks_json <> ''
                      AND picks_json <> '[]'
                      AND slate_date < :et_today
                """), {"et_today": et_today})
                if result.rowcount:
                    print(f"[STARTUP] Backfilled official_picks_json for {result.rowcount} past WNBA slates (ET cutoff: {et_today})")
    except Exception as e:
        print(f"[STARTUP] wnba_daily_articles official-call migration skipped: {e}")

    thread = threading.Thread(target=auto_generate_house_lineup, daemon=True)
    thread.start()

def get_current_user(request: Request, db: Session = Depends(get_db)):
    token = request.cookies.get("session_token")
    if token:
        user_id = auth.get_session_user(db, token)
        if user_id:
            return db.query(models.User).filter(models.User.id == user_id).first()
    return None

def get_coach_rank(wins: int) -> dict:
    """Calculate coach rank based on total wins."""
    ranks = [
        {"name": "Champion", "min_wins": 500, "frame": "/static/avatars/frames/frame_champion.png"},
        {"name": "Grandmaster", "min_wins": 300, "frame": "/static/avatars/frames/frame_grandmaster.png"},
        {"name": "Master", "min_wins": 150, "frame": "/static/avatars/frames/frame_master.png"},
        {"name": "Diamond", "min_wins": 75, "frame": "/static/avatars/frames/frame_diamond.png"},
        {"name": "Gold", "min_wins": 35, "frame": "/static/avatars/frames/frame_gold.png"},
        {"name": "Silver", "min_wins": 15, "frame": "/static/avatars/frames/frame_silver.png"},
        {"name": "Bronze", "min_wins": 1, "frame": "/static/avatars/frames/frame_bronze.png"},
    ]
    for rank in ranks:
        if wins >= rank["min_wins"]:
            return rank
    return {"name": "Unranked", "frame": None}

def set_session_cookie(response: Response, token: str):
    response.set_cookie(
        "session_token", 
        token, 
        max_age=604800,
        httponly=True,
        samesite="lax",
        path="/"
    )

def html_redirect(url: str, token: str = None, extra_cookies: dict = None):
    response = RedirectResponse(url=url, status_code=303)
    if token:
        set_session_cookie(response, token)
    if extra_cookies:
        for k, v in extra_cookies.items():
            response.set_cookie(k, v["value"], max_age=v.get("max_age", 60), httponly=v.get("httponly", False))
    return response

def normalize_name(name):
    import unicodedata
    import re
    name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII')
    name = re.sub(r'\s+(Jr\.?|Sr\.?|II|III|IV)$', '', name, flags=re.IGNORECASE)
    return name.strip()

def get_player_headshots():
    headshots = {}
    name_aliases = {
        "Luka Doncic": "doncilu01",
        "Nikola Jokic": "jokicni01",
        "Bogdan Bogdanovic": "bogdabo01",
        "Bojan Bogdanovic": "bogdabo02",
        "Nikola Vucevic": "vlovenucevo01",
        "Jonas Valanciunas": "valanjo01",
        "Domantas Sabonis": "sabondo01",
        "Kristaps Porzingis": "paborni01",
    }
    for name, bbref_id in name_aliases.items():
        headshots[name] = f"https://www.basketball-reference.com/req/202106291/images/headshots/{bbref_id}.jpg"
    try:
        rows = data_access.get_player_headshots()
        for row in rows:
            original_name = row[0]
            url = row[1]
            headshots[original_name] = url
            normalized = normalize_name(original_name)
            if normalized != original_name:
                headshots[normalized] = url
            base_name = original_name.replace(" Jr.", "").replace(" Sr.", "").replace(" III", "").replace(" II", "").replace(" IV", "").strip()
            if base_name != original_name:
                headshots[base_name] = url
    except:
        pass
    return headshots

@app.get("/chart-screenshot/{chart_type}/{target}")
async def chart_screenshot_route(request: Request, chart_type: str, target: str):
    return templates.TemplateResponse("chart_screenshot.html", {
        "request": request, "chart_type": chart_type, "target": target
    })

def get_league(request: Request) -> str:
    """Resolve the active league: ?league= query param wins, else the
    pirtdica_league cookie, else 'nba'."""
    q = (request.query_params.get("league") or "").lower()
    if q in ("nba", "wnba"):
        return q
    c = (request.cookies.get("pirtdica_league") or "").lower()
    if c in ("nba", "wnba"):
        return c
    return "nba"


def _american_str(odds):
    try:
        v = int(odds)
    except (TypeError, ValueError):
        return ""
    return f"+{v}" if v > 0 else str(v)


def build_wnba_board(page_title: str, subtitle: str):
    """Shared WNBA view context built from real Odds API data (parallel
    wnba_* tables). Returns games for the nearest slate plus props grouped
    by stat. Honest empty state when no slate is live."""
    import pandas as pd
    games_df = data_access.get_wnba_games()
    props_df = data_access.get_wnba_props()

    slate_date = None
    games = []
    stat_groups = []
    today_str = get_eastern_today().isoformat()

    if games_df is not None and not games_df.empty and 'game_date' in games_df.columns:
        upcoming = sorted([d for d in games_df['game_date'].dropna().unique() if d >= today_str])
        all_dates = sorted([d for d in games_df['game_date'].dropna().unique()])
        slate_date = upcoming[0] if upcoming else (all_dates[-1] if all_dates else None)

    if slate_date:
        gsel = games_df[games_df['game_date'] == slate_date].copy()
        if 'commence_time' in gsel.columns:
            gsel = gsel.sort_values('commence_time')
        for _, g in gsel.iterrows():
            tip = ''
            try:
                dt = datetime.fromisoformat(str(g['commence_time']).replace('Z', '+00:00'))
                tip = dt.astimezone(EASTERN).strftime('%-I:%M %p ET')
            except Exception:
                tip = ''
            games.append({
                'away_team': g.get('away_team', ''),
                'home_team': g.get('home_team', ''),
                'tipoff': tip,
            })

        if props_df is not None and not props_df.empty and 'game_date' in props_df.columns:
            psel = props_df[props_df['game_date'] == slate_date].copy()
            stat_order = [('PTS', 'Points'), ('REB', 'Rebounds'), ('AST', 'Assists'), ('3PM', '3-Pointers')]
            for stat_key, stat_label in stat_order:
                rows = psel[psel['stat'] == stat_key]
                if rows.empty:
                    continue
                rows = rows.sort_values('line', ascending=False)
                lines = []
                for _, r in rows.iterrows():
                    lines.append({
                        'player': r.get('player_name', ''),
                        'line': r.get('line'),
                        'over': _american_str(r.get('over_odds')),
                        'under': _american_str(r.get('under_odds')),
                        'matchup': f"{r.get('away_team', '')} @ {r.get('home_team', '')}",
                    })
                stat_groups.append({'key': stat_key, 'label': stat_label, 'lines': lines})

    return {
        'page_title': page_title,
        'subtitle': subtitle,
        'slate_date': slate_date,
        'wnba_games': games,
        'wnba_stat_groups': stat_groups,
    }


def build_wnba_standings():
    """Return (east, west) WNBA standings in the same shape the home page uses
    for NBA, so templates/home.html renders WNBA without a separate template."""
    east, west = [], []
    df = data_access.get_wnba_standings()
    if df is None or df.empty:
        return east, west

    playing_names = set()
    try:
        games_df = data_access.get_wnba_games()
        today_str = get_eastern_today().isoformat()
        if games_df is not None and not games_df.empty and 'game_date' in games_df.columns:
            todays = games_df[games_df['game_date'] == today_str]
            for _, g in todays.iterrows():
                playing_names.add(str(g.get('home_team', '')))
                playing_names.add(str(g.get('away_team', '')))
    except Exception:
        pass

    df = df.sort_values('win_pct', ascending=False)
    for _, r in df.iterrows():
        nickname = str(r.get('team_name', ''))
        playing = bool(nickname) and any(nickname in pn for pn in playing_names)
        try:
            gb = float(r.get('games_behind', 0) or 0)
        except (TypeError, ValueError):
            gb = 0.0
        entry = {
            "team": r.get('team', ''),
            "team_name": nickname,
            "wins": int(r.get('wins', 0) or 0),
            "losses": int(r.get('losses', 0) or 0),
            "gb": gb,
            "win_pct": f"{float(r.get('win_pct', 0) or 0):.3f}",
            "playing": playing,
            "logo": r.get('logo', '') or '',
        }
        if str(r.get('conference', '')).startswith('E'):
            east.append(entry)
        else:
            west.append(entry)
    return east, west


def _render_wnba_articles(request: Request, user, db: Session):
    """Render the shared articles.html with REAL WNBA modeled content
    (Claude analysis + Pillow header + prop recs), mirroring the NBA page."""
    # Select the article for the ACTIVE WNBA slate (nearest upcoming games, else
    # the latest), matching build_wnba_board and the rest of the WNBA pages.
    # Ordering purely by slate_date.desc() surfaced a far-future slate (e.g. a
    # schedule gap jumping to 06-30) instead of tonight's games.
    active_slate = None
    try:
        from datetime import date as _date
        games_df = data_access.get_wnba_games()
        if games_df is not None and not games_df.empty and 'game_date' in games_df.columns:
            today_str = get_eastern_today().isoformat()
            dates = sorted(str(d) for d in games_df['game_date'].dropna().unique())
            upcoming = [d for d in dates if d >= today_str]
            chosen = upcoming[0] if upcoming else (dates[-1] if dates else None)
            if chosen:
                try:
                    active_slate = _date.fromisoformat(chosen)
                except Exception:
                    active_slate = None
    except Exception as e:
        print(f"[WNBA ARTICLES] active-slate calc failed: {e}")
        active_slate = None

    article = None
    try:
        q = db.query(models.WNBADailyArticle)
        # Always prefer TODAY's article when one exists // the active-slate
        # calc reads the local SQLite wnba_games table, which can be stale
        # (e.g. a deployment snapshot taken before the daily scrape ran) and
        # would otherwise pin the page to an old slate even though a fresh
        # article is already in the database.
        today_et = get_eastern_today()
        article = q.filter(models.WNBADailyArticle.slate_date == today_et).first()
        if not article and active_slate is not None:
            article = q.filter(models.WNBADailyArticle.slate_date == active_slate).first()
            if not article:
                # No article for the active slate yet // show the most recent one
                # at or before it, never a far-future slate.
                article = q.filter(models.WNBADailyArticle.slate_date <= active_slate).order_by(
                    models.WNBADailyArticle.slate_date.desc()
                ).first()
        if not article:
            article = q.order_by(models.WNBADailyArticle.slate_date.desc()).first()
    except Exception as e:
        print(f"[WNBA ARTICLES] DB query failed: {e}")
        article = None

    try:
        from backend.stripe_billing import has_picks_access
        has_access = has_picks_access(user, db)
    except Exception as e:
        print(f"[WNBA ARTICLES] Stripe check failed: {e}")
        has_access = False

    picks, analysis, prop_recs = [], [], []
    if article and has_access:
        try:
            if article.official_picks_json:
                picks = json.loads(article.official_picks_json)
            elif article.picks_json:
                picks = json.loads(article.picks_json)
        except (json.JSONDecodeError, TypeError):
            picks = []
        try:
            if article.analysis_json:
                analysis = json.loads(article.analysis_json)
        except (json.JSONDecodeError, TypeError):
            analysis = []
        try:
            import pandas as pd
            prop_csv = os.path.join(os.path.dirname(__file__), '..', 'wnba_prop_recommendations.csv')
            if os.path.exists(prop_csv):
                df = pd.read_csv(prop_csv)
                # Keep only props for the displayed article's slate // the recs
                # CSV spans multiple game_dates, so without this the props table
                # would mix in a different slate than the article header shows.
                if article and getattr(article, 'slate_date', None) is not None and 'game_date' in df.columns:
                    slate_str = article.slate_date.isoformat()
                    # Strict: only the displayed slate's props // if there are none
                    # for that slate, show an empty table rather than another slate.
                    df = df[df['game_date'].astype(str) == slate_str]
                if 'composite_score' in df.columns:
                    df = df.sort_values('composite_score', ascending=False)
                for _, row in df.head(20).iterrows():
                    book_line = row.get('book_line')
                    proj = row.get('projected_value', row.get('player_avg', 0))
                    avg = row.get('player_avg', 0)
                    edge = row.get('vs_book_edge')
                    edge_str = f"+{edge}%" if edge and edge > 0 else f"{edge}%" if edge else ""
                    prop_recs.append({
                        'player': row.get('player', ''),
                        'team': row.get('team', ''),
                        'opponent': row.get('opponent', ''),
                        'stat': row.get('stat', ''),
                        'avg': round(avg, 1) if pd.notna(avg) else '',
                        'line': round(book_line, 1) if pd.notna(book_line) else '',
                        'projected': round(proj, 1) if pd.notna(proj) else '',
                        'edge': edge_str,
                        'pick': row.get('recommendation', ''),
                        'confidence': row.get('confidence', 'LOW'),
                        'hit_rate': f"{row['hit_rate']:.0f}%" if pd.notna(row.get('hit_rate')) else '',
                        'cv': f"{row['cv']:.2f}" if pd.notna(row.get('cv')) else '',
                        'composite': round(row.get('composite_score', 0), 1) if pd.notna(row.get('composite_score')) else '',
                    })
        except Exception as e:
            print(f"[WNBA ARTICLES] Prop recs load failed: {e}")

    grading_report = []
    if article and has_access:
        try:
            from datetime import timedelta as _td
            from utils.timezone import get_eastern_today as _get_et_today
            yesterday = _get_et_today() - _td(days=1)
            grades = db.query(models.WNBADailyPickGrade).filter(
                models.WNBADailyPickGrade.slate_date == yesterday
            ).all()
            for g in grades:
                grading_report.append({
                    'player': g.player,
                    'stat': g.stat,
                    'book_line': g.book_line,
                    'direction': g.direction,
                    'projected': g.projected,
                    'actual': g.actual,
                    'hit': g.hit,
                    'analysis': g.claude_analysis or '',
                })
        except Exception as e:
            print(f"[WNBA ARTICLES] Grading report load failed: {e}")

    official_locked = bool(article and article.official_locked_at)
    official_locked_at_str = None
    if official_locked:
        try:
            from utils.timezone import EASTERN as _EASTERN
            official_locked_at_str = article.official_locked_at.astimezone(_EASTERN).strftime('%-I:%M %p ET')
        except Exception:
            official_locked_at_str = article.official_locked_at.strftime('%H:%M ET')

    return templates.TemplateResponse("articles.html", {
        "request": request,
        "user": user,
        "article": article,
        "picks": picks,
        "analysis": analysis,
        "has_access": has_access,
        "pre_lock": False,
        "prop_recs": prop_recs,
        "grading_report": grading_report,
        "official_locked": official_locked,
        "official_locked_at_str": official_locked_at_str,
        "league": "wnba",
    })


def _render_wnba_trends(request: Request, user, db: Session):
    """Render the shared trends.html with WNBA chart images (value/upside/DVP).
    Charts with no WNBA data source (referee, play-types, shot-zone tracking,
    archetype clusters) are gated off in the template via league == 'wnba'."""
    import os as _os
    import time as _time
    from datetime import datetime as _dt
    try:
        from backend.stripe_billing import has_statpack_access
        has_access = has_statpack_access(user, db)
    except Exception as e:
        print(f"[WNBA TRENDS] Stripe check failed: {e}")
        has_access = False
    if not has_access:
        return templates.TemplateResponse("trends_paywall.html", {
            "request": request, "user": user,
        })

    chart_files = [
        "static/images/wnba_value_chart.png",
        "static/images/wnba_upside_chart.png",
        "static/images/wnba_dvp_heatmap.png",
        "static/images/wnba_dvp_position_heatmap.png",
    ]
    mtimes = [_os.path.getmtime(f) for f in chart_files if _os.path.exists(f)]
    charts_last_updated, charts_stale = None, True
    if mtimes:
        charts_last_updated = _dt.fromtimestamp(max(mtimes), tz=EASTERN)
        charts_stale = charts_last_updated.date() < get_eastern_today()

    # Prop recommendations table (same columns the NBA table uses; the WNBA CSV
    # already matches the template field names player/opponent/stat/player_avg/
    # projected_value/book_line/vs_book_edge/confidence/hit_rate/recommendation/
    # composite_score).
    props = []
    try:
        import pandas as _pd
        prop_csv = _os.path.join(_os.path.dirname(__file__), '..', 'wnba_prop_recommendations.csv')
        if _os.path.exists(prop_csv):
            pdf = _pd.read_csv(prop_csv)
            if 'composite_score' in pdf.columns:
                pdf = pdf.sort_values('composite_score', ascending=False)
            props = pdf.head(15).to_dict('records')
    except Exception as e:
        print(f"[WNBA TRENDS] prop table load failed: {e}")

    # Player Explorer rows: every player with a WNBA shot-zone profile, plus
    # position from wnba_player_stats (G/F/C). No archetype/creation columns //
    # there is no WNBA tracking source, mirroring how WNBA articles use position.
    explorer_players = []
    try:
        import sqlite3 as _sq
        _conn = _sq.connect("dfs_nba.db")
        _tables = {r[0] for r in _conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "wnba_player_shot_zones" in _tables:
            pos_map = {}
            if "wnba_player_stats" in _tables:
                for nm, ps in _conn.execute(
                        "SELECT player_name, position FROM wnba_player_stats"):
                    if nm:
                        pos_map[nm] = (ps or "").strip()
            for nm, tm in _conn.execute(
                    "SELECT player_name, team FROM wnba_player_shot_zones "
                    "ORDER BY player_name"):
                if not nm:
                    continue
                explorer_players.append({
                    "player_name": nm,
                    "true_position": pos_map.get(nm, ""),
                    "team": tm or "",
                    "opponent": "",
                    "injury_status": "",
                })
        _conn.close()
    except Exception as e:
        print(f"[WNBA TRENDS] explorer build failed: {e}")

    # Slump Risk Engine: rotation players most likely to cool off, with the
    # observable signals that drove the flag. WNBA-only // there is no NBA
    # equivalent table, so the section is gated to league == 'wnba'.
    slump_risk = []
    try:
        srdf = data_access.get_wnba_slump_risk()
        if srdf is not None and not srdf.empty:
            srdf = srdf[srdf['risk_level'].isin(['HIGH', 'MODERATE'])].head(20)
            for _, r in srdf.iterrows():
                try:
                    fac = json.loads(r.get('factors_json') or '[]')
                except (json.JSONDecodeError, TypeError):
                    fac = []
                rest = r.get('rest_days')
                slump_risk.append({
                    'player': r.get('player_name', ''),
                    'team': r.get('team', ''),
                    'position': r.get('position', ''),
                    'score': r.get('overall_score'),
                    'level': r.get('risk_level', ''),
                    'opponent': r.get('next_opponent') or '',
                    'rest_days': int(rest) if rest is not None and rest == rest else None,
                    'factors': fac,
                    'narrative': r.get('narrative') or '',
                })
    except Exception as e:
        print(f"[WNBA TRENDS] slump risk load failed: {e}")

    return templates.TemplateResponse("trends.html", {
        "request": request,
        "user": user,
        "top_value": [],
        "props": props,
        "targeted": [],
        "slump_risk": slump_risk,
        "ref_chart_exists": _os.path.exists("static/images/wnba_ref_foul_chart.png"),
        "cache_bust": int(_time.time()),
        "explorer_players": explorer_players,
        "headshots": {},
        "charts_last_updated": charts_last_updated,
        "charts_stale": charts_stale,
        "league": "wnba",
        "chart_pre": "wnba_",
        "dvp_position_chart_exists": _os.path.exists(
            "static/images/wnba_dvp_position_heatmap.png"),
    })


@app.get("/articles")
async def articles_page(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    league = get_league(request)
    if league == "wnba":
        resp = _render_wnba_articles(request, user, db)
        resp.set_cookie("pirtdica_league", "wnba", max_age=60 * 60 * 24 * 30, samesite="lax")
        return resp
    today = get_eastern_today()
    try:
        article = db.query(models.DailyArticle).filter(
            models.DailyArticle.slate_date == today
        ).first()
        if not article:
            article = db.query(models.DailyArticle).order_by(
                models.DailyArticle.slate_date.desc()
            ).first()
    except Exception as e:
        print(f"[ARTICLES] DB query failed: {e}")
        article = None
    picks = []
    analysis = []
    try:
        from backend.stripe_billing import has_picks_access
        has_access = has_picks_access(user, db)
    except Exception as e:
        print(f"[ARTICLES] Stripe check failed: {e}")
        has_access = False
    pre_lock = False
    PREGAME_REFRESH_ET = 18, 15
    PREGAME_FALLBACK_ET = 19, 30
    prop_recs = []
    if article and has_access:
        from zoneinfo import ZoneInfo
        now_et = get_eastern_now()
        is_todays_article = article.slate_date == now_et.date()
        if is_todays_article:
            refresh_time = now_et.replace(hour=PREGAME_REFRESH_ET[0], minute=PREGAME_REFRESH_ET[1], second=0, microsecond=0)
            fallback_time = now_et.replace(hour=PREGAME_FALLBACK_ET[0], minute=PREGAME_FALLBACK_ET[1], second=0, microsecond=0)
            article_refreshed = False
            if article.updated_at:
                article_updated = article.updated_at
                if article_updated.tzinfo is None:
                    article_updated = article_updated.replace(tzinfo=ZoneInfo("UTC"))
                article_updated_et = article_updated.astimezone(EASTERN)
                if article_updated_et >= refresh_time:
                    article_refreshed = True
            in_pre_lock_window = now_et >= refresh_time and now_et < fallback_time
            if not article_refreshed and in_pre_lock_window:
                pre_lock = True

        if not pre_lock:
            try:
                if article.picks_json:
                    picks = json.loads(article.picks_json)
            except (json.JSONDecodeError, TypeError):
                picks = []
            # Task #45: Once the official call is locked, render the locked
            # snapshot so subscribers see EXACTLY what was frozen pre-tipoff
            # (which is also what gets graded). Before lock we keep showing
            # the working picks_json so the page is never blank.
            if article.official_picks_json:
                try:
                    picks = json.loads(article.official_picks_json)
                except (json.JSONDecodeError, TypeError):
                    pass
            try:
                if article.analysis_json:
                    analysis = json.loads(article.analysis_json)
            except (json.JSONDecodeError, TypeError):
                analysis = []
            try:
                import pandas as pd
                prop_csv = os.path.join(os.path.dirname(__file__), '..', 'prop_recommendations.csv')
                if os.path.exists(prop_csv):
                    df = pd.read_csv(prop_csv)
                    if 'composite_score' in df.columns:
                        df = df.sort_values('composite_score', ascending=False)
                    top = df.head(20)
                    for _, row in top.iterrows():
                        book_line = row.get('book_line')
                        proj = row.get('projected_value', row.get('adjusted_avg', 0))
                        avg = row.get('player_avg', 0)
                        edge = row.get('vs_book_edge')
                        edge_str = f"+{edge}%" if edge and edge > 0 else f"{edge}%" if edge else ""
                        prop_recs.append({
                            'player': row.get('player', ''),
                            'team': row.get('team', ''),
                            'opponent': row.get('opponent', ''),
                            'stat': row.get('stat', ''),
                            'avg': round(avg, 1) if pd.notna(avg) else '',
                            'line': round(book_line, 1) if pd.notna(book_line) else '',
                            'projected': round(proj, 1) if pd.notna(proj) else '',
                            'edge': edge_str,
                            'pick': row.get('recommendation', ''),
                            'confidence': row.get('confidence', 'LOW'),
                            'hit_rate': f"{row['hit_rate']:.0f}%" if pd.notna(row.get('hit_rate')) else '',
                            'cv': f"{row['cv']:.2f}" if pd.notna(row.get('cv')) else '',
                            'composite': round(row.get('composite_score', 0), 1) if pd.notna(row.get('composite_score')) else '',
                        })
            except Exception as e:
                print(f"[ARTICLES] Prop recs load failed: {e}")
    grading_report = []
    if article and has_access and not pre_lock:
        try:
            from datetime import timedelta as _td
            yesterday = today - _td(days=1)
            grades = db.query(models.DailyPickGrade).filter(
                models.DailyPickGrade.slate_date == yesterday
            ).all()
            for g in grades:
                grading_report.append({
                    'player': g.player,
                    'stat': g.stat,
                    'book_line': g.book_line,
                    'direction': g.direction,
                    'projected': g.projected,
                    'actual': g.actual,
                    'hit': g.hit,
                    'analysis': g.claude_analysis or '',
                })
        except Exception as e:
            print(f"[ARTICLES] Grading report load failed: {e}")

    # Task #45: surface the official-call lock state for the badge + hint above
    # the picks card. `official_locked_at` is stored as TIMESTAMPTZ so it comes
    # back as an aware UTC datetime // convert to ET for display.
    official_locked = bool(article and article.official_locked_at)
    official_locked_at_str = None
    if official_locked:
        try:
            from utils.timezone import EASTERN as _EASTERN
            official_locked_at_str = article.official_locked_at.astimezone(_EASTERN).strftime('%-I:%M %p ET')
        except Exception:
            official_locked_at_str = article.official_locked_at.strftime('%H:%M ET')
    return templates.TemplateResponse("articles.html", {
        "request": request,
        "user": user,
        "article": article,
        "picks": picks,
        "analysis": analysis,
        "has_access": has_access,
        "pre_lock": pre_lock,
        "prop_recs": prop_recs,
        "grading_report": grading_report,
        "official_locked": official_locked,
        "official_locked_at_str": official_locked_at_str,
    })

@app.get("/subscribe")
async def subscribe_page(request: Request, db: Session = Depends(get_db)):
    from backend.stripe_billing import create_checkout_session, has_any_subscription, _load_stripe_keys
    user = get_current_user(request, db)
    if not user:
        return html_redirect("/login")
    keys = _load_stripe_keys()
    if not keys.get("secret"):
        print("[Stripe] ERROR: No Stripe secret key configured")
        return templates.TemplateResponse("error.html", {
            "request": request, "user": user,
            "error_title": "Payment System Unavailable",
            "error_message": "Subscriptions are temporarily unavailable. Please try again later.",
        }, status_code=503)
    plan_key = request.query_params.get("plan", "picks")
    if plan_key not in ("picks", "statpack", "bundle"):
        plan_key = "picks"
    cancel_map = {"picks": "/articles", "statpack": "/trends", "bundle": "/"}
    base_url = str(request.base_url).rstrip("/")
    if base_url.startswith("http://") and request.headers.get("x-forwarded-proto") == "https":
        base_url = base_url.replace("http://", "https://", 1)
    try:
        session, customer_id = create_checkout_session(
            user,
            success_url=f"{base_url}/subscribe/success?session_id={{CHECKOUT_SESSION_ID}}&plan={plan_key}",
            cancel_url=f"{base_url}{cancel_map.get(plan_key, '/')}",
            plan_key=plan_key,
        )
    except Exception as e:
        import traceback
        print(f"[Stripe] Checkout error: {type(e).__name__}: {e}")
        traceback.print_exc()
        return templates.TemplateResponse("error.html", {
            "request": request, "user": user,
            "error_title": "Payment Error",
            "error_message": "Something went wrong connecting to our payment provider. Please try again.",
        }, status_code=500)
    if user.stripe_customer_id != customer_id:
        user.stripe_customer_id = customer_id
        db.commit()
    return html_redirect(session.url)


@app.get("/subscribe/success")
async def subscribe_success(request: Request, db: Session = Depends(get_db)):
    from backend.stripe_billing import (get_stripe_client, resolve_plan_from_subscription,
                                         upsert_user_subscription, cancel_individual_subs_for_bundle,
                                         sync_user_subscription_fields, get_subscription_period_end,
                                         _so_get)
    user = get_current_user(request, db)
    if not user:
        print(f"[Stripe] SUCCESS redirect: user not logged in (session lost during Stripe redirect)")
        return html_redirect("/login")
    plan_param = request.query_params.get("plan", "picks")
    redirect_map = {"picks": "/articles", "statpack": "/trends", "bundle": "/"}
    session_id = request.query_params.get("session_id")
    if not session_id:
        print(f"[Stripe] SUCCESS redirect: no session_id in URL for user {user.id} ({user.username})")
        return html_redirect(redirect_map.get(plan_param, "/"))
    client = get_stripe_client()
    try:
        session = client.checkout.Session.retrieve(session_id)
        print(f"[Stripe] SUCCESS redirect: user={user.id} ({user.username}), session={session_id}, payment_status={session.payment_status}, subscription={session.subscription}, customer={session.customer}")
        session_user_id = _so_get(_so_get(session, "metadata"), "user_id", "")
        if str(user.id) != str(session_user_id):
            print(f"[Stripe] Session user_id mismatch: session={session_user_id}, logged_in={user.id}")
            return html_redirect("/articles")
        if not session.subscription:
            print(f"[Stripe] SUCCESS redirect: no subscription on session (payment_status={session.payment_status})")
            return html_redirect(redirect_map.get(plan_param, "/"))
        sub, plan_key = resolve_plan_from_subscription(client, session.subscription)
        print(f"[Stripe] Resolved plan={plan_key}, sub_status={sub.status}, sub_id={sub.id}")
        user.stripe_customer_id = session.customer
        period_end_ts = get_subscription_period_end(sub)
        period_end = datetime.fromtimestamp(period_end_ts) if period_end_ts else None
        _, is_new = upsert_user_subscription(db, user.id, sub.id, plan_key, sub.status, period_end)
        sync_user_subscription_fields(db, user, plan_key, sub.id, sub.status, period_end_ts)
        if plan_key == "bundle":
            cancel_individual_subs_for_bundle(db, user.id, sub.id)
        if is_new:
            from backend.events import emit_subscription_activated
            emit_subscription_activated(db, user.id, user.username, plan_key)
        db.commit()
        print(f"[Stripe] SUCCESS: Activated {plan_key} for user {user.id} ({user.username}), is_new={is_new}")
        if is_new:
            from backend.email_service import process_email_queue
            try:
                process_email_queue(db)
            except Exception:
                pass
    except Exception as e:
        import traceback
        print(f"[Stripe] ERROR in success redirect for user {user.id} ({user.username}): {type(e).__name__}: {e}")
        traceback.print_exc()
    return html_redirect(redirect_map.get(plan_param, "/"))


@app.post("/stripe/webhook")
async def stripe_webhook(request: Request):
    from backend.stripe_billing import construct_webhook_event
    from backend.database import SessionLocal
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")
    try:
        event = construct_webhook_event(payload, sig_header)
    except Exception as e:
        print(f"[Stripe Webhook] Verification failed: {e}")
        return JSONResponse({"error": "Invalid signature"}, status_code=400)

    print(f"[Stripe Webhook] Received event: {event.type} (id={event.id})")
    db = SessionLocal()
    try:
        from backend.stripe_billing import (get_stripe_client, resolve_plan_from_subscription,
                                             upsert_user_subscription, cancel_individual_subs_for_bundle,
                                             sync_user_subscription_fields, get_subscription_period_end,
                                             _so_get)

        if event.type == "checkout.session.completed":
            session = event.data.object
            customer_id = _so_get(session, "customer")
            subscription_id = _so_get(session, "subscription")
            metadata = _so_get(session, "metadata")
            metadata_user_id = _so_get(metadata, "user_id")
            metadata_plan = _so_get(metadata, "plan")
            print(f"[Stripe Webhook] checkout.session.completed: customer={customer_id}, sub={subscription_id}, metadata_user_id={metadata_user_id}, metadata_plan={metadata_plan}")
            if customer_id and subscription_id:
                user = db.query(models.User).filter(models.User.stripe_customer_id == customer_id).first()
                if not user and metadata_user_id:
                    try:
                        user = db.query(models.User).filter(models.User.id == int(metadata_user_id)).first()
                        if user:
                            print(f"[Stripe Webhook] User found via metadata user_id={metadata_user_id} (username={user.username})")
                            user.stripe_customer_id = customer_id
                    except (ValueError, TypeError):
                        pass
                if not user:
                    print(f"[Stripe Webhook] ERROR: Could not find user for customer={customer_id}, metadata_user_id={metadata_user_id}. Subscription {subscription_id} is ORPHANED.")
                if user:
                    client = get_stripe_client()
                    sub, plan_key = resolve_plan_from_subscription(client, subscription_id)
                    period_end_ts = get_subscription_period_end(sub)
                    period_end = datetime.fromtimestamp(period_end_ts) if period_end_ts else None
                    _, is_new = upsert_user_subscription(db, user.id, subscription_id, plan_key, sub.status, period_end)
                    sync_user_subscription_fields(db, user, plan_key, subscription_id, sub.status, period_end_ts)
                    if plan_key == "bundle":
                        cancel_individual_subs_for_bundle(db, user.id, subscription_id)
                    if is_new:
                        from backend.events import emit_subscription_activated
                        emit_subscription_activated(db, user.id, user.username, plan_key)
                    db.commit()
                    print(f"[Stripe Webhook] SUCCESS: Activated {plan_key} for user {user.id} ({user.username})")
            else:
                print(f"[Stripe Webhook] checkout.session.completed missing customer_id or subscription_id")

        elif event.type in ("invoice.payment_succeeded", "customer.subscription.updated"):
            sub_data = event.data.object
            if event.type == "invoice.payment_succeeded":
                subscription_id = _so_get(sub_data, "subscription")
                customer_id = _so_get(sub_data, "customer")
            else:
                subscription_id = _so_get(sub_data, "id")
                customer_id = _so_get(sub_data, "customer")
            print(f"[Stripe Webhook] {event.type}: sub={subscription_id}, customer={customer_id}")
            if subscription_id:
                from backend.models import UserSubscription
                user_sub = db.query(UserSubscription).filter(
                    UserSubscription.stripe_subscription_id == subscription_id
                ).first()
                user = None
                if user_sub:
                    user = db.query(models.User).filter(models.User.id == user_sub.user_id).first()
                if not user:
                    user = db.query(models.User).filter(models.User.stripe_subscription_id == subscription_id).first()
                if not user and customer_id:
                    user = db.query(models.User).filter(models.User.stripe_customer_id == customer_id).first()
                if not user:
                    print(f"[Stripe Webhook] WARNING: No user found for {event.type} sub={subscription_id}, customer={customer_id}")
                if user:
                    client = get_stripe_client()
                    sub, plan_key = resolve_plan_from_subscription(client, subscription_id)
                    period_end_ts = get_subscription_period_end(sub)
                    period_end = datetime.fromtimestamp(period_end_ts) if period_end_ts else None
                    upsert_user_subscription(db, user.id, subscription_id, plan_key, sub.status, period_end)
                    sync_user_subscription_fields(db, user, plan_key, subscription_id, sub.status, period_end_ts)
                    db.commit()
                    print(f"[Stripe Webhook] SUCCESS: Updated {plan_key} for user {user.id} ({user.username})")

        elif event.type == "customer.subscription.deleted":
            sub_data = event.data.object
            subscription_id = _so_get(sub_data, "id")
            print(f"[Stripe Webhook] customer.subscription.deleted: sub={subscription_id}")
            if subscription_id:
                from backend.models import UserSubscription
                user_sub = db.query(UserSubscription).filter(
                    UserSubscription.stripe_subscription_id == subscription_id
                ).first()
                if user_sub:
                    user_sub.status = "canceled"
                    user = db.query(models.User).filter(models.User.id == user_sub.user_id).first()
                    if user and user.stripe_subscription_id == subscription_id:
                        user.subscription_status = "canceled"
                    db.commit()
                    print(f"[Stripe Webhook] SUCCESS: Canceled subscription for user {user_sub.user_id}")
                else:
                    user = db.query(models.User).filter(models.User.stripe_subscription_id == subscription_id).first()
                    if user:
                        user.subscription_status = "canceled"
                        db.commit()
                        print(f"[Stripe Webhook] SUCCESS: Canceled legacy subscription for user {user.id}")
                    else:
                        print(f"[Stripe Webhook] WARNING: No user found for deleted subscription {subscription_id}")
        else:
            print(f"[Stripe Webhook] Unhandled event type: {event.type}")
    except Exception as e:
        import traceback
        print(f"[Stripe Webhook] Error processing {event.type}: {e}")
        traceback.print_exc()
        db.rollback()
        db.close()
        return JSONResponse({"received": True, "error": str(e)})
    finally:
        db.close()

    return JSONResponse({"received": True})


@app.post("/billing/recover")
async def billing_recover(request: Request, db: Session = Depends(get_db)):
    from backend.stripe_billing import (get_stripe_client, resolve_plan_from_subscription,
                                         upsert_user_subscription, sync_user_subscription_fields,
                                         get_subscription_period_end)
    user = get_current_user(request, db)
    if not user:
        return JSONResponse({"error": "Not logged in"}, status_code=401)
    if not user.stripe_customer_id:
        return JSONResponse({"error": "No Stripe customer on file", "recovered": False})
    try:
        client = get_stripe_client()
        subs = client.Subscription.list(customer=user.stripe_customer_id, status="active", limit=10)
        recovered = []
        for stripe_sub in subs.data:
            sub, plan_key = resolve_plan_from_subscription(client, stripe_sub.id)
            period_end_ts = get_subscription_period_end(sub)
            period_end = datetime.fromtimestamp(period_end_ts) if period_end_ts else None
            _, is_new = upsert_user_subscription(db, user.id, stripe_sub.id, plan_key, sub.status, period_end)
            sync_user_subscription_fields(db, user, plan_key, stripe_sub.id, sub.status, period_end_ts)
            recovered.append(plan_key)
        db.commit()
        if recovered:
            print(f"[Stripe Recovery] Recovered {recovered} for user {user.id} ({user.username})")
        return JSONResponse({"recovered": bool(recovered), "plans": recovered})
    except Exception as e:
        print(f"[Stripe Recovery] Error for user {user.id}: {e}")
        return JSONResponse({"error": "Recovery check failed", "recovered": False})


@app.get("/billing")
async def billing_page(request: Request, db: Session = Depends(get_db)):
    from backend.stripe_billing import PLANS, PLAN_DISPLAY_NAMES
    from backend.models import UserSubscription
    user = get_current_user(request, db)
    if not user:
        return html_redirect("/login")

    subs_db = db.query(UserSubscription).filter(
        UserSubscription.user_id == user.id,
        UserSubscription.status.in_(["active", "canceled"]),
    ).order_by(UserSubscription.created_at.desc()).all()

    active_subs = [s for s in subs_db if s.status == "active"]
    if not active_subs and user.stripe_customer_id:
        try:
            from backend.stripe_billing import (get_stripe_client, resolve_plan_from_subscription,
                                                 upsert_user_subscription, sync_user_subscription_fields,
                                                 get_subscription_period_end)
            client = get_stripe_client()
            stripe_subs = client.Subscription.list(customer=user.stripe_customer_id, status="active", limit=10)
            for stripe_sub in stripe_subs.data:
                sub, plan_key = resolve_plan_from_subscription(client, stripe_sub.id)
                period_end_ts = get_subscription_period_end(sub)
                period_end = datetime.fromtimestamp(period_end_ts) if period_end_ts else None
                upsert_user_subscription(db, user.id, stripe_sub.id, plan_key, sub.status, period_end)
                sync_user_subscription_fields(db, user, plan_key, stripe_sub.id, sub.status, period_end_ts)
            db.commit()
            subs_db = db.query(UserSubscription).filter(
                UserSubscription.user_id == user.id,
                UserSubscription.status.in_(["active", "canceled"]),
            ).order_by(UserSubscription.created_at.desc()).all()
            print(f"[Stripe Recovery] Auto-recovered subscriptions for user {user.id} on billing page")
        except Exception as e:
            print(f"[Stripe Recovery] Auto-recovery failed for user {user.id}: {e}")

    subscriptions = []
    for s in subs_db:
        plan_info = PLANS.get(s.plan, {})
        amount = plan_info.get("amount", 0)
        interval = plan_info.get("interval", "month")
        price_str = f"${amount / 100:.0f}/{interval}" if amount else ""
        is_stripe_managed = bool(s.stripe_subscription_id and s.stripe_subscription_id.startswith("sub_"))
        subscriptions.append({
            "display_name": PLAN_DISPLAY_NAMES.get(s.plan, s.plan),
            "description": plan_info.get("description", ""),
            "status": s.status,
            "period_end": s.current_period_end.strftime("%B %-d, %Y") if s.current_period_end else None,
            "price": price_str,
            "plan_key": s.plan,
            "is_stripe_managed": is_stripe_managed,
        })

    any_stripe_managed = any(sub["is_stripe_managed"] for sub in subscriptions)

    available_plans = []
    for key, plan in PLANS.items():
        available_plans.append({
            "key": key,
            "name": plan["name"],
            "description": plan["description"],
            "price": f"${plan['amount'] / 100:.0f}/{plan['interval']}",
        })

    return templates.TemplateResponse("billing.html", {
        "request": request,
        "user": user,
        "subscriptions": subscriptions,
        "available_plans": available_plans,
    })


@app.get("/billing/portal")
async def billing_portal_redirect(request: Request, db: Session = Depends(get_db)):
    from backend.stripe_billing import create_billing_portal_session
    user = get_current_user(request, db)
    if not user:
        return html_redirect("/login")
    if not user.stripe_customer_id:
        return html_redirect("/billing")
    base_url = str(request.base_url).rstrip("/")
    if base_url.startswith("http://") and request.headers.get("x-forwarded-proto") == "https":
        base_url = base_url.replace("http://", "https://", 1)
    try:
        session = create_billing_portal_session(user.stripe_customer_id, f"{base_url}/billing")
        return html_redirect(session.url)
    except Exception as e:
        err_str = str(e)
        print(f"[Stripe] Portal session error for user {user.id}: {e}")
        if "No such customer" in err_str or "resource_missing" in err_str:
            user.stripe_customer_id = None
            db.commit()
            print(f"[Stripe] Cleared stale stripe_customer_id for user {user.id}")
            return templates.TemplateResponse("error.html", {
                "request": request, "user": user,
                "error_title": "No Stripe Subscription Found",
                "error_message": "This account doesn't have an active Stripe-managed subscription to edit. If you have a complimentary or manually-granted plan, contact support to make changes. Otherwise, head back to Billing to start a new subscription.",
            }, status_code=400)
        if "configuration" in err_str.lower():
            return templates.TemplateResponse("error.html", {
                "request": request, "user": user,
                "error_title": "Customer Portal Not Configured",
                "error_message": "The Stripe Customer Portal needs to be activated by an admin (Stripe Dashboard \u2192 Settings \u2192 Billing \u2192 Customer Portal). Please try again shortly.",
            }, status_code=503)
        return templates.TemplateResponse("error.html", {
            "request": request, "user": user,
            "error_title": "Billing Portal Unavailable",
            "error_message": "Could not connect to the billing portal. Please try again in a moment.",
        }, status_code=503)


def _build_edge_insights(props_df):
    """Top-3 prop edges (by absolute edge%) for the home page 'Tonight's Edge'
    ticker. Works for both NBA and WNBA recs // skips the salary filter when the
    frame has no salary column (WNBA recs do not carry salary)."""
    import math
    insights = []
    try:
        if props_df is None or props_df.empty:
            return insights
        if 'salary' in props_df.columns:
            props_df = props_df[props_df['salary'] > 0]
        has_book = 'vs_book_edge' in props_df.columns and props_df['vs_book_edge'].notna().any()
        edge_col = 'vs_book_edge' if has_book else 'edge_pct'
        if edge_col not in props_df.columns:
            return insights
        valid = props_df[props_df[edge_col].notna() & props_df[edge_col].apply(
            lambda x: not (isinstance(x, float) and math.isnan(x)))]
        if valid.empty:
            return insights
        valid_abs = valid.copy()
        valid_abs['_abs_edge'] = valid_abs[edge_col].abs()
        for _, row in valid_abs.nlargest(3, '_abs_edge').iterrows():
            book_line = row.get('book_line', None)
            line_display = ''
            if book_line is not None and not (isinstance(book_line, float) and math.isnan(book_line)):
                line_display = str(book_line)
            elif row.get('adjusted_avg') is not None and not (
                    isinstance(row.get('adjusted_avg'), float) and math.isnan(row.get('adjusted_avg', 0))):
                line_display = f"proj {row['adjusted_avg']}"
            edge_val = row.get(edge_col, 0)
            if isinstance(edge_val, float) and math.isnan(edge_val):
                edge_val = 0
            insights.append({
                "player": row.get('player', row.get('player_name', '')),
                "stat": row.get('stat', ''),
                "line": line_display,
                "edge": round(float(edge_val), 1),
                "rec": row.get('recommendation', 'OVER'),
                "opponent": row.get('opponent', ''),
            })
    except Exception:
        return insights
    return insights


def _nba_slate_is_today():
    """True only when tonight's NBA odds were scraped today (ET). Leftover
    game_odds rows from a past slate keep a stale away/home pair around through
    the NBA offseason, so a plain row-count is not enough // we compare the
    freshest scraped_at date to today before trusting the NBA slate.

    scraped_at is written as naive UTC (scrape_game_odds.py uses
    datetime.utcnow().isoformat()), so we MUST convert UTC -> ET before comparing
    dates // a raw string slice would mark an evening-scraped fresh slate (UTC
    date already rolled to tomorrow) as stale and wrongly fall back to WNBA."""
    try:
        from zoneinfo import ZoneInfo as _ZI
        et = _ZI("America/New_York")
        utc = _ZI("UTC")
        et_today = datetime.now(et).date()
        latest = None
        if data_access.use_postgres():
            from backend.database import engine as pg_engine
            from sqlalchemy import text as sa_text
            try:
                with pg_engine.connect() as pg_conn:
                    latest = pg_conn.execute(sa_text("SELECT MAX(scraped_at) FROM game_odds_live")).scalar()
            except Exception:
                latest = None
        if latest is None:
            import sqlite3 as sl3
            conn_g = sl3.connect("dfs_nba.db")
            try:
                latest = conn_g.execute("SELECT MAX(scraped_at) FROM game_odds").fetchone()[0]
            except Exception:
                latest = None
            finally:
                conn_g.close()
        if not latest:
            return False
        if isinstance(latest, datetime):
            dt = latest
        else:
            dt = datetime.fromisoformat(str(latest).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=utc)
        return dt.astimezone(et).date() == et_today
    except Exception:
        return False


def _wnba_active_slate_recs(recs_df):
    """Filter a WNBA recs frame to the active slate (earliest game_date that is
    today-or-later in ET, else the most recent). The recs CSV can span multiple
    slates, so without this the ticker could show a future day's props."""
    try:
        if recs_df is None or recs_df.empty or 'game_date' not in recs_df.columns:
            return recs_df
        from zoneinfo import ZoneInfo as _ZI
        et_today = datetime.now(_ZI("America/New_York")).date()
        dates = sorted({
            datetime.strptime(str(d), "%Y-%m-%d").date()
            for d in recs_df['game_date'].dropna().unique()
        })
        if not dates:
            return recs_df
        upcoming = [d for d in dates if d >= et_today]
        slate = (upcoming[0] if upcoming else dates[-1]).isoformat()
        return recs_df[recs_df['game_date'].astype(str) == slate].reset_index(drop=True)
    except Exception:
        return recs_df


@app.api_route("/", methods=["GET", "POST"])
async def home(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    league = get_league(request)
    today = get_eastern_today()
    
    contest = db.query(models.Contest).filter(models.Contest.slate_date == today).first()
    is_todays_contest = contest is not None
    
    if not contest:
        contest = db.query(models.Contest).order_by(models.Contest.slate_date.desc()).first()
    
    house_players = []
    user_entry = None
    headshots = get_player_headshots()
    no_games_today = False
    
    if contest:
        house_players = db.query(models.HouseLineupPlayer).filter(
            models.HouseLineupPlayer.contest_id == contest.id
        ).all()
        if user:
            user_entry = db.query(models.ContestEntry).filter(
                models.ContestEntry.contest_id == contest.id,
                models.ContestEntry.user_id == user.id
            ).first()
    else:
        try:
            count = data_access.get_player_salary_count()
            no_games_today = (count == 0)
        except:
            no_games_today = True
    
    next_game_iso = None
    games_started = False
    try:
        from zoneinfo import ZoneInfo
        eastern = ZoneInfo("America/New_York")
        now_et = datetime.now(eastern)
        game_times_raw = data_access.get_all_game_times()
        
        if not game_times_raw:
            no_games_today = True
        else:
            upcoming = []
            for gt in game_times_raw:
                try:
                    parsed = datetime.strptime(gt, "%I:%M%p")
                    game_dt = parsed.replace(year=now_et.year, month=now_et.month, day=now_et.day, tzinfo=eastern)
                    if game_dt > now_et:
                        upcoming.append(game_dt)
                except:
                    pass
            
            if upcoming:
                next_game = min(upcoming)
                next_game_iso = next_game.isoformat()
            else:
                games_started = True
    except Exception as e:
        no_games_today = True
    
    slate_games = []
    try:
        if data_access.use_postgres():
            from backend.database import engine as pg_engine
            from sqlalchemy import text as sa_text
            with pg_engine.connect() as pg_conn:
                try:
                    rows = pg_conn.execute(sa_text("SELECT away_team, home_team, spread, total FROM game_odds_live")).fetchall()
                    for row in rows:
                        slate_games.append({"away": row[0], "home": row[1], "spread": row[2], "total": row[3]})
                except Exception:
                    pass
        if not slate_games:
            import sqlite3 as sl3
            conn_g = sl3.connect("dfs_nba.db")
            cur_g = conn_g.cursor()
            cur_g.execute("SELECT away_team, home_team, spread, total FROM game_odds")
            for row in cur_g.fetchall():
                slate_games.append({"away": row[0], "home": row[1], "spread": row[2], "total": row[3]})
            conn_g.close()
    except:
        pass

    nba_team_ids = {
        "ATL": 1, "BOS": 2, "BKN": 17, "CHA": 30, "CHI": 4,
        "CLE": 5, "DAL": 6, "DEN": 7, "DET": 8, "GS": 9, "GSW": 9,
        "HOU": 10, "IND": 11, "LAC": 12, "LAL": 13, "MEM": 29,
        "MIA": 14, "MIL": 15, "MIN": 16, "NO": 3, "NOP": 3,
        "NY": 18, "NYK": 18, "OKC": 25, "ORL": 19, "PHI": 20,
        "PHX": 21, "POR": 22, "SA": 24, "SAS": 24, "SAC": 23,
        "TOR": 28, "UTA": 26, "WAS": 27
    }

    team_names = {
        "ATL": "Hawks", "BOS": "Celtics", "BKN": "Nets", "CHA": "Hornets",
        "CHI": "Bulls", "CLE": "Cavaliers", "DAL": "Mavericks", "DEN": "Nuggets",
        "DET": "Pistons", "GS": "Warriors", "GSW": "Warriors", "HOU": "Rockets",
        "IND": "Pacers", "LAC": "Clippers", "LAL": "Lakers", "MEM": "Grizzlies",
        "MIA": "Heat", "MIL": "Bucks", "MIN": "Timberwolves", "NO": "Pelicans",
        "NOP": "Pelicans", "NY": "Knicks", "NYK": "Knicks", "OKC": "Thunder",
        "ORL": "Magic", "PHI": "76ers", "PHX": "Suns", "POR": "Trail Blazers",
        "SA": "Spurs", "SAS": "Spurs", "SAC": "Kings", "TOR": "Raptors",
        "UTA": "Jazz", "WAS": "Wizards"
    }

    espn_abbr_map = {
        "GS": "gs", "GSW": "gs", "NO": "no", "NOP": "no",
        "NY": "ny", "NYK": "ny", "SA": "sa", "SAS": "sa",
        "UTA": "utah", "PHX": "phx", "CHA": "cha",
    }

    for g in slate_games:
        for side in ("away", "home"):
            abbr = g[side]
            espn_slug = espn_abbr_map.get(abbr, abbr.lower())
            g[f"{side}_logo"] = f"https://a.espncdn.com/i/teamlogos/nba/500/{espn_slug}.png"
            g[f"{side}_name"] = team_names.get(abbr, abbr)

    east_teams = {"ATL", "BOS", "BKN", "CHA", "CHI", "CLE", "DET", "IND",
                  "MIA", "MIL", "NY", "NYK", "ORL", "PHI", "TOR", "WAS"}
    west_teams = {"DAL", "DEN", "GS", "GSW", "HOU", "LAC", "LAL", "MEM", "MIN",
                  "NO", "NOP", "OKC", "PHX", "PHO", "POR", "SA", "SAS", "SAC", "UTA"}

    abbr_normalize = {
        "SA": "SAS", "NO": "NOP", "NY": "NYK", "GSW": "GS", "PHX": "PHO",
        "SAS": "SAS", "NOP": "NOP", "NYK": "NYK", "GS": "GS", "PHO": "PHO",
    }

    playing_today = set()
    for g in slate_games:
        for side in ("away", "home"):
            raw = g[side]
            playing_today.add(abbr_normalize.get(raw, raw))

    standings_east = []
    standings_west = []
    try:
        standings_rows = []
        try:
            if data_access.use_postgres():
                from backend.database import engine as pg_engine_st
                from sqlalchemy import text as sa_text_st
                with pg_engine_st.connect() as pg_conn_st:
                    standings_rows = pg_conn_st.execute(sa_text_st(
                        "SELECT team, team_name, wins, losses, games_behind, win_pct FROM team_standings_live ORDER BY win_pct DESC"
                    )).fetchall()
        except Exception:
            pass

        if not standings_rows:
            import sqlite3 as sl3
            conn_st = sl3.connect("dfs_nba.db")
            cur_st = conn_st.cursor()
            cur_st.execute("SELECT team, team_name, wins, losses, games_behind, win_pct FROM team_standings ORDER BY win_pct DESC")
            standings_rows = cur_st.fetchall()
            conn_st.close()

        for row in standings_rows:
            abbr = row[0]
            canon = abbr_normalize.get(abbr, abbr)
            espn_slug = espn_abbr_map.get(abbr, abbr.lower())
            entry = {
                "team": abbr,
                "team_name": row[1],
                "wins": row[2],
                "losses": row[3],
                "gb": row[4],
                "win_pct": f"{row[5]:.3f}",
                "playing": canon in playing_today,
                "logo": f"https://a.espncdn.com/i/teamlogos/nba/500/{espn_slug}.png",
            }
            if abbr in east_teams:
                standings_east.append(entry)
            else:
                standings_west.append(entry)
    except Exception as e:
        print(f"[Home] Standings query failed: {e}")

    if league == "wnba":
        try:
            standings_east, standings_west = build_wnba_standings()
        except Exception as e:
            print(f"[Home] WNBA standings build failed: {e}")
            standings_east, standings_west = [], []

    edge_insights = []
    edge_league = "nba"
    player_count = 0
    game_count = len(slate_games)
    if not user:
        try:
            edge_insights = _build_edge_insights(data_access.get_prop_recommendations())
        except Exception:
            edge_insights = []
        # When there is no fresh NBA slate today (e.g. the NBA offseason), or NBA
        # produced no edges, fall back to the active WNBA slate so 'Tonight's
        # Edge' still shows real props. We check odds freshness (scraped today)
        # rather than a row count, so leftover stale NBA props/odds do not keep
        # WNBA off the ticker on a no-NBA-games night.
        if not _nba_slate_is_today() or not edge_insights:
            try:
                wnba_recs = _wnba_active_slate_recs(data_access.get_wnba_prop_recommendations())
                wnba_edges = _build_edge_insights(wnba_recs)
                if wnba_edges:
                    edge_insights = wnba_edges
                    edge_league = "wnba"
            except Exception:
                pass
        try:
            dfs_df = data_access.get_dfs_players()
            if not dfs_df.empty:
                player_count = len(dfs_df[dfs_df.get('salary', dfs_df.get('salary', 0)) > 0]) if 'salary' in dfs_df.columns else len(dfs_df)
        except:
            pass

    resp = templates.TemplateResponse("home.html", {
        "request": request,
        "user": user,
        "league": league,
        "contest": contest,
        "house_players": house_players,
        "user_entry": user_entry,
        "headshots": headshots,
        "no_games_today": no_games_today,
        "is_todays_contest": is_todays_contest,
        "next_game_iso": next_game_iso,
        "games_started": games_started,
        "slate_games": slate_games,
        "standings_east": standings_east,
        "standings_west": standings_west,
        "edge_insights": edge_insights,
        "edge_league": edge_league,
        "player_count": player_count,
        "game_count": game_count,
    })
    if league == "wnba":
        resp.set_cookie("pirtdica_league", "wnba", max_age=60 * 60 * 24 * 30, samesite="lax")
    return resp

@app.get("/register")
async def register_page(request: Request):
    return templates.TemplateResponse("register.html", {"request": request})

@app.post("/register")
async def register(
    request: Request,
    username: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db)
):
    from backend.profanity_filter import check_username
    is_valid, filter_reason = check_username(username)
    if not is_valid:
        return templates.TemplateResponse("register.html", {
            "request": request,
            "error": filter_reason
        })

    existing = db.query(models.User).filter(
        (models.User.username == username) | (models.User.email == email)
    ).first()
    if existing:
        return templates.TemplateResponse("register.html", {
            "request": request,
            "error": "Username or email already exists"
        })
    
    try:
        user = models.User(
            username=username,
            email=email,
            password_hash=auth.hash_password(password),
            display_name=username,
            coins=100
        )
        db.add(user)
        db.commit()
        db.refresh(user)
    except Exception as e:
        db.rollback()
        print(f"[Register] Error creating user: {e}")
        return templates.TemplateResponse("register.html", {
            "request": request,
            "error": "Username or email already exists"
        })

    try:
        db.add(models.CurrencyTransaction(
            user_id=user.id,
            amount=100,
            transaction_type="signup_bonus",
            description="Welcome bonus!"
        ))
        db.commit()
    except Exception:
        db.rollback()

    try:
        from backend.events import emit_welcome
        emit_welcome(db, user.id, username)
        db.commit()
    except Exception:
        db.rollback()
    
    token = auth.create_session(db, user.id)
    extra_cookies = {}
    try:
        from backend.stripe_billing import has_any_subscription
        if not has_any_subscription(user, db):
            extra_cookies["show_subscribe_prompt"] = {"value": "1", "max_age": 60, "httponly": False}
    except Exception:
        extra_cookies["show_subscribe_prompt"] = {"value": "1", "max_age": 60, "httponly": False}
    return html_redirect("/", token=token, extra_cookies=extra_cookies)

@app.get("/login")
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
async def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db)
):
    user = db.query(models.User).filter(models.User.username == username).first()
    if not user:
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": "Wrong username — no account found with that name"
        })
    if not auth.verify_password(password, user.password_hash):
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": "Wrong password — try again"
        })
    
    if getattr(user, 'is_banned', False):
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": "This account has been banned"
        })
    
    token = auth.create_session(db, user.id)
    extra_cookies = {}
    try:
        from backend.stripe_billing import has_any_subscription
        if not has_any_subscription(user, db):
            extra_cookies["show_subscribe_prompt"] = {"value": "1", "max_age": 60, "httponly": False}
    except Exception as e:
        print(f"[LOGIN] Stripe check failed for {username}, skipping: {e}")
    return html_redirect("/", token=token, extra_cookies=extra_cookies)

@app.get("/forgot-password")
async def forgot_password_page(request: Request):
    return templates.TemplateResponse("forgot_password.html", {"request": request})


@app.post("/forgot-password")
async def forgot_password(
    request: Request,
    email: str = Form(...),
    db: Session = Depends(get_db),
):
    generic_ok = templates.TemplateResponse("forgot_password.html", {
        "request": request,
        "sent": True,
    })

    email_clean = (email or "").strip().lower()
    if not email_clean or "@" not in email_clean:
        return generic_ok

    user = db.query(models.User).filter(models.User.email == email_clean).first()
    if not user:
        return generic_ok

    try:
        raw_token = auth.create_password_reset_token(db, user.id)
    except Exception as e:
        print(f"[forgot_password] token create failed for user {user.id}: {e}")
        return generic_ok

    base = str(request.base_url).rstrip("/")
    reset_url = f"{base}/reset-password?token={raw_token}"
    subject = "Reset your PIRTDICA password"
    body_html = f"""
    <div style="font-family:Arial,sans-serif;max-width:520px;margin:0 auto;color:#111;">
      <h2 style="margin:0 0 16px 0;">Reset your password</h2>
      <p>Someone (hopefully you) asked to reset the password for your PIRTDICA account.</p>
      <p>Click the button below to choose a new password. This link expires in {auth.PASSWORD_RESET_TTL_MINUTES} minutes and can only be used once.</p>
      <p style="margin:24px 0;">
        <a href="{reset_url}" style="background:#d4af37;color:#111;padding:12px 20px;text-decoration:none;border-radius:6px;font-weight:bold;">Reset password</a>
      </p>
      <p style="color:#555;font-size:13px;">If the button does not work, paste this link into your browser:</p>
      <p style="word-break:break-all;color:#555;font-size:13px;">{reset_url}</p>
      <hr style="border:none;border-top:1px solid #eee;margin:24px 0;">
      <p style="color:#888;font-size:12px;">If you did not request a password reset, you can safely ignore this email // your password will not change.</p>
    </div>
    """

    try:
        from backend.email_service import send_email
        ok, err = send_email(user.email, subject, body_html)
        if not ok:
            print(f"[forgot_password] send failed for user {user.id}: {err}")
    except Exception as e:
        print(f"[forgot_password] send exception for user {user.id}: {e}")

    return generic_ok


@app.get("/reset-password")
async def reset_password_page(request: Request, token: str = "", db: Session = Depends(get_db)):
    record = auth.consume_password_reset_token(db, token)
    if not record:
        return templates.TemplateResponse("reset_password.html", {
            "request": request,
            "invalid": True,
        })
    return templates.TemplateResponse("reset_password.html", {
        "request": request,
        "token": token,
    })


@app.post("/reset-password")
async def reset_password(
    request: Request,
    token: str = Form(...),
    password: str = Form(...),
    confirm_password: str = Form(...),
    db: Session = Depends(get_db),
):
    record = auth.consume_password_reset_token(db, token)
    if not record:
        return templates.TemplateResponse("reset_password.html", {
            "request": request,
            "invalid": True,
        })

    if len(password or "") < 6:
        return templates.TemplateResponse("reset_password.html", {
            "request": request,
            "token": token,
            "error": "Password must be at least 6 characters.",
        })
    if password != confirm_password:
        return templates.TemplateResponse("reset_password.html", {
            "request": request,
            "token": token,
            "error": "Passwords do not match.",
        })

    user = db.query(models.User).filter(models.User.id == record.user_id).first()
    if not user:
        return templates.TemplateResponse("reset_password.html", {
            "request": request,
            "invalid": True,
        })

    user.password_hash = auth.hash_password(password)
    auth.mark_password_reset_token_used(db, record)

    try:
        db.query(auth.UserSession).filter(auth.UserSession.user_id == user.id).delete()
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"[reset_password] session purge failed for user {user.id}: {e}")

    return templates.TemplateResponse("reset_password.html", {
        "request": request,
        "success": True,
    })


@app.get("/logout")
async def logout(request: Request, db: Session = Depends(get_db)):
    token = request.cookies.get("session_token")
    if token:
        auth.delete_session(db, token)
    response = html_redirect("/")
    response.delete_cookie("session_token")
    return response

@app.get("/trends")
async def trends(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return html_redirect("/login")
    league = get_league(request)
    if league == "wnba":
        resp = _render_wnba_trends(request, user, db)
        resp.set_cookie("pirtdica_league", "wnba", max_age=60 * 60 * 24 * 30, samesite="lax")
        return resp
    try:
        from backend.stripe_billing import has_statpack_access
        has_access = has_statpack_access(user, db)
    except Exception as e:
        print(f"[TRENDS] Stripe check failed: {e}")
        has_access = False
    if not has_access:
        return templates.TemplateResponse("trends_paywall.html", {
            "request": request,
            "user": user,
        })
    import pandas as pd
    import time
    
    top_value = []
    props = []
    
    try:
        dfs_df = data_access.get_dfs_players()
        if not dfs_df.empty:
            dfs_df = dfs_df[dfs_df['salary'] > 0]
            reliable = dfs_df.copy()
            if 'low_gp_flag' in reliable.columns:
                reliable = reliable[reliable['low_gp_flag'] == False]
            if 'games_pct' in reliable.columns:
                reliable = reliable[reliable['games_pct'] >= 50]
            if 'projected_min' in reliable.columns:
                reliable = reliable[reliable['projected_min'] >= 10]
            if 'proj_fp' in reliable.columns:
                reliable = reliable[reliable['proj_fp'] >= 10]
            if 'value_vs_tier' in dfs_df.columns and 'value_ratio' in dfs_df.columns:
                valid_df = reliable[~reliable['value_vs_tier'].isin([float('inf'), float('-inf')])]
                value_cols = ['player_name', 'team', 'salary', 'proj_fp', 'value_ratio', 'value_vs_tier', 'tier', 'ceiling', 'floor', 'fp_sd', 'archetype', 'projected_min', 'games_pct']
                value_cols = [c for c in value_cols if c in valid_df.columns]
                top_value = valid_df.nlargest(10, 'value_vs_tier')[value_cols].to_dict('records')
            elif 'value' in dfs_df.columns:
                top_value = reliable.nlargest(10, 'value')[['player_name', 'team', 'salary', 'proj_fp', 'value', 'salary_tier']].to_dict('records')
    except:
        pass
    
    try:
        props_df = data_access.get_prop_recommendations()
        if not props_df.empty and 'salary' in props_df.columns:
            props_df = props_df[props_df['salary'] > 0]
            props = props_df.head(15).to_dict('records')
    except:
        pass
    
    targeted = []
    try:
        targeted_df = data_access.get_targeted_plays()
        if not targeted_df.empty:
            targeted = targeted_df.head(20).to_dict('records')
    except:
        pass
    
    import os
    from datetime import datetime
    ref_chart_exists = os.path.exists("static/images/ref_foul_chart.png")

    chart_files = [
        "static/images/value_chart.png",
        "static/images/upside_chart.png",
        "static/images/dvp_heatmap.png",
        "static/images/ref_foul_chart.png",
    ]
    chart_mtimes = [os.path.getmtime(f) for f in chart_files if os.path.exists(f)]
    charts_last_updated = None
    charts_stale = True
    if chart_mtimes:
        latest_mtime = max(chart_mtimes)
        charts_last_updated = datetime.fromtimestamp(latest_mtime, tz=EASTERN)
        charts_stale = charts_last_updated.date() < get_eastern_today()

    explorer_players = []
    headshots = get_player_headshots()
    try:
        def _safe_normalize(name):
            if not name or not isinstance(name, str):
                return ''
            return normalize_name(name).lower()

        explorer_df = dfs_df.copy() if not dfs_df.empty else pd.DataFrame()
        injury_df = data_access.get_injury_alerts()

        inj_norm_map = {}
        if not injury_df.empty:
            injury_df = injury_df.dropna(subset=['player_name'])
            for _, irow in injury_df.iterrows():
                nk = _safe_normalize(irow['player_name'])
                if nk:
                    inj_norm_map[nk] = irow['status']

        if not explorer_df.empty:
            explorer_df['injury_status'] = explorer_df['player_name'].apply(
                lambda n: inj_norm_map.get(_safe_normalize(n), '')
            )
            if 'true_position' in explorer_df.columns:
                pos_df = data_access.get_player_positions()
                derived_map = {}
                if pos_df is not None and not pos_df.empty:
                    pos_cols = ['pg_pct', 'sg_pct', 'sf_pct', 'pf_pct', 'c_pct']
                    pos_labels = ['PG', 'SG', 'SF', 'PF', 'C']
                    def derive_pos(row):
                        vals = [row.get(c, 0) or 0 for c in pos_cols]
                        if max(vals) == 0:
                            return ''
                        return pos_labels[vals.index(max(vals))]
                    pos_df['derived_pos'] = pos_df.apply(derive_pos, axis=1)
                    derived_map = dict(zip(pos_df['player_name'], pos_df['derived_pos']))
                fd_map = {}
                if 'fd_position' in explorer_df.columns:
                    fd_map = dict(zip(explorer_df['player_name'], explorer_df['fd_position'].fillna('')))
                def resolve_position(row):
                    tp = row['true_position']
                    if pd.notna(tp) and str(tp).strip() != '':
                        return str(tp).strip()
                    name = row['player_name']
                    if name in derived_map and derived_map[name]:
                        return derived_map[name]
                    return fd_map.get(name, '')
                explorer_df['true_position'] = explorer_df.apply(resolve_position, axis=1)
            if 'opponent' in explorer_df.columns:
                explorer_df['opponent'] = explorer_df['opponent'].fillna('')

        salary_df = data_access.get_player_salaries()
        if not salary_df.empty and not injury_df.empty:
            existing_norm = set()
            if not explorer_df.empty:
                existing_norm = set(explorer_df['player_name'].apply(_safe_normalize))

            opp_map = {}
            try:
                odds_df = data_access._pg_query("SELECT away_team, home_team FROM game_odds_live") if data_access.use_postgres() else pd.DataFrame()
                if odds_df.empty:
                    import sqlite3 as _sq
                    _conn = _sq.connect("dfs_nba.db")
                    odds_df = pd.read_sql_query("SELECT away_team, home_team FROM game_odds", _conn)
                    _conn.close()
                for _, orow in odds_df.iterrows():
                    opp_map[orow['away_team']] = orow['home_team']
                    opp_map[orow['home_team']] = orow['away_team']
            except Exception:
                pass

            today_teams = set(opp_map.keys())
            if not today_teams:
                pass
            else:
                seen_norm = set(existing_norm)
                missing_rows = []
                salary_df = salary_df.dropna(subset=['player_name'])
                for _, srow in salary_df.iterrows():
                    pname = srow['player_name']
                    nk = _safe_normalize(pname)
                    if not nk or nk in seen_norm:
                        continue
                    status = inj_norm_map.get(nk, '')
                    if not status:
                        continue
                    team = srow.get('team', '')
                    if team not in today_teams:
                        continue
                    fd_pos = str(srow.get('position', '') or '')
                    pos = fd_pos.split('/')[0] if fd_pos else ''
                    opp = opp_map.get(team, '')
                    missing_rows.append({
                        'player_name': pname,
                        'true_position': pos,
                        'team': team,
                        'opponent': opp,
                        'injury_status': status,
                    })
                    seen_norm.add(nk)

                if missing_rows:
                    missing_df = pd.DataFrame(missing_rows)
                    if explorer_df.empty:
                        explorer_df = missing_df
                    else:
                        keep = ['player_name', 'true_position', 'team', 'opponent', 'injury_status']
                        keep = [c for c in keep if c in explorer_df.columns]
                        explorer_df = pd.concat([explorer_df[keep], missing_df[keep]], ignore_index=True)

        try:
            shot_universe = data_access.get_player_shot_zones()
            if shot_universe is not None and not shot_universe.empty:
                positions_df = data_access.get_player_positions()
                pos_lookup = {}
                if positions_df is not None and not positions_df.empty:
                    pos_cols = ['pg_pct', 'sg_pct', 'sf_pct', 'pf_pct', 'c_pct']
                    pos_labels = ['PG', 'SG', 'SF', 'PF', 'C']
                    for _, prow in positions_df.iterrows():
                        tp = prow.get('true_position')
                        if pd.notna(tp) and str(tp).strip():
                            pos_lookup[prow['player_name']] = str(tp).strip()
                            continue
                        vals = [prow.get(c, 0) or 0 for c in pos_cols]
                        if max(vals) > 0:
                            pos_lookup[prow['player_name']] = pos_labels[vals.index(max(vals))]

                existing_norm_full = set()
                if not explorer_df.empty:
                    existing_norm_full = set(explorer_df['player_name'].apply(_safe_normalize))

                offslate_rows = []
                for _, srow in shot_universe.iterrows():
                    pname = srow.get('player_name')
                    if not pname:
                        continue
                    nk = _safe_normalize(pname)
                    if not nk or nk in existing_norm_full:
                        continue
                    offslate_rows.append({
                        'player_name': pname,
                        'true_position': pos_lookup.get(pname, ''),
                        'team': srow.get('team', '') or '',
                        'opponent': '',
                        'injury_status': 'off-slate',
                    })
                    existing_norm_full.add(nk)

                if offslate_rows:
                    offslate_df = pd.DataFrame(offslate_rows)
                    if explorer_df.empty:
                        explorer_df = offslate_df
                    else:
                        keep = ['player_name', 'true_position', 'team', 'opponent', 'injury_status']
                        keep = [c for c in keep if c in explorer_df.columns]
                        explorer_df = pd.concat([explorer_df[keep], offslate_df[keep]], ignore_index=True)
        except Exception:
            import traceback
            traceback.print_exc()

        if not explorer_df.empty:
            keep_cols = ['player_name', 'true_position', 'team', 'opponent', 'injury_status']
            keep_cols = [c for c in keep_cols if c in explorer_df.columns]
            explorer_players = explorer_df[keep_cols].to_dict('records')
    except Exception:
        import traceback
        traceback.print_exc()

    return templates.TemplateResponse("trends.html", {
        "request": request,
        "user": user,
        "top_value": top_value,
        "props": props,
        "targeted": targeted,
        "ref_chart_exists": ref_chart_exists,
        "cache_bust": int(time.time()),
        "explorer_players": explorer_players,
        "headshots": headshots,
        "charts_last_updated": charts_last_updated,
        "charts_stale": charts_stale,
        "league": "nba",
        "chart_pre": "",
    })

@app.get("/leaderboard")
async def leaderboard(request: Request, period: str = "daily", db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    
    leaderboard_data = db.query(
        models.User.id,
        models.User.username,
        models.User.display_name,
        models.User.avatar_url,
        models.User.division,
        models.User.division_tier,
        models.User.mmr,
        func.count(models.ContestEntry.id).label("entries"),
        func.sum(models.ContestEntry.beat_house.cast(Integer)).label("wins"),
        func.avg(models.ContestEntry.actual_score).label("avg_score")
    ).join(models.ContestEntry).group_by(models.User.id).order_by(
        desc("wins")
    ).limit(50).all()
    
    leaderboard_with_ranks = []
    for entry in leaderboard_data:
        rank = get_coach_rank(entry.wins or 0)
        leaderboard_with_ranks.append({
            "id": entry.id,
            "username": entry.username,
            "display_name": entry.display_name,
            "avatar_url": entry.avatar_url,
            "division": entry.division or "Bronze",
            "division_tier": entry.division_tier or 3,
            "mmr": entry.mmr or 1000,
            "entries": entry.entries,
            "wins": entry.wins,
            "avg_score": entry.avg_score,
            "rank": rank,
        })

    return templates.TemplateResponse("leaderboard.html", {
        "request": request,
        "user": user,
        "leaderboard": leaderboard_with_ranks,
        "period": period
    })

@app.get("/profile/{username}")
async def profile(request: Request, username: str, db: Session = Depends(get_db)):
    current_user = get_current_user(request, db)
    profile_user = db.query(models.User).filter(models.User.username == username).first()
    
    if not profile_user:
        raise HTTPException(status_code=404, detail="User not found")
    
    entries = db.query(models.ContestEntry).filter(
        models.ContestEntry.user_id == profile_user.id
    ).order_by(desc(models.ContestEntry.created_at)).limit(20).all()
    
    stats = db.query(
        func.count(models.ContestEntry.id).label("total_entries"),
        func.sum(models.ContestEntry.beat_house.cast(Integer)).label("wins"),
        func.avg(models.ContestEntry.actual_score).label("avg_score")
    ).filter(models.ContestEntry.user_id == profile_user.id).first()
    
    user_achievements = db.query(models.UserAchievement).filter(
        models.UserAchievement.user_id == profile_user.id
    ).all()
    earned_codes = {ua.achievement_code: ua.achieved_at for ua in user_achievements}
    
    all_achievements = db.query(models.Achievement).all()
    
    badge_type_order = ["competitive_earned", "statistical_earned", "event_limited", "secret_earned"]
    badge_type_labels = {
        "competitive_earned": "Competitive Milestones",
        "statistical_earned": "Statistical Achievements",
        "event_limited": "Division Achievements",
        "secret_earned": "Secret Badges",
    }
    badge_groups = {}
    for a in all_achievements:
        bt = getattr(a, 'badge_type', None) or "competitive_earned"
        if bt == "cosmetic_purchased":
            continue
        is_hidden = getattr(a, 'is_hidden', False)
        is_earned = a.code in earned_codes
        if is_hidden and not is_earned:
            badge_entry = {
                "code": a.code,
                "name": "???",
                "description": "Hidden achievement — keep competing to discover it!",
                "icon": "?",
                "coin_reward": 0,
                "rarity": getattr(a, 'rarity', 'common'),
                "badge_type": bt,
                "earned": False,
                "achieved_at": None,
                "is_hidden": True,
            }
        else:
            badge_entry = {
                "code": a.code,
                "name": a.name,
                "description": a.description,
                "icon": a.icon,
                "coin_reward": a.coin_reward,
                "rarity": getattr(a, 'rarity', 'common'),
                "badge_type": bt,
                "earned": is_earned,
                "achieved_at": earned_codes.get(a.code),
                "is_hidden": is_hidden,
            }
        if bt not in badge_groups:
            badge_groups[bt] = []
        badge_groups[bt].append(badge_entry)
    
    ordered_badge_groups = []
    for bt in badge_type_order:
        if bt in badge_groups:
            ordered_badge_groups.append({
                "category": bt,
                "label": badge_type_labels.get(bt, bt.replace("_", " ").title()),
                "badges": badge_groups[bt],
            })
    
    from sqlalchemy import or_
    h2h_completed = db.query(models.H2HChallenge).filter(
        models.H2HChallenge.status == "completed",
        or_(
            models.H2HChallenge.challenger_id == profile_user.id,
            models.H2HChallenge.opponent_id == profile_user.id
        )
    ).all()
    h2h_wins = sum(1 for c in h2h_completed if c.winner_id == profile_user.id)
    h2h_losses = len(h2h_completed) - h2h_wins - sum(1 for c in h2h_completed if c.winner_id is None)
    h2h_ties = sum(1 for c in h2h_completed if c.winner_id is None)
    h2h_earnings = 0
    for c in h2h_completed:
        if c.winner_id == profile_user.id:
            total_pot = c.wager * 2
            house_cut = max(1, int(total_pot * 0.1))
            h2h_earnings += (total_pot - house_cut - c.wager)
    
    h2h_recent = db.query(models.H2HChallenge).filter(
        models.H2HChallenge.status == "completed",
        or_(
            models.H2HChallenge.challenger_id == profile_user.id,
            models.H2HChallenge.opponent_id == profile_user.id
        )
    ).order_by(desc(models.H2HChallenge.created_at)).limit(10).all()
    
    h2h_history = []
    for c in h2h_recent:
        if c.challenger_id == profile_user.id:
            opp = db.query(models.User).filter(models.User.id == c.opponent_id).first()
            my_score = c.challenger_score
            opp_score = c.opponent_score
        else:
            opp = db.query(models.User).filter(models.User.id == c.challenger_id).first()
            my_score = c.opponent_score
            opp_score = c.challenger_score
        h2h_history.append({
            "id": c.id,
            "opponent": opp.display_name or opp.username if opp else "Unknown",
            "my_score": my_score,
            "opp_score": opp_score,
            "wager": c.wager,
            "won": c.winner_id == profile_user.id,
            "tied": c.winner_id is None,
            "date": c.created_at,
        })
    
    coin_transactions = db.query(models.CurrencyTransaction).filter(
        models.CurrencyTransaction.user_id == profile_user.id
    ).order_by(desc(models.CurrencyTransaction.created_at)).limit(20).all()

    error_msg = request.query_params.get("error", "")
    success_msg = request.query_params.get("success", "")

    total_wins = (stats.wins or 0) if stats else 0
    coach_rank = get_coach_rank(total_wins)

    import json as _json
    theme_data = None
    if profile_user.active_theme:
        theme_item = db.query(models.ShopItem).filter(models.ShopItem.code == profile_user.active_theme).first()
        if theme_item and theme_item.item_data:
            try:
                theme_data = _json.loads(theme_item.item_data)
                theme_data["name"] = theme_item.name
            except:
                pass
    
    equipped_badge_codes = _json.loads(profile_user.equipped_badges or "[]")
    cosmetic_badges = []
    if equipped_badge_codes:
        badge_items = db.query(models.ShopItem).filter(models.ShopItem.code.in_(equipped_badge_codes)).all()
        for bi in badge_items:
            try:
                bd = _json.loads(bi.item_data) if bi.item_data else {}
            except:
                bd = {}
            cosmetic_badges.append({
                "code": bi.code,
                "name": bi.name,
                "rarity": bi.rarity,
                "data": bd,
            })
    
    try:
        from backend.stripe_billing import get_user_plan_display
        plan_display = get_user_plan_display(db, profile_user.id) if current_user and current_user.id == profile_user.id else None
    except Exception as e:
        print(f"[PROFILE] Stripe check failed: {e}")
        plan_display = "Free"

    return templates.TemplateResponse("profile.html", {
        "request": request,
        "user": current_user,
        "profile": profile_user,
        "entries": entries,
        "stats": stats,
        "badge_groups": ordered_badge_groups,
        "h2h_stats": {"wins": h2h_wins, "losses": h2h_losses, "ties": h2h_ties, "total": len(h2h_completed), "earnings": h2h_earnings},
        "h2h_history": h2h_history,
        "coin_transactions": coin_transactions,
        "coach_rank": coach_rank,
        "error": error_msg,
        "success": success_msg,
        "theme_data": theme_data,
        "cosmetic_badges": cosmetic_badges,
        "plan_display": plan_display,
    })

@app.get("/history")
async def history(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return html_redirect("/login")
    
    entries = db.query(models.ContestEntry).filter(
        models.ContestEntry.user_id == user.id
    ).order_by(desc(models.ContestEntry.created_at)).all()
    
    entry_details = []
    for entry in entries:
        players = db.query(models.EntryPlayer).filter(
            models.EntryPlayer.entry_id == entry.id
        ).all()
        
        house_players = []
        if entry.house_lineup_snapshot:
            import json as json_mod
            try:
                house_players = json_mod.loads(entry.house_lineup_snapshot)
            except:
                pass
        
        if not house_players:
            hp_records = db.query(models.HouseLineupPlayer).filter(
                models.HouseLineupPlayer.contest_id == entry.contest_id
            ).all()
            house_players = [{"player_name": hp.player_name, "position": hp.position, "team": hp.team, "salary": hp.salary, "proj_fp": hp.proj_fp} for hp in hp_records]
        
        house_total_proj = sum(p.get("proj_fp", 0) or 0 for p in house_players) if house_players else (entry.house_proj_score or 0)
        
        entry_details.append({
            "entry": entry,
            "players": players,
            "house_players": house_players,
            "house_total_proj": house_total_proj,
        })
    
    total_entries = len(entries)
    completed_entries = [e for e in entries if e.contest and e.contest.status == 'completed']
    wins = sum(1 for e in completed_entries if e.beat_house)
    losses = len(completed_entries) - wins
    total_coins = sum(e.coins_earned or 0 for e in entries)
    best_score = max((e.actual_score or 0 for e in completed_entries), default=0)
    win_rate = (wins / len(completed_entries) * 100) if completed_entries else 0
    
    current_streak = 0
    for e in completed_entries:
        if e.beat_house:
            current_streak += 1
        else:
            break
    
    stats = {
        "total_entries": total_entries,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "total_coins": total_coins,
        "best_score": best_score,
        "current_streak": current_streak,
    }
    
    return templates.TemplateResponse("history.html", {
        "request": request,
        "user": user,
        "entry_details": entry_details,
        "stats": stats,
    })

@app.get("/shop")
async def shop(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return html_redirect("/login")
    
    items = db.query(models.ShopItem).filter(models.ShopItem.is_active == True).all()
    owned_items = db.query(models.UserItem.item_id).filter(
        models.UserItem.user_id == user.id
    ).all()
    owned_ids = [i[0] for i in owned_items]
    
    import json as _json
    equipped_badge_codes = _json.loads(user.equipped_badges or "[]")
    
    pillars = {
        "identity": {"label": "Identity", "description": "Customize your Coach profile — avatars, themes, and badges that show who you are.", "shop_items": []},
        "prestige": {"label": "Prestige", "description": "Climb the ranks — ranked ladders, high-stakes rooms, and seasonal battle passes.", "shop_items": []},
        "access": {"label": "Access", "description": "Unlock advanced tools — matchup visualizations, scouting reports, and ceiling charts.", "shop_items": []},
        "analytics": {"label": "Analytics", "description": "Fine-tune your game — custom lineup templates, optimizer presets, and DVS sliders.", "shop_items": []},
    }
    for item in items:
        p = item.pillar or "identity"
        if p in pillars:
            pillars[p]["shop_items"].append(item)
    
    return templates.TemplateResponse("shop.html", {
        "request": request,
        "user": user,
        "items": items,
        "owned_ids": owned_ids,
        "pillars": pillars,
        "active_pillar": "identity",
        "active_theme_code": user.active_theme,
        "equipped_badge_codes": equipped_badge_codes,
    })

@app.post("/shop/buy/{item_id}")
async def buy_item(request: Request, item_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        raise HTTPException(status_code=401, detail="Not logged in")
    
    item = db.query(models.ShopItem).filter(models.ShopItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    
    existing = db.query(models.UserItem).filter(
        models.UserItem.user_id == user.id,
        models.UserItem.item_id == item_id
    ).first()
    if existing:
        raise HTTPException(status_code=400, detail="Already owned")
    
    if user.coins < item.price:
        raise HTTPException(status_code=400, detail="Not enough coins")
    
    user.coins -= item.price
    db.add(models.UserItem(user_id=user.id, item_id=item_id))
    db.add(models.CurrencyTransaction(
        user_id=user.id,
        amount=-item.price,
        transaction_type="purchase",
        description=f"Purchased {item.name}"
    ))
    if item.category == "theme" and item.code:
        user.active_theme = item.code
    
    db.commit()
    
    return html_redirect("/shop")

@app.post("/shop/equip/{item_id}")
async def equip_item(request: Request, item_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        raise HTTPException(status_code=401, detail="Not logged in")
    
    owned = db.query(models.UserItem).filter(
        models.UserItem.user_id == user.id,
        models.UserItem.item_id == item_id
    ).first()
    if not owned:
        raise HTTPException(status_code=400, detail="You don't own this item")
    
    item = db.query(models.ShopItem).filter(models.ShopItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    
    if item.category == "theme":
        user.active_theme = item.code
    elif item.category == "badge":
        import json
        current = json.loads(user.equipped_badges or "[]")
        if item.code not in current:
            if len(current) >= 3:
                current.pop(0)
            current.append(item.code)
        user.equipped_badges = json.dumps(current)
    
    db.commit()
    return html_redirect("/shop")

@app.post("/shop/unequip/{item_id}")
async def unequip_item(request: Request, item_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        raise HTTPException(status_code=401, detail="Not logged in")
    
    item = db.query(models.ShopItem).filter(models.ShopItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    
    if item.category == "theme":
        user.active_theme = None
    elif item.category == "badge":
        import json
        current = json.loads(user.equipped_badges or "[]")
        if item.code in current:
            current.remove(item.code)
        user.equipped_badges = json.dumps(current)
    
    db.commit()
    return html_redirect("/shop")

@app.get("/play")
async def play(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return html_redirect("/login")
    
    today = get_eastern_today()
    contest = db.query(models.Contest).filter(models.Contest.slate_date == today).first()
    
    if not contest:
        return templates.TemplateResponse("no_contest.html", {
            "request": request,
            "user": user
        })
    
    existing_entry = db.query(models.ContestEntry).filter(
        models.ContestEntry.contest_id == contest.id,
        models.ContestEntry.user_id == user.id
    ).first()
    
    if existing_entry:
        return html_redirect(f"/entry/{existing_entry.id}")
    
    import pandas as pd
    from datetime import datetime
    
    try:
        players_df = data_access.get_dfs_players()
        if players_df.empty:
            raise ValueError("No player data available")
        players_df = players_df.dropna(subset=['fd_position', 'salary'])
        players_df['salary'] = players_df['salary'].astype(int)
        
        game_times_df = data_access.get_player_salaries_game_times()
        
        injury_df = data_access.get_injury_alerts()
        injury_map = dict(zip(injury_df['player_name'], injury_df['status'])) if not injury_df.empty else {}
        
        game_times = dict(zip(game_times_df['game'], game_times_df['game_time'])) if not game_times_df.empty else {}
        
        from zoneinfo import ZoneInfo
        eastern = ZoneInfo("America/New_York")
        now = datetime.now(eastern)
        
        def is_game_locked(game_str):
            game_time_str = game_times.get(game_str)
            if not game_time_str:
                return False
            try:
                game_dt = datetime.strptime(game_time_str, "%I:%M%p")
                game_dt = game_dt.replace(year=now.year, month=now.month, day=now.day, tzinfo=eastern)
                return now >= game_dt
            except:
                return False
        
        players_df['game'] = players_df['team'] + " vs " + players_df['opponent']
        
        team_aliases = {
            'NYK': 'NY', 'NY': 'NYK',
            'GS': 'GSW', 'GSW': 'GS',
            'SA': 'SAS', 'SAS': 'SA',
            'NO': 'NOP', 'NOP': 'NO',
            'UTAH': 'UTA', 'UTA': 'UTAH',
            'PHX': 'PHO', 'PHO': 'PHX',
            'CHA': 'CHO', 'CHO': 'CHA',
            'BKN': 'BK', 'BK': 'BKN',
        }
        
        for game_key in list(game_times.keys()):
            if " @ " not in game_key:
                continue
            away, home = game_key.split(" @ ")
            combos = [(away, home)]
            away_alt = team_aliases.get(away)
            home_alt = team_aliases.get(home)
            if away_alt:
                combos.append((away_alt, home))
            if home_alt:
                combos.append((away, home_alt))
            if away_alt and home_alt:
                combos.append((away_alt, home_alt))
            for a, h in combos:
                game_times[f"{a} vs {h}"] = game_times[game_key]
                game_times[f"{h} vs {a}"] = game_times[game_key]
        
        players_df['is_locked'] = players_df.apply(
            lambda row: is_game_locked(f"{row['team']} vs {row['opponent']}") or 
                       is_game_locked(f"{row['opponent']} vs {row['team']}"),
            axis=1
        )
        players_df['game_time'] = players_df.apply(
            lambda row: game_times.get(f"{row['team']} vs {row['opponent']}") or 
                       game_times.get(f"{row['opponent']} vs {row['team']}") or "",
            axis=1
        )
        
        players_df['injury_status'] = players_df['player_name'].map(injury_map).fillna('')
        
        players_df['position'] = players_df['fd_position']
        
        players_df['matchup'] = players_df.apply(
            lambda row: f"{row['team']} vs {row['opponent']}", axis=1
        )
        
        players = players_df.to_dict("records")
    except Exception as e:
        print(f"Error loading players: {e}")
        import traceback
        traceback.print_exc()
        players = []
    
    house_players = db.query(models.HouseLineupPlayer).filter(
        models.HouseLineupPlayer.contest_id == contest.id
    ).all()
    house_proj_total = sum(hp.proj_fp or 0 for hp in house_players)
    
    headshots = get_player_headshots()
    return templates.TemplateResponse("play.html", {
        "request": request,
        "user": user,
        "contest": contest,
        "players": players,
        "house_players": house_players,
        "house_proj_total": house_proj_total,
        "headshots": headshots
    })

@app.post("/submit-lineup")
async def submit_lineup(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        raise HTTPException(status_code=401, detail="Not logged in")
    
    form = await request.form()
    player_ids = form.getlist("players")
    
    today = get_eastern_today()
    contest = db.query(models.Contest).filter(models.Contest.slate_date == today).first()
    if not contest:
        raise HTTPException(status_code=400, detail="No active contest")
    
    if contest.status != "open":
        raise HTTPException(status_code=400, detail="Contest is locked")
    
    if get_eastern_now().replace(tzinfo=None) >= contest.lock_time:
        raise HTTPException(status_code=400, detail="Contest is locked - games have started")
    
    existing = db.query(models.ContestEntry).filter(
        models.ContestEntry.contest_id == contest.id,
        models.ContestEntry.user_id == user.id
    ).first()
    if existing:
        raise HTTPException(status_code=400, detail="You already have an entry for this contest")
    
    if len(player_ids) != 9:
        raise HTTPException(status_code=400, detail="Lineup must have exactly 9 players")
    
    import pandas as pd
    players_df = data_access.get_dfs_players()
    if players_df.empty:
        raise HTTPException(status_code=400, detail="No player data available")
    
    total_salary = 0
    total_proj = 0
    player_entries = []
    
    for player_name in player_ids:
        matches = players_df[players_df["player_name"] == player_name]
        if len(matches) == 0:
            raise HTTPException(status_code=400, detail=f"Player not found: {player_name}")
        player_data = matches.iloc[0]
        total_salary += int(player_data.get("salary", 0))
        total_proj += float(player_data.get("proj_fp", 0))
        player_entries.append(player_data)
    
    SALARY_CAP = 60000
    if total_salary > SALARY_CAP:
        raise HTTPException(status_code=400, detail=f"Lineup exceeds salary cap: ${total_salary:,} > ${SALARY_CAP:,}")
    
    import json
    house_players = db.query(models.HouseLineupPlayer).filter(
        models.HouseLineupPlayer.contest_id == contest.id
    ).all()
    house_proj_total = sum(hp.proj_fp or 0 for hp in house_players)
    house_snapshot = json.dumps([{
        "player_name": hp.player_name,
        "position": hp.position,
        "team": hp.team or "",
        "salary": hp.salary or 0,
        "proj_fp": round(hp.proj_fp or 0, 1)
    } for hp in house_players])
    
    entry = models.ContestEntry(
        user_id=user.id,
        contest_id=contest.id,
        total_salary=total_salary,
        proj_score=total_proj,
        house_proj_score=house_proj_total,
        house_lineup_snapshot=house_snapshot
    )
    db.add(entry)
    db.flush()
    
    for player_data in player_entries:
        ep = models.EntryPlayer(
            entry_id=entry.id,
            player_name=str(player_data.get("player_name", "")),
            position=str(player_data.get("fd_position", "")),
            team=str(player_data.get("team", "")),
            salary=int(player_data.get("salary", 0)),
            proj_fp=float(player_data.get("proj_fp", 0))
        )
        db.add(ep)
    
    db.commit()
    
    try:
        from backend.achievements import check_contest_achievements
        check_contest_achievements(db, user.id, entry)
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"Achievement check error: {e}")
    
    return html_redirect(f"/entry/{entry.id}")

@app.get("/entry/{entry_id}")
async def view_entry(request: Request, entry_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    entry = db.query(models.ContestEntry).filter(models.ContestEntry.id == entry_id).first()
    
    if not entry:
        raise HTTPException(status_code=404, detail="Entry not found")
    
    players = db.query(models.EntryPlayer).filter(
        models.EntryPlayer.entry_id == entry_id
    ).all()
    
    import json
    house_snapshot = None
    if entry.house_lineup_snapshot:
        try:
            house_snapshot = json.loads(entry.house_lineup_snapshot)
        except:
            pass
    
    if house_snapshot:
        house_players = house_snapshot
        house_total = entry.house_proj_score
    else:
        hp_records = db.query(models.HouseLineupPlayer).filter(
            models.HouseLineupPlayer.contest_id == entry.contest_id
        ).all()
        house_players = [{"player_name": hp.player_name, "position": hp.position, "team": hp.team, "salary": hp.salary, "proj_fp": hp.proj_fp} for hp in hp_records]
        house_total = sum(hp.proj_fp or 0 for hp in hp_records)
    
    locked_teams, any_started, team_game_times = get_game_lock_status()
    
    is_live = any_started and entry.contest.status in ('open', 'active')
    
    headshots = get_player_headshots()
    return templates.TemplateResponse("entry.html", {
        "request": request,
        "user": user,
        "entry": entry,
        "players": players,
        "house_players": house_players,
        "house_total": house_total,
        "locked_teams": locked_teams,
        "any_started": any_started,
        "is_live": is_live,
        "team_game_times": team_game_times,
        "headshots": headshots,
    })

from sqlalchemy import Integer

refresh_status = {"running": False, "log": [], "last_run": None, "success": None}

def run_daily_update():
    global refresh_status
    refresh_status["running"] = True
    refresh_status["log"] = []
    refresh_status["success"] = None
    
    try:
        process = subprocess.Popen(
            [sys.executable, "run_daily_update.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        
        for line in process.stdout:
            refresh_status["log"].append(line.rstrip())
            if len(refresh_status["log"]) > 200:
                refresh_status["log"] = refresh_status["log"][-200:]
        
        process.wait()
        refresh_status["success"] = process.returncode == 0
        refresh_status["last_run"] = get_eastern_now().isoformat()
    except Exception as e:
        refresh_status["log"].append(f"Error: {str(e)}")
        refresh_status["success"] = False
    finally:
        refresh_status["running"] = False

ADMIN_USERNAME = "data"

def require_admin(user):
    """Check if user is authorized for admin access"""
    if not user or user.username != ADMIN_USERNAME:
        return False
    return True

@app.get("/admin")
async def admin_page(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not require_admin(user):
        return RedirectResponse(url="/", status_code=302)
    
    today = get_eastern_today()
    contest = db.query(models.Contest).filter(models.Contest.slate_date == today).first()
    if not contest:
        contest = db.query(models.Contest).order_by(models.Contest.slate_date.desc()).first()
    
    house_players = []
    if contest:
        house_players = db.query(models.HouseLineupPlayer).filter(
            models.HouseLineupPlayer.contest_id == contest.id
        ).all()
    
    # Task #45: surface today's official-call lock state for the admin panel.
    today_article = db.query(models.DailyArticle).filter(
        models.DailyArticle.slate_date == today
    ).first()
    official_lock_state = {
        "slate_date": today.strftime("%Y-%m-%d"),
        "locked": False,
        "locked_at": None,
        "pick_count": 0,
        "working_pick_count": 0,
    }
    if today_article:
        if today_article.picks_json:
            try:
                official_lock_state["working_pick_count"] = len(json.loads(today_article.picks_json) or [])
            except Exception:
                pass
        if today_article.official_locked_at:
            official_lock_state["locked"] = True
            try:
                from utils.timezone import EASTERN as _EASTERN
                official_lock_state["locked_at"] = today_article.official_locked_at.astimezone(_EASTERN).strftime('%Y-%m-%d %-I:%M %p ET')
            except Exception:
                official_lock_state["locked_at"] = today_article.official_locked_at.strftime('%Y-%m-%d %H:%M ET')
            try:
                official_lock_state["pick_count"] = len(json.loads(today_article.official_picks_json or "[]") or [])
            except Exception:
                pass
    return templates.TemplateResponse("admin.html", {
        "request": request,
        "user": user,
        "refresh_status": refresh_status,
        "contest": contest,
        "house_players": house_players,
        "official_lock_state": official_lock_state,
    })

@app.post("/admin/relock-official-call")
async def relock_official_call(request: Request, db: Session = Depends(get_db)):
    """Task #45: re-snapshot today's picks_json into official_picks_json.

    Use case: a major injury news drops 30 min pre-tip, the prop-movement
    regen rebuilt picks_json with sharper picks, and we want THAT to be the
    official call instead of the T-60 pregame wave's snapshot. Admin-only,
    one-shot, immediately reflected on /articles + grading.
    """
    user = get_current_user(request, db)
    if not require_admin(user):
        return {"success": False, "message": "Unauthorized"}
    today = get_eastern_today()
    article = db.query(models.DailyArticle).filter(
        models.DailyArticle.slate_date == today
    ).first()
    if not article:
        return {"success": False, "message": f"No article exists for {today}"}
    if not article.picks_json:
        return {"success": False, "message": "Article has no picks_json to snapshot"}
    try:
        parsed = json.loads(article.picks_json) or []
    except Exception:
        return {"success": False, "message": "picks_json is not valid JSON"}
    if not parsed:
        return {"success": False, "message": "picks_json is empty // nothing to lock"}
    from utils.timezone import get_eastern_now as _get_et_now
    was_already_locked = bool(article.official_locked_at)
    article.official_picks_json = article.picks_json
    article.official_locked_at = _get_et_now()
    db.commit()
    # Greppable in prod: distinguish admin re-locks from auto-locks. The
    # `override=` flag tells us whether this was a fresh first-lock done
    # manually (override=False) or an explicit overwrite of an existing
    # auto-lock (override=True).
    print(f"[OFFICIAL CALL][source=admin-relock][override={was_already_locked}] "
          f"User={user.username} resnapshotted {len(parsed)} picks for {today} "
          f"at {article.official_locked_at.strftime('%Y-%m-%d %H:%M ET')}")
    return {
        "success": True,
        "message": f"Re-snapshotted {len(parsed)} picks for {today} at {article.official_locked_at.strftime('%-I:%M %p ET')}",
        "pick_count": len(parsed),
        "locked_at": article.official_locked_at.strftime('%Y-%m-%d %-I:%M %p ET'),
    }


@app.post("/admin/refresh")
async def trigger_refresh(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not require_admin(user):
        return {"status": "error", "message": "Unauthorized"}
    
    if refresh_status["running"]:
        return {"status": "already_running", "message": "Refresh already in progress"}
    
    thread = threading.Thread(target=run_daily_update)
    thread.start()
    
    return {"status": "started", "message": "Data refresh started"}

@app.get("/admin/refresh-status")
async def get_refresh_status(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not require_admin(user):
        return {"status": "error", "message": "Unauthorized"}
    return {
        "running": refresh_status["running"],
        "log": refresh_status["log"][-50:],
        "last_run": refresh_status["last_run"],
        "success": refresh_status["success"]
    }

@app.post("/admin/add-injury")
async def add_injury(request: Request, db: Session = Depends(get_db), player_name: str = Form(...), reason: str = Form("Manual override")):
    user = get_current_user(request, db)
    if not require_admin(user):
        return {"success": False, "message": "Unauthorized"}
    
    from utils.name_normalize import normalize_player_name
    
    try:
        normalized = normalize_player_name(player_name)
        
        success = data_access.write_manual_injury(player_name, normalized, status="OUT", reason=reason)
        
        if success:
            return {"success": True, "message": f"Added {normalized} as OUT ({reason})"}
        else:
            return {"success": False, "message": "Failed to write injury record"}
    except Exception as e:
        return {"success": False, "message": f"Error: {str(e)}"}

@app.get("/admin/scan-usernames")
async def scan_usernames(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not require_admin(user):
        return {"status": "error", "message": "Unauthorized"}
    
    from backend.profanity_filter import scan_usernames as scan_fn, check_username
    all_users = db.query(models.User.id, models.User.username, models.User.display_name).all()
    usernames = [u.username for u in all_users]
    flagged = scan_fn(usernames)
    
    flagged_with_ids = []
    seen_ids = set()
    for f in flagged:
        match = next((u for u in all_users if u.username == f["username"]), None)
        if match:
            seen_ids.add(match.id)
            flagged_with_ids.append({
                "id": match.id,
                "username": match.username,
                "display_name": match.display_name,
                "reason": f["reason"]
            })
    
    for u in all_users:
        if u.id not in seen_ids and u.display_name and u.display_name != u.username:
            is_valid, reason = check_username(u.display_name)
            if not is_valid and reason and "inappropriate" in reason:
                flagged_with_ids.append({
                    "id": u.id,
                    "username": u.username,
                    "display_name": u.display_name,
                    "reason": f"Display name flagged: {reason}"
                })
    
    return {"flagged": flagged_with_ids, "total_scanned": len(usernames)}

@app.post("/admin/force-rename")
async def force_rename_user(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not require_admin(user):
        return {"success": False, "message": "Unauthorized"}
    
    body = await request.json()
    user_id = body.get("user_id")
    new_username = body.get("new_username")
    
    if not user_id or not new_username:
        return {"success": False, "message": "Missing user_id or new_username"}
    
    from backend.profanity_filter import check_username
    is_valid, reason = check_username(new_username)
    if not is_valid:
        return {"success": False, "message": reason}
    
    target = db.query(models.User).filter(models.User.id == user_id).first()
    if not target:
        return {"success": False, "message": "User not found"}
    
    existing = db.query(models.User).filter(models.User.username == new_username, models.User.id != user_id).first()
    if existing:
        return {"success": False, "message": "That username is already taken"}
    
    old_name = target.username
    target.username = new_username
    target.display_name = new_username
    db.commit()
    return {"success": True, "message": f"Renamed '{old_name}' to '{new_username}'"}

@app.post("/admin/ban-user")
async def ban_user(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not require_admin(user):
        return {"success": False, "message": "Unauthorized"}
    
    body = await request.json()
    user_id = body.get("user_id")
    
    target = db.query(models.User).filter(models.User.id == user_id).first()
    if not target:
        return {"success": False, "message": "User not found"}
    
    target.is_banned = True
    db.commit()
    return {"success": True, "message": f"Banned user '{target.username}'"}


@app.get("/admin/scheduler-status")
async def admin_scheduler_status(request: Request, db: Session = Depends(get_db)):
    """Read scheduler log files + state files from /tmp so the admin can verify
    that the production schedulers are actually firing on the Reserved VM."""
    user = get_current_user(request, db)
    if not require_admin(user):
        return {"success": False, "message": "Unauthorized"}

    log_dir = "/tmp/pirtdica_logs"
    state_files = {
        "pregame_refresh": "/tmp/pirtdica_pregame_state.json",
        "props_refresh": "/tmp/pirtdica_movement_state.json",
    }
    schedulers = ["chart_refresh", "pregame_refresh", "postgame_pipeline", "props_refresh"]

    out = {"success": True, "schedulers_enabled": os.environ.get("SCHEDULERS_ENABLED") == "1", "items": []}

    for name in schedulers:
        log_path = os.path.join(log_dir, f"{name}.log")
        item = {"name": name, "log_exists": os.path.exists(log_path), "log_size": 0,
                "log_mtime": None, "tail": [], "state": None}
        if item["log_exists"]:
            try:
                st = os.stat(log_path)
                item["log_size"] = st.st_size
                item["log_mtime"] = datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                with open(log_path, "rb") as f:
                    f.seek(max(0, st.st_size - 8000))
                    item["tail"] = f.read().decode("utf-8", errors="replace").splitlines()[-40:]
            except Exception as e:
                item["tail"] = [f"<read error: {e}>"]

        sp = state_files.get(name)
        if sp and os.path.exists(sp):
            try:
                with open(sp) as f:
                    item["state"] = json.load(f)
            except Exception as e:
                item["state"] = {"_error": str(e)}

        out["items"].append(item)

    return out


@app.get("/admin/edge-calibration")
async def admin_edge_calibration(request: Request, league: str = "wnba", db: Session = Depends(get_db)):
    """Edge-honesty tracker: win rate by claimed edge size + projection error by
    stat, from the graded pick history. Shows whether bigger claimed edges
    actually win more, and whether the HIGH gate is currently tightened."""
    user = get_current_user(request, db)
    if not require_admin(user):
        return {"success": False, "message": "Unauthorized"}
    if league not in ("wnba", "nba"):
        return {"success": False, "message": "league must be wnba or nba"}
    try:
        from analysis.edge_calibration import compute_calibration
        report = compute_calibration(league)
        report["success"] = True
        return report
    except Exception as e:
        return {"success": False, "message": f"calibration failed: {e}"}


@app.get("/admin/lookup-user")
async def admin_lookup_user(request: Request, username: str = "", db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not require_admin(user):
        return {"success": False, "message": "Unauthorized"}
    from backend.models import UserSubscription
    from backend.stripe_billing import PLANS, PLAN_DISPLAY_NAMES

    uname = (username or "").strip()
    if not uname:
        return {"success": False, "message": "Username required"}

    target = db.query(models.User).filter(models.User.username.ilike(uname)).first()
    if not target:
        return {"success": False, "message": f"User '{uname}' not found"}

    subs = db.query(UserSubscription).filter(
        UserSubscription.user_id == target.id,
        UserSubscription.status.in_(["active", "canceled"]),
    ).order_by(UserSubscription.created_at.desc()).all()

    sub_list = []
    for s in subs:
        is_manual = bool(s.stripe_subscription_id and s.stripe_subscription_id.startswith("manual_"))
        sub_list.append({
            "id": s.id,
            "plan": s.plan,
            "plan_name": PLAN_DISPLAY_NAMES.get(s.plan, s.plan),
            "status": s.status,
            "period_end": s.current_period_end.strftime("%Y-%m-%d") if s.current_period_end else "Lifetime",
            "stripe_subscription_id": s.stripe_subscription_id,
            "is_manual": is_manual,
        })

    return {
        "success": True,
        "user": {
            "id": target.id,
            "username": target.username,
            "email": target.email,
            "stripe_customer_id": target.stripe_customer_id,
        },
        "subscriptions": sub_list,
        "available_plans": [{"key": k, "name": v["name"]} for k, v in PLANS.items()],
    }


@app.post("/admin/grant-subscription")
async def admin_grant_subscription(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not require_admin(user):
        return {"success": False, "message": "Unauthorized"}
    from backend.models import UserSubscription
    from backend.stripe_billing import PLANS, sync_user_subscription_fields

    body = await request.json()
    uname = (body.get("username") or "").strip()
    plan = (body.get("plan") or "").strip().lower()
    days_raw = body.get("days")

    if not uname or plan not in PLANS:
        return {"success": False, "message": "Username and valid plan required"}

    target = db.query(models.User).filter(models.User.username.ilike(uname)).first()
    if not target:
        return {"success": False, "message": f"User '{uname}' not found"}

    period_end = None
    if days_raw not in (None, "", 0, "0"):
        try:
            days = int(days_raw)
            if days > 0:
                period_end = datetime.utcnow() + timedelta(days=days)
        except (TypeError, ValueError):
            return {"success": False, "message": "Days must be a positive integer or empty for lifetime"}

    from sqlalchemy.exc import IntegrityError

    existing = db.query(UserSubscription).filter(
        UserSubscription.user_id == target.id,
        UserSubscription.plan == plan,
        UserSubscription.status == "active",
        UserSubscription.stripe_subscription_id.like("manual_%"),
    ).first()

    manual_id = None
    if existing:
        existing.current_period_end = period_end
        existing.status = "active"
        action = "extended"
        manual_id = existing.stripe_subscription_id
    else:
        manual_id = f"manual_{plan}_{target.id}_{int(datetime.utcnow().timestamp())}"
        new_sub = UserSubscription(
            user_id=target.id,
            stripe_subscription_id=manual_id,
            plan=plan,
            status="active",
            current_period_end=period_end,
        )
        db.add(new_sub)
        action = "granted"

    has_stripe_managed = db.query(UserSubscription).filter(
        UserSubscription.user_id == target.id,
        UserSubscription.status == "active",
        UserSubscription.stripe_subscription_id.like("sub_%"),
    ).first() is not None

    if not has_stripe_managed:
        sync_user_subscription_fields(db, target, plan, manual_id, "active", period_end)
    else:
        print(f"[Admin Grant] User {target.id} has active Stripe sub; not overwriting legacy fields")

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return {"success": False, "message": "Concurrent grant detected — please retry."}

    expiry_str = period_end.strftime("%Y-%m-%d") if period_end else "lifetime"
    print(f"[Admin Grant] {action} {plan} for user {target.id} ({target.username}), expires {expiry_str}")
    return {
        "success": True,
        "message": f"{action.capitalize()} {plan} for {target.username} ({expiry_str})",
    }


@app.post("/admin/revoke-subscription")
async def admin_revoke_subscription(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not require_admin(user):
        return {"success": False, "message": "Unauthorized"}
    from backend.models import UserSubscription

    body = await request.json()
    sub_id = body.get("subscription_id")
    if not sub_id:
        return {"success": False, "message": "subscription_id required"}

    sub = db.query(UserSubscription).filter(UserSubscription.id == int(sub_id)).first()
    if not sub:
        return {"success": False, "message": "Subscription not found"}

    if not (sub.stripe_subscription_id or "").startswith("manual_"):
        return {"success": False, "message": "Only manually-granted subscriptions can be revoked here. Use Stripe Dashboard for paid subs."}

    target = db.query(models.User).filter(models.User.id == sub.user_id).first()
    sub.status = "canceled"

    if target and target.stripe_subscription_id == sub.stripe_subscription_id:
        remaining = db.query(UserSubscription).filter(
            UserSubscription.user_id == target.id,
            UserSubscription.status == "active",
            UserSubscription.id != sub.id,
        ).first()
        if remaining:
            target.subscription_plan = remaining.plan
            target.stripe_subscription_id = remaining.stripe_subscription_id
            target.subscription_current_period_end = remaining.current_period_end
        else:
            target.subscription_status = "canceled"

    db.commit()
    print(f"[Admin Revoke] Revoked manual {sub.plan} (sub.id={sub.id}) for user {sub.user_id}")
    return {"success": True, "message": f"Revoked {sub.plan} subscription"}


_live_scores_cache = {"data": {}, "timestamp": 0}
LIVE_SCORES_CACHE_TTL = 30

@app.get("/api/live-scores")
async def api_live_scores(request: Request, db: Session = Depends(get_db)):
    now = time.time()
    if now - _live_scores_cache["timestamp"] < LIVE_SCORES_CACHE_TTL and _live_scores_cache["data"]:
        return {"scores": _live_scores_cache["data"], "cached": True}
    
    try:
        from scrape_live_scores import get_live_scores_summary
        scores = get_live_scores_summary()
        _live_scores_cache["data"] = scores
        _live_scores_cache["timestamp"] = now
        return {"scores": scores, "cached": False}
    except Exception as e:
        return {"scores": _live_scores_cache.get("data", {}), "error": str(e)}

_live_wnba_cache = {"data": {}, "timestamp": 0}
LIVE_WNBA_CACHE_TTL = 30

@app.get("/api/live-wnba-scores")
async def api_live_wnba_scores(request: Request):
    now = time.time()
    if now - _live_wnba_cache["timestamp"] < LIVE_WNBA_CACHE_TTL and _live_wnba_cache["data"]:
        cached = dict(_live_wnba_cache["data"])
        cached["cached"] = True
        return cached
    try:
        from live_wnba_scores import get_wnba_live_scoreboard
        board = get_wnba_live_scoreboard()
        _live_wnba_cache["data"] = board
        _live_wnba_cache["timestamp"] = now
        out = dict(board)
        out["cached"] = False
        return out
    except Exception as e:
        fallback = dict(_live_wnba_cache.get("data", {}) or {"has_games": False, "games": []})
        fallback["error"] = str(e)
        return fallback

def get_game_lock_status():
    from zoneinfo import ZoneInfo
    eastern = ZoneInfo("America/New_York")
    now = datetime.now(eastern)
    
    team_aliases = {
        'NYK': 'NY', 'NY': 'NYK', 'GS': 'GSW', 'GSW': 'GS',
        'SA': 'SAS', 'SAS': 'SA', 'NO': 'NOP', 'NOP': 'NO',
        'UTAH': 'UTA', 'UTA': 'UTAH', 'PHX': 'PHO', 'PHO': 'PHX',
        'CHA': 'CHO', 'CHO': 'CHA', 'BKN': 'BK', 'BK': 'BKN',
    }
    
    try:
        rows = data_access.get_game_lock_rows()
    except:
        return set(), False, {}
    
    locked_teams = set()
    any_started = False
    team_game_times = {}
    
    for game, game_time_str in rows:
        try:
            game_dt = datetime.strptime(game_time_str, "%I:%M%p")
            game_dt = game_dt.replace(year=now.year, month=now.month, day=now.day, tzinfo=eastern)
            started = now >= game_dt
        except:
            started = False
        
        if ' @ ' in game:
            away, home = game.split(' @ ')
            teams = [away, home]
            for t in list(teams):
                alt = team_aliases.get(t)
                if alt:
                    teams.append(alt)
            for t in teams:
                team_game_times[t] = game_time_str
                if started:
                    locked_teams.add(t)
            if started:
                any_started = True
    
    return locked_teams, any_started, team_game_times

@app.get("/api/live-entry/{entry_id}")
async def api_live_entry(request: Request, entry_id: int, db: Session = Depends(get_db)):
    entry = db.query(models.ContestEntry).filter(models.ContestEntry.id == entry_id).first()
    if not entry:
        return {"error": "Entry not found"}
    
    now = time.time()
    if now - _live_scores_cache["timestamp"] >= LIVE_SCORES_CACHE_TTL or not _live_scores_cache["data"]:
        try:
            from scrape_live_scores import get_live_scores_summary
            scores = get_live_scores_summary()
            _live_scores_cache["data"] = scores
            _live_scores_cache["timestamp"] = now
        except Exception as e:
            scores = _live_scores_cache.get("data", {})
    else:
        scores = _live_scores_cache["data"]
    
    from utils.name_normalize import normalize_player_name
    
    locked_teams, any_started, team_game_times = get_game_lock_status()
    
    entry_players = db.query(models.EntryPlayer).filter(
        models.EntryPlayer.entry_id == entry_id
    ).all()
    
    your_live = []
    your_total = 0
    for p in entry_players:
        norm = normalize_player_name(p.player_name)
        live = scores.get(norm, {})
        game_started = (p.team or '') in locked_teams
        fp = live.get('fp', 0) if game_started else 0
        your_total += fp
        your_live.append({
            'player_name': p.player_name,
            'position': p.position,
            'team': p.team or '',
            'salary': p.salary,
            'proj_fp': p.proj_fp,
            'live_fp': fp,
            'game_started': game_started,
            'game_time': team_game_times.get(p.team or '', ''),
        })
    
    house_live = []
    house_total = 0
    if entry.house_lineup_snapshot:
        try:
            snapshot = json.loads(entry.house_lineup_snapshot)
            for hp in snapshot:
                norm = normalize_player_name(hp['player_name'])
                live = scores.get(norm, {})
                team = hp.get('team', '')
                game_started = team in locked_teams
                fp = live.get('fp', 0) if game_started else 0
                house_total += fp
                house_live.append({
                    'player_name': hp['player_name'],
                    'position': hp['position'],
                    'team': team,
                    'salary': hp.get('salary', 0),
                    'proj_fp': hp.get('proj_fp', 0),
                    'live_fp': fp,
                    'game_started': game_started,
                    'game_time': team_game_times.get(team, ''),
                })
        except:
            pass
    
    if not house_live:
        hp_records = db.query(models.HouseLineupPlayer).filter(
            models.HouseLineupPlayer.contest_id == entry.contest_id
        ).all()
        for hp in hp_records:
            norm = normalize_player_name(hp.player_name)
            live = scores.get(norm, {})
            team = hp.team or ''
            game_started = team in locked_teams
            fp = live.get('fp', 0) if game_started else 0
            house_total += fp
            house_live.append({
                'player_name': hp.player_name,
                'position': hp.position,
                'team': team,
                'salary': hp.salary,
                'proj_fp': hp.proj_fp,
                'live_fp': fp,
                'game_started': game_started,
                'game_time': team_game_times.get(team, ''),
            })
    
    return {
        'your_players': your_live,
        'your_total': round(your_total, 1),
        'house_players': house_live,
        'house_total': round(house_total, 1),
        'status': entry.contest.status,
        'any_game_started': any_started,
    }

@app.get("/api/archetype-clusters")
async def api_archetype_clusters():
    import numpy as np
    import pandas as pd
    import unicodedata
    import re as re_mod

    def _clean_name(name):
        if not name or not isinstance(name, str):
            return name
        fixed = name
        for _ in range(2):
            try:
                fixed = fixed.encode('latin-1').decode('utf-8')
            except (UnicodeDecodeError, UnicodeEncodeError):
                break
        return fixed

    def _ascii_key(name):
        if not name or not isinstance(name, str):
            return ""
        fixed = name
        for _ in range(2):
            try:
                fixed = fixed.encode('latin-1').decode('utf-8')
            except (UnicodeDecodeError, UnicodeEncodeError):
                break
        nfkd = unicodedata.normalize('NFKD', fixed)
        ascii_name = ''.join(c for c in nfkd if not unicodedata.combining(c))
        ascii_name = re_mod.sub(r'[^a-zA-Z\s]', '', ascii_name).lower().strip()
        ascii_name = re_mod.sub(r'\s+', ' ', ascii_name)
        for suffix in [' iv', ' iii', ' ii', ' jr', ' sr', ' v']:
            if ascii_name.endswith(suffix):
                ascii_name = ascii_name[:-len(suffix)].strip()
                break
        return ascii_name

    try:
        arch_df = data_access.get_player_archetypes()
        if arch_df.empty:
            return {"error": "Archetype data not yet available.", "players": [], "archetypes": []}

        composite_features = [
            'creation_idx', 'playmaking_idx', 'interior_idx', 'perimeter_idx',
            'offball_idx', 'rebound_idx', 'defense_idx', 'size_idx',
        ]
        has_composites = all(c in arch_df.columns for c in composite_features)
        if not has_composites:
            return {"error": "Composite indices not yet computed. Run daily update.", "players": [], "archetypes": []}

        for col in arch_df.select_dtypes(include=['object']).columns:
            try:
                arch_df[col] = pd.to_numeric(arch_df[col], errors='ignore')
            except Exception:
                pass
        for col in arch_df.columns:
            if col in composite_features or col == 'cluster':
                arch_df[col] = pd.to_numeric(arch_df[col], errors='coerce')
                if col in composite_features:
                    arch_df[col] = arch_df[col].fillna(0)

        arch_df['player_name'] = arch_df['player_name'].apply(_clean_name)
        arch_df = arch_df[~arch_df['team'].isin(['2TM', '3TM', 'TOT'])]
        arch_df = arch_df.dropna(subset=['archetype'])

        per100 = data_access.get_player_per100()
        usage = data_access.get_player_usage()
        if not per100.empty:
            for tbl in [arch_df, per100, usage]:
                tbl['_mk'] = tbl['player_name'].apply(_ascii_key)
            for col in per100.select_dtypes(include=['object']).columns:
                try:
                    per100[col] = pd.to_numeric(per100[col], errors='ignore')
                except Exception:
                    pass
            for col in usage.select_dtypes(include=['object']).columns:
                try:
                    usage[col] = pd.to_numeric(usage[col], errors='ignore')
                except Exception:
                    pass
            df = arch_df.merge(per100[['_mk', 'pts_per100', 'reb_per100', 'ast_per100']].drop_duplicates(subset='_mk'), on='_mk', how='left')
            df = df.merge(usage[['_mk', 'usg_pct']].drop_duplicates(subset='_mk'), on='_mk', how='left')
            df = df.drop(columns=['_mk'])
        else:
            df = arch_df.copy()
            df['pts_per100'] = 0
            df['reb_per100'] = 0
            df['ast_per100'] = 0
            df['usg_pct'] = 0

        for col in ['pts_per100', 'reb_per100', 'ast_per100', 'usg_pct']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        from sklearn.preprocessing import StandardScaler
        from sklearn.decomposition import PCA
        X = df[composite_features].values.astype(float)
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        pca = PCA(n_components=2)
        coords = pca.fit_transform(X_scaled)

        archetypes = sorted(df['archetype'].unique().tolist())
        players = []
        for i, row in df.iterrows():
            players.append({
                "name": row['player_name'],
                "team": row['team'],
                "archetype": row['archetype'],
                "x": round(float(coords[df.index.get_loc(i)][0]), 3),
                "y": round(float(coords[df.index.get_loc(i)][1]), 3),
                "pts": round(float(row['pts_per100']), 1),
                "reb": round(float(row['reb_per100']), 1),
                "ast": round(float(row['ast_per100']), 1),
                "usg": round(float(row['usg_pct']), 1),
            })

        var_explained = [round(float(v * 100), 1) for v in pca.explained_variance_ratio_]

        return {"players": players, "archetypes": archetypes, "variance_explained": var_explained}
    except Exception as e:
        return {"error": str(e), "players": [], "archetypes": []}


@app.get("/api/dva")
async def api_dva():
    try:
        rows, profiles = data_access.get_dva_data()
        if rows is None:
            return {"error": "DVA data not yet available. Run daily update.", "teams": [], "archetypes": []}
        if profiles is None:
            profiles = {}

        teams = sorted(set(r[0] for r in rows))
        archetypes = sorted(set(r[1] for r in rows))

        data = {}
        for r in rows:
            team = r[0]
            if team not in data:
                data[team] = {}
            data[team][r[1]] = {
                "fp_pm": round(r[2], 4),
                "fp_pm_diff": round(r[3], 4),
                "sample_n": int(r[4]),
                "stat_diffs": {
                    "pts": round(r[5], 4), "reb": round(r[6], 4), "ast": round(r[7], 4),
                    "stl": round(r[8], 4), "blk": round(r[9], 4), "fg3m": round(r[10], 4), "tov": round(r[11], 4)
                },
                "dvs_multiplier": round(r[12], 2) if r[12] is not None else 0,
                "dvs_components": {
                    "pts": round(r[13], 2) if r[13] else 0, "reb": round(r[14], 2) if r[14] else 0,
                    "ast": round(r[15], 2) if r[15] else 0, "stl": round(r[16], 2) if r[16] else 0,
                    "blk": round(r[17], 2) if r[17] else 0, "fg3m": round(r[18], 2) if r[18] else 0,
                    "tov": round(r[19], 2) if r[19] else 0
                }
            }

        return {"teams": teams, "archetypes": archetypes, "data": data, "profiles": profiles}
    except Exception as e:
        return {"error": str(e), "teams": [], "archetypes": []}


@app.get("/api/player-trend/{player_name}/{stat}")
async def api_player_trend(player_name: str, stat: str, n: int = 10):
    stat_map = {
        'PTS': 'pts', 'REB': 'reb', 'AST': 'ast',
        'STL': 'stl', 'BLK': 'blk', 'FP': 'fp',
        'MIN': 'min', 'TOV': 'tov', '3PM': 'fg3m'
    }
    col = stat_map.get(stat.upper())
    if not col:
        return {"error": "Invalid stat", "games": []}
    try:
        rows, error = data_access.get_player_game_log(player_name, col, n)
        if error:
            return {"error": error, "games": []}
        if not rows:
            return {"error": "No data found", "games": []}
        games = [{"date": r[0], "matchup": r[1], "value": r[2] or 0} for r in reversed(rows)]
        values = [g["value"] for g in games]
        avg = round(sum(values) / len(values), 1) if values else 0
        return {"player": player_name, "stat": stat.upper(), "games": games, "avg": avg}
    except Exception as e:
        return {"error": str(e), "games": []}

@app.get("/api/player-shot-chart/{player_name}")
async def api_player_shot_chart(player_name: str, league: str = "nba"):
    import unicodedata
    import re as re_mod

    def _ascii_key(name):
        if not name or not isinstance(name, str):
            return ""
        fixed = name
        for _ in range(2):
            try:
                fixed = fixed.encode('latin-1').decode('utf-8')
            except (UnicodeDecodeError, UnicodeEncodeError):
                break
        nfkd = unicodedata.normalize('NFKD', fixed)
        ascii_name = ''.join(c for c in nfkd if not unicodedata.combining(c))
        ascii_name = re_mod.sub(r'[^a-zA-Z\s]', '', ascii_name).lower().strip()
        ascii_name = re_mod.sub(r'\s+', ' ', ascii_name)
        for suffix in [' iv', ' iii', ' ii', ' jr', ' sr', ' v']:
            if ascii_name.endswith(suffix):
                ascii_name = ascii_name[:-len(suffix)].strip()
                break
        return ascii_name

    try:
        df = data_access.get_player_shot_zone_detail(player_name, league=league)
        if df is None or df.empty:
            return {"error": "Shot zone data not yet available.", "zones": {}}

        search_key = _ascii_key(player_name)

        all_player_names = df['player_name'].unique().tolist()
        matched_name = None
        for n in all_player_names:
            if _ascii_key(n) == search_key:
                matched_name = n
                break
        if not matched_name:
            for n in all_player_names:
                if search_key in _ascii_key(n) or _ascii_key(n) in search_key:
                    matched_name = n
                    break
        if not matched_name:
            return {"error": f"No shot data for {player_name}", "zones": {}}

        player_row = df[df['player_name'] == matched_name].iloc[0]

        total_fga = int(player_row.get('total_fga', 0) or 0)
        zones = {}
        if total_fga > 0:
            ra_fga = int(player_row.get('ra_fga', 0) or 0)
            ra_fgm = int(player_row.get('ra_fgm', 0) or 0)
            paint_fga = int(player_row.get('paint_fga', 0) or 0)
            paint_fgm = int(player_row.get('paint_fgm', 0) or 0)
            mid_fga = int(player_row.get('mid_fga', 0) or 0)
            mid_fgm = int(player_row.get('mid_fgm', 0) or 0)
            three_fga = int(player_row.get('three_fga', 0) or 0)
            three_fgm = int(player_row.get('three_fgm', 0) or 0)
            corner3_fga = int(player_row.get('corner3_fga', 0) or 0)
            atb3_fga = int(player_row.get('atb3_fga', 0) or 0)

            def zone_data(fga, fgm, total):
                return {
                    "fga": fga, "fgm": fgm,
                    "fg_pct": round(fgm / fga * 100, 1) if fga > 0 else 0,
                    "freq": round(fga / total * 100, 1) if total > 0 else 0,
                }

            zones["Restricted Area"] = zone_data(ra_fga, ra_fgm, total_fga)
            zones["Paint (Non-RA)"] = zone_data(paint_fga, paint_fgm, total_fga)
            zones["Mid-Range"] = zone_data(mid_fga, mid_fgm, total_fga)
            zones["Above Break 3"] = zone_data(atb3_fga, three_fgm if corner3_fga == 0 else max(0, three_fgm - int(corner3_fga * three_fgm / three_fga)) if three_fga > 0 else 0, total_fga)
            corner3_fgm_est = int(corner3_fga * (three_fgm / three_fga)) if three_fga > 0 else 0
            zones["Corner 3"] = zone_data(corner3_fga, corner3_fgm_est, total_fga)
            zones["Above Break 3"]["fgm"] = max(0, three_fgm - corner3_fgm_est)
            zones["Above Break 3"]["fg_pct"] = round(zones["Above Break 3"]["fgm"] / atb3_fga * 100, 1) if atb3_fga > 0 else 0

        league_avgs = {}
        lg_df = df[df['total_fga'].fillna(0) >= 100]
        if not lg_df.empty:
            tot_fga = int(lg_df['total_fga'].sum())
            tot_ra = int(lg_df['ra_fga'].fillna(0).sum())
            tot_ra_m = int(lg_df['ra_fgm'].fillna(0).sum())
            tot_paint = int(lg_df['paint_fga'].fillna(0).sum())
            tot_paint_m = int(lg_df['paint_fgm'].fillna(0).sum())
            tot_mid = int(lg_df['mid_fga'].fillna(0).sum())
            tot_mid_m = int(lg_df['mid_fgm'].fillna(0).sum())
            tot_c3 = int(lg_df['corner3_fga'].fillna(0).sum()) if 'corner3_fga' in lg_df.columns else 0
            tot_atb3 = int(lg_df['atb3_fga'].fillna(0).sum()) if 'atb3_fga' in lg_df.columns else 0
            tot_3m = int(lg_df['three_fgm'].fillna(0).sum()) if 'three_fgm' in lg_df.columns else 0
            tot_3a = int(lg_df['three_fga'].fillna(0).sum()) if 'three_fga' in lg_df.columns else 0
            c3_m_est = int(tot_c3 * (tot_3m / tot_3a)) if tot_3a > 0 else 0
            atb3_m_est = max(0, tot_3m - c3_m_est)

            def lg_zone(fga, fgm, total):
                return {"freq": round(fga / total * 100, 1) if total > 0 else 0, "fg_pct": round(fgm / fga * 100, 1) if fga > 0 else 0}

            league_avgs["Restricted Area"] = lg_zone(tot_ra, tot_ra_m, tot_fga)
            league_avgs["Paint (Non-RA)"] = lg_zone(tot_paint, tot_paint_m, tot_fga)
            league_avgs["Mid-Range"] = lg_zone(tot_mid, tot_mid_m, tot_fga)
            league_avgs["Above Break 3"] = lg_zone(tot_atb3, atb3_m_est, tot_fga)
            league_avgs["Corner 3"] = lg_zone(tot_c3, c3_m_est, tot_fga)

        archetype = None
        arch_df = data_access.get_player_archetypes()
        if not arch_df.empty:
            arch_match = arch_df[arch_df['player_name'] == matched_name]
            if arch_match.empty:
                matched_key = _ascii_key(matched_name)
                for n in arch_df['player_name'].unique():
                    if _ascii_key(n) == matched_key:
                        arch_match = arch_df[arch_df['player_name'] == n]
                        break
            if arch_match.empty:
                matched_key = _ascii_key(matched_name)
                for n in arch_df['player_name'].unique():
                    nk = _ascii_key(n)
                    if matched_key in nk or nk in matched_key:
                        arch_match = arch_df[arch_df['player_name'] == n]
                        break
            if not arch_match.empty:
                archetype = arch_match.iloc[0]['archetype']

        if archetype is None:
            try:
                import os
                dfs_csv = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'dfs_players_valued.csv')
                if os.path.exists(dfs_csv):
                    import pandas as pd
                    dfs_df = pd.read_csv(dfs_csv)
                    dfs_match = dfs_df[dfs_df['player_name'] == matched_name]
                    if dfs_match.empty:
                        matched_key = _ascii_key(matched_name)
                        for _, drow in dfs_df.iterrows():
                            if _ascii_key(str(drow.get('player_name', ''))) == matched_key:
                                dfs_match = dfs_df[dfs_df['player_name'] == drow['player_name']]
                                break
                    if not dfs_match.empty and pd.notna(dfs_match.iloc[0].get('archetype')):
                        archetype = dfs_match.iloc[0]['archetype']
            except Exception:
                pass

        team = player_row.get('team', None)

        return {
            "player": matched_name,
            "team": team,
            "archetype": archetype,
            "total_fga": total_fga,
            "zones": zones,
            "league_avg": league_avgs,
        }
    except Exception as e:
        return {"error": str(e), "zones": {}}


@app.get("/api/team-defense-shot-chart/{team}")
async def api_team_defense_shot_chart(team: str, league: str = "nba"):
    try:
        row, all_teams = data_access.get_team_defense_shot_zone(team, league=league)

        if not row:
            return {"error": f"No data for team {team}", "zones": {}}

        total_fga = row[2]
        zones = {
            "Restricted Area": {"fga": row[3], "fgm": row[4], "fg_pct": row[18], "freq": row[13]},
            "Paint (Non-RA)": {"fga": row[5], "fgm": row[6], "fg_pct": row[19], "freq": row[14]},
            "Mid-Range": {"fga": row[7], "fgm": row[8], "fg_pct": row[20], "freq": row[15]},
            "Corner 3": {"fga": row[9], "fgm": row[10], "fg_pct": row[21], "freq": row[16]},
            "Above Break 3": {"fga": row[11], "fgm": row[12], "fg_pct": row[22], "freq": row[17]},
        }

        league_avg = {}
        if all_teams:
            t_fga = sum(r[0] for r in all_teams)
            t_ra = sum(r[1] for r in all_teams)
            t_ra_m = sum(r[2] for r in all_teams)
            t_paint = sum(r[3] for r in all_teams)
            t_paint_m = sum(r[4] for r in all_teams)
            t_mid = sum(r[5] for r in all_teams)
            t_mid_m = sum(r[6] for r in all_teams)
            t_c3 = sum(r[7] for r in all_teams)
            t_c3_m = sum(r[8] for r in all_teams)
            t_atb3 = sum(r[9] for r in all_teams)
            t_atb3_m = sum(r[10] for r in all_teams)

            def lg_z(fga, fgm, total):
                return {
                    "freq": round(fga / total * 100, 1) if total > 0 else 0,
                    "fg_pct": round(fgm / fga * 100, 1) if fga > 0 else 0,
                }

            league_avg["Restricted Area"] = lg_z(t_ra, t_ra_m, t_fga)
            league_avg["Paint (Non-RA)"] = lg_z(t_paint, t_paint_m, t_fga)
            league_avg["Mid-Range"] = lg_z(t_mid, t_mid_m, t_fga)
            league_avg["Corner 3"] = lg_z(t_c3, t_c3_m, t_fga)
            league_avg["Above Break 3"] = lg_z(t_atb3, t_atb3_m, t_fga)

        teams_list = data_access.get_team_defense_teams(league=league)

        return {
            "team": row[0],
            "team_name": row[1],
            "total_fga": total_fga,
            "zones": zones,
            "league_avg": league_avg,
            "teams": teams_list,
        }
    except Exception as e:
        return {"error": str(e), "zones": {}}


@app.get("/api/team-defense-shot-chart-teams")
async def api_team_defense_teams(league: str = "nba"):
    try:
        teams = data_access.get_team_defense_teams(league=league)
        return {"teams": teams}
    except Exception:
        return {"teams": []}


@app.get("/api/team-schemes")
async def api_team_schemes(team: str = None):
    try:
        off_rows, def_rows = data_access.get_team_play_types()

        if off_rows is None:
            return {"error": "Play type data not yet available. Run the scheme scraper first.", "teams": []}

        teams_set = sorted(set(r[0] for r in off_rows))

        offense = {}
        for r in off_rows:
            t = r[0]
            if t not in offense:
                offense[t] = []
            offense[t].append({
                "play_type": r[1],
                "label": r[2],
                "poss_pct": round(r[3] * 100, 1),
                "ppp": round(r[4], 3),
                "fg_pct": round(r[5] * 100, 1),
                "tov_pct": round(r[6] * 100, 1),
                "score_pct": round(r[7] * 100, 1),
                "efg_pct": round(r[8] * 100, 1),
                "percentile": round(r[9] * 100),
            })

        defense = {}
        for r in def_rows:
            t = r[0]
            if t not in defense:
                defense[t] = []
            defense[t].append({
                "play_type": r[1],
                "label": r[2],
                "poss_pct": round(r[3] * 100, 1),
                "ppp": round(r[4], 3),
                "fg_pct": round(r[5] * 100, 1),
                "tov_pct": round(r[6] * 100, 1),
                "score_pct": round(r[7] * 100, 1),
                "efg_pct": round(r[8] * 100, 1),
                "percentile": round(r[9] * 100),
            })

        league_avg_off = {}
        for t_plays in offense.values():
            for p in t_plays:
                pt = p["play_type"]
                if pt not in league_avg_off:
                    league_avg_off[pt] = {"poss_pct": [], "ppp": []}
                league_avg_off[pt]["poss_pct"].append(p["poss_pct"])
                league_avg_off[pt]["ppp"].append(p["ppp"])
        league_avg = {}
        for pt, vals in league_avg_off.items():
            league_avg[pt] = {
                "poss_pct": round(sum(vals["poss_pct"]) / len(vals["poss_pct"]), 1),
                "ppp": round(sum(vals["ppp"]) / len(vals["ppp"]), 3),
            }

        return {
            "teams": teams_set,
            "offense": offense,
            "defense": defense,
            "league_avg": league_avg,
        }
    except Exception as e:
        return {"error": str(e), "teams": []}

@app.get("/h2h")
async def h2h_lobby(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return html_redirect("/login")

    today = get_eastern_today()
    contest = db.query(models.Contest).filter(models.Contest.slate_date == today).first()

    open_challenges = []
    active_challenges = []
    history_challenges = []

    if contest:
        open_challenges = db.query(models.H2HChallenge).filter(
            models.H2HChallenge.contest_id == contest.id,
            models.H2HChallenge.status == "open",
            models.H2HChallenge.challenger_id != user.id
        ).order_by(desc(models.H2HChallenge.created_at)).all()

        active_challenges = db.query(models.H2HChallenge).filter(
            models.H2HChallenge.contest_id == contest.id,
            models.H2HChallenge.status.in_(["open", "accepted", "locked"]),
            (models.H2HChallenge.challenger_id == user.id) | (models.H2HChallenge.opponent_id == user.id)
        ).order_by(desc(models.H2HChallenge.created_at)).all()

    history_challenges = db.query(models.H2HChallenge).filter(
        models.H2HChallenge.status == "completed",
        (models.H2HChallenge.challenger_id == user.id) | (models.H2HChallenge.opponent_id == user.id)
    ).order_by(desc(models.H2HChallenge.created_at)).limit(20).all()

    error_msg = request.query_params.get("error", "")
    queued = request.query_params.get("queued", "")

    user_division = format_division(user.division or "Bronze", user.division_tier or 3)
    division_color = DIVISION_COLORS.get(user.division or "Bronze", "#CD7F32")

    return templates.TemplateResponse("h2h_lobby.html", {
        "request": request,
        "user": user,
        "contest": contest,
        "open_challenges": open_challenges,
        "active_challenges": active_challenges,
        "history_challenges": history_challenges,
        "error": error_msg,
        "queued": queued,
        "user_division": user_division,
        "division_color": division_color,
        "user_mmr": user.mmr or 1000,
    })

@app.post("/h2h/create")
async def h2h_create(request: Request, wager: int = Form(...), db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        raise HTTPException(status_code=401, detail="Not logged in")

    today = get_eastern_today()
    contest = db.query(models.Contest).filter(models.Contest.slate_date == today).first()
    if not contest or contest.status != "open":
        raise HTTPException(status_code=400, detail="No active contest today")

    if wager < 5 or wager > 500:
        return html_redirect(f"/h2h?error=Entry+fee+must+be+between+5+and+500+Coach+Coin")

    if user.coins < wager:
        return html_redirect(f"/h2h?error=Not+enough+Coach+Coin.+You+have+{user.coins}+but+tried+to+wager+{wager}.")
    user.coins -= wager
    db.add(models.CurrencyTransaction(
        user_id=user.id,
        amount=-wager,
        transaction_type="h2h_entry_fee",
        description=f"H2H match entry fee ({wager} Coach Coin)"
    ))

    challenge = models.H2HChallenge(
        contest_id=contest.id,
        challenger_id=user.id,
        wager=wager,
        currency_mode="coin",
        match_type="casual",
        status="open"
    )
    db.add(challenge)
    db.commit()

    return html_redirect("/h2h")

@app.post("/h2h/accept/{challenge_id}")
async def h2h_accept(request: Request, challenge_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        raise HTTPException(status_code=401, detail="Not logged in")

    challenge = db.query(models.H2HChallenge).filter(models.H2HChallenge.id == challenge_id).first()
    if not challenge:
        raise HTTPException(status_code=404, detail="Challenge not found")
    if challenge.status != "open":
        return html_redirect("/h2h?error=Challenge+is+no+longer+open")
    if challenge.challenger_id == user.id:
        return html_redirect("/h2h?error=Cannot+accept+your+own+challenge")

    if user.coins < challenge.wager:
        return html_redirect(f"/h2h?error=Not+enough+Coach+Coin.+You+have+{user.coins}+but+need+{challenge.wager}+to+accept.")
    user.coins -= challenge.wager
    db.add(models.CurrencyTransaction(
        user_id=user.id,
        amount=-challenge.wager,
        transaction_type="h2h_entry_fee",
        description=f"Accepted H2H match ({challenge.wager} Coach Coin)"
    ))

    challenge.opponent_id = user.id
    challenge.status = "accepted"
    db.commit()

    return html_redirect(f"/h2h/match/{challenge.id}")

@app.post("/h2h/cancel/{challenge_id}")
async def h2h_cancel(request: Request, challenge_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        raise HTTPException(status_code=401, detail="Not logged in")

    challenge = db.query(models.H2HChallenge).filter(models.H2HChallenge.id == challenge_id).first()
    if not challenge:
        raise HTTPException(status_code=404, detail="Challenge not found")
    if challenge.challenger_id != user.id:
        raise HTTPException(status_code=403, detail="Only the challenger can cancel")
    if challenge.status != "open":
        raise HTTPException(status_code=400, detail="Can only cancel open challenges")

    user.coins += challenge.wager
    db.add(models.CurrencyTransaction(
        user_id=user.id,
        amount=challenge.wager,
        transaction_type="h2h_refund",
        description=f"H2H challenge cancelled - refund ({challenge.wager} Coach Coin)"
    ))
    challenge.status = "cancelled"
    db.commit()

    return html_redirect("/h2h")

@app.get("/h2h/lineup/{challenge_id}")
async def h2h_lineup(request: Request, challenge_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return html_redirect("/login")

    challenge = db.query(models.H2HChallenge).filter(models.H2HChallenge.id == challenge_id).first()
    if not challenge:
        raise HTTPException(status_code=404, detail="Challenge not found")

    is_challenger = challenge.challenger_id == user.id
    is_opponent = challenge.opponent_id == user.id
    if not is_challenger and not is_opponent:
        raise HTTPException(status_code=403, detail="You are not part of this challenge")

    if is_challenger and challenge.challenger_lineup_submitted:
        return html_redirect(f"/h2h/match/{challenge_id}")
    if is_opponent and challenge.opponent_lineup_submitted:
        return html_redirect(f"/h2h/match/{challenge_id}")

    opponent_name = ""
    if is_challenger and challenge.opponent:
        opponent_name = challenge.opponent.display_name or challenge.opponent.username
    elif is_opponent:
        opponent_name = challenge.challenger.display_name or challenge.challenger.username

    import pandas as pd

    try:
        players_df = data_access.get_dfs_players()
        if players_df.empty:
            raise ValueError("No player data available")
        players_df = players_df.dropna(subset=['fd_position', 'salary'])
        players_df['salary'] = players_df['salary'].astype(int)

        game_times_df = data_access.get_player_salaries_game_times()
        injury_df = data_access.get_injury_alerts()
        injury_map = dict(zip(injury_df['player_name'], injury_df['status'])) if not injury_df.empty else {}
        game_times = dict(zip(game_times_df['game'], game_times_df['game_time'])) if not game_times_df.empty else {}

        from zoneinfo import ZoneInfo
        eastern = ZoneInfo("America/New_York")
        now = datetime.now(eastern)

        team_aliases = {
            'NYK': 'NY', 'NY': 'NYK', 'GS': 'GSW', 'GSW': 'GS',
            'SA': 'SAS', 'SAS': 'SA', 'NO': 'NOP', 'NOP': 'NO',
            'UTAH': 'UTA', 'UTA': 'UTAH', 'PHX': 'PHO', 'PHO': 'PHX',
            'CHA': 'CHO', 'CHO': 'CHA', 'BKN': 'BK', 'BK': 'BKN',
        }

        def is_game_locked(game_str):
            game_time_str = game_times.get(game_str)
            if not game_time_str:
                return False
            try:
                game_dt = datetime.strptime(game_time_str, "%I:%M%p")
                game_dt = game_dt.replace(year=now.year, month=now.month, day=now.day, tzinfo=eastern)
                return now >= game_dt
            except:
                return False

        players_df['game'] = players_df['team'] + " vs " + players_df['opponent']

        for game_key in list(game_times.keys()):
            if " @ " not in game_key:
                continue
            away, home = game_key.split(" @ ")
            combos = [(away, home)]
            away_alt = team_aliases.get(away)
            home_alt = team_aliases.get(home)
            if away_alt:
                combos.append((away_alt, home))
            if home_alt:
                combos.append((away, home_alt))
            if away_alt and home_alt:
                combos.append((away_alt, home_alt))
            for a, h in combos:
                game_times[f"{a} vs {h}"] = game_times[game_key]
                game_times[f"{h} vs {a}"] = game_times[game_key]

        players_df['is_locked'] = players_df.apply(
            lambda row: is_game_locked(f"{row['team']} vs {row['opponent']}") or
                       is_game_locked(f"{row['opponent']} vs {row['team']}"),
            axis=1
        )
        players_df['game_time'] = players_df.apply(
            lambda row: game_times.get(f"{row['team']} vs {row['opponent']}") or
                       game_times.get(f"{row['opponent']} vs {row['team']}") or "",
            axis=1
        )
        players_df['injury_status'] = players_df['player_name'].map(injury_map).fillna('')
        players_df['position'] = players_df['fd_position']
        players = players_df.to_dict("records")
    except Exception as e:
        print(f"Error loading players for H2H: {e}")
        import traceback
        traceback.print_exc()
        players = []

    headshots = get_player_headshots()
    return templates.TemplateResponse("h2h_lineup.html", {
        "request": request,
        "user": user,
        "challenge": challenge,
        "opponent_name": opponent_name,
        "players": players,
        "headshots": headshots,
    })

@app.post("/h2h/submit-lineup")
async def h2h_submit_lineup(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        raise HTTPException(status_code=401, detail="Not logged in")

    form = await request.form()
    player_ids = form.getlist("players")
    challenge_id = int(form.get("challenge_id", 0))

    challenge = db.query(models.H2HChallenge).filter(models.H2HChallenge.id == challenge_id).first()
    if not challenge:
        raise HTTPException(status_code=404, detail="Challenge not found")

    is_challenger = challenge.challenger_id == user.id
    is_opponent = challenge.opponent_id == user.id
    if not is_challenger and not is_opponent:
        raise HTTPException(status_code=403, detail="You are not part of this challenge")

    if is_challenger and challenge.challenger_lineup_submitted:
        raise HTTPException(status_code=400, detail="You already submitted your lineup")
    if is_opponent and challenge.opponent_lineup_submitted:
        raise HTTPException(status_code=400, detail="You already submitted your lineup")

    today = get_eastern_today()
    contest = db.query(models.Contest).filter(models.Contest.slate_date == today).first()
    if not contest or contest.status not in ("open", "active"):
        raise HTTPException(status_code=400, detail="Contest is not open")

    if get_eastern_now().replace(tzinfo=None) >= contest.lock_time:
        raise HTTPException(status_code=400, detail="Contest is locked - games have started")

    if len(player_ids) != 9:
        raise HTTPException(status_code=400, detail="Lineup must have exactly 9 players")

    import pandas as pd
    players_df = data_access.get_dfs_players()
    if players_df.empty:
        raise HTTPException(status_code=400, detail="No player data available")

    locked_teams, _, _ = get_game_lock_status()

    total_salary = 0
    player_entries = []
    for player_name in player_ids:
        matches = players_df[players_df["player_name"] == player_name]
        if len(matches) == 0:
            raise HTTPException(status_code=400, detail=f"Player not found: {player_name}")
        player_data = matches.iloc[0]
        team = str(player_data.get("team", ""))
        if team in locked_teams:
            raise HTTPException(status_code=400, detail=f"{player_name}'s game has already started")
        total_salary += int(player_data.get("salary", 0))
        player_entries.append(player_data)

    SALARY_CAP = 60000
    if total_salary > SALARY_CAP:
        raise HTTPException(status_code=400, detail=f"Lineup exceeds salary cap: ${total_salary:,} > ${SALARY_CAP:,}")

    for player_data in player_entries:
        lp = models.H2HLineupPlayer(
            challenge_id=challenge.id,
            user_id=user.id,
            player_name=str(player_data.get("player_name", "")),
            position=str(player_data.get("fd_position", "")),
            team=str(player_data.get("team", "")),
            salary=int(player_data.get("salary", 0)),
            proj_fp=float(player_data.get("proj_fp", 0))
        )
        db.add(lp)

    if is_challenger:
        challenge.challenger_lineup_submitted = True
    else:
        challenge.opponent_lineup_submitted = True

    if challenge.challenger_lineup_submitted and challenge.opponent_lineup_submitted:
        challenge.status = "locked"

    db.commit()

    return html_redirect(f"/h2h/match/{challenge.id}")

@app.get("/h2h/match/{challenge_id}")
async def h2h_match(request: Request, challenge_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return html_redirect("/login")

    challenge = db.query(models.H2HChallenge).filter(models.H2HChallenge.id == challenge_id).first()
    if not challenge:
        raise HTTPException(status_code=404, detail="Challenge not found")

    is_challenger = challenge.challenger_id == user.id
    is_opponent = challenge.opponent_id == user.id

    challenger_players = db.query(models.H2HLineupPlayer).filter(
        models.H2HLineupPlayer.challenge_id == challenge.id,
        models.H2HLineupPlayer.user_id == challenge.challenger_id
    ).all()

    opponent_players = []
    if challenge.opponent_id:
        opponent_players = db.query(models.H2HLineupPlayer).filter(
            models.H2HLineupPlayer.challenge_id == challenge.id,
            models.H2HLineupPlayer.user_id == challenge.opponent_id
        ).all()

    locked_teams, any_started, team_game_times = get_game_lock_status()
    is_live = any_started and challenge.status in ("locked", "accepted")

    needs_lineup = False
    if is_challenger and not challenge.challenger_lineup_submitted:
        needs_lineup = True
    elif is_opponent and not challenge.opponent_lineup_submitted:
        needs_lineup = True

    headshots = get_player_headshots()
    return templates.TemplateResponse("h2h_match.html", {
        "request": request,
        "user": user,
        "challenge": challenge,
        "challenger_players": challenger_players,
        "opponent_players": opponent_players,
        "locked_teams": locked_teams,
        "any_started": any_started,
        "is_live": is_live,
        "team_game_times": team_game_times,
        "is_challenger": is_challenger,
        "is_opponent": is_opponent,
        "needs_lineup": needs_lineup,
        "headshots": headshots,
        "match_type": challenge.match_type or "casual",
        "mmr_change_challenger": challenge.mmr_change_challenger or 0,
        "mmr_change_opponent": challenge.mmr_change_opponent or 0,
    })

@app.get("/api/live-h2h/{challenge_id}")
async def api_live_h2h(request: Request, challenge_id: int, db: Session = Depends(get_db)):
    challenge = db.query(models.H2HChallenge).filter(models.H2HChallenge.id == challenge_id).first()
    if not challenge:
        return {"error": "Challenge not found"}

    now = time.time()
    if now - _live_scores_cache["timestamp"] >= LIVE_SCORES_CACHE_TTL or not _live_scores_cache["data"]:
        try:
            from scrape_live_scores import get_live_scores_summary
            scores = get_live_scores_summary()
            _live_scores_cache["data"] = scores
            _live_scores_cache["timestamp"] = now
        except Exception as e:
            scores = _live_scores_cache.get("data", {})
    else:
        scores = _live_scores_cache["data"]

    from utils.name_normalize import normalize_player_name
    locked_teams, any_started, team_game_times = get_game_lock_status()

    def build_live_list(players_query):
        live_list = []
        total = 0
        for p in players_query:
            norm = normalize_player_name(p.player_name)
            live = scores.get(norm, {})
            game_started = (p.team or '') in locked_teams
            fp = live.get('fp', 0) if game_started else 0
            total += fp
            live_list.append({
                'player_name': p.player_name,
                'position': p.position,
                'team': p.team or '',
                'salary': p.salary,
                'proj_fp': p.proj_fp,
                'live_fp': fp,
                'game_started': game_started,
                'game_time': team_game_times.get(p.team or '', ''),
            })
        return live_list, total

    challenger_ps = db.query(models.H2HLineupPlayer).filter(
        models.H2HLineupPlayer.challenge_id == challenge.id,
        models.H2HLineupPlayer.user_id == challenge.challenger_id
    ).all()

    opponent_ps = []
    if challenge.opponent_id:
        opponent_ps = db.query(models.H2HLineupPlayer).filter(
            models.H2HLineupPlayer.challenge_id == challenge.id,
            models.H2HLineupPlayer.user_id == challenge.opponent_id
        ).all()

    challenger_live, challenger_total = build_live_list(challenger_ps)
    opponent_live, opponent_total = build_live_list(opponent_ps)

    contest = challenge.contest
    if contest and contest.status == 'completed' and challenge.status == 'locked':
        try:
            settle_h2h_challenges(db)
            db.refresh(challenge)
        except Exception as e:
            print(f"H2H auto-settle error: {e}")

    return {
        'challenger_players': challenger_live,
        'challenger_total': round(challenger_total, 1),
        'opponent_players': opponent_live,
        'opponent_total': round(opponent_total, 1),
        'status': challenge.status,
        'any_game_started': any_started,
    }

@app.post("/h2h/queue")
async def h2h_ranked_queue(request: Request, match_type: str = Form("ranked"), db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        raise HTTPException(status_code=401, detail="Not logged in")
    
    today = get_eastern_today()
    contest = db.query(models.Contest).filter(models.Contest.slate_date == today).first()
    if not contest or contest.status != "open":
        return html_redirect("/h2h?error=No+active+contest+today")
    
    if match_type not in ("ranked", "match_night"):
        match_type = "ranked"
    
    existing = db.query(models.H2HChallenge).filter(
        models.H2HChallenge.contest_id == contest.id,
        models.H2HChallenge.challenger_id == user.id,
        models.H2HChallenge.match_type.in_(["ranked", "match_night"]),
        models.H2HChallenge.status == "open"
    ).first()
    if existing:
        return html_redirect("/h2h?error=Already+in+ranked+queue")
    
    mmr_low, mmr_high = get_matchmaking_range(user.mmr or 1000)
    
    match = db.query(models.H2HChallenge).filter(
        models.H2HChallenge.contest_id == contest.id,
        models.H2HChallenge.status == "open",
        models.H2HChallenge.match_type == match_type,
        models.H2HChallenge.challenger_id != user.id
    ).join(models.User, models.User.id == models.H2HChallenge.challenger_id).filter(
        models.User.mmr >= mmr_low,
        models.User.mmr <= mmr_high
    ).order_by(func.abs(models.User.mmr - (user.mmr or 1000))).first()
    
    if match:
        match.opponent_id = user.id
        match.status = "accepted"
        db.commit()
        return html_redirect(f"/h2h/match/{match.id}")
    else:
        challenge = models.H2HChallenge(
            contest_id=contest.id,
            challenger_id=user.id,
            wager=0,
            currency_mode="coin",
            match_type=match_type,
            status="open"
        )
        db.add(challenge)
        db.commit()
        return html_redirect("/h2h?queued=1")

@app.get("/api/notifications")
async def api_notifications(request: Request, category: str = None,
                            limit: int = 20, offset: int = 0,
                            db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    from backend.notifications import get_notifications, notification_to_dict
    notifs = get_notifications(db, user.id, category=category, limit=limit, offset=offset)
    return JSONResponse({"notifications": [notification_to_dict(n) for n in notifs]})


@app.get("/api/notifications/unread-count")
async def api_notifications_unread(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return JSONResponse({"count": 0})
    from backend.notifications import get_unread_count
    return JSONResponse({"count": get_unread_count(db, user.id)})


@app.post("/api/notifications/{notification_id}/read")
async def api_notification_mark_read(request: Request, notification_id: int,
                                     db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    from backend.notifications import mark_read
    success = mark_read(db, user.id, notification_id)
    db.commit()
    return JSONResponse({"success": success})


@app.post("/api/notifications/read-all")
async def api_notifications_read_all(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    from backend.notifications import mark_all_read
    count = mark_all_read(db, user.id)
    db.commit()
    return JSONResponse({"marked": count})


def settle_h2h_challenges(db: Session):
    locked_challenges = db.query(models.H2HChallenge).filter(
        models.H2HChallenge.status == "locked"
    ).all()

    if not locked_challenges:
        return

    try:
        from scrape_live_scores import get_live_scores_summary
        scores = get_live_scores_summary()
    except:
        scores = {}

    from utils.name_normalize import normalize_player_name

    for challenge in locked_challenges:
        challenger_ps = db.query(models.H2HLineupPlayer).filter(
            models.H2HLineupPlayer.challenge_id == challenge.id,
            models.H2HLineupPlayer.user_id == challenge.challenger_id
        ).all()
        opponent_ps = db.query(models.H2HLineupPlayer).filter(
            models.H2HLineupPlayer.challenge_id == challenge.id,
            models.H2HLineupPlayer.user_id == challenge.opponent_id
        ).all()

        c_total = 0
        for p in challenger_ps:
            norm = normalize_player_name(p.player_name)
            fp = scores.get(norm, {}).get('fp', 0)
            p.actual_fp = fp
            c_total += fp

        o_total = 0
        for p in opponent_ps:
            norm = normalize_player_name(p.player_name)
            fp = scores.get(norm, {}).get('fp', 0)
            p.actual_fp = fp
            o_total += fp

        challenge.challenger_score = round(c_total, 1)
        challenge.opponent_score = round(o_total, 1)

        is_ranked = (challenge.match_type or "casual") in ("ranked", "match_night")

        if c_total > o_total:
            challenge.winner_id = challenge.challenger_id
            winner = db.query(models.User).filter(models.User.id == challenge.challenger_id).first()
        elif o_total > c_total:
            challenge.winner_id = challenge.opponent_id
            winner = db.query(models.User).filter(models.User.id == challenge.opponent_id).first()
        else:
            if not is_ranked and challenge.wager > 0:
                challenger_user = db.query(models.User).filter(models.User.id == challenge.challenger_id).first()
                opponent_user = db.query(models.User).filter(models.User.id == challenge.opponent_id).first()
                if challenger_user:
                    challenger_user.coins += challenge.wager
                    db.add(models.CurrencyTransaction(
                        user_id=challenger_user.id, amount=challenge.wager,
                        transaction_type="h2h_tie_refund", description="H2H tie - Coach Coin refunded"
                    ))
                if opponent_user:
                    opponent_user.coins += challenge.wager
                    db.add(models.CurrencyTransaction(
                        user_id=opponent_user.id, amount=challenge.wager,
                        transaction_type="h2h_tie_refund", description="H2H tie - Coach Coin refunded"
                    ))
            if is_ranked:
                challenge.mmr_change_challenger = 0
                challenge.mmr_change_opponent = 0
            challenge.status = "completed"
            continue

        if not is_ranked and challenge.wager > 0 and winner:
            total_pot = challenge.wager * 2
            house_cut = max(1, int(total_pot * 0.1))
            winnings = total_pot - house_cut
            winner.coins += winnings
            db.add(models.CurrencyTransaction(
                user_id=winner.id, amount=winnings,
                transaction_type="h2h_win", description=f"H2H match won! (+{winnings} Coach Coin)"
            ))

        challenge.status = "completed"

        # Apply MMR changes for ranked matches
        match_type = challenge.match_type or "casual"
        if match_type in ("ranked", "match_night"):
            challenger_user_r = db.query(models.User).filter(models.User.id == challenge.challenger_id).first()
            opponent_user_r = db.query(models.User).filter(models.User.id == challenge.opponent_id).first()
            
            if challenger_user_r and opponent_user_r and challenge.winner_id:
                winner_is_challenger = challenge.winner_id == challenge.challenger_id
                w_mmr = challenger_user_r.mmr if winner_is_challenger else opponent_user_r.mmr
                l_mmr = opponent_user_r.mmr if winner_is_challenger else challenger_user_r.mmr
                w_score = challenge.challenger_score if winner_is_challenger else challenge.opponent_score
                l_score = challenge.opponent_score if winner_is_challenger else challenge.challenger_score
                
                w_proj = sum(p.proj_fp or 0 for p in challenger_ps) if winner_is_challenger else sum(p.proj_fp or 0 for p in opponent_ps)
                l_proj = sum(p.proj_fp or 0 for p in opponent_ps) if winner_is_challenger else sum(p.proj_fp or 0 for p in challenger_ps)
                
                w_change, l_change = calculate_mmr_change(
                    w_mmr or 1000, l_mmr or 1000, w_score, l_score, w_proj, l_proj, match_type
                )
                
                winner_user = challenger_user_r if winner_is_challenger else opponent_user_r
                loser_user = opponent_user_r if winner_is_challenger else challenger_user_r
                
                winner_result = update_user_ranking(winner_user, challenge.winner_id, w_change)
                loser_result = update_user_ranking(loser_user, challenge.winner_id, l_change)
                
                if winner_is_challenger:
                    challenge.mmr_change_challenger = w_change
                    challenge.mmr_change_opponent = l_change
                else:
                    challenge.mmr_change_challenger = l_change
                    challenge.mmr_change_opponent = w_change

                try:
                    from backend.achievements import check_ranked_achievements
                    check_ranked_achievements(db, winner_user.id, challenge, winner_result)
                    check_ranked_achievements(db, loser_user.id, challenge, loser_result)
                except Exception as e:
                    print(f"Ranked achievement check error: {e}")

                try:
                    from backend.events import emit_rank_change
                    emit_rank_change(
                        db, winner_user.id,
                        old_mmr=winner_result["old_mmr"], new_mmr=winner_result["new_mmr"],
                        old_division=winner_result["old_division"], new_division=winner_result["new_division"],
                        mmr_change=winner_result["mmr_change"],
                        promoted=winner_result.get("promoted", False),
                        demoted=winner_result.get("demoted", False),
                    )
                    emit_rank_change(
                        db, loser_user.id,
                        old_mmr=loser_result["old_mmr"], new_mmr=loser_result["new_mmr"],
                        old_division=loser_result["old_division"], new_division=loser_result["new_division"],
                        mmr_change=loser_result["mmr_change"],
                        promoted=loser_result.get("promoted", False),
                        demoted=loser_result.get("demoted", False),
                    )
                except Exception as e:
                    print(f"Rank notification error: {e}")

        try:
            from backend.events import emit_h2h_result
            if challenge.winner_id:
                loser_id = challenge.opponent_id if challenge.winner_id == challenge.challenger_id else challenge.challenger_id
                winner_u = db.query(models.User).filter(models.User.id == challenge.winner_id).first()
                loser_u = db.query(models.User).filter(models.User.id == loser_id).first()
                w_score = challenge.challenger_score if challenge.winner_id == challenge.challenger_id else challenge.opponent_score
                l_score = challenge.opponent_score if challenge.winner_id == challenge.challenger_id else challenge.challenger_score
                is_ranked_match = (challenge.match_type or "casual") in ("ranked", "match_night")
                w_mmr_delta = challenge.mmr_change_challenger if challenge.winner_id == challenge.challenger_id else challenge.mmr_change_opponent
                l_mmr_delta = challenge.mmr_change_opponent if challenge.winner_id == challenge.challenger_id else challenge.mmr_change_challenger
                if winner_u and loser_u:
                    emit_h2h_result(db, challenge.winner_id, loser_u.display_name or loser_u.username,
                                    won=True, user_score=w_score, opponent_score=l_score,
                                    mmr_change=w_mmr_delta or 0, is_ranked=is_ranked_match)
                    emit_h2h_result(db, loser_id, winner_u.display_name or winner_u.username,
                                    won=False, user_score=l_score, opponent_score=w_score,
                                    mmr_change=l_mmr_delta or 0, is_ranked=is_ranked_match)
        except Exception as e:
            print(f"H2H notification error: {e}")

        try:
            from backend.achievements import check_h2h_achievements
            if challenge.winner_id:
                check_h2h_achievements(db, challenge.winner_id, challenge)
        except Exception as e:
            print(f"H2H achievement check error: {e}")

    db.commit()

@app.post("/api/cookie-consent")
async def set_cookie_consent(request: Request, db: Session = Depends(get_db)):
    import hashlib, secrets
    user = get_current_user(request, db)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid request")
    analytics = body.get("analytics", False)
    consent_id = request.cookies.get("consent_id")
    if not consent_id:
        consent_id = secrets.token_urlsafe(32)
    existing = db.query(models.CookieConsent).filter(
        models.CookieConsent.consent_id == consent_id
    ).first()
    if existing:
        existing.analytics_consent = analytics
        if user:
            existing.user_id = user.id
    else:
        record = models.CookieConsent(
            consent_id=consent_id,
            analytics_consent=analytics,
            user_id=user.id if user else None,
        )
        db.add(record)
    db.commit()
    resp = JSONResponse({"status": "ok", "consent_id": consent_id, "analytics": analytics})
    resp.set_cookie(
        "consent_id", consent_id,
        max_age=365 * 24 * 3600, httponly=True, samesite="lax", path="/",
    )
    resp.set_cookie(
        "analytics_consent", "1" if analytics else "0",
        max_age=365 * 24 * 3600, httponly=False, samesite="lax", path="/",
    )
    return resp


@app.get("/api/cookie-consent")
async def get_cookie_consent(request: Request, db: Session = Depends(get_db)):
    consent_id = request.cookies.get("consent_id")
    if not consent_id:
        return JSONResponse({"has_consent": False, "analytics": False})
    record = db.query(models.CookieConsent).filter(
        models.CookieConsent.consent_id == consent_id
    ).first()
    if not record:
        return JSONResponse({"has_consent": False, "analytics": False})
    return JSONResponse({
        "has_consent": True,
        "analytics": record.analytics_consent,
    })


@app.post("/api/track")
async def track_page_view(request: Request, db: Session = Depends(get_db)):
    import hashlib
    consent_id = request.cookies.get("consent_id")
    if not consent_id:
        return JSONResponse({"status": "skipped", "reason": "no_consent"})
    consent_record = db.query(models.CookieConsent).filter(
        models.CookieConsent.consent_id == consent_id,
        models.CookieConsent.analytics_consent == True,
    ).first()
    if not consent_record:
        return JSONResponse({"status": "skipped", "reason": "no_consent"})
    user = get_current_user(request, db)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid request")
    path = body.get("path", "")[:500]
    referrer = body.get("referrer", "")[:1000] or None
    ua = request.headers.get("user-agent", "")[:1000]
    forwarded = request.headers.get("x-forwarded-for", request.client.host if request.client else "")
    ip_hash = hashlib.sha256(forwarded.encode()).hexdigest()[:16] if forwarded else None
    ua_lower = ua.lower()
    if "mobile" in ua_lower or "android" in ua_lower or "iphone" in ua_lower:
        device_type = "mobile"
    elif "tablet" in ua_lower or "ipad" in ua_lower:
        device_type = "tablet"
    else:
        device_type = "desktop"
    pv = models.PageView(
        path=path,
        referrer=referrer,
        user_agent=ua,
        ip_hash=ip_hash,
        user_id=user.id if user else None,
        session_id=request.cookies.get("consent_id"),
        device_type=device_type,
    )
    db.add(pv)
    db.commit()
    return JSONResponse({"status": "ok"})


@app.get("/cookie-settings")
async def cookie_settings_page(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    consent_id = request.cookies.get("consent_id")
    analytics_on = False
    if consent_id:
        record = db.query(models.CookieConsent).filter(
            models.CookieConsent.consent_id == consent_id
        ).first()
        if record:
            analytics_on = record.analytics_consent
    return templates.TemplateResponse("cookie_settings.html", {
        "request": request,
        "user": user,
        "analytics_on": analytics_on,
    })


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
