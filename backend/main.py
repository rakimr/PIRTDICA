from fastapi import FastAPI, Request, Depends, Form, HTTPException, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from datetime import datetime, date
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

class NoCacheMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/static"):
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response

app.add_middleware(NoCacheMiddleware)
app.mount("/static", StaticFiles(directory="static"), name="static")
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
        samesite="lax"
    )

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

@app.get("/")
async def home(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
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
        else:
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

    edge_insights = []
    player_count = 0
    game_count = len(slate_games)
    if not user:
        try:
            import math
            props_df = data_access.get_prop_recommendations()
            if not props_df.empty:
                props_df = props_df[props_df.get('salary', props_df.get('salary', 0)) > 0] if 'salary' in props_df.columns else props_df
                has_book = 'vs_book_edge' in props_df.columns and props_df['vs_book_edge'].notna().any()
                edge_col = 'vs_book_edge' if has_book else 'edge_pct'
                if edge_col in props_df.columns:
                    valid = props_df[props_df[edge_col].notna() & props_df[edge_col].apply(lambda x: not (isinstance(x, float) and math.isnan(x)))]
                    if not valid.empty:
                        valid_abs = valid.copy()
                        valid_abs['_abs_edge'] = valid_abs[edge_col].abs()
                        top_props = valid_abs.nlargest(3, '_abs_edge')
                        for _, row in top_props.iterrows():
                            rec = row.get('recommendation', 'OVER')
                            stat = row.get('stat', '')
                            book_line = row.get('book_line', None)
                            line_display = ''
                            if book_line is not None and not (isinstance(book_line, float) and math.isnan(book_line)):
                                line_display = str(book_line)
                            elif row.get('adjusted_avg') is not None and not (isinstance(row.get('adjusted_avg'), float) and math.isnan(row.get('adjusted_avg', 0))):
                                line_display = f"proj {row['adjusted_avg']}"
                            edge_val = row.get(edge_col, 0)
                            if isinstance(edge_val, float) and math.isnan(edge_val):
                                edge_val = 0
                            name = row.get('player', row.get('player_name', ''))
                            opp = row.get('opponent', '')
                            edge_insights.append({
                                "player": name, "stat": stat, "line": line_display,
                                "edge": round(float(edge_val), 1), "rec": rec, "opponent": opp
                            })
        except:
            pass
        try:
            dfs_df = data_access.get_dfs_players()
            if not dfs_df.empty:
                player_count = len(dfs_df[dfs_df.get('salary', dfs_df.get('salary', 0)) > 0]) if 'salary' in dfs_df.columns else len(dfs_df)
        except:
            pass

    return templates.TemplateResponse("home.html", {
        "request": request,
        "user": user,
        "contest": contest,
        "house_players": house_players,
        "user_entry": user_entry,
        "headshots": headshots,
        "no_games_today": no_games_today,
        "is_todays_contest": is_todays_contest,
        "next_game_iso": next_game_iso,
        "games_started": games_started,
        "slate_games": slate_games,
        "edge_insights": edge_insights,
        "player_count": player_count,
        "game_count": game_count,
    })

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
    
    db.add(models.CurrencyTransaction(
        user_id=user.id,
        amount=100,
        transaction_type="signup_bonus",
        description="Welcome bonus!"
    ))
    db.commit()
    
    token = auth.create_session(db, user.id)
    response = RedirectResponse(url="/", status_code=303)
    set_session_cookie(response, token)
    return response

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
    if not user or not auth.verify_password(password, user.password_hash):
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": "Invalid username or password"
        })
    
    if getattr(user, 'is_banned', False):
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": "This account has been banned"
        })
    
    token = auth.create_session(db, user.id)
    response = RedirectResponse(url="/", status_code=303)
    set_session_cookie(response, token)
    return response

@app.get("/logout")
async def logout(request: Request, db: Session = Depends(get_db)):
    token = request.cookies.get("session_token")
    if token:
        auth.delete_session(db, token)
    response = RedirectResponse(url="/", status_code=303)
    response.delete_cookie("session_token")
    return response

@app.get("/trends")
async def trends(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
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
        explorer_df = dfs_df.copy() if not dfs_df.empty else pd.DataFrame()
        if not explorer_df.empty:
            injury_df = data_access.get_injury_alerts()
            if not injury_df.empty:
                inj_map = dict(zip(injury_df['player_name'], injury_df['status']))
                explorer_df['injury_status'] = explorer_df['player_name'].map(inj_map).fillna('')
            else:
                explorer_df['injury_status'] = ''
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
            keep_cols = ['player_name', 'true_position', 'team', 'opponent', 'injury_status']
            keep_cols = [c for c in keep_cols if c in explorer_df.columns]
            explorer_players = explorer_df[keep_cols].to_dict('records')
    except:
        pass

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
    
    cash_transactions = db.query(models.CashTransaction).filter(
        models.CashTransaction.user_id == profile_user.id
    ).order_by(desc(models.CashTransaction.created_at)).limit(20).all()

    coin_transactions = db.query(models.CurrencyTransaction).filter(
        models.CurrencyTransaction.user_id == profile_user.id
    ).order_by(desc(models.CurrencyTransaction.created_at)).limit(20).all()

    h2h_cash_earnings = 0
    for c in h2h_completed:
        if c.winner_id == profile_user.id and (c.currency_mode or "coin") == "cash":
            total_pot = c.wager * 2
            house_cut = max(1, int(total_pot * 0.1))
            h2h_cash_earnings += (total_pot - house_cut - c.wager)

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
    
    return templates.TemplateResponse("profile.html", {
        "request": request,
        "user": current_user,
        "profile": profile_user,
        "entries": entries,
        "stats": stats,
        "badge_groups": ordered_badge_groups,
        "h2h_stats": {"wins": h2h_wins, "losses": h2h_losses, "ties": h2h_ties, "total": len(h2h_completed), "earnings": h2h_earnings, "cash_earnings": h2h_cash_earnings},
        "h2h_history": h2h_history,
        "cash_transactions": cash_transactions,
        "coin_transactions": coin_transactions,
        "coach_rank": coach_rank,
        "error": error_msg,
        "success": success_msg,
        "theme_data": theme_data,
        "cosmetic_badges": cosmetic_badges,
    })

@app.get("/history")
async def history(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    
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
        return RedirectResponse(url="/login", status_code=303)
    
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
    
    return RedirectResponse(url="/shop", status_code=303)

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
    return RedirectResponse(url="/shop", status_code=303)

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
    return RedirectResponse(url="/shop", status_code=303)

@app.get("/play")
async def play(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    
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
        return RedirectResponse(url=f"/entry/{existing_entry.id}", status_code=303)
    
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
    
    return RedirectResponse(url=f"/entry/{entry.id}", status_code=303)

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
    
    return templates.TemplateResponse("admin.html", {
        "request": request,
        "user": user,
        "refresh_status": refresh_status,
        "contest": contest,
        "house_players": house_players
    })

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
async def api_player_shot_chart(player_name: str):
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
        df = data_access.get_player_shot_zone_detail(player_name)
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
async def api_team_defense_shot_chart(team: str):
    try:
        row, all_teams = data_access.get_team_defense_shot_zone(team)

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

        teams_list = data_access.get_team_defense_teams()

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
async def api_team_defense_teams():
    try:
        teams = data_access.get_team_defense_teams()
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

CASH_TO_COIN_RATE = 5

@app.post("/convert-cash")
async def convert_cash_to_coin(request: Request, amount: int = Form(...), db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    
    if amount < 1:
        return RedirectResponse(url=f"/profile/{user.username}?error=Minimum+conversion+is+1+Coach+Cash", status_code=303)
    
    if user.coach_cash < amount:
        return RedirectResponse(url=f"/profile/{user.username}?error=Not+enough+Coach+Cash", status_code=303)
    
    coin_gain = amount * CASH_TO_COIN_RATE
    user.coach_cash -= amount
    user.coins += coin_gain
    
    db.add(models.CashTransaction(
        user_id=user.id,
        amount=-amount,
        transaction_type="convert_to_coin",
        description=f"Converted {amount} Coach Cash to {coin_gain} Coach Coin"
    ))
    db.add(models.CurrencyTransaction(
        user_id=user.id,
        amount=coin_gain,
        transaction_type="convert_from_cash",
        description=f"Converted from {amount} Coach Cash (1:{CASH_TO_COIN_RATE} rate)"
    ))
    db.commit()
    
    return RedirectResponse(url=f"/profile/{user.username}?success=Converted+{amount}+Coach+Cash+to+{coin_gain}+Coach+Coin", status_code=303)

@app.get("/h2h")
async def h2h_lobby(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)

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
async def h2h_create(request: Request, wager: int = Form(...), currency_mode: str = Form("coin"), db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        raise HTTPException(status_code=401, detail="Not logged in")

    today = get_eastern_today()
    contest = db.query(models.Contest).filter(models.Contest.slate_date == today).first()
    if not contest or contest.status != "open":
        raise HTTPException(status_code=400, detail="No active contest today")

    if currency_mode not in ("coin", "cash"):
        currency_mode = "coin"

    if wager < 5 or wager > 500:
        label = "Coach Coin" if currency_mode == "coin" else "Coach Cash"
        return RedirectResponse(url=f"/h2h?error=Entry+fee+must+be+between+5+and+500+{label}", status_code=303)

    if currency_mode == "coin":
        if user.coins < wager:
            return RedirectResponse(url=f"/h2h?error=Not+enough+Coach+Coin.+You+have+{user.coins}+but+tried+to+wager+{wager}.", status_code=303)
        user.coins -= wager
        db.add(models.CurrencyTransaction(
            user_id=user.id,
            amount=-wager,
            transaction_type="h2h_entry_fee",
            description=f"H2H match entry fee ({wager} Coach Coin)"
        ))
    else:
        if user.coach_cash < wager:
            return RedirectResponse(url=f"/h2h?error=Not+enough+Coach+Cash.+You+have+{user.coach_cash}+but+tried+to+wager+{wager}.", status_code=303)
        user.coach_cash -= wager
        db.add(models.CashTransaction(
            user_id=user.id,
            amount=-wager,
            transaction_type="h2h_entry_fee",
            description=f"H2H match entry fee ({wager} Coach Cash)"
        ))

    challenge = models.H2HChallenge(
        contest_id=contest.id,
        challenger_id=user.id,
        wager=wager,
        currency_mode=currency_mode,
        match_type="casual",
        status="open"
    )
    db.add(challenge)
    db.commit()

    return RedirectResponse(url="/h2h", status_code=303)

@app.post("/h2h/accept/{challenge_id}")
async def h2h_accept(request: Request, challenge_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        raise HTTPException(status_code=401, detail="Not logged in")

    challenge = db.query(models.H2HChallenge).filter(models.H2HChallenge.id == challenge_id).first()
    if not challenge:
        raise HTTPException(status_code=404, detail="Challenge not found")
    if challenge.status != "open":
        return RedirectResponse(url="/h2h?error=Challenge+is+no+longer+open", status_code=303)
    if challenge.challenger_id == user.id:
        return RedirectResponse(url="/h2h?error=Cannot+accept+your+own+challenge", status_code=303)

    mode = challenge.currency_mode or "coin"
    if mode == "coin":
        if user.coins < challenge.wager:
            return RedirectResponse(url=f"/h2h?error=Not+enough+Coach+Coin.+You+have+{user.coins}+but+need+{challenge.wager}+to+accept.", status_code=303)
        user.coins -= challenge.wager
        db.add(models.CurrencyTransaction(
            user_id=user.id,
            amount=-challenge.wager,
            transaction_type="h2h_entry_fee",
            description=f"Accepted H2H match ({challenge.wager} Coach Coin)"
        ))
    else:
        if user.coach_cash < challenge.wager:
            return RedirectResponse(url=f"/h2h?error=Not+enough+Coach+Cash.+You+have+{user.coach_cash}+but+need+{challenge.wager}+to+accept.", status_code=303)
        user.coach_cash -= challenge.wager
        db.add(models.CashTransaction(
            user_id=user.id,
            amount=-challenge.wager,
            transaction_type="h2h_entry_fee",
            description=f"Accepted H2H match ({challenge.wager} Coach Cash)"
        ))

    challenge.opponent_id = user.id
    challenge.status = "accepted"
    db.commit()

    return RedirectResponse(url=f"/h2h/match/{challenge.id}", status_code=303)

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

    mode = challenge.currency_mode or "coin"
    if mode == "coin":
        user.coins += challenge.wager
        db.add(models.CurrencyTransaction(
            user_id=user.id,
            amount=challenge.wager,
            transaction_type="h2h_refund",
            description=f"H2H challenge cancelled - refund ({challenge.wager} Coach Coin)"
        ))
    else:
        user.coach_cash += challenge.wager
        db.add(models.CashTransaction(
            user_id=user.id,
            amount=challenge.wager,
            transaction_type="h2h_refund",
            description=f"H2H challenge cancelled - refund ({challenge.wager} Coach Cash)"
        ))
    challenge.status = "cancelled"
    db.commit()

    return RedirectResponse(url="/h2h", status_code=303)

@app.get("/h2h/lineup/{challenge_id}")
async def h2h_lineup(request: Request, challenge_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)

    challenge = db.query(models.H2HChallenge).filter(models.H2HChallenge.id == challenge_id).first()
    if not challenge:
        raise HTTPException(status_code=404, detail="Challenge not found")

    is_challenger = challenge.challenger_id == user.id
    is_opponent = challenge.opponent_id == user.id
    if not is_challenger and not is_opponent:
        raise HTTPException(status_code=403, detail="You are not part of this challenge")

    if is_challenger and challenge.challenger_lineup_submitted:
        return RedirectResponse(url=f"/h2h/match/{challenge_id}", status_code=303)
    if is_opponent and challenge.opponent_lineup_submitted:
        return RedirectResponse(url=f"/h2h/match/{challenge_id}", status_code=303)

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

    return RedirectResponse(url=f"/h2h/match/{challenge.id}", status_code=303)

@app.get("/h2h/match/{challenge_id}")
async def h2h_match(request: Request, challenge_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)

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
        return RedirectResponse(url="/h2h?error=No+active+contest+today", status_code=303)
    
    if match_type not in ("ranked", "match_night"):
        match_type = "ranked"
    
    existing = db.query(models.H2HChallenge).filter(
        models.H2HChallenge.contest_id == contest.id,
        models.H2HChallenge.challenger_id == user.id,
        models.H2HChallenge.match_type.in_(["ranked", "match_night"]),
        models.H2HChallenge.status == "open"
    ).first()
    if existing:
        return RedirectResponse(url="/h2h?error=Already+in+ranked+queue", status_code=303)
    
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
        return RedirectResponse(url=f"/h2h/match/{match.id}", status_code=303)
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
        return RedirectResponse(url="/h2h?queued=1", status_code=303)

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
        mode = challenge.currency_mode or "coin"

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
                if mode == "coin":
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
                else:
                    if challenger_user:
                        challenger_user.coach_cash += challenge.wager
                        db.add(models.CashTransaction(
                            user_id=challenger_user.id, amount=challenge.wager,
                            transaction_type="h2h_tie_refund", description="H2H tie - Coach Cash refunded"
                        ))
                    if opponent_user:
                        opponent_user.coach_cash += challenge.wager
                        db.add(models.CashTransaction(
                            user_id=opponent_user.id, amount=challenge.wager,
                            transaction_type="h2h_tie_refund", description="H2H tie - Coach Cash refunded"
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
            if mode == "coin":
                winner.coins += winnings
                db.add(models.CurrencyTransaction(
                    user_id=winner.id, amount=winnings,
                    transaction_type="h2h_win", description=f"H2H match won! (+{winnings} Coach Coin)"
                ))
            else:
                winner.coach_cash += winnings
                db.add(models.CashTransaction(
                    user_id=winner.id, amount=winnings,
                    transaction_type="h2h_win", description=f"H2H match won! (+{winnings} Coach Cash)"
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
            from backend.achievements import check_h2h_achievements
            if challenge.winner_id:
                check_h2h_achievements(db, challenge.winner_id, challenge)
        except Exception as e:
            print(f"H2H achievement check error: {e}")

    db.commit()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
