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
from utils.timezone import get_eastern_today, get_eastern_now

from backend.database import engine, get_db, Base
from backend import models, auth

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
        
        if os.path.exists("dfs_players.csv") and (not existing or not has_house_players):
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
    import sqlite3
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
        conn = sqlite3.connect("dfs_nba.db")
        cursor = conn.cursor()
        cursor.execute("SELECT player_name, headshot_url FROM player_headshots")
        for row in cursor.fetchall():
            original_name = row[0]
            url = row[1]
            headshots[original_name] = url
            normalized = normalize_name(original_name)
            if normalized != original_name:
                headshots[normalized] = url
            base_name = original_name.replace(" Jr.", "").replace(" Sr.", "").replace(" III", "").replace(" II", "").replace(" IV", "").strip()
            if base_name != original_name:
                headshots[base_name] = url
        conn.close()
    except:
        pass
    return headshots

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
        import sqlite3
        try:
            conn = sqlite3.connect("dfs_nba.db")
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM player_salaries")
            count = cursor.fetchone()[0]
            conn.close()
            no_games_today = (count == 0)
        except:
            no_games_today = True
    
    next_game_iso = None
    try:
        import sqlite3 as sl3
        from zoneinfo import ZoneInfo
        eastern = ZoneInfo("America/New_York")
        now_et = datetime.now(eastern)
        conn2 = sl3.connect("dfs_nba.db")
        cur2 = conn2.cursor()
        cur2.execute("SELECT DISTINCT game_time FROM player_salaries WHERE game_time IS NOT NULL")
        game_times_raw = [row[0] for row in cur2.fetchall()]
        conn2.close()
        
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
    except Exception as e:
        pass
    
    slate_games = []
    try:
        import sqlite3 as sl3
        conn_g = sl3.connect("dfs_nba.db")
        cur_g = conn_g.cursor()
        cur_g.execute("SELECT away_team, home_team, spread, total FROM game_odds")
        for row in cur_g.fetchall():
            slate_games.append({
                "away": row[0], "home": row[1],
                "spread": row[2], "total": row[3]
            })
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
        "slate_games": slate_games
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
    import pandas as pd
    import time
    
    top_value = []
    props = []
    
    try:
        dfs_df = pd.read_csv("dfs_players.csv")
        dfs_df = dfs_df[dfs_df['salary'] > 0]
        if 'value_vs_tier' in dfs_df.columns and 'value_ratio' in dfs_df.columns:
            valid_df = dfs_df[~dfs_df['value_vs_tier'].isin([float('inf'), float('-inf')])]
            value_cols = ['player_name', 'team', 'salary', 'proj_fp', 'value_ratio', 'value_vs_tier', 'tier', 'ceiling', 'floor', 'fp_sd', 'archetype']
            value_cols = [c for c in value_cols if c in valid_df.columns]
            top_value = valid_df.nlargest(10, 'value_vs_tier')[value_cols].to_dict('records')
        else:
            valued_df = pd.read_csv("dfs_players_valued.csv")
            valued_df = valued_df[valued_df['salary'] > 0]
            top_value = valued_df.nlargest(10, 'value')[['player_name', 'team', 'salary', 'proj_fp', 'value', 'salary_tier']].to_dict('records')
    except:
        pass
    
    try:
        props_df = pd.read_csv("prop_recommendations.csv")
        props_df = props_df[props_df['salary'] > 0]
        props = props_df.head(15).to_dict('records')
    except:
        pass
    
    targeted = []
    try:
        targeted_df = pd.read_csv("targeted_plays.csv")
        targeted = targeted_df.head(20).to_dict('records')
    except:
        pass
    
    import os
    ref_chart_exists = os.path.exists("static/images/ref_foul_chart.png")
    
    return templates.TemplateResponse("trends.html", {
        "request": request,
        "user": user,
        "top_value": top_value,
        "props": props,
        "targeted": targeted,
        "ref_chart_exists": ref_chart_exists,
        "cache_bust": int(time.time())
    })

@app.post("/trends/refresh")
async def refresh_trends(request: Request):
    """Re-run scrapers and analysis to get fresh data."""
    import subprocess
    import os
    
    os.chdir("/home/runner/workspace")
    
    try:
        subprocess.run(["python", "scrape_player_salaries.py"], timeout=60, capture_output=True)
        subprocess.run(["python", "scrape_game_odds.py"], timeout=60, capture_output=True)
        subprocess.run(["python", "scrape_dvp.py"], timeout=60, capture_output=True)
        subprocess.run(["python", "scrape_per100.py"], timeout=60, capture_output=True)
        subprocess.run(["python", "dfs_players.py"], timeout=60, capture_output=True)
        subprocess.run(["python", "scrape_player_props.py"], timeout=60, capture_output=True)
        subprocess.run(["python", "analysis/player_value.py"], timeout=60, capture_output=True)
        subprocess.run(["python", "generate_house_lineup.py", "--force"], timeout=120, capture_output=True)
    except Exception as e:
        print(f"Refresh error: {e}")
    
    from starlette.responses import RedirectResponse
    return RedirectResponse(url="/trends", status_code=303)

@app.get("/leaderboard")
async def leaderboard(request: Request, period: str = "daily", db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    
    leaderboard_data = db.query(
        models.User.id,
        models.User.username,
        models.User.display_name,
        models.User.avatar_url,
        func.count(models.ContestEntry.id).label("entries"),
        func.sum(models.ContestEntry.beat_house.cast(Integer)).label("wins"),
        func.avg(models.ContestEntry.actual_score).label("avg_score")
    ).join(models.ContestEntry).group_by(models.User.id).order_by(
        desc("wins")
    ).limit(50).all()
    
    return templates.TemplateResponse("leaderboard.html", {
        "request": request,
        "user": user,
        "leaderboard": leaderboard_data,
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
    
    category_order = ["competitive", "coach_vs_coach", "archetype", "prestige", "engagement"]
    category_labels = {
        "competitive": "Competitive",
        "coach_vs_coach": "Coach vs Coach",
        "archetype": "Archetype",
        "prestige": "Prestige",
        "engagement": "Engagement",
    }
    badge_groups = {}
    for a in all_achievements:
        cat = a.category or "competitive"
        if cat not in badge_groups:
            badge_groups[cat] = []
        badge_groups[cat].append({
            "code": a.code,
            "name": a.name,
            "description": a.description,
            "icon": a.icon,
            "coin_reward": a.coin_reward,
            "earned": a.code in earned_codes,
            "achieved_at": earned_codes.get(a.code),
        })
    
    ordered_badge_groups = []
    for cat in category_order:
        if cat in badge_groups:
            ordered_badge_groups.append({
                "category": cat,
                "label": category_labels.get(cat, cat.replace("_", " ").title()),
                "badges": badge_groups[cat],
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
        "error": error_msg,
        "success": success_msg,
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
        "active_pillar": "identity"
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
    import sqlite3
    from datetime import datetime
    
    try:
        players_df = pd.read_csv("dfs_players.csv")
        players_df = players_df.dropna(subset=['fd_position', 'salary'])
        players_df['salary'] = players_df['salary'].astype(int)
        
        conn = sqlite3.connect("dfs_nba.db")
        game_times_df = pd.read_sql_query(
            "SELECT DISTINCT game, game_time FROM player_salaries WHERE game_time IS NOT NULL", 
            conn
        )
        
        injury_df = pd.read_sql_query(
            "SELECT player_name, status FROM injury_alerts WHERE status IN ('OUT', 'QUESTIONABLE', 'PROBABLE', 'DOUBTFUL', 'GTD')",
            conn
        )
        injury_map = dict(zip(injury_df['player_name'], injury_df['status']))
        
        conn.close()
        game_times = dict(zip(game_times_df['game'], game_times_df['game_time']))
        
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
    players_df = pd.read_csv("dfs_players.csv")
    
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
        refresh_status["last_run"] = datetime.now().isoformat()
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
    
    import sqlite3
    from utils.name_normalize import normalize_player_name
    
    try:
        normalized = normalize_player_name(player_name)
        now = datetime.now().isoformat()
        
        conn = sqlite3.connect('dfs_nba.db')
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS manual_injuries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_name TEXT UNIQUE,
                status TEXT DEFAULT 'OUT',
                reason TEXT,
                added_at TEXT
            )
        """)
        
        conn.execute("""
            INSERT OR REPLACE INTO manual_injuries (player_name, status, reason, added_at)
            VALUES (?, 'OUT', ?, ?)
        """, (normalized, reason, now))
        
        conn.execute("""
            INSERT OR REPLACE INTO injury_alerts (player_name, status, reason, alert_title, scraped_at)
            VALUES (?, 'OUT', ?, ?, ?)
        """, (normalized, reason, f"MANUAL: {normalized} OUT", now))
        
        conn.commit()
        conn.close()
        
        return {"success": True, "message": f"Added {normalized} as OUT ({reason})"}
    except Exception as e:
        return {"success": False, "message": f"Error: {str(e)}"}

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
    import sqlite3
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
        conn = sqlite3.connect("dfs_nba.db")
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT game, game_time FROM player_salaries WHERE game_time IS NOT NULL")
        rows = cur.fetchall()
        conn.close()
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
    import sqlite3 as sqlite3_mod
    import numpy as np
    import pandas as pd
    import unicodedata
    import re as re_mod

    _TEAM_MAP = {
        'CHO': 'CHA', 'NOP': 'NO', 'BRK': 'BKN', 'PHO': 'PHX',
        'SAS': 'SA', 'GOS': 'GS', 'NOR': 'NO',
    }

    def _ascii_key(name):
        if not name or not isinstance(name, str):
            return ""
        fixed = name
        for _ in range(2):
            try:
                fixed = fixed.encode('latin-1').decode('utf-8')
            except (UnicodeDecodeError, UnicodeEncodeError):
                break
        _cyr = {'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'e','ж':'zh',
                'з':'z','и':'i','й':'y','к':'k','л':'l','м':'m','н':'n','о':'o',
                'п':'p','р':'r','с':'s','т':'t','у':'u','ф':'f','х':'kh','ц':'ts',
                'ч':'ch','ш':'sh','щ':'shch','ъ':'','ы':'y','ь':'','э':'e','ю':'yu','я':'ya'}
        fixed = ''.join(_cyr.get(c, _cyr.get(c.lower(), c)) for c in fixed)
        nfkd = unicodedata.normalize('NFKD', fixed)
        ascii_name = ''.join(c for c in nfkd if not unicodedata.combining(c))
        ascii_name = re_mod.sub(r'[^a-zA-Z\s]', '', ascii_name).lower().strip()
        ascii_name = re_mod.sub(r'\s+', ' ', ascii_name)
        for suffix in [' iv', ' iii', ' ii', ' jr', ' sr', ' v']:
            if ascii_name.endswith(suffix):
                ascii_name = ascii_name[:-len(suffix)].strip()
                break
        return ascii_name

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

    try:
        sconn = sqlite3_mod.connect("dfs_nba.db")

        arch_df = pd.read_sql_query("SELECT player_name, team, archetype, cluster FROM player_archetypes", sconn)
        per100 = pd.read_sql_query("""
            SELECT player_name, pts_per100, reb_per100, ast_per100, stl_per100, blk_per100
            FROM player_per100 WHERE games_played >= 10 AND mpg >= 12
        """, sconn)
        positions = pd.read_sql_query("SELECT player_name, pg_pct, sg_pct, sf_pct, pf_pct, c_pct FROM player_positions", sconn)
        usage = pd.read_sql_query("SELECT player_name, usg_pct FROM player_stats", sconn)
        game_logs = pd.read_sql_query("""
            SELECT player_name, AVG(fg3m) as fg3m_pg, AVG(min) as min_pg
            FROM player_game_logs WHERE min >= 10
            GROUP BY player_name HAVING COUNT(*) >= 5
        """, sconn)
        sconn.close()

        for tbl in [arch_df, per100, positions, usage, game_logs]:
            tbl['_mk'] = tbl['player_name'].apply(_ascii_key)

        arch_df['player_name'] = arch_df['player_name'].apply(_clean_name)
        arch_df['team'] = arch_df['team'].replace(_TEAM_MAP)
        arch_df = arch_df[~arch_df['team'].isin(['2TM', '3TM'])]

        df = arch_df.merge(per100.drop(columns=['player_name']), on='_mk', how='left')
        df = df.merge(positions.drop(columns=['player_name']), on='_mk', how='left')
        df = df.merge(usage.drop(columns=['player_name']), on='_mk', how='left')
        df = df.merge(game_logs.drop(columns=['player_name']), on='_mk', how='left')
        df = df.drop(columns=['_mk'])
        df = df.dropna(subset=['pts_per100'])

        df['usg_pct'] = df['usg_pct'].fillna(df['usg_pct'].median())
        df['fg3m_pg'] = df['fg3m_pg'].fillna(0)
        df['min_pg'] = df['min_pg'].fillna(1)
        df['pg_pct'] = df['pg_pct'].fillna(0)
        df['sg_pct'] = df['sg_pct'].fillna(0)
        df['sf_pct'] = df['sf_pct'].fillna(0)
        df['pf_pct'] = df['pf_pct'].fillna(0)
        df['c_pct'] = df['c_pct'].fillna(0)
        df['fg3m_per100'] = np.where(df['min_pg'] > 0, df['fg3m_pg'] / df['min_pg'] * 100, 0)
        df['guard_pct'] = df['pg_pct'] + df['sg_pct']
        df['forward_pct'] = df['sf_pct'] + df['pf_pct']
        df['big_pct'] = df['c_pct']
        df['ast_to_reb_ratio'] = np.where(df['reb_per100'] > 0, df['ast_per100'] / df['reb_per100'], df['ast_per100'])
        df['scoring_versatility'] = np.where(df['pts_per100'] > 0, df['fg3m_per100'] / df['pts_per100'], 0)

        features = ['pts_per100', 'reb_per100', 'ast_per100', 'stl_per100', 'blk_per100',
                     'fg3m_per100', 'usg_pct', 'guard_pct', 'forward_pct', 'big_pct',
                     'ast_to_reb_ratio', 'scoring_versatility']
        for col in features:
            df[col] = df[col].fillna(0)

        from sklearn.preprocessing import StandardScaler
        from sklearn.decomposition import PCA
        X = df[features].values
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
    import sqlite3 as sqlite3_mod
    try:
        sconn = sqlite3_mod.connect("dfs_nba.db")
        cur = sconn.cursor()
        tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        if 'dva_stats' not in tables:
            sconn.close()
            return {"error": "DVA data not yet available. Run daily update.", "teams": [], "archetypes": []}

        rows = cur.execute("""
            SELECT opp_team, archetype, fp_pm, fp_pm_diff, sample_n,
                   pts_pm_diff, reb_pm_diff, ast_pm_diff, stl_pm_diff, blk_pm_diff, fg3m_pm_diff, tov_pm_diff,
                   dvs_multiplier,
                   pts_component, reb_component, ast_component, stl_component, blk_component, fg3m_component, tov_component
            FROM dva_stats ORDER BY opp_team, archetype
        """).fetchall()

        profiles = {}
        if 'archetype_profiles' in tables:
            prof_rows = cur.execute("SELECT * FROM archetype_profiles").fetchall()
            prof_cols = [d[0] for d in cur.description]
            for r in prof_rows:
                rd = dict(zip(prof_cols, r))
                profiles[rd['archetype']] = {k: rd[k] for k in rd if k != 'archetype'}

        sconn.close()

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
    import sqlite3 as sqlite3_mod
    stat_map = {
        'PTS': 'pts', 'REB': 'reb', 'AST': 'ast',
        'STL': 'stl', 'BLK': 'blk', 'FP': 'fp',
        'MIN': 'min', 'TOV': 'tov', '3PM': 'fg3m'
    }
    col = stat_map.get(stat.upper())
    if not col:
        return {"error": "Invalid stat", "games": []}
    try:
        conn = sqlite3_mod.connect("dfs_nba.db")
        existing_cols = [r[1] for r in conn.execute("PRAGMA table_info(player_game_logs)").fetchall()]
        if col not in existing_cols:
            conn.close()
            return {"error": f"{stat.upper()} data not yet available. Will populate on next daily update.", "games": []}
        rows = conn.execute(
            f"SELECT game_date, matchup, {col} FROM player_game_logs WHERE player_name = ? ORDER BY game_date DESC LIMIT ?",
            (player_name, n)
        ).fetchall()
        conn.close()
        if not rows:
            return {"error": "No data found", "games": []}
        games = [{"date": r[0], "matchup": r[1], "value": r[2] or 0} for r in reversed(rows)]
        values = [g["value"] for g in games]
        avg = round(sum(values) / len(values), 1) if values else 0
        return {"player": player_name, "stat": stat.upper(), "games": games, "avg": avg}
    except Exception as e:
        return {"error": str(e), "games": []}

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

    return templates.TemplateResponse("h2h_lobby.html", {
        "request": request,
        "user": user,
        "contest": contest,
        "open_challenges": open_challenges,
        "active_challenges": active_challenges,
        "history_challenges": history_challenges,
        "error": error_msg,
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
        return RedirectResponse(url=f"/h2h?error=Wager+must+be+between+5+and+500+{label}", status_code=303)

    if currency_mode == "coin":
        if user.coins < wager:
            return RedirectResponse(url=f"/h2h?error=Not+enough+Coach+Coin.+You+have+{user.coins}+but+tried+to+wager+{wager}.", status_code=303)
        user.coins -= wager
        db.add(models.CurrencyTransaction(
            user_id=user.id,
            amount=-wager,
            transaction_type="h2h_wager",
            description=f"H2H challenge wager ({wager} Coach Coin)"
        ))
    else:
        if user.coach_cash < wager:
            return RedirectResponse(url=f"/h2h?error=Not+enough+Coach+Cash.+You+have+{user.coach_cash}+but+tried+to+wager+{wager}.", status_code=303)
        user.coach_cash -= wager
        db.add(models.CashTransaction(
            user_id=user.id,
            amount=-wager,
            transaction_type="h2h_wager",
            description=f"H2H challenge wager ({wager} Coach Cash)"
        ))

    challenge = models.H2HChallenge(
        contest_id=contest.id,
        challenger_id=user.id,
        wager=wager,
        currency_mode=currency_mode,
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
            transaction_type="h2h_wager",
            description=f"Accepted H2H challenge ({challenge.wager} Coach Coin)"
        ))
    else:
        if user.coach_cash < challenge.wager:
            return RedirectResponse(url=f"/h2h?error=Not+enough+Coach+Cash.+You+have+{user.coach_cash}+but+need+{challenge.wager}+to+accept.", status_code=303)
        user.coach_cash -= challenge.wager
        db.add(models.CashTransaction(
            user_id=user.id,
            amount=-challenge.wager,
            transaction_type="h2h_wager",
            description=f"Accepted H2H challenge ({challenge.wager} Coach Cash)"
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
    import sqlite3

    try:
        players_df = pd.read_csv("dfs_players.csv")
        players_df = players_df.dropna(subset=['fd_position', 'salary'])
        players_df['salary'] = players_df['salary'].astype(int)

        conn = sqlite3.connect("dfs_nba.db")
        game_times_df = pd.read_sql_query(
            "SELECT DISTINCT game, game_time FROM player_salaries WHERE game_time IS NOT NULL",
            conn
        )
        injury_df = pd.read_sql_query(
            "SELECT player_name, status FROM injury_alerts WHERE status IN ('OUT', 'QUESTIONABLE', 'PROBABLE', 'DOUBTFUL', 'GTD')",
            conn
        )
        injury_map = dict(zip(injury_df['player_name'], injury_df['status']))
        conn.close()
        game_times = dict(zip(game_times_df['game'], game_times_df['game_time']))

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
    players_df = pd.read_csv("dfs_players.csv")

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

        mode = challenge.currency_mode or "coin"
        total_pot = challenge.wager * 2
        house_cut = max(1, int(total_pot * 0.1))
        winnings = total_pot - house_cut

        if c_total > o_total:
            challenge.winner_id = challenge.challenger_id
            winner = db.query(models.User).filter(models.User.id == challenge.challenger_id).first()
        elif o_total > c_total:
            challenge.winner_id = challenge.opponent_id
            winner = db.query(models.User).filter(models.User.id == challenge.opponent_id).first()
        else:
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
            challenge.status = "completed"
            continue

        if winner:
            if mode == "coin":
                winner.coins += winnings
                db.add(models.CurrencyTransaction(
                    user_id=winner.id, amount=winnings,
                    transaction_type="h2h_win", description=f"H2H challenge won! (+{winnings} Coach Coin)"
                ))
            else:
                winner.coach_cash += winnings
                db.add(models.CashTransaction(
                    user_id=winner.id, amount=winnings,
                    transaction_type="h2h_win", description=f"H2H challenge won! (+{winnings} Coach Cash)"
                ))

        challenge.status = "completed"

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
