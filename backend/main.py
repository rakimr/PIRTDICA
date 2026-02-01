from fastapi import FastAPI, Request, Depends, Form, HTTPException, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from datetime import datetime, date
import os

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
        conn.close()
    except:
        pass
    return headshots

@app.get("/")
async def home(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    today = date.today()
    
    contest = db.query(models.Contest).filter(models.Contest.slate_date == today).first()
    
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
    
    return templates.TemplateResponse("home.html", {
        "request": request,
        "user": user,
        "contest": contest,
        "house_players": house_players,
        "user_entry": user_entry,
        "headshots": headshots,
        "no_games_today": no_games_today
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
    
    achievements = db.query(models.UserAchievement).join(models.Achievement).filter(
        models.UserAchievement.user_id == profile_user.id
    ).all()
    
    return templates.TemplateResponse("profile.html", {
        "request": request,
        "user": current_user,
        "profile": profile_user,
        "entries": entries,
        "stats": stats,
        "achievements": achievements
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
    
    return templates.TemplateResponse("shop.html", {
        "request": request,
        "user": user,
        "items": items,
        "owned_ids": owned_ids
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
    
    today = date.today()
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
        
        conn = sqlite3.connect("dfs_nba.db")
        game_times_df = pd.read_sql_query(
            "SELECT DISTINCT game, game_time FROM player_salaries WHERE game_time IS NOT NULL", 
            conn
        )
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
        
        for game_key in game_times.keys():
            away, home = game_key.split(" @ ")
            game_times[f"{away} vs {home}"] = game_times[game_key]
            game_times[f"{home} vs {away}"] = game_times[game_key]
        
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
        
        players = players_df.to_dict("records")
    except Exception as e:
        print(f"Error loading players: {e}")
        players = []
    
    house_players = db.query(models.HouseLineupPlayer).filter(
        models.HouseLineupPlayer.contest_id == contest.id
    ).all()
    
    return templates.TemplateResponse("play.html", {
        "request": request,
        "user": user,
        "contest": contest,
        "players": players,
        "house_players": house_players
    })

@app.post("/submit-lineup")
async def submit_lineup(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        raise HTTPException(status_code=401, detail="Not logged in")
    
    form = await request.form()
    player_ids = form.getlist("players")
    
    today = date.today()
    contest = db.query(models.Contest).filter(models.Contest.slate_date == today).first()
    if not contest:
        raise HTTPException(status_code=400, detail="No active contest")
    
    if contest.status != "open":
        raise HTTPException(status_code=400, detail="Contest is locked")
    
    if datetime.now() >= contest.lock_time:
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
    
    entry = models.ContestEntry(
        user_id=user.id,
        contest_id=contest.id,
        total_salary=total_salary,
        proj_score=total_proj
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
    
    house_players = db.query(models.HouseLineupPlayer).filter(
        models.HouseLineupPlayer.contest_id == entry.contest_id
    ).all()
    
    return templates.TemplateResponse("entry.html", {
        "request": request,
        "user": user,
        "entry": entry,
        "players": players,
        "house_players": house_players
    })

from sqlalchemy import Integer

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
