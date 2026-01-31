from passlib.context import CryptContext
from datetime import datetime, timedelta
import secrets
import hashlib

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

sessions = {}

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def create_session(user_id: int) -> str:
    token = secrets.token_urlsafe(32)
    sessions[token] = {
        "user_id": user_id,
        "created_at": datetime.utcnow(),
        "expires_at": datetime.utcnow() + timedelta(days=7)
    }
    return token

def get_session_user(token: str) -> int | None:
    if token in sessions:
        session = sessions[token]
        if session["expires_at"] > datetime.utcnow():
            return session["user_id"]
        else:
            del sessions[token]
    return None

def delete_session(token: str):
    if token in sessions:
        del sessions[token]
