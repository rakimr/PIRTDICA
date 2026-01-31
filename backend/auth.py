from passlib.context import CryptContext
from datetime import datetime, timedelta
import secrets
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from backend.database import Base

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

class UserSession(Base):
    __tablename__ = "user_sessions"
    
    id = Column(Integer, primary_key=True, index=True)
    token = Column(String(64), unique=True, index=True, nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=False)

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def create_session(db: Session, user_id: int) -> str:
    token = secrets.token_urlsafe(32)
    expires_at = datetime.utcnow() + timedelta(days=7)
    
    session = UserSession(
        token=token,
        user_id=user_id,
        expires_at=expires_at
    )
    db.add(session)
    db.commit()
    return token

def get_session_user(db: Session, token: str) -> int | None:
    if not token:
        return None
    
    session = db.query(UserSession).filter(UserSession.token == token).first()
    if session:
        if session.expires_at > datetime.utcnow():
            return session.user_id
        else:
            db.delete(session)
            db.commit()
    return None

def delete_session(db: Session, token: str):
    session = db.query(UserSession).filter(UserSession.token == token).first()
    if session:
        db.delete(session)
        db.commit()

def cleanup_expired_sessions(db: Session):
    db.query(UserSession).filter(UserSession.expires_at < datetime.utcnow()).delete()
    db.commit()
