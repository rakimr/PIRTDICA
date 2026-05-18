from passlib.context import CryptContext
from datetime import datetime, timedelta
import secrets
import hashlib
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from backend.database import Base

PASSWORD_RESET_TTL_MINUTES = 60


def _hash_reset_token(raw_token: str) -> str:
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


def create_password_reset_token(db: Session, user_id: int) -> str:
    from backend.models import PasswordResetToken
    db.query(PasswordResetToken).filter(
        PasswordResetToken.user_id == user_id,
        PasswordResetToken.used_at.is_(None),
        PasswordResetToken.expires_at > datetime.utcnow(),
    ).update({"used_at": datetime.utcnow()}, synchronize_session=False)

    raw = secrets.token_urlsafe(32)
    record = PasswordResetToken(
        user_id=user_id,
        token_hash=_hash_reset_token(raw),
        expires_at=datetime.utcnow() + timedelta(minutes=PASSWORD_RESET_TTL_MINUTES),
    )
    db.add(record)
    db.commit()
    return raw


def consume_password_reset_token(db: Session, raw_token: str):
    from backend.models import PasswordResetToken
    if not raw_token:
        return None
    record = db.query(PasswordResetToken).filter(
        PasswordResetToken.token_hash == _hash_reset_token(raw_token)
    ).first()
    if not record:
        return None
    if record.used_at is not None:
        return None
    if record.expires_at <= datetime.utcnow():
        return None
    return record


def mark_password_reset_token_used(db: Session, record) -> None:
    record.used_at = datetime.utcnow()
    db.commit()

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
