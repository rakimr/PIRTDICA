from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func as sa_func
from backend.models import Notification


CATEGORY_MAP = {
    "rank_change": "competitive",
    "contest_result": "competitive",
    "h2h_result": "competitive",
    "promotion": "competitive",
    "rival_activity": "competitive",
    "edge_alert": "competitive",
    "payout": "financial",
    "currency": "financial",
    "subscription": "financial",
    "account": "system",
    "welcome": "system",
}


def create_notification(db: Session, user_id: int, type: str, priority: int,
                        title: str, body: str, action_url: str = None,
                        expires_at: datetime = None):
    if expires_at is None:
        if priority >= 3:
            expires_at = datetime.utcnow() + timedelta(days=30)
        else:
            expires_at = datetime.utcnow() + timedelta(days=90)

    if action_url and not action_url.startswith('/'):
        action_url = None

    notif = Notification(
        user_id=user_id,
        type=type,
        priority=priority,
        title=title[:255],
        body=body[:2000] if body else body,
        action_url=action_url,
        expires_at=expires_at,
    )
    db.add(notif)
    db.flush()
    return notif


def get_unread_count(db: Session, user_id: int) -> int:
    return db.query(sa_func.count(Notification.id)).filter(
        Notification.user_id == user_id,
        Notification.is_read == False,
    ).scalar() or 0


def get_notifications(db: Session, user_id: int, category: str = None,
                      limit: int = 20, offset: int = 0):
    query = db.query(Notification).filter(
        Notification.user_id == user_id,
    )

    if category and category != "all":
        matching_types = [t for t, c in CATEGORY_MAP.items() if c == category]
        if matching_types:
            query = query.filter(Notification.type.in_(matching_types))

    return query.order_by(Notification.created_at.desc()).offset(offset).limit(limit).all()


def mark_read(db: Session, user_id: int, notification_id: int) -> bool:
    notif = db.query(Notification).filter(
        Notification.id == notification_id,
        Notification.user_id == user_id,
    ).first()
    if notif:
        notif.is_read = True
        return True
    return False


def mark_all_read(db: Session, user_id: int) -> int:
    count = db.query(Notification).filter(
        Notification.user_id == user_id,
        Notification.is_read == False,
    ).update({"is_read": True})
    return count


def cleanup_expired(db: Session) -> int:
    now = datetime.utcnow()
    count = db.query(Notification).filter(
        Notification.expires_at < now,
    ).delete()
    db.commit()
    return count


def notification_to_dict(notif: Notification) -> dict:
    now = datetime.utcnow()
    delta = now - notif.created_at if notif.created_at else timedelta(0)

    if delta.total_seconds() < 60:
        time_ago = "just now"
    elif delta.total_seconds() < 3600:
        mins = int(delta.total_seconds() / 60)
        time_ago = f"{mins}m ago"
    elif delta.total_seconds() < 86400:
        hours = int(delta.total_seconds() / 3600)
        time_ago = f"{hours}h ago"
    elif delta.days < 7:
        time_ago = f"{delta.days}d ago"
    else:
        time_ago = notif.created_at.strftime("%b %d") if notif.created_at else ""

    return {
        "id": notif.id,
        "type": notif.type,
        "category": CATEGORY_MAP.get(notif.type, "system"),
        "priority": notif.priority,
        "title": notif.title,
        "body": notif.body,
        "action_url": notif.action_url,
        "is_read": notif.is_read,
        "time_ago": time_ago,
        "created_at": notif.created_at.isoformat() if notif.created_at else None,
    }
