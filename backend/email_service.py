import os
import json
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func as sa_func
from backend.models import EmailQueue, User

MAX_EMAILS_PER_USER_PER_DAY = 2


def queue_email(db: Session, user_id: int, email_type: str, subject: str,
                body_html: str) -> bool:
    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    sent_today = db.query(sa_func.count(EmailQueue.id)).filter(
        EmailQueue.user_id == user_id,
        EmailQueue.created_at >= today_start,
        EmailQueue.status.in_(["pending", "sent"]),
    ).scalar() or 0

    if sent_today >= MAX_EMAILS_PER_USER_PER_DAY:
        return False

    email = EmailQueue(
        user_id=user_id,
        email_type=email_type,
        subject=subject,
        body_html=body_html,
        status="pending",
    )
    db.add(email)
    db.flush()
    return True


def send_email(to: str, subject: str, html_body: str) -> tuple[bool, str]:
    api_key = os.environ.get("RESEND_API_KEY", "")
    if not api_key:
        return False, "RESEND_API_KEY not configured"

    try:
        import requests
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "from": "PIRTDICA <notifications@pirtdica.com>",
                "to": [to],
                "subject": subject,
                "html": html_body,
            },
            timeout=10,
        )
        if resp.status_code in (200, 201):
            return True, ""
        return False, f"Resend API error {resp.status_code}: {resp.text[:200]}"
    except Exception as e:
        return False, str(e)


def process_email_queue(db: Session) -> dict:
    pending = db.query(EmailQueue).filter(
        EmailQueue.status == "pending",
    ).order_by(EmailQueue.created_at.asc()).limit(20).all()

    results = {"sent": 0, "failed": 0, "skipped": 0}

    for email in pending:
        user = db.query(User).filter(User.id == email.user_id).first()
        if not user or not user.email:
            email.status = "failed"
            email.error = "No user or email address"
            results["failed"] += 1
            continue

        success, error = send_email(user.email, email.subject, email.body_html)
        if success:
            email.status = "sent"
            email.sent_at = datetime.utcnow()
            results["sent"] += 1
        else:
            email.status = "failed"
            email.error = error
            results["failed"] += 1

    db.commit()
    return results


def should_email_user(db: Session, user_id: int, hours_threshold: int = 12) -> bool:
    from backend.auth import UserSession
    cutoff = datetime.utcnow() - timedelta(hours=hours_threshold)
    recent_session = db.query(UserSession).filter(
        UserSession.user_id == user_id,
        UserSession.created_at >= cutoff,
    ).first()
    return recent_session is None
