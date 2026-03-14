from sqlalchemy.orm import Session
from backend.notifications import create_notification


def emit_contest_result(db: Session, user_id: int, score: float,
                        house_score: float, beat_house: bool, rank: int = 0,
                        payout: int = 0):
    diff = abs(score - house_score)

    if beat_house:
        title = "You beat the house"
        body = f"Your lineup scored {score:.1f} FP vs the house's {house_score:.1f} FP — you won by {diff:.1f}."
        if payout > 0:
            body += f" +{payout} Coach Coin earned."
        priority = 2
    else:
        title = "Close loss" if diff < 10 else "Contest result"
        if diff < 10:
            body = f"You scored {score:.1f} FP — just {diff:.1f} FP behind the house ({house_score:.1f})."
        else:
            body = f"You scored {score:.1f} FP vs the house's {house_score:.1f} FP."
        priority = 2 if diff < 10 else 3

    create_notification(
        db, user_id,
        type="contest_result",
        priority=priority,
        title=title,
        body=body,
        action_url="/history",
    )


def emit_rank_change(db: Session, user_id: int, old_mmr: int, new_mmr: int,
                     old_division: str, new_division: str, mmr_change: int,
                     promoted: bool = False, demoted: bool = False):
    if promoted:
        title = f"Promoted to {new_division}"
        body = f"You've been promoted from {old_division} to {new_division}. MMR: {new_mmr} (+{mmr_change})."
        priority = 1
    elif demoted:
        title = f"Demoted to {new_division}"
        body = f"You've dropped from {old_division} to {new_division}. MMR: {new_mmr} ({mmr_change})."
        priority = 2
    else:
        direction = "up" if mmr_change > 0 else "down"
        title = f"MMR moved {direction} to {new_mmr}"
        body = f"Your MMR changed by {mmr_change:+d} ({old_mmr} → {new_mmr}). Division: {new_division}."
        priority = 3

    create_notification(
        db, user_id,
        type="rank_change",
        priority=priority,
        title=title,
        body=body,
        action_url=f"/profile/",
    )


def emit_h2h_result(db: Session, user_id: int, opponent_name: str,
                     won: bool, user_score: float, opponent_score: float,
                     mmr_change: int = 0, is_ranked: bool = False,
                     winnings: int = 0, currency: str = "coin"):
    diff = abs(user_score - opponent_score)

    if won:
        title = f"You beat {opponent_name}"
        body = f"Your {user_score:.1f} FP vs their {opponent_score:.1f} FP — won by {diff:.1f}."
        if mmr_change and is_ranked:
            body += f" MMR: +{mmr_change}."
        if winnings > 0:
            body += f" +{winnings} Coach {'Cash' if currency == 'cash' else 'Coin'}."
        priority = 2
    else:
        title = f"Lost to {opponent_name}"
        body = f"Your {user_score:.1f} FP vs their {opponent_score:.1f} FP — lost by {diff:.1f}."
        if mmr_change and is_ranked:
            body += f" MMR: {mmr_change}."
        priority = 2 if diff < 10 else 3

    create_notification(
        db, user_id,
        type="h2h_result",
        priority=priority,
        title=title,
        body=body,
        action_url="/h2h",
    )


def emit_promotion_result(db: Session, user_id: int, promoted: bool,
                          new_division: str):
    if promoted:
        title = f"Promoted to {new_division}!"
        body = f"You won your promotion series and advanced to {new_division}. Keep climbing."
        priority = 1
    else:
        title = "Promotion series lost"
        body = "You didn't win enough matches to advance. Keep grinding — you'll get there."
        priority = 2

    create_notification(
        db, user_id,
        type="promotion",
        priority=priority,
        title=title,
        body=body,
        action_url=f"/profile/",
    )


def emit_subscription_activated(db: Session, user_id: int, username: str, plan_key: str):
    from backend.stripe_billing import PLAN_DISPLAY_NAMES

    plan_name = PLAN_DISPLAY_NAMES.get(plan_key, plan_key)

    access_map = {
        "picks": "Articles page (daily HIGH Confidence Props analysis)",
        "statpack": "Chart Gallery (DVP heatmaps, referee analysis, archetype clusters, shot charts)",
        "bundle": "Articles page and Chart Gallery — the complete analytics suite",
    }
    access_desc = access_map.get(plan_key, "premium content")

    action_map = {
        "picks": "/articles",
        "statpack": "/trends",
        "bundle": "/articles",
    }

    create_notification(
        db, user_id,
        type="subscription",
        priority=1,
        title=f"{plan_name} activated",
        body=f"Your {plan_name} subscription is now active. You have access to: {access_desc}.",
        action_url=action_map.get(plan_key, "/"),
    )

    try:
        from backend.email_service import queue_email
        from backend.email_templates import subscription_activated_email
        subject, html = subscription_activated_email(username, plan_name, access_desc)
        queue_email(db, user_id, "subscription_activated", subject, html)
    except Exception as e:
        print(f"[Email] Failed to queue subscription email for user {user_id}: {e}")


def emit_welcome(db: Session, user_id: int, username: str):
    create_notification(
        db, user_id,
        type="welcome",
        priority=3,
        title="Welcome to PIRTDICA",
        body=f"Welcome, {username}. You start with 100 Coach Coin. Enter today's contest or challenge another coach.",
        action_url="/play",
    )
