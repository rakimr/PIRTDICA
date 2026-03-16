import os
import stripe
import logging

logger = logging.getLogger(__name__)

_stripe_keys = None

def _load_stripe_keys():
    global _stripe_keys
    if _stripe_keys:
        return _stripe_keys
    secret = os.environ.get("STRIPE_SECRET_KEY", "")
    publishable = os.environ.get("STRIPE_PUBLISHABLE_KEY", "")
    source = "environment variables" if secret else None
    if not secret:
        keys_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".stripe_keys.json")
        if os.path.exists(keys_file):
            try:
                import json
                with open(keys_file) as f:
                    data = json.load(f)
                secret = data.get("secret", "")
                publishable = data.get("publishable", "")
                source = ".stripe_keys.json"
            except Exception as e:
                logger.error(f"Failed to read .stripe_keys.json: {e}")
    if secret:
        print(f"[Stripe] Keys loaded from {source} (secret: {secret[:8]}...)")
    else:
        print("[Stripe] WARNING: No Stripe keys found — checked STRIPE_SECRET_KEY env var and .stripe_keys.json")
    _stripe_keys = {"secret": secret, "publishable": publishable}
    return _stripe_keys


def get_stripe_client():
    keys = _load_stripe_keys()
    stripe.api_key = keys["secret"]
    return stripe


def get_publishable_key():
    keys = _load_stripe_keys()
    return keys["publishable"]


PRICE_ID_CACHE = {}

PLANS = {
    "picks": {
        "name": "PIRTDICA Picks",
        "description": "Access daily HIGH Confidence Props analysis, projection breakdowns, and betting edges.",
        "amount": 900,
        "interval": "week",
        "tier": "picks",
    },
    "statpack": {
        "name": "PIRTDICA Stat Pack",
        "description": "Full access to the Chart Gallery — DVP heatmaps, referee analysis, archetype clusters, shot charts, and more.",
        "amount": 1600,
        "interval": "week",
        "tier": "statpack",
    },
    "bundle": {
        "name": "PIRTDICA Bundle",
        "description": "PIRTDICA Picks + PIRTDICA Stat Pack — the complete analytics suite at 40% off.",
        "amount": 6000,
        "interval": "month",
        "tier": "bundle",
    },
}

PLAN_DISPLAY_NAMES = {
    "picks": "PIRTDICA Picks",
    "statpack": "PIRTDICA Stat Pack",
    "bundle": "PIRTDICA Bundle",
}


def ensure_product_and_price(plan_key="picks"):
    client = get_stripe_client()
    cache_key = f"{plan_key}_price"
    if PRICE_ID_CACHE.get(cache_key):
        return PRICE_ID_CACHE[cache_key]

    plan = PLANS[plan_key]

    product = None
    try:
        all_products = client.Product.list(limit=100, active=True)
        for p in all_products.data:
            if p.name == plan["name"]:
                product = p
                break
    except Exception as e:
        logger.error(f"Failed to list Stripe products: {e}")

    if not product:
        product = client.Product.create(
            name=plan["name"],
            description=plan["description"],
            metadata={"tier": plan["tier"]},
        )
        logger.info(f"Created Stripe product: {product.id} ({plan['name']})")

    prices = client.Price.list(product=product.id, active=True, limit=10)
    target_price = None
    for p in prices.data:
        if p.recurring and p.recurring.interval == plan["interval"] and p.unit_amount == plan["amount"]:
            target_price = p
            break

    if not target_price:
        target_price = client.Price.create(
            product=product.id,
            unit_amount=plan["amount"],
            currency="usd",
            recurring={"interval": plan["interval"]},
        )
        logger.info(f"Created Stripe price: {target_price.id} ({plan['name']})")

    PRICE_ID_CACHE[cache_key] = target_price.id
    return target_price.id


def create_checkout_session(user, success_url, cancel_url, plan_key="picks"):
    client = get_stripe_client()
    price_id = ensure_product_and_price(plan_key)

    customer_id = user.stripe_customer_id
    if customer_id:
        try:
            client.Customer.retrieve(customer_id)
        except Exception:
            logger.warning(f"Stored customer {customer_id} not found in current Stripe environment — creating new customer")
            customer_id = None

    if not customer_id:
        customer = client.Customer.create(
            email=user.email,
            metadata={"user_id": str(user.id), "username": user.username},
        )
        customer_id = customer.id

    session = client.checkout.Session.create(
        customer=customer_id,
        payment_method_types=["card"],
        line_items=[{"price": price_id, "quantity": 1}],
        mode="subscription",
        success_url=success_url,
        cancel_url=cancel_url,
        metadata={"user_id": str(user.id), "plan": plan_key},
    )
    return session, customer_id


def create_billing_portal_session(customer_id, return_url):
    client = get_stripe_client()
    session = client.billing_portal.Session.create(
        customer=customer_id,
        return_url=return_url,
    )
    return session


def construct_webhook_event(payload, sig_header):
    client = get_stripe_client()
    webhook_secret = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
    if not webhook_secret:
        logger.error("STRIPE_WEBHOOK_SECRET not set — rejecting webhook")
        raise ValueError("Webhook secret not configured")
    event = client.Webhook.construct_event(payload, sig_header, webhook_secret)
    return event


def resolve_plan_from_subscription(client, subscription_id):
    sub = client.Subscription.retrieve(subscription_id, expand=["items.data.price.product"])
    plan_key = "picks"
    for item in sub["items"]["data"]:
        product = item["price"]["product"]
        product_name = product["name"] if isinstance(product, dict) else ""
        if "Bundle" in product_name:
            plan_key = "bundle"
            break
        elif "Stat Pack" in product_name:
            plan_key = "statpack"
    return sub, plan_key


def upsert_user_subscription(db, user_id, stripe_subscription_id, plan_key, status, current_period_end=None):
    from backend.models import UserSubscription
    from datetime import datetime

    existing = db.query(UserSubscription).filter(
        UserSubscription.stripe_subscription_id == stripe_subscription_id
    ).first()

    if existing:
        existing.plan = plan_key
        existing.status = status
        if current_period_end:
            existing.current_period_end = current_period_end
        db.flush()
        return existing, False

    new_sub = UserSubscription(
        user_id=user_id,
        stripe_subscription_id=stripe_subscription_id,
        plan=plan_key,
        status=status,
        current_period_end=current_period_end,
    )
    db.add(new_sub)
    db.flush()
    return new_sub, True


def cancel_individual_subs_for_bundle(db, user_id, bundle_subscription_id):
    from backend.models import UserSubscription
    client = get_stripe_client()

    individual_subs = db.query(UserSubscription).filter(
        UserSubscription.user_id == user_id,
        UserSubscription.plan.in_(["picks", "statpack"]),
        UserSubscription.status == "active",
        UserSubscription.stripe_subscription_id != bundle_subscription_id,
    ).all()

    canceled = []
    for sub in individual_subs:
        try:
            client.Subscription.cancel(sub.stripe_subscription_id)
            sub.status = "canceled"
            canceled.append(sub.plan)
            logger.info(f"Canceled {sub.plan} subscription {sub.stripe_subscription_id} for user {user_id} (upgraded to bundle)")
        except Exception as e:
            logger.error(f"Failed to cancel {sub.plan} sub {sub.stripe_subscription_id}: {e}")

    if canceled:
        db.flush()
    return canceled


def get_user_active_plans(db, user_id):
    from backend.models import UserSubscription
    subs = db.query(UserSubscription).filter(
        UserSubscription.user_id == user_id,
        UserSubscription.status == "active",
    ).all()
    return [s.plan for s in subs]


def _legacy_check(user, allowed_plans):
    status = getattr(user, 'subscription_status', None)
    plan = getattr(user, 'subscription_plan', None)
    if status != 'active':
        return False
    return plan in allowed_plans


def has_picks_access(user, db=None):
    if not user:
        return False
    if db:
        plans = get_user_active_plans(db, user.id)
        if plans:
            return any(p in ("picks", "bundle", "pro") for p in plans)
    return _legacy_check(user, ('picks', 'bundle', 'pro'))


def has_statpack_access(user, db=None):
    if not user:
        return False
    if db:
        plans = get_user_active_plans(db, user.id)
        if plans:
            return any(p in ("statpack", "bundle") for p in plans)
    return _legacy_check(user, ('statpack', 'bundle'))


def has_any_subscription(user, db=None):
    if not user:
        return False
    if db:
        plans = get_user_active_plans(db, user.id)
        if plans:
            return True
    return getattr(user, 'subscription_status', None) == 'active'


def is_subscriber(user, db=None):
    return has_picks_access(user, db)


def get_user_plan_display(db, user_id):
    plans = get_user_active_plans(db, user_id)
    if not plans:
        return "Free"
    if "bundle" in plans:
        return "PIRTDICA Bundle"
    names = []
    if "picks" in plans:
        names.append("PIRTDICA Picks")
    if "statpack" in plans:
        names.append("PIRTDICA Stat Pack")
    return " + ".join(names) if names else "Free"


def sync_user_subscription_fields(db, user, plan_key, stripe_sub_id, status, period_end=None):
    user.stripe_subscription_id = stripe_sub_id
    user.subscription_status = status
    user.subscription_plan = plan_key
    if period_end:
        from datetime import datetime
        if isinstance(period_end, (int, float)):
            user.subscription_current_period_end = datetime.fromtimestamp(period_end)
        else:
            user.subscription_current_period_end = period_end
