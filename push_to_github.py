import sys
import os
import json
import base64
import urllib.request
from datetime import datetime

REPO = "rakimr/PIRTDICA"
BRANCH = "main"

PIPELINE_OUTPUT_FILES = [
    "dfs_players.csv",
    "dfs_players_valued.csv",
    "ownership_projections.csv",
    "prop_recommendations.csv",
    "targeted_plays.csv",
    "static/images/dvp_heatmap.png",
    "static/images/ref_foul_chart.png",
    "static/images/upside_chart.png",
    "static/images/value_chart.png",
]

def _find_article_headers():
    import glob
    return glob.glob("static/images/article_header_*.png")

SOURCE_CODE_FILES = [
    "backend/__init__.py",
    "backend/main.py",
    "backend/models.py",
    "backend/database.py",
    "backend/data_access.py",
    "backend/stripe_billing.py",
    "backend/notifications.py",
    "backend/events.py",
    "backend/email_service.py",
    "backend/email_templates.py",
    "backend/auth.py",
    "backend/ranking.py",
    "backend/profanity_filter.py",
    "backend/achievements.py",
    "score_contest.py",
    "generate_house_lineup.py",
    "generate_article.py",
    "generate_header.py",
    "run_daily_update.py",
    "push_to_github.py",
    "sync_to_postgres.py",
    "utils/nba_api_helpers.py",
    "scrape_bref_gamelogs.py",
    "refresh_nba_data.py",
    "scrape_nba_gamelogs.py",
    "build_player_archetypes.py",
    "calculate_dva.py",
    "calculate_context_engine.py",
    "calculate_projections.py",
    "calculate_player_value.py",
    "estimate_ownership.py",
    "scrape_salaries.py",
    "scrape_dvp.py",
    "scrape_depth_charts.py",
    "scrape_odds.py",
    "scrape_per100.py",
    "scrape_referee_stats.py",
    "scrape_injuries.py",
    "scrape_play_types.py",
    "scrape_shot_zones.py",
    "scrape_shot_chart_detail.py",
    "scrape_team_defense_zones.py",
    "scrape_hustle_stats.py",
    "scrape_props.py",
    "scrape_fta_ownership.py",
    "requirements.txt",
    "static/js/app.js",
    "static/css/style.css",
    "templates/base.html",
    "templates/home.html",
    "templates/index.html",
    "templates/projections.html",
    "templates/contest.html",
    "templates/login.html",
    "templates/register.html",
    "templates/history.html",
    "templates/leaderboard.html",
    "templates/profile.html",
    "templates/shop.html",
    "templates/articles.html",
    "templates/trends.html",
    "templates/trends_paywall.html",
    "templates/pricing_cards.html",
    "templates/no_contest.html",
    "templates/play.html",
    "templates/entry.html",
    "templates/chart_screenshot.html",
    "templates/h2h_lobby.html",
    "templates/h2h_lineup.html",
    "templates/h2h_match.html",
    "templates/admin.html",
    "templates/error.html",
    "migrations/enable_rls.sql",
    "migrations/add_notifications.sql",
    ".stripe_keys.json",
    "run_pregame_refresh.py",
    "scheduler_pregame.py",
    "refresh_charts.py",
    "scheduler_charts.py",
    "start_production.sh",
    "replit.md",
    ".gitignore",
]

NEVER_PUSH = {"articles/", "conversation_logs/", ".local/", "__pycache__/", ".pythonlibs/"}


def get_github_token():
    hostname = os.environ.get("REPLIT_CONNECTORS_HOSTNAME", "")
    identity = os.environ.get("REPL_IDENTITY", "")
    if not hostname or not identity:
        token = os.environ.get("GITHUB_TOKEN")
        return token

    url = f"https://{hostname}/api/v2/connection?include_secrets=true&connector_names=github"
    req = urllib.request.Request(url, headers={
        "Accept": "application/json",
        "X-Replit-Token": f"repl {identity}"
    })
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            items = data.get("items", [])
            if items:
                return items[0]["settings"]["access_token"]
    except Exception as e:
        print(f"  Connector token failed: {e}")

    token = os.environ.get("GITHUB_TOKEN")
    return token


def gh_api(path, method="GET", data=None, token=None):
    url = f"https://api.github.com{path}"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err_body = e.read().decode() if e.fp else ""
        raise Exception(f"GitHub API {e.code}: {err_body[:300]}")


def get_existing_tree(token):
    ref = gh_api(f"/repos/{REPO}/git/ref/heads/{BRANCH}", token=token)
    commit_sha = ref["object"]["sha"]
    commit = gh_api(f"/repos/{REPO}/git/commits/{commit_sha}", token=token)
    tree_sha = commit["tree"]["sha"]
    return commit_sha, tree_sha


def create_blob(token, content_bytes, is_binary=False):
    if is_binary:
        encoded = base64.b64encode(content_bytes).decode()
        data = {"content": encoded, "encoding": "base64"}
    else:
        data = {"content": content_bytes.decode("utf-8", errors="replace"), "encoding": "utf-8"}
    result = gh_api(f"/repos/{REPO}/git/blobs", method="POST", data=data, token=token)
    return result["sha"]


def should_skip(filepath):
    for prefix in NEVER_PUSH:
        if filepath.startswith(prefix):
            return True
    return False


def main():
    print("=" * 50)
    print("Push to GitHub (API)")
    print("=" * 50)

    token = get_github_token()
    if not token:
        print("SKIPPED: GitHub token not available")
        sys.exit(1)

    today = datetime.now().strftime("%Y-%m-%d %H:%M")
    commit_msg = " ".join(sys.argv[1:]).strip() if len(sys.argv) > 1 else ""
    cwd = "/home/runner/workspace"

    all_files = PIPELINE_OUTPUT_FILES + _find_article_headers() + SOURCE_CODE_FILES
    existing = []
    for f in all_files:
        full = os.path.join(cwd, f)
        if os.path.exists(full) and not should_skip(f):
            existing.append(f)

    if not existing:
        print("No files found to push")
        return

    print(f"  Preparing {len(existing)} files...")

    commit_sha, base_tree_sha = get_existing_tree(token)

    tree_items = []
    for filepath in existing:
        full = os.path.join(cwd, filepath)
        is_binary = filepath.endswith(('.png', '.jpg', '.jpeg', '.gif', '.ico', '.woff', '.woff2', '.ttf'))
        try:
            with open(full, 'rb') as f:
                content = f.read()
            blob_sha = create_blob(token, content, is_binary=is_binary)
            tree_items.append({
                "path": filepath,
                "mode": "100644",
                "type": "blob",
                "sha": blob_sha,
            })
        except Exception as e:
            print(f"  WARN: Skipping {filepath}: {e}")

    if not tree_items:
        print("No files to commit")
        return

    print(f"  Created {len(tree_items)} blobs, building tree...")

    tree = gh_api(f"/repos/{REPO}/git/trees", method="POST", data={
        "base_tree": base_tree_sha,
        "tree": tree_items,
    }, token=token)
    new_tree_sha = tree["sha"]

    if commit_msg:
        full_msg = f"{commit_msg} ({today})"
    else:
        full_msg = f"Daily pipeline update ({today})"

    new_commit = gh_api(f"/repos/{REPO}/git/commits", method="POST", data={
        "message": full_msg,
        "tree": new_tree_sha,
        "parents": [commit_sha],
    }, token=token)
    new_commit_sha = new_commit["sha"]

    gh_api(f"/repos/{REPO}/git/refs/heads/{BRANCH}", method="PATCH", data={
        "sha": new_commit_sha,
        "force": True,
    }, token=token)

    print(f"  Pushed {len(tree_items)} files to GitHub ({new_commit_sha[:8]})")
    print(f"  Commit: {full_msg}")

    trigger_render_deploy()


def trigger_render_deploy():
    hook_url = os.environ.get("RENDER_DEPLOY_HOOK_URL")
    if not hook_url:
        print("  Render deploy hook not configured — skipping")
        return
    try:
        req = urllib.request.Request(hook_url, method="POST", data=b"")
        with urllib.request.urlopen(req, timeout=15) as resp:
            print(f"  Render deploy triggered (HTTP {resp.status})")
    except Exception as e:
        print(f"  Render deploy hook failed: {e}")


if __name__ == "__main__":
    main()
