import subprocess
import sys
import os
import json
import urllib.request
from datetime import datetime

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


def get_github_token():
    hostname = os.environ.get("REPLIT_CONNECTORS_HOSTNAME", "")
    identity = os.environ.get("REPL_IDENTITY", "")
    if not hostname or not identity:
        return None

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
        print(f"Could not retrieve GitHub token: {e}")
    return None


def main():
    print("=" * 50)
    print("Push to GitHub")
    print("=" * 50)

    token = get_github_token()
    if not token:
        print("SKIPPED: GitHub token not available")
        sys.exit(1)

    today = datetime.now().strftime("%Y-%m-%d")
    remote_url = f"https://x-access-token:{token}@github.com/rakimr/PIRTDICA.git"
    cwd = "/home/runner/workspace"

    existing = [f for f in PIPELINE_OUTPUT_FILES if os.path.exists(os.path.join(cwd, f))]
    if not existing:
        print("No pipeline output files found")
        return

    subprocess.run(["rm", "-f", ".git/index.lock"], cwd=cwd, capture_output=True)

    subprocess.run(["git", "add"] + existing, cwd=cwd, capture_output=True, text=True, timeout=30)

    diff = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=cwd, capture_output=True, timeout=10)
    if diff.returncode == 0:
        print("No changes to push — GitHub is up to date")
        return

    commit = subprocess.run(
        ["git", "commit", "-m", f"Daily pipeline update {today}"],
        cwd=cwd, capture_output=True, text=True, timeout=30
    )
    print(f"Commit: {commit.stdout.strip()}")

    push = subprocess.run(
        ["git", "push", remote_url, "main", "--no-thin"],
        cwd=cwd, capture_output=True, text=True, timeout=60
    )

    if push.returncode == 0:
        print(f"Pushed daily update for {today} to GitHub")
    else:
        safe_err = push.stderr.strip() if push.stderr else "unknown error"
        if "x-access-token" in safe_err:
            safe_err = "[error redacted — contains token]"
        print(f"Push failed: {safe_err}")
        sys.exit(1)


if __name__ == "__main__":
    main()
