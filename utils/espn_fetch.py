"""Shared ESPN JSON fetcher with a system-curl fallback.

Aug 2026: ESPN's edge began 403-ing the bare spoofed "Mozilla/5.0"
User-Agent on site.api.espn.com (teams/roster/scoreboard/summary/standings)
— a classic bot-signature block. Default client UAs (python-requests, curl)
pass fine. This silently killed the WNBA game-log scrape (and with it
nightly pick grading) while cache-backed scrapers appeared to keep working.

Strategy: NEVER spoof a browser User-Agent — send each client's default.
requests first (fast path), then a curl subprocess on any non-200/network
failure (belt-and-suspenders against future edge-rule changes). Once a host
has rejected requests and curl has succeeded, we go curl-first for that host
for the rest of the process. Failures are loud so pipeline logs show WHY a
fetch degraded.
"""
import json
import shutil
import subprocess
import time
from urllib.parse import urlsplit

import requests

CURL = shutil.which("curl")

# Hosts where requests was rejected and curl succeeded this process.
_curl_first_hosts = set()


def _curl_get(url, timeout):
    if not CURL:
        return None
    try:
        p = subprocess.run(
            [CURL, "-sS", "--fail", "--max-time", str(timeout), url],
            capture_output=True, timeout=timeout + 5,
        )
        if p.returncode == 0 and p.stdout:
            return json.loads(p.stdout)
    except (subprocess.SubprocessError, OSError, json.JSONDecodeError):
        pass
    return None


def espn_get_json(url, tries=3, timeout=20):
    """GET a JSON document from ESPN. Returns parsed JSON or None."""
    host = urlsplit(url).netloc
    last_status = None
    for attempt in range(tries):
        if host not in _curl_first_hosts:
            try:
                r = requests.get(url, timeout=timeout)
                last_status = r.status_code
                if r.status_code == 200:
                    return r.json()
            except requests.RequestException:
                last_status = "network-error"

        data = _curl_get(url, timeout)
        if data is not None:
            if host not in _curl_first_hosts:
                _curl_first_hosts.add(host)
                print(f"  [espn_fetch] requests got {last_status} for {host} — "
                      f"using curl fallback for this host from now on")
            return data

        time.sleep(0.8 * (attempt + 1))

    print(f"  [espn_fetch] FAILED after {tries} tries "
          f"(last requests status={last_status}): {url[:120]}")
    return None
