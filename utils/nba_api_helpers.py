import time
import random

MAX_RETRIES = 4
RETRY_DELAYS = [5, 15, 30, 60]
NBA_TIMEOUT = 180

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
]


def get_nba_headers():
    return {
        'Host': 'stats.nba.com',
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'x-nba-stats-origin': 'stats',
        'x-nba-stats-token': 'true',
        'Connection': 'keep-alive',
        'Referer': 'https://www.nba.com/',
        'Origin': 'https://www.nba.com',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
    }


def nba_api_call_with_retry(endpoint_class, label, **kwargs):
    warmup = random.uniform(2, 5)
    print(f"  Warming up {warmup:.1f}s before {label}...")
    time.sleep(warmup)

    for attempt in range(MAX_RETRIES):
        headers = get_nba_headers()
        try:
            result = endpoint_class(**kwargs, timeout=NBA_TIMEOUT, headers=headers)
            return result.get_data_frames()[0]
        except Exception as e:
            base_delay = RETRY_DELAYS[attempt] if attempt < len(RETRY_DELAYS) else 60
            jitter = random.uniform(-3, 3)
            delay = max(3, base_delay + jitter)
            print(f"  Attempt {attempt+1}/{MAX_RETRIES} for {label} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                print(f"  Retrying in {delay:.0f}s...")
                time.sleep(delay)
    print(f"  WARNING: All {MAX_RETRIES} attempts failed for {label} - NBA.com may be unreachable")
    return None


def inter_call_delay():
    delay = random.uniform(3, 8)
    print(f"  Cooling down {delay:.1f}s between NBA.com calls...")
    time.sleep(delay)
