import time
import random
import json
import os

MAX_RETRIES = 2
RETRY_DELAYS = [5, 15]
NBA_TIMEOUT_FIRST = 45
NBA_TIMEOUT_RETRY = 90
CIRCUIT_BREAKER_FILE = '/tmp/nba_circuit_breaker.json'

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
]


def reset_circuit():
    try:
        if os.path.exists(CIRCUIT_BREAKER_FILE):
            os.remove(CIRCUIT_BREAKER_FILE)
    except OSError:
        pass


def trip_circuit(label):
    try:
        with open(CIRCUIT_BREAKER_FILE, 'w') as f:
            json.dump({'tripped': True, 'tripped_by': label, 'time': time.time()}, f)
    except OSError:
        pass


def is_circuit_open():
    try:
        if os.path.exists(CIRCUIT_BREAKER_FILE):
            with open(CIRCUIT_BREAKER_FILE, 'r') as f:
                data = json.load(f)
            return data.get('tripped', False)
    except (OSError, json.JSONDecodeError):
        pass
    return False


def get_circuit_info():
    try:
        if os.path.exists(CIRCUIT_BREAKER_FILE):
            with open(CIRCUIT_BREAKER_FILE, 'r') as f:
                return json.load(f)
    except (OSError, json.JSONDecodeError):
        pass
    return None


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
    if is_circuit_open():
        print(f"  CIRCUIT BREAKER: Skipping {label} — NBA.com marked unreachable this run")
        return None

    warmup = random.uniform(2, 5)
    print(f"  Warming up {warmup:.1f}s before {label}...")
    time.sleep(warmup)

    for attempt in range(MAX_RETRIES):
        headers = get_nba_headers()
        timeout = NBA_TIMEOUT_FIRST if attempt == 0 else NBA_TIMEOUT_RETRY
        try:
            result = endpoint_class(**kwargs, timeout=timeout, headers=headers)
            return result.get_data_frames()[0]
        except Exception as e:
            base_delay = RETRY_DELAYS[attempt] if attempt < len(RETRY_DELAYS) else 15
            jitter = random.uniform(-3, 3)
            delay = max(3, base_delay + jitter)
            print(f"  Attempt {attempt+1}/{MAX_RETRIES} for {label} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                print(f"  Retrying in {delay:.0f}s...")
                time.sleep(delay)

    print(f"  WARNING: All {MAX_RETRIES} attempts failed for {label} — tripping circuit breaker")
    trip_circuit(label)
    return None


def inter_call_delay():
    if is_circuit_open():
        return
    delay = random.uniform(3, 8)
    print(f"  Cooling down {delay:.1f}s between NBA.com calls...")
    time.sleep(delay)
