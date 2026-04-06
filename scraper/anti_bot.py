"""
scraper/anti_bot.py
-------------------
Anti-detection utilities shared across the scraping modules.

Provides:
- random_delay()    — sleep for a random duration to mimic human behaviour
- get_random_ua()   — pick a random User-Agent string from the pool
- build_session()   — create a requests.Session with browser-like headers and
                      automatic retry logic
- RateLimiter       — token-bucket limiter to avoid hammering external servers
"""

import random
import time
import threading
import warnings

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.exceptions import InsecureRequestWarning

# Suppress InsecureRequestWarning — intentional for scraping self-signed certs
warnings.filterwarnings("ignore", category=InsecureRequestWarning)

from config.settings import USER_AGENTS, REQUEST_TIMEOUT, MAX_RETRIES, RATE_LIMIT_RPM


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------

def random_delay(range_tuple: tuple) -> None:
    """Sleep for a uniformly random duration within range_tuple (min, max) seconds."""
    duration = random.uniform(*range_tuple)
    time.sleep(duration)


# ---------------------------------------------------------------------------
# User-Agent rotation
# ---------------------------------------------------------------------------

def get_random_ua() -> str:
    """Return a random User-Agent string from the configured pool."""
    return random.choice(USER_AGENTS)


# ---------------------------------------------------------------------------
# requests.Session builder
# ---------------------------------------------------------------------------

def build_session(ua: str = None) -> requests.Session:
    """
    Create a requests.Session that looks like a real browser:
    - Rotated User-Agent header
    - Standard browser Accept/Accept-Language headers
    - Retry adapter: 2 retries with 0.5-second back-off for 429/5xx responses
    """
    session = requests.Session()

    selected_ua = ua or get_random_ua()

    session.headers.update({
        "User-Agent":               selected_ua,
        "Accept":                   "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language":          "en-US,en;q=0.5",
        "Accept-Encoding":          "gzip, deflate, br",
        "DNT":                      "1",
        "Connection":               "keep-alive",
        "Upgrade-Insecure-Requests":"1",
    })

    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://",  adapter)
    session.mount("https://", adapter)

    return session


# ---------------------------------------------------------------------------
# Rate limiter (token bucket)
# ---------------------------------------------------------------------------

class RateLimiter:
    """
    Simple token-bucket rate limiter.

    Ensures that no more than `rpm` requests are made per minute across all
    calls to `acquire()`.  Thread-safe — multiple scraper threads can share
    one instance.

    Usage:
        limiter = RateLimiter(rpm=20)
        ...
        limiter.acquire()    # blocks if the bucket is empty
        session.get(url)
    """

    def __init__(self, rpm: int = RATE_LIMIT_RPM) -> None:
        self._interval = 60.0 / rpm   # seconds between tokens
        self._lock = threading.Lock()
        self._next_token_at = time.monotonic()

    def acquire(self) -> None:
        """Block until a request token is available."""
        with self._lock:
            now = time.monotonic()
            wait = self._next_token_at - now
            if wait > 0:
                time.sleep(wait)
            # Schedule the next available token
            self._next_token_at = max(self._next_token_at, time.monotonic()) + self._interval
