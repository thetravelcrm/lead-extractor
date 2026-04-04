"""
scraper/google_search.py
------------------------
Uses Playwright (headless Chromium) to search Google and collect company
website URLs from organic results.

Key functions:
- build_query()          — construct a targeted Google search query
- search_google()        — async: paginate through Google, return URL list
- parse_search_results() — extract result URLs from a page's HTML
- is_excluded_domain()   — filter out social media, directories, etc.

Why Playwright instead of requests?
  Google's search results page is rendered client-side and aggressively
  detects headless bots.  Playwright launches a real Chromium browser so we
  get proper HTML, can handle dynamic page loads, and can detect CAPTCHAs.
"""

import asyncio
import random
import re
from typing import Callable, List
from urllib.parse import urlparse, urlencode, quote_plus

from config.settings import (
    USER_AGENTS,
    EXCLUDED_DOMAINS,
    DELAY_BETWEEN_SEARCHES,
    DELAY_PAGE_LOAD,
)


# ---------------------------------------------------------------------------
# Query construction
# ---------------------------------------------------------------------------

def build_query(country: str, business_type: str) -> str:
    """
    Build a Google search query that targets companies with public contact info.

    Example output:
        '"plumbing companies" "United Kingdom" email contact
         -site:facebook.com -site:linkedin.com -site:yelp.com'
    """
    # Exclude the most common directories / social networks at query time
    static_exclusions = [
        "-site:facebook.com",
        "-site:linkedin.com",
        "-site:instagram.com",
        "-site:twitter.com",
        "-site:x.com",
        "-site:yelp.com",
        "-site:tripadvisor.com",
        "-site:yellowpages.com",
        "-site:wikipedia.org",
        "-site:amazon.com",
    ]
    exclusion_str = " ".join(static_exclusions)
    return f'"{business_type}" "{country}" email contact {exclusion_str}'


# ---------------------------------------------------------------------------
# Domain filtering
# ---------------------------------------------------------------------------

def is_excluded_domain(url: str) -> bool:
    """
    Return True if the URL belongs to a social network, directory, or
    any other domain we want to skip.
    """
    try:
        host = urlparse(url).netloc.lower()
        # Strip leading 'www.'
        if host.startswith("www."):
            host = host[4:]
        # Check against our exclusion set using suffix matching
        for blocked in EXCLUDED_DOMAINS:
            if host == blocked or host.endswith("." + blocked):
                return True
    except Exception:
        return True
    return False


# ---------------------------------------------------------------------------
# HTML parsing — extract result URLs from a Google SERP
# ---------------------------------------------------------------------------

def parse_search_results(html: str) -> List[str]:
    """
    Extract organic result URLs from raw Google search result HTML.

    Google wraps result links in <a> tags inside div#search.  We look for
    href attributes that start with 'http' and don't belong to google.com.
    """
    # Quick-and-dirty regex approach (avoids BeautifulSoup dependency here)
    # Matches href="https://..." inside anchor tags within the results block
    pattern = re.compile(r'href="(https?://[^"]+)"', re.IGNORECASE)
    raw_urls = pattern.findall(html)

    urls: List[str] = []
    seen: set = set()

    for url in raw_urls:
        # Skip Google-internal URLs
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        if "google." in host:
            continue
        # Strip tracking parameters added by Google (/url?q=...)
        if "/url?q=" in url:
            m = re.search(r'/url\?q=(https?://[^&]+)', url)
            if m:
                url = m.group(1)
        # Normalise
        url = url.split("&")[0]  # drop extra params
        if url in seen:
            continue
        if not url.startswith("http"):
            continue
        seen.add(url)
        urls.append(url)

    return urls


# ---------------------------------------------------------------------------
# Main async search function
# ---------------------------------------------------------------------------

async def search_google(
    query: str,
    max_pages: int,
    emit_fn: Callable,
) -> List[str]:
    """
    Launch headless Chromium, paginate through Google search results, and
    return a list of company website URLs (filtered, deduplicated).

    Parameters
    ----------
    query     : str       — the search query string
    max_pages : int       — how many Google result pages to scrape (10 results each)
    emit_fn   : Callable  — SSE emit function: emit_fn(level, message, data=None)

    Returns
    -------
    list[str] — URLs of company websites
    """
    from playwright.async_api import async_playwright

    all_urls: List[str] = []
    seen: set = set()

    emit_fn("info", f"Starting Google search: {query[:80]}...")

    async with async_playwright() as p:
        # Launch a real Chromium browser (not Firefox — Google blocks it more)
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",  # hide automation flag
            ],
        )

        for page_num in range(max_pages):
            start_index = page_num * 10
            ua = random.choice(USER_AGENTS)

            # Create a fresh browser context per page for better anonymity
            context = await browser.new_context(
                user_agent=ua,
                viewport={"width": random.choice([1280, 1366, 1440, 1920]),
                          "height": random.choice([768, 800, 900, 1080])},
                locale="en-US",
                java_script_enabled=True,
            )
            # Override the webdriver property to avoid easy detection
            await context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )

            page = await context.new_page()

            # Set realistic request headers
            await page.set_extra_http_headers({
                "Accept-Language": "en-US,en;q=0.9",
                "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer":         "https://www.google.com/",
            })

            google_url = (
                "https://www.google.com/search?"
                + urlencode({"q": query, "start": start_index, "hl": "en", "num": 10})
            )

            try:
                await page.goto(google_url, wait_until="domcontentloaded", timeout=30_000)

                # Random wait to simulate reading time
                await page.wait_for_timeout(
                    int(random.uniform(*DELAY_PAGE_LOAD) * 1000)
                )

                # ---- CAPTCHA detection ----
                title = await page.title()
                body_text = await page.inner_text("body")

                if (
                    "unusual traffic" in title.lower()
                    or "captcha" in title.lower()
                    or "our systems have detected" in body_text.lower()
                    or "verify you're a human" in body_text.lower()
                ):
                    emit_fn(
                        "warn",
                        f"Google CAPTCHA detected on page {page_num + 1}. "
                        "Waiting 30 seconds and skipping this page...",
                    )
                    await asyncio.sleep(30)
                    await page.close()
                    await context.close()
                    continue

                # ---- Extract URLs ----
                html = await page.content()
                page_urls = parse_search_results(html)

                new_count = 0
                for url in page_urls:
                    if url not in seen and not is_excluded_domain(url):
                        seen.add(url)
                        all_urls.append(url)
                        new_count += 1

                emit_fn(
                    "info",
                    f"Page {page_num + 1}/{max_pages}: found {new_count} new URLs "
                    f"(total so far: {len(all_urls)})",
                    data={"current": page_num + 1, "total": max_pages},
                )

            except Exception as exc:
                emit_fn("warn", f"Error on search page {page_num + 1}: {exc}")

            finally:
                try:
                    await page.close()
                    await context.close()
                except Exception:
                    pass

            # Delay between search pages to avoid rate-limiting
            if page_num < max_pages - 1:
                await asyncio.sleep(random.uniform(*DELAY_BETWEEN_SEARCHES))

        await browser.close()

    emit_fn("success", f"Google search complete. Collected {len(all_urls)} URLs to visit.")
    return all_urls
