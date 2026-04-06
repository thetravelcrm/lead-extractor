"""
scraper/website_visitor.py
--------------------------
Visits individual company websites and returns their HTML + visible text.

Strategy:
1. Try a plain requests GET (fast, low overhead).
2. If the response body looks empty / JS-only (SPA), fall back to Playwright
   to get the fully rendered HTML.
3. Also attempts to find and fetch the company's /contact or /about page,
   since those are the most likely places to find email addresses.
"""

import asyncio
import random
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from config.settings import REQUEST_TIMEOUT, DELAY_BETWEEN_VISITS, USER_AGENTS
from scraper.anti_bot import build_session, random_delay


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _make_absolute(base_url: str, href: str) -> str:
    """Convert a relative href to an absolute URL using the page's base URL."""
    try:
        return urljoin(base_url, href)
    except Exception:
        return ""


def needs_js_render(html: str) -> bool:
    """
    Heuristic: if BeautifulSoup can extract fewer than 200 characters of
    visible text from the page body, it's probably a JS-rendered SPA that
    requires Playwright.
    """
    try:
        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text(separator=" ", strip=True)
        return len(text) < 200
    except Exception:
        return False


def get_direct_contact_urls(base_url: str) -> list:
    """
    Return a list of common contact page URLs to try directly,
    even if they aren't linked from the homepage.
    e.g. https://company.com/contact, /contact-us, /about, /about-us
    """
    from urllib.parse import urlparse
    parsed = urlparse(base_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    slugs = [
        "/contact", "/contact-us", "/contact_us", "/contactus",
        "/about", "/about-us", "/about_us", "/aboutus",
        "/reach-us", "/get-in-touch", "/enquiry", "/info",
        "/contacto", "/kontakt", "/impressum", "/team",
        "/get-quote", "/quote", "/request-quote", "/book-now",
        "/enquiry-form", "/contact-form", "/send-email",
        "/company", "/who-we-are", "/our-story",
    ]
    return [base + s for s in slugs]


def get_contact_page_urls(base_url: str, soup: BeautifulSoup) -> list:
    """
    Scan the page for internal links that likely lead to a contact or about
    page.  Returns up to 3 candidate URLs.
    """
    keywords = {"contact", "about", "impressum", "reach", "reach-us",
                "reach us", "get-in-touch", "get in touch", "touch",
                "enquire", "enquiry", "info", "quote", "book",
                "email", "call", "phone", "whatsapp", "team",
                "who we are", "our story", "company"}
    base_host = urlparse(base_url).netloc.lower()
    candidates = []
    seen = set()

    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        text = (tag.get_text() or "").lower()
        href_lower = href.lower()

        # Check if any keyword appears in the href path or link text
        if not any(k in href_lower or k in text for k in keywords):
            continue

        abs_url = _make_absolute(base_url, href)
        if not abs_url.startswith("http"):
            continue

        # Keep only internal links (same host)
        target_host = urlparse(abs_url).netloc.lower()
        if target_host != base_host:
            continue

        if abs_url not in seen:
            seen.add(abs_url)
            candidates.append(abs_url)

        if len(candidates) >= 3:
            break

    return candidates


# ---------------------------------------------------------------------------
# Playwright JS-render fallback
# ---------------------------------------------------------------------------

async def _fetch_with_playwright(url: str) -> Optional[str]:
    """Use headless Chromium to render a JS-heavy page and return its HTML."""
    try:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
            context = await browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                viewport={"width": 1280, "height": 800},
            )
            page = await context.new_page()
            await page.goto(url, wait_until="networkidle", timeout=20_000)
            html = await page.content()
            await browser.close()
            return html
    except Exception:
        return None


def _fetch_with_playwright_sync(url: str) -> Optional[str]:
    """Synchronous wrapper around the async Playwright fetch."""
    try:
        return asyncio.run(_fetch_with_playwright(url))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Main visitor function
# ---------------------------------------------------------------------------

def visit_website(url: str, session: requests.Session = None) -> Optional[dict]:
    """
    Fetch a website and return a dict with its content.

    Returns None if the site is unreachable or yields no useful data.

    Return format:
    {
        "url":      str,   # original URL
        "html":     str,   # raw HTML of the page (+ contact subpages merged)
        "text":     str,   # visible text extracted by BeautifulSoup
        "title":    str,   # page <title>
        "rendered": bool,  # True if Playwright fallback was used
    }
    """
    if session is None:
        session = build_session()

    html_parts = []
    rendered = False
    title = ""
    main_soup = None

    # ---- Step 1: Fetch the main page ----
    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT, verify=True, allow_redirects=True)
        resp.raise_for_status()
        raw_html = resp.text
    except requests.exceptions.SSLError:
        # Retry without SSL verification (self-signed certs are common)
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT, verify=False, allow_redirects=True)
            raw_html = resp.text
        except Exception:
            return None
    except Exception:
        return None

    # ---- Step 2: JS-render fallback ----
    if needs_js_render(raw_html):
        js_html = _fetch_with_playwright_sync(url)
        if js_html and not needs_js_render(js_html):
            raw_html = js_html
            rendered = True
        # If even Playwright gives us nothing, just continue with what we have

    html_parts.append(raw_html)

    # ---- Step 3: Parse main page ----
    try:
        main_soup = BeautifulSoup(raw_html, "lxml")
        title_tag = main_soup.find("title")
        title = title_tag.get_text(strip=True) if title_tag else ""
    except Exception:
        pass

    # ---- Step 4: Fetch contact/about sub-pages ----
    visited_sub = set()

    # 4a. Links found on the main page
    if main_soup:
        for sub_url in get_contact_page_urls(url, main_soup)[:3]:
            if sub_url in visited_sub:
                continue
            visited_sub.add(sub_url)
            try:
                sub_resp = session.get(sub_url, timeout=REQUEST_TIMEOUT, verify=False)
                if sub_resp.status_code == 200:
                    html_parts.append(sub_resp.text)
                random_delay((0.5, 1.0))
            except Exception:
                continue

    # 4b. Probe common contact URLs directly (catches sites where contact page
    #     is not linked on the homepage)
    direct_urls = get_direct_contact_urls(url)
    probed = 0
    for sub_url in direct_urls:
        if probed >= 6:
            break
        if sub_url in visited_sub:
            continue
        visited_sub.add(sub_url)
        try:
            sub_resp = session.get(sub_url, timeout=8, verify=False, allow_redirects=True)
            if sub_resp.status_code == 200 and len(sub_resp.text) > 200:
                html_parts.append(sub_resp.text)
                probed += 1
            random_delay((0.3, 0.8))
        except Exception:
            continue

    # ---- Step 5: Build combined output ----
    combined_html = "\n".join(html_parts)
    try:
        combined_soup = BeautifulSoup(combined_html, "lxml")
        combined_text = combined_soup.get_text(separator=" ", strip=True)
    except Exception:
        combined_text = ""

    if not combined_text.strip():
        return None

    return {
        "url":      url,
        "html":     combined_html,
        "text":     combined_text,
        "title":    title,
        "rendered": rendered,
    }
