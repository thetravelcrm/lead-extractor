"""
scraper/website_visitor.py
--------------------------
BULLETPROOF website visitor — ALWAYS uses Playwright for full JS rendering.
Extracts emails from homepage, all internal pages, and raw HTML source.

Strategy:
1. ALWAYS use Playwright (full JS rendering — catches React/Next.js/SPA sites).
2. Extract emails from entire HTML source (mailto:, data-attrs, hidden text).
3. Follow ALL internal links to find additional pages with emails.
4. Visit footer/about/contact/team pages explicitly.
5. Merge emails from ALL pages into one comprehensive list.
"""

import asyncio
import random
import re
from typing import Optional, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from config.settings import REQUEST_TIMEOUT, DELAY_BETWEEN_VISITS, USER_AGENTS
from scraper.anti_bot import build_session, random_delay


# ---------------------------------------------------------------------------
# Aggressive email extraction from raw HTML
# ---------------------------------------------------------------------------

def extract_emails_from_source(html: str) -> Set[str]:
    """
    Aggressively extract ALL email addresses from raw HTML source.
    Catches emails in:
    - mailto: links
    - Direct text in body
    - data attributes (data-email, data-contact, data-mail)
    - JavaScript variables
    - Hidden divs/spans
    - Meta tags
    """
    emails = set()
    
    # Pattern 1: mailto: links (most reliable)
    mailto_pattern = re.compile(r'mailto:([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})', re.IGNORECASE)
    emails.update(m.lower() for m in mailto_pattern.findall(html))
    
    # Pattern 2: Direct email addresses everywhere in HTML
    email_pattern = re.compile(
        r'(?<![=/])\b([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})\b',
        re.IGNORECASE
    )
    emails.update(e.lower() for e in email_pattern.findall(html))
    
    # Pattern 3: data-* attributes
    data_pattern = re.compile(
        r'(?:data-email|data-contact|data-mail)=["\']([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})["\']',
        re.IGNORECASE
    )
    emails.update(e.lower() for e in data_pattern.findall(html))
    
    # Filter out false positives
    filtered = set()
    for email in emails:
        # Skip known false positives
        if any(skip in email for skip in ['google.com', 'example.com', 'schema.org', 'w3.org', 'blogger.com']):
            continue
        # Skip image/CSS/JS file extensions masquerading as TLDs
        if any(email.endswith(ext) for ext in ['.png', '.jpg', '.gif', '.svg', '.css', '.js', '.ico']):
            continue
        filtered.add(email)
    
    return filtered


# ---------------------------------------------------------------------------
# Playwright — ALWAYS used for full JS rendering
# ---------------------------------------------------------------------------

async def _fetch_with_playwright(url: str) -> Optional[str]:
    """Use headless Chromium to render a page and return its HTML (with all JS executed)."""
    try:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
            context = await browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                viewport={"width": 1400, "height": 900},
            )
            page = await context.new_page()
            await page.goto(url, wait_until="networkidle", timeout=25_000)
            # Wait extra for lazy-loaded content (footers, popups, etc.)
            await page.wait_for_timeout(3000)
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
# Internal helpers
# ---------------------------------------------------------------------------

def _make_absolute(base_url: str, href: str) -> str:
    """Convert a relative href to an absolute URL using the page's base URL."""
    try:
        return urljoin(base_url, href)
    except Exception:
        return ""


def _is_same_domain(url: str, base_url: str) -> bool:
    """Check if a URL belongs to the same domain as the base URL."""
    try:
        base_host = urlparse(base_url).netloc.lower().replace("www.", "")
        target_host = urlparse(url).netloc.lower().replace("www.", "")
        return base_host == target_host
    except Exception:
        return False


def get_contact_page_urls(base_url: str, soup: BeautifulSoup) -> list:
    """
    Scan the page for internal links that likely lead to a contact or about
    page. Returns up to 5 candidate URLs.
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

        if len(candidates) >= 5:
            break

    return candidates


def get_all_internal_urls(base_url: str, soup: BeautifulSoup) -> list:
    """
    Get ALL internal page URLs from the homepage.
    This catches emails on pages like /team, /careers, /partners, etc.
    """
    base_host = urlparse(base_url).netloc.lower()
    urls = []
    seen = set()
    
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        abs_url = _make_absolute(base_url, href)
        
        if not abs_url.startswith("http"):
            continue
        if not _is_same_domain(abs_url, base_url):
            continue
        # Skip anchors, mailto, tel, javascript
        if abs_url.startswith(("mailto:", "tel:", "javascript:", "#")):
            continue
        # Skip common non-content URLs
        if any(skip in abs_url.lower() for skip in ['/wp-admin', '/wp-login', '/admin', '/login', '.xml', '.json']):
            continue
        
        if abs_url not in seen:
            seen.add(abs_url)
            urls.append(abs_url)
    
    return urls[:15]  # Limit to first 15 pages for speed


# ---------------------------------------------------------------------------
# Main visitor function — BULLETPROOF
# ---------------------------------------------------------------------------

def visit_website(url: str, session: requests.Session = None) -> Optional[dict]:
    """
    Fetch a website using Playwright (full JS rendering) and return its content.
    
    BULLETPROOF features:
    - Always uses Playwright (catches React/Next.js/WordPress JS sites)
    - Extracts emails from entire HTML source (not just visible text)
    - Visits contact/about/team pages explicitly
    - Scans footer, header, and all internal pages
    """
    if session is None:
        session = build_session()

    all_html = []
    all_emails = set()
    title = ""
    rendered = False

    # ---- Step 1: ALWAYS use Playwright for homepage (full JS rendering) ----
    js_html = _fetch_with_playwright_sync(url)
    if js_html:
        all_html.append(js_html)
        rendered = True
        # Extract emails from homepage HTML source
        all_emails.update(extract_emails_from_source(js_html))
    else:
        # Fallback to requests if Playwright fails
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT, verify=False, allow_redirects=True)
            if resp.status_code == 200:
                all_html.append(resp.text)
                all_emails.update(extract_emails_from_source(resp.text))
        except Exception:
            return None

    # ---- Step 2: Parse main page for internal links ----
    try:
        main_soup = BeautifulSoup(all_html[0], "lxml")
        title_tag = main_soup.find("title")
        title = title_tag.get_text(strip=True) if title_tag else ""
    except Exception:
        main_soup = None

    if not main_soup:
        return None

    # ---- Step 3: Visit contact/about/team pages ----
    visited_urls = {url}
    
    # 3a. Links found on the main page
    contact_links = get_contact_page_urls(url, main_soup)
    for sub_url in contact_links[:5]:
        if sub_url in visited_urls:
            continue
        visited_urls.add(sub_url)
        
        sub_html = _fetch_with_playwright_sync(sub_url)
        if sub_html:
            all_html.append(sub_html)
            all_emails.update(extract_emails_from_source(sub_html))
        else:
            try:
                sub_resp = session.get(sub_url, timeout=REQUEST_TIMEOUT, verify=False, allow_redirects=True)
                if sub_resp.status_code == 200:
                    all_html.append(sub_resp.text)
                    all_emails.update(extract_emails_from_source(sub_resp.text))
            except Exception:
                pass
        
        random_delay((0.3, 0.7))

    # 3b. Scan a few more internal pages for emails
    all_internal = get_all_internal_urls(url, main_soup)
    pages_visited = 0
    for sub_url in all_internal:
        if pages_visited >= 5 or sub_url in visited_urls:
            continue
        visited_urls.add(sub_url)
        pages_visited += 1
        
        sub_html = _fetch_with_playwright_sync(sub_url)
        if sub_html:
            all_html.append(sub_html)
            all_emails.update(extract_emails_from_source(sub_html))
        
        random_delay((0.3, 0.5))

    # ---- Step 4: Build combined output ----
    combined_html = "\n".join(all_html)
    try:
        combined_soup = BeautifulSoup(combined_html, "lxml")
        combined_text = combined_soup.get_text(separator=" ", strip=True)
    except Exception:
        combined_text = ""

    if not combined_text.strip():
        return None

    return {
        "url":         url,
        "html":        combined_html,
        "text":        combined_text,
        "title":       title,
        "rendered":    rendered,
        "found_emails": list(all_emails),  # Return all emails found
    }
