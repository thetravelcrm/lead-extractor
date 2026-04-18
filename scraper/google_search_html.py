"""
scraper/google_search_html.py
-----------------------------
Scrape Google Search HTML results (not Maps) to discover business names + websites.
Much less aggressive bot detection than Google Maps on datacenter IPs.

Returns List[Dict] with keys: name, category, website_url, phone, address, city, country, source
"""

import re
import random
from typing import Callable, List, Dict
from urllib.parse import quote_plus, urlparse

import requests
from bs4 import BeautifulSoup

from config.settings import USER_AGENTS


_JUNK_DOMAINS = {
    "google.com", "google.co.in", "google.co.uk", "youtube.com", "wikipedia.org",
    "facebook.com", "instagram.com", "linkedin.com", "twitter.com", "x.com",
    "amazon.com", "amazon.in", "flipkart.com", "snapdeal.com",
    "justdial.com", "indiamart.com", "sulekha.com", "tradeindia.com",
    "yellowpages.in", "quora.com", "reddit.com", "maps.google.com",
    "yelp.com", "tripadvisor.com", "zomato.com", "swiggy.com",
}


def _is_valid_business_url(url: str) -> bool:
    """Return True if url looks like a real company website (not a directory or social)."""
    try:
        netloc = urlparse(url).netloc.lower().removeprefix("www.")
        if not netloc:
            return False
        for junk in _JUNK_DOMAINS:
            if netloc == junk or netloc.endswith("." + junk):
                return False
        return True
    except Exception:
        return False


def search_google_html(
    business_type: str,
    city: str,
    country: str,
    max_results: int,
    emit_fn: Callable,
) -> List[Dict]:
    """
    Scrape Google Search HTML results for business listings.
    Synchronous (uses requests + BeautifulSoup).
    Returns up to max_results business dicts.
    """
    query = f"{business_type} in {city} {country}"
    url = f"https://www.google.com/search?q={quote_plus(query)}&num=20&hl=en"

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.google.com/",
        "DNT": "1",
    }

    emit_fn("info", f"Google Search HTML: searching '{query}'")
    results: List[Dict] = []
    seen_names: set = set()

    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            emit_fn("warn", f"Google Search HTML: HTTP {resp.status_code}")
            return results

        soup = BeautifulSoup(resp.text, "html.parser")

        # Multiple selector strategies for resilience across Google's changing HTML
        result_blocks = (
            soup.select("div.g")
            or soup.select("div[data-sokoban-container]")
            or soup.select("div.tF2Cxc")
            or soup.select("div.N54PNb")
        )

        emit_fn("info", f"Google Search HTML: found {len(result_blocks)} raw result blocks")

        for block in result_blocks:
            if len(results) >= max_results:
                break

            title_el = block.select_one("h3")
            if not title_el:
                continue
            name = title_el.get_text(strip=True)
            if not name or len(name) < 3 or name.lower() in seen_names:
                continue

            skip_keywords = [
                "wikipedia", "quora", "reddit", "youtube", "how to", "what is",
                "definition", "meaning", "list of", "top 10", "best ",
            ]
            if any(kw in name.lower() for kw in skip_keywords):
                continue

            link_el = block.select_one("a[href]")
            href = link_el.get("href", "") if link_el else ""
            if href.startswith("/url?q="):
                href = href[7:].split("&")[0]
            if not href.startswith("http"):
                continue

            website_url = href if _is_valid_business_url(href) else ""

            snippet_el = block.select_one("div.VwiC3b") or block.select_one("span.aCOpRe")
            snippet = snippet_el.get_text(" ", strip=True) if snippet_el else ""

            phone_match = re.search(r'[\+\d][\d\s\-\(\)]{8,15}\d', snippet)
            phone = phone_match.group(0).strip() if phone_match else ""

            seen_names.add(name.lower())
            results.append({
                "name": name,
                "category": business_type,
                "website_url": website_url,
                "phone": phone,
                "address": "",
                "city": city,
                "country": country,
                "source": "google_search_html",
            })

        emit_fn("info", f"Google Search HTML: extracted {len(results)} business listings")

    except Exception as exc:
        emit_fn("warn", f"Google Search HTML error: {exc}")

    return results
