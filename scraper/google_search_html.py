"""
scraper/google_search_html.py
-----------------------------
Scrape search engine HTML results to discover business names + websites.
Tries DuckDuckGo first (minimal bot detection on cloud IPs), then Bing.
Google Search HTML is unreliable on datacenter IPs (serves CAPTCHA pages).

Returns List[Dict] with keys: name, category, website_url, phone, address, city, country, source
"""

import re
import random
from typing import Callable, List, Dict
from urllib.parse import quote_plus, urlparse, unquote

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
    "duckduckgo.com", "bing.com", "yahoo.com",
}

_SKIP_TITLE_KEYWORDS = [
    "wikipedia", "quora", "reddit", "youtube", "how to", "what is",
    "definition", "meaning", "list of", "top 10",
]


def _is_valid_business_url(url: str) -> bool:
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


def _extract_phone(text: str) -> str:
    m = re.search(r'[\+\d][\d\s\-\(\)]{8,15}\d', text)
    return m.group(0).strip() if m else ""


def _build_result(name: str, href: str, snippet: str,
                  business_type: str, city: str, country: str) -> Dict:
    website_url = href if _is_valid_business_url(href) else ""
    return {
        "name": name,
        "category": business_type,
        "website_url": website_url,
        "phone": _extract_phone(snippet),
        "address": "",
        "city": city,
        "country": country,
        "source": "search_html",
    }


def _search_duckduckgo(query: str, max_results: int,
                       emit_fn: Callable, business_type: str,
                       city: str, country: str) -> List[Dict]:
    """Scrape DuckDuckGo HTML endpoint — works reliably on cloud/datacenter IPs."""
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://duckduckgo.com/",
    }

    emit_fn("info", f"DuckDuckGo: searching '{query}'")
    results: List[Dict] = []
    seen: set = set()

    try:
        resp = requests.post(url, headers=headers, timeout=15,
                             data={"q": query, "b": "", "kl": "us-en"})
        if resp.status_code != 200:
            emit_fn("warn", f"DuckDuckGo: HTTP {resp.status_code}")
            return results

        soup = BeautifulSoup(resp.text, "html.parser")

        # DuckDuckGo HTML result structure: div.result > a.result__a (title+href)
        result_links = soup.select("a.result__a")
        emit_fn("info", f"DuckDuckGo: found {len(result_links)} raw result links")

        for link in result_links:
            if len(results) >= max_results:
                break

            name = link.get_text(strip=True)
            if not name or len(name) < 3 or name.lower() in seen:
                continue
            if any(kw in name.lower() for kw in _SKIP_TITLE_KEYWORDS):
                continue

            # DuckDuckGo wraps href in redirect: //duckduckgo.com/l/?uddg=<encoded_url>
            href = link.get("href", "")
            if "uddg=" in href:
                try:
                    href = unquote(href.split("uddg=")[1].split("&")[0])
                except Exception:
                    pass
            if not href.startswith("http"):
                continue

            # Snippet text for phone extraction
            parent = link.find_parent("div", class_="result")
            snippet = ""
            if parent:
                snippet_el = parent.select_one("a.result__snippet")
                snippet = snippet_el.get_text(" ", strip=True) if snippet_el else ""

            seen.add(name.lower())
            results.append(_build_result(name, href, snippet, business_type, city, country))

        emit_fn("info", f"DuckDuckGo: extracted {len(results)} listings")

    except Exception as exc:
        emit_fn("warn", f"DuckDuckGo error: {exc}")

    return results


def _search_bing(query: str, max_results: int,
                 emit_fn: Callable, business_type: str,
                 city: str, country: str) -> List[Dict]:
    """Scrape Bing HTML results — second option when DuckDuckGo fails."""
    url = f"https://www.bing.com/search?q={quote_plus(query)}&count=20"
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    emit_fn("info", f"Bing Search: searching '{query}'")
    results: List[Dict] = []
    seen: set = set()

    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            emit_fn("warn", f"Bing Search: HTTP {resp.status_code}")
            return results

        soup = BeautifulSoup(resp.text, "html.parser")

        # Bing organic results: li.b_algo > h2 > a
        result_items = soup.select("li.b_algo")
        emit_fn("info", f"Bing Search: found {len(result_items)} raw result items")

        for item in result_items:
            if len(results) >= max_results:
                break

            title_el = item.select_one("h2 a") or item.select_one("h3 a")
            if not title_el:
                continue
            name = title_el.get_text(strip=True)
            if not name or len(name) < 3 or name.lower() in seen:
                continue
            if any(kw in name.lower() for kw in _SKIP_TITLE_KEYWORDS):
                continue

            href = title_el.get("href", "")
            if not href.startswith("http"):
                continue

            snippet_el = item.select_one("p") or item.select_one("div.b_caption p")
            snippet = snippet_el.get_text(" ", strip=True) if snippet_el else ""

            seen.add(name.lower())
            results.append(_build_result(name, href, snippet, business_type, city, country))

        emit_fn("info", f"Bing Search: extracted {len(results)} listings")

    except Exception as exc:
        emit_fn("warn", f"Bing Search error: {exc}")

    return results


def search_google_html(
    business_type: str,
    city: str,
    country: str,
    max_results: int,
    emit_fn: Callable,
) -> List[Dict]:
    """
    Search for business listings using DuckDuckGo → Bing fallback.
    Both work much more reliably than Google on datacenter IPs.
    """
    query = f"{business_type} in {city} {country}"

    results = _search_duckduckgo(query, max_results, emit_fn, business_type, city, country)

    if not results:
        emit_fn("warn", "DuckDuckGo returned 0 — trying Bing Search...")
        results = _search_bing(query, max_results, emit_fn, business_type, city, country)

    return results
