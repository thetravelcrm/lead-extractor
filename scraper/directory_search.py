"""
scraper/directory_search.py
---------------------------
Scrape Indian business directories for company listings.
Sources: Justdial, IndiaMART

Both return List[Dict] with keys:
    name, category, website_url, phone, address, city, country, source
"""

import re
import random
import time
from typing import Callable, List, Dict
from urllib.parse import quote_plus, urlparse

import requests
from bs4 import BeautifulSoup

from config.settings import USER_AGENTS


def _get(url: str, timeout: int = 15) -> requests.Response:
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
    }
    return requests.get(url, headers=headers, timeout=timeout)


def search_justdial(
    business_type: str,
    city: str,
    country: str,
    max_results: int,
    emit_fn: Callable,
) -> List[Dict]:
    """
    Scrape Justdial for Indian business listings.
    Only used when country is India.
    """
    if "india" not in country.lower():
        return []

    city_slug = city.strip().replace(" ", "-").title()
    bt_slug = business_type.strip().replace(" ", "-").title()
    url = f"https://www.justdial.com/{city_slug}/{bt_slug}"

    emit_fn("info", f"Justdial: scraping {url}")
    results: List[Dict] = []
    seen: set = set()

    try:
        resp = _get(url)
        if resp.status_code != 200:
            emit_fn("warn", f"Justdial: HTTP {resp.status_code}")
            return results

        soup = BeautifulSoup(resp.text, "html.parser")

        cards = (
            soup.select("li.cntanr")
            or soup.select("div.resultbox_info")
            or soup.select("div[class*='resultbox']")
            or soup.select("article")
        )

        emit_fn("info", f"Justdial: found {len(cards)} raw cards")

        for card in cards:
            if len(results) >= max_results:
                break

            name_el = (
                card.select_one("span.lng_78")
                or card.select_one("a.ert")
                or card.select_one("span[class*='companyname']")
                or card.select_one("h2")
                or card.select_one("h3")
            )
            name = name_el.get_text(strip=True) if name_el else ""
            if not name or name.lower() in seen:
                continue

            # Phone — Justdial often stores in data attributes
            phone = ""
            phone_el = card.select_one("[data-phone]")
            if phone_el:
                phone = phone_el.get("data-phone", "")
            if not phone:
                for sel in ["p.contact-info", "span.mobilesv", "p.contact_info"]:
                    ph_el = card.select_one(sel)
                    if ph_el:
                        m = re.search(r'[\d\+][\d\s\-]{8,14}\d', ph_el.get_text())
                        phone = m.group(0).strip() if m else ""
                        break

            # Website (skip justdial internal links)
            website_url = ""
            for a in card.select("a[href]"):
                href = a.get("href", "")
                parsed = urlparse(href)
                if parsed.scheme in ("http", "https") and "justdial" not in parsed.netloc:
                    website_url = href
                    break

            addr_el = (
                card.select_one("p.address-info")
                or card.select_one("span[class*='address']")
                or card.select_one("p.address")
            )
            address = addr_el.get_text(strip=True) if addr_el else ""

            seen.add(name.lower())
            results.append({
                "name": name,
                "category": business_type,
                "website_url": website_url,
                "phone": phone,
                "address": address,
                "city": city,
                "country": country,
                "source": "justdial",
            })
            time.sleep(0.2)

        emit_fn("info", f"Justdial: extracted {len(results)} listings")

    except Exception as exc:
        emit_fn("warn", f"Justdial error: {exc}")

    return results


def search_indiamart(
    business_type: str,
    city: str,
    country: str,
    max_results: int,
    emit_fn: Callable,
) -> List[Dict]:
    """
    Scrape IndiaMART for B2B/service business listings.
    Only used when country is India.
    """
    if "india" not in country.lower():
        return []

    query = f"{business_type} in {city}"
    url = f"https://www.indiamart.com/search.mp?ss={quote_plus(query)}"

    emit_fn("info", f"IndiaMART: scraping '{query}'")
    results: List[Dict] = []
    seen: set = set()

    try:
        resp = _get(url)
        if resp.status_code != 200:
            emit_fn("warn", f"IndiaMART: HTTP {resp.status_code}")
            return results

        soup = BeautifulSoup(resp.text, "html.parser")

        cards = (
            soup.select("div.dummywrap")
            or soup.select("div[class*='bx']")
            or soup.select("div.p-card")
            or soup.select("div.organic-card")
        )

        emit_fn("info", f"IndiaMART: found {len(cards)} raw cards")

        for card in cards:
            if len(results) >= max_results:
                break

            name_el = (
                card.select_one("a.clr-blk")
                or card.select_one("h2.comp-name")
                or card.select_one("span.clr-orng")
                or card.select_one("h3")
                or card.select_one("h2")
            )
            name = name_el.get_text(strip=True) if name_el else ""
            if not name or name.lower() in seen:
                continue

            phone = ""
            ph_el = card.select_one("span[data-mobile]") or card.select_one("a[href^='tel:']")
            if ph_el:
                phone = ph_el.get("data-mobile") or ph_el.get("href", "").replace("tel:", "")

            website_url = ""
            for a in card.select("a[href]"):
                href = a.get("href", "")
                if href.startswith("http") and "indiamart" not in href:
                    website_url = href
                    break

            addr_el = card.select_one("span.add") or card.select_one("p.address")
            address = addr_el.get_text(strip=True) if addr_el else ""

            seen.add(name.lower())
            results.append({
                "name": name,
                "category": business_type,
                "website_url": website_url,
                "phone": phone,
                "address": address,
                "city": city,
                "country": country,
                "source": "indiamart",
            })

        emit_fn("info", f"IndiaMART: extracted {len(results)} listings")

    except Exception as exc:
        emit_fn("warn", f"IndiaMART error: {exc}")

    return results
