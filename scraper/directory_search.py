"""
scraper/directory_search.py
---------------------------
Scrape Indian business directories for company listings.
Sources: Justdial, IndiaMART, Sulekha

All return List[Dict] with keys:
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
    Uses updated selectors for Justdial's React-based layout (2024+).
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

        # 2024 layout: div.resultbox is the card container
        cards = (
            soup.select("div.resultbox")
            or soup.select("div.resultbox_info")
            or soup.select("li.cntanr")
            or soup.select("article")
        )

        emit_fn("info", f"Justdial: found {len(cards)} raw cards")

        for card in cards:
            if len(results) >= max_results:
                break

            # 2024: name is in h2.resultbox_title or span.resultbox_title_anc
            name_el = (
                card.select_one("span.resultbox_title_anc")
                or card.select_one("h2.resultbox_title")
                or card.select_one("a.resultbox_title_anc")
                or card.select_one("span.lng_78")
                or card.select_one("h2")
            )
            name = name_el.get_text(strip=True) if name_el else ""
            if not name or len(name) < 3 or name.lower() in seen:
                continue
            # Skip generic category headers
            if name.lower().startswith("showing results"):
                continue

            # Phone: Justdial hides real numbers behind JS; try data attrs first
            phone = ""
            for attr_sel in ["[data-phone]", "[data-mobile]"]:
                ph_el = card.select_one(attr_sel)
                if ph_el:
                    phone = ph_el.get("data-phone") or ph_el.get("data-mobile") or ""
                    if phone:
                        break
            if not phone:
                for sel in ["p.contact-info", "span.mobilesv", "p.contact_info",
                            "div.resultbox_contact"]:
                    ph_el = card.select_one(sel)
                    if ph_el:
                        m = re.search(r'[\d\+][\d\s\-]{8,14}\d', ph_el.get_text())
                        phone = m.group(0).strip() if m else ""
                        if phone:
                            break

            # Address
            addr_el = (
                card.select_one("ul.resultbox_address")
                or card.select_one("p.address-info")
                or card.select_one("span[class*='address']")
            )
            address = addr_el.get_text(strip=True) if addr_el else ""

            # Website (skip justdial internal links)
            website_url = ""
            for a in card.select("a[href]"):
                href = a.get("href", "")
                parsed = urlparse(href)
                if parsed.scheme in ("http", "https") and "justdial" not in parsed.netloc:
                    website_url = href
                    break

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
    Scrape IndiaMART directory for business listings.
    IndiaMART embeds company data as JSON in page source (2024+ layout).
    Only used when country is India.
    """
    if "india" not in country.lower():
        return []

    query = f"{business_type} {city}"
    url = f"https://dir.indiamart.com/search.mp?ss={quote_plus(query)}"

    emit_fn("info", f"IndiaMART: scraping '{query}'")
    results: List[Dict] = []
    seen: set = set()

    try:
        resp = _get(url)
        if resp.status_code != 200:
            emit_fn("warn", f"IndiaMART: HTTP {resp.status_code}")
            return results

        text = resp.text

        # Extract all company name occurrences and paired pns (phone) fields
        # Pattern: "companyname":"Acme Corp"  ...nearby...  "pns":"9876543210"
        # Use finditer to get positions and pair them
        name_matches = list(re.finditer(r'"companyname"\s*:\s*"([^"]+)"', text))
        emit_fn("info", f"IndiaMART: found {len(name_matches)} company records in JSON")

        for m in name_matches:
            if len(results) >= max_results:
                break

            name = m.group(1).strip()
            if not name or name.lower() in seen or len(name) < 3:
                continue
            # Skip obviously generic/junk entries
            if name.lower() in {"trustseal", "verified", "indiamart"}:
                continue

            # Look for phone (pns field) within 500 chars after this name occurrence
            snippet = text[m.start(): m.start() + 600]
            phone = ""
            pns_m = re.search(r'"pns"\s*:\s*"([^"]+)"', snippet)
            if pns_m:
                phone = pns_m.group(1).strip()

            # City
            city_m = re.search(r'"city"\s*:\s*"([^"]+)"', snippet)
            city_val = city_m.group(1) if city_m else city

            # Address
            addr_m = re.search(r'"address"\s*:\s*"([^"]*)"', snippet)
            address = addr_m.group(1).strip() if addr_m else ""

            seen.add(name.lower())
            results.append({
                "name": name,
                "category": business_type,
                "website_url": "",
                "phone": phone,
                "address": address,
                "city": city_val or city,
                "country": country,
                "source": "indiamart",
            })

        emit_fn("info", f"IndiaMART: extracted {len(results)} listings")

    except Exception as exc:
        emit_fn("warn", f"IndiaMART error: {exc}")

    return results


def search_sulekha(
    business_type: str,
    city: str,
    country: str,
    max_results: int,
    emit_fn: Callable,
) -> List[Dict]:
    """
    Scrape Sulekha.com for Indian business listings.
    Only used when country is India.
    """
    if "india" not in country.lower():
        return []

    bt_slug = business_type.strip().lower().replace(" ", "-")
    city_slug = city.strip().lower().replace(" ", "-")
    url = f"https://www.sulekha.com/{bt_slug}/{city_slug}"

    emit_fn("info", f"Sulekha: scraping '{business_type}' in '{city}'")
    results: List[Dict] = []
    seen: set = set()

    try:
        resp = _get(url)
        if resp.status_code != 200:
            emit_fn("warn", f"Sulekha: HTTP {resp.status_code}")
            return results

        soup = BeautifulSoup(resp.text, "html.parser")

        cards = (
            soup.select("div.inlistingcard")
            or soup.select("li.listing-card")
            or soup.select("div[class*='listing']")
            or soup.select("div.biz-listing")
        )

        emit_fn("info", f"Sulekha: found {len(cards)} raw cards")

        for card in cards:
            if len(results) >= max_results:
                break

            name_el = (
                card.select_one("h2.name")
                or card.select_one("h3.name")
                or card.select_one("a.companyname")
                or card.select_one("span.name")
                or card.select_one("h2")
                or card.select_one("h3")
            )
            name = name_el.get_text(strip=True) if name_el else ""
            if not name or name.lower() in seen:
                continue

            phone = ""
            ph_el = (
                card.select_one("span.mobilenumber")
                or card.select_one("span[class*='phone']")
                or card.select_one("a[href^='tel:']")
            )
            if ph_el:
                href = ph_el.get("href", "")
                if href.startswith("tel:"):
                    phone = href.replace("tel:", "").strip()
                else:
                    m = re.search(r'[\d\+][\d\s\-]{8,14}\d', ph_el.get_text())
                    phone = m.group(0).strip() if m else ""

            website_url = ""
            for a in card.select("a[href]"):
                href = a.get("href", "")
                parsed = urlparse(href)
                if (parsed.scheme in ("http", "https") and
                        "sulekha.com" not in parsed.netloc):
                    website_url = href
                    break

            addr_el = (
                card.select_one("span.address")
                or card.select_one("p.address")
                or card.select_one("div[class*='address']")
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
                "source": "sulekha",
            })

        emit_fn("info", f"Sulekha: extracted {len(results)} listings")

    except Exception as exc:
        emit_fn("warn", f"Sulekha error: {exc}")

    return results


def search_yello_ae(
    business_type: str,
    city: str,
    country: str,
    max_results: int,
    emit_fn: Callable,
) -> List[Dict]:
    """
    Scrape Yello.ae (UAE Yellow Pages) for business listings.
    Only used when country is UAE / United Arab Emirates.
    Paginates through /category/{slug}/{page}/city:{city} until max_results reached.
    """
    uae_names = {"uae", "united arab emirates", "emirates", "dubai", "abu dhabi",
                 "sharjah", "ajman", "fujairah", "ras al khaimah", "umm al quwain"}
    if not any(n in country.lower() or n in city.lower() for n in uae_names):
        return []

    city_slug = city.strip().lower().replace(" ", "-") or "dubai"
    # Map common business type names to Yello.ae category slugs
    _SLUG_MAP = {
        "travel agency": "travel-agents",
        "travel agencies": "travel-agents",
        "hotel": "hotels",
        "restaurant": "restaurants",
        "real estate": "real-estate-agents",
    }
    bt_lower = business_type.strip().lower()
    bt_slug = _SLUG_MAP.get(bt_lower, bt_lower.replace(" ", "-"))

    emit_fn("info", f"Yello.ae: scraping '{business_type}' in '{city}' (slug={bt_slug})")
    results: List[Dict] = []
    seen: set = set()

    # Page 1 URL: /category/{slug}/city:{city}
    # Page N URL: /category/{slug}/{N}/city:{city}
    page = 1
    max_pages = max(1, max_results // 20) + 2  # ~21 cards per page

    while len(results) < max_results and page <= max_pages:
        if page == 1:
            url = f"https://www.yello.ae/category/{bt_slug}/city:{city_slug}"
        else:
            url = f"https://www.yello.ae/category/{bt_slug}/{page}/city:{city_slug}"

        try:
            resp = _get(url)
            if resp.status_code != 200:
                emit_fn("warn", f"Yello.ae page {page}: HTTP {resp.status_code}")
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            cards = soup.select("div.company")

            if not cards:
                emit_fn("info", f"Yello.ae: no cards on page {page}, stopping")
                break

            emit_fn("info", f"Yello.ae page {page}: {len(cards)} cards")

            for card in cards:
                if len(results) >= max_results:
                    break

                name_el = card.select_one("h3 a") or card.select_one("h2 a")
                name = name_el.get_text(strip=True) if name_el else ""
                if not name or name.lower() in seen:
                    continue

                # Phone: look for div.s containing phone icon
                phone = ""
                for s_div in card.select("div.s"):
                    icon = s_div.select_one("i[aria-label]")
                    if icon and "phone" in icon.get("aria-label", "").lower():
                        span = s_div.select_one("span")
                        if span:
                            phone = span.get_text(strip=True)
                        break
                # Fallback: tel: link
                if not phone:
                    tel = card.select_one("a[href^='tel:']")
                    if tel:
                        phone = tel.get("href", "").replace("tel:", "").strip()

                # Address
                addr_el = card.select_one("div.address")
                address = addr_el.get_text(strip=True) if addr_el else ""
                # Strip "Address: " prefix
                if address.startswith("Address:"):
                    address = address[8:].strip()

                # Company page URL on Yello.ae (we use it as website_url fallback key;
                # the pipeline's SmartFallback will search for the real site)
                company_href = name_el.get("href", "") if name_el else ""
                website_url = ""  # Real website requires visiting company page

                seen.add(name.lower())
                results.append({
                    "name": name,
                    "category": business_type,
                    "website_url": website_url,
                    "phone": phone,
                    "address": address,
                    "city": city,
                    "country": country,
                    "source": "yello_ae",
                })

            # Check if there's a next page
            next_link = soup.select_one("a[rel='next'], a.pages_arrow[href*='city:']")
            if not next_link:
                break
            page += 1
            time.sleep(0.5)

        except Exception as exc:
            emit_fn("warn", f"Yello.ae page {page} error: {exc}")
            break

    emit_fn("info", f"Yello.ae: extracted {len(results)} listings")

    return results
