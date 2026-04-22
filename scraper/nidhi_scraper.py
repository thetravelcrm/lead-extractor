"""
scraper/nidhi_scraper.py
------------------------
Async scraper for India's NIDHI tourism portal.
Extracts all registered businesses from:
  - categoryCode=04 (Online Travel Aggregator)
  - categoryCode=02 (Tourism Service Provider)

Returns List[Dict] with keys: name, email, whatsapp, website, location, category
"""

import asyncio
import re
from typing import Callable, List, Dict

import aiohttp
from bs4 import BeautifulSoup

_BASE = "https://nidhi.tourism.gov.in"
_DIR_URL = _BASE + "/home/directory"

_CATEGORY_LABELS = {
    "04": "Online Travel Aggregator",
    "02": "Tourism Service Provider",
}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://nidhi.tourism.gov.in/",
}


def _parse_page(html: str, category_code: str) -> List[Dict]:
    """Parse one page of listing-block cards."""
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("div.listing-block")
    results = []

    for card in cards:
        name_el = card.select_one("h2.hotel-heading")
        name = name_el.get_text(strip=True) if name_el else ""
        if not name:
            continue

        addr_el = card.select_one("p.address")
        location = addr_el.get_text(strip=True) if addr_el else ""

        email, whatsapp, website = "", "", ""

        for mail_div in card.select("div.mail-details"):
            img = mail_div.select_one("img")
            if not img:
                continue
            alt = img.get("alt", "").lower()
            text = mail_div.get_text(strip=True)

            if alt == "mail":
                m = re.search(r'[\w.\-+]+@[\w.\-]+\.\w{2,}', text)
                email = m.group(0) if m else ""
            elif alt == "call":
                m = re.search(r'[\d\+][\d\s\-]{7,14}\d', text)
                whatsapp = m.group(0).strip() if m else ""
            elif alt in ("website", "web", "globe"):
                a = mail_div.select_one("a[href]")
                if a:
                    website = a["href"].strip()
                else:
                    m = re.search(r'https?://\S+', text)
                    website = m.group(0).rstrip(".,)") if m else ""

        results.append({
            "name": name,
            "email": email,
            "whatsapp": whatsapp,
            "website": website,
            "location": location,
            "category": _CATEGORY_LABELS.get(category_code, category_code),
        })

    return results


def _get_total_pages(html: str) -> int:
    """Parse total record count from h3.listing-heading and compute page count."""
    soup = BeautifulSoup(html, "html.parser")
    heading = soup.select_one("h3.listing-heading")
    if heading:
        m = re.search(r'(\d[\d,]*)\s+registered', heading.get_text())
        if m:
            total = int(m.group(1).replace(",", ""))
            return max(1, (total + 39) // 40)  # ceil(total / 40)
    # Fallback: parse highest page number from pagination links
    last_page_links = soup.select("ul.pagination li a")
    if last_page_links:
        nums = [int(a.get_text(strip=True)) for a in last_page_links
                if a.get_text(strip=True).isdigit()]
        if nums:
            return max(nums)
    return 1


async def _fetch_page(session: aiohttp.ClientSession, category_code: str,
                      page: int) -> str:
    params = {"stateCode": "", "categoryCode": category_code, "pageno": page}
    async with session.get(_DIR_URL, params=params, headers=_HEADERS,
                           timeout=aiohttp.ClientTimeout(total=30)) as resp:
        resp.raise_for_status()
        return await resp.text()


async def scrape_nidhi(
    category_codes: List[str],
    emit_fn: Callable,
    concurrency: int = 15,
    cancelled_fn: Callable = None,
) -> List[Dict]:
    """
    Scrape all pages for each category_code concurrently.
    emit_fn(level, message) for SSE progress.
    cancelled_fn() returns True when the job is aborted.
    """
    all_results: List[Dict] = []
    sem = asyncio.Semaphore(concurrency)

    connector = aiohttp.TCPConnector(limit=concurrency, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:

        for cat in category_codes:
            if cancelled_fn and cancelled_fn():
                break

            emit_fn("info", f"NIDHI: fetching page 1 for categoryCode={cat}")
            try:
                page1_html = await _fetch_page(session, cat, 1)
            except Exception as exc:
                emit_fn("warn", f"NIDHI: failed to fetch page 1 for cat={cat}: {exc}")
                continue

            total_pages = _get_total_pages(page1_html)
            page1_records = _parse_page(page1_html, cat)
            all_results.extend(page1_records)
            emit_fn("info", f"NIDHI: cat={cat} has {total_pages} pages, "
                            f"page 1 → {len(page1_records)} records")

            if total_pages <= 1:
                continue

            # Fetch remaining pages concurrently under semaphore
            async def fetch_and_parse(page_num: int, cat_code: str = cat) -> List[Dict]:
                async with sem:
                    if cancelled_fn and cancelled_fn():
                        return []
                    try:
                        html = await _fetch_page(session, cat_code, page_num)
                        records = _parse_page(html, cat_code)
                        emit_fn("info",
                                f"NIDHI: cat={cat_code} page {page_num} "
                                f"→ {len(records)} records")
                        return records
                    except Exception as exc:
                        emit_fn("warn",
                                f"NIDHI: cat={cat_code} page {page_num} error: {exc}")
                        return []

            tasks = [fetch_and_parse(p) for p in range(2, total_pages + 1)]
            pages_data = await asyncio.gather(*tasks)
            for page_records in pages_data:
                all_results.extend(page_records)

    emit_fn("info", f"NIDHI: total extracted = {len(all_results)} records")
    return all_results
