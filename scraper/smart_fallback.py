"""
scraper/smart_fallback.py
--------------------------
Smart fallback system for extracting contact info when website is missing.

Features:
- Automatic Google search for missing contact info
- Scrapes multiple sources:
    - Justdial
    - IndiaMART
    - Facebook
    - LinkedIn (company pages)
    - Business directories
- Extracts emails, phones, websites from these sources
- Async Playwright-based for performance
"""

import asyncio
import re
import random
from typing import Dict, List, Optional, Callable
from urllib.parse import quote_plus, unquote

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout


class SmartFallback:
    """
    Smart fallback system for extracting contact info when website is missing.

    Searches multiple sources to find emails, phones, and websites for companies
    that don't have a website listed on Google Maps.
    """

    def __init__(self, emit_fn: Callable = None):
        self.emit_fn = emit_fn or (lambda level, msg: None)

    async def find_contact_info(self, company_name: str, city: str = "", country: str = "") -> Dict:
        """
        Find contact info for a company using multiple fallback sources.

        Args:
            company_name: Company name
            city: City name
            country: Country name

        Returns:
            {
                "emails": ["email1@domain.com"],
                "phones": ["+919876543210"],
                "website": "https://company.com",
                "sources_used": ["google", "justdial", "indiamart"],
            }
        """
        result = {
            "emails": [],
            "phones": [],
            "website": "",
            "sources_used": [],
        }

        location = f"{city}, {country}" if city else country
        search_base = f"{company_name} {location}".strip()

        self.emit_fn("info", f"  🔍 Fallback search for: {company_name}")

        is_india = "india" in country.lower()

        # DuckDuckGo search is first (works on all cloud IPs)
        # India-specific directories only run for Indian companies
        sources = [
            ("ddg", self._search_google),       # Actually DuckDuckGo now
            ("facebook", self._search_facebook),
        ]
        if is_india:
            sources += [
                ("sulekha", self._search_sulekha),
                ("justdial", self._search_justdial),
                ("indiamart", self._search_indiamart),
            ]

        for source_name, search_fn in sources:
            # Stop if we already have emails and website
            if result["emails"] and result["website"]:
                break

            try:
                source_result = await search_fn(company_name, location)
                if source_result:
                    # Merge results
                    if source_result.get("emails"):
                        for email in source_result["emails"]:
                            if email not in result["emails"]:
                                result["emails"].append(email)

                    if source_result.get("phones"):
                        for phone in source_result["phones"]:
                            if phone not in result["phones"]:
                                result["phones"].append(phone)

                    if source_result.get("website") and not result["website"]:
                        result["website"] = source_result["website"]

                    if source_result.get("emails") or source_result.get("phones") or source_result.get("website"):
                        result["sources_used"].append(source_name)

            except Exception as exc:
                self.emit_fn("warn", f"  {source_name} search failed: {str(exc)[:60]}")

        return result

    async def _search_google(self, company_name: str, location: str) -> Optional[Dict]:
        """
        Search DuckDuckGo (HTML endpoint) for company contact info.
        Uses requests — no Playwright — so it works on cloud/datacenter IPs
        where Google blocks headless browsers.
        """
        result = {"emails": [], "phones": [], "website": ""}

        _JUNK = {"google.com", "facebook.com", "instagram.com", "linkedin.com",
                 "youtube.com", "justdial.com", "indiamart.com", "sulekha.com",
                 "tripadvisor.com", "yelp.com", "wikipedia.org", "yello.ae",
                 "twitter.com", "x.com"}

        from config.settings import USER_AGENTS

        search_queries = [
            f"{company_name} {location} email contact",
            f"{company_name} {location} official website",
        ]

        for query in search_queries:
            if result["emails"] and result["website"]:
                break
            try:
                url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
                headers = {
                    "User-Agent": random.choice(USER_AGENTS),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                }
                resp = requests.post(url, headers=headers, timeout=12,
                                     data={"q": query, "b": "", "kl": "us-en"})
                if resp.status_code != 200:
                    continue

                soup = BeautifulSoup(resp.text, "html.parser")
                page_text = soup.get_text(" ", strip=True)
                page_text = unquote(page_text)

                # Extract emails
                email_pattern = re.compile(r'[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,7}')
                for email in email_pattern.findall(page_text):
                    email = email.lower().strip()
                    if email not in result["emails"] and self._is_valid_email(email):
                        result["emails"].append(email)

                # Extract website from first non-junk result link
                if not result["website"]:
                    for link in soup.select("a.result__a"):
                        href = link.get("href", "")
                        if "uddg=" in href:
                            try:
                                href = unquote(href.split("uddg=")[1].split("&")[0])
                            except Exception:
                                pass
                        if href.startswith("http"):
                            from urllib.parse import urlparse
                            netloc = urlparse(href).netloc.lower().removeprefix("www.")
                            if not any(j in netloc for j in _JUNK):
                                result["website"] = href.split("?")[0]
                                break

            except Exception:
                continue

        return result if (result["emails"] or result["website"]) else None

    async def _search_justdial(self, company_name: str, location: str) -> Optional[Dict]:
        """Search Justdial for company contact info."""
        result = {"emails": [], "phones": [], "website": ""}

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-setuid-sandbox"]
                )
                context = await browser.new_context(
                    viewport={"width": 1400, "height": 900},
                    locale="en-US",
                )
                page = await context.new_page()

                # Search Justdial
                city_part = location.split(',')[0].strip() if ',' in location else location
                search_query = f"{company_name} {city_part}"
                encoded_query = quote_plus(search_query)
                jd_url = f"https://www.justdial.com/{city_part}/{encoded_query.replace(' ', '-')}"

                await page.goto(jd_url, wait_until="domcontentloaded", timeout=15000)
                await page.wait_for_timeout(3000)

                # Extract phone numbers
                page_text = await page.inner_text("body")
                phone_pattern = re.compile(r'(\+?[\d\s\-\(\)]{8,})')
                phones = phone_pattern.findall(page_text)

                for phone in phones:
                    cleaned = re.sub(r'[^\d+]', '', phone).strip()
                    if len(cleaned) >= 8 and len(cleaned) <= 15:
                        if not cleaned.startswith('+'):
                            cleaned = '+91' + cleaned if len(cleaned) == 10 else '+' + cleaned
                        if cleaned not in result["phones"]:
                            result["phones"].append(cleaned)

                # Extract emails
                email_pattern = re.compile(r'[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,7}')
                emails = email_pattern.findall(page_text)
                for email in emails:
                    email = email.lower().strip()
                    if email not in result["emails"] and self._is_valid_email(email):
                        result["emails"].append(email)

                await browser.close()

        except Exception:
            pass

        return result if result["emails"] or result["phones"] else None

    async def _search_indiamart(self, company_name: str, location: str) -> Optional[Dict]:
        """Search IndiaMART for company contact info."""
        result = {"emails": [], "phones": [], "website": ""}

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-setuid-sandbox"]
                )
                context = await browser.new_context(
                    viewport={"width": 1400, "height": 900},
                    locale="en-US",
                )
                page = await context.new_page()

                # Search IndiaMART
                search_query = f"{company_name} {location}"
                encoded_query = quote_plus(search_query)
                im_url = f"https://dir.indiamart.com/search.mp?word={encoded_query}"

                await page.goto(im_url, wait_until="domcontentloaded", timeout=15000)
                await page.wait_for_timeout(3000)

                # Extract contact info
                page_text = await page.inner_text("body")

                # Find phones
                phone_pattern = re.compile(r'(\+?[\d\s\-\(\)]{8,})')
                phones = phone_pattern.findall(page_text)
                for phone in phones:
                    cleaned = re.sub(r'[^\d+]', '', phone).strip()
                    if len(cleaned) >= 8:
                        if not cleaned.startswith('+'):
                            cleaned = '+91' + cleaned if len(cleaned) == 10 else '+' + cleaned
                        if cleaned not in result["phones"]:
                            result["phones"].append(cleaned)

                # Find emails
                email_pattern = re.compile(r'[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,7}')
                emails = email_pattern.findall(page_text)
                for email in emails:
                    email = email.lower().strip()
                    if email not in result["emails"] and self._is_valid_email(email):
                        result["emails"].append(email)

                await browser.close()

        except Exception:
            pass

        return result if result["emails"] or result["phones"] else None

    async def _search_facebook(self, company_name: str, location: str) -> Optional[Dict]:
        """
        Find Facebook business page via DuckDuckGo, then scrape it directly.
        Public Facebook business pages show email/phone in the Intro section without login.
        """
        result = {"emails": [], "phones": [], "website": ""}

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-gpu"]
                )
                context = await browser.new_context(
                    viewport={"width": 1400, "height": 900},
                    locale="en-US",
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                )
                page = await context.new_page()

                # Step 1: Find Facebook page URL via DuckDuckGo
                search_query = f"{company_name} {location} facebook"
                ddg_url = f"https://html.duckduckgo.com/html/?q={quote_plus(search_query)}"

                fb_url = ""
                try:
                    await page.goto(ddg_url, wait_until="domcontentloaded", timeout=15000)
                    await page.wait_for_timeout(1500)

                    links = await page.locator("a.result__a").all()
                    for link in links:
                        href = await link.get_attribute("href") or ""
                        # Decode DDG redirect
                        if "uddg=" in href:
                            try:
                                href = unquote(href.split("uddg=")[1].split("&")[0])
                            except Exception:
                                pass
                        if "facebook.com" in href and "/p/" not in href:
                            # Accept pages/ or people/ or direct business URLs
                            if any(p in href for p in ["/pages/", "/people/", "facebook.com/"]):
                                fb_url = href.split("?")[0]
                                break
                except Exception:
                    pass

                if not fb_url:
                    await browser.close()
                    return None

                self.emit_fn("info", f"  📘 Facebook: visiting {fb_url[:70]}")

                # Step 2: Visit the actual Facebook page
                try:
                    await page.goto(fb_url, wait_until="domcontentloaded", timeout=20000)
                    await page.wait_for_timeout(3000)

                    # Dismiss any login prompts by scrolling / clicking close
                    for close_sel in ['div[aria-label="Close"]', 'div[role="button"][tabindex="0"]']:
                        try:
                            close_btn = page.locator(close_sel).first
                            if await close_btn.is_visible(timeout=1500):
                                await close_btn.click()
                                await page.wait_for_timeout(800)
                                break
                        except Exception:
                            pass

                    page_text = await page.inner_text("body")

                    # Extract emails
                    email_pattern = re.compile(r'[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,7}')
                    for email in email_pattern.findall(page_text):
                        email = email.lower().strip()
                        if email not in result["emails"] and self._is_valid_email(email):
                            result["emails"].append(email)

                    # Extract phone via tel: links first, then regex
                    tel_links = await page.locator("a[href^='tel:']").all()
                    for tl in tel_links:
                        href = await tl.get_attribute("href") or ""
                        phone = re.sub(r"[^\d+]", "", href.replace("tel:", ""))
                        if len(phone) >= 8 and phone not in result["phones"]:
                            result["phones"].append(phone)

                    if not result["phones"]:
                        phone_pattern = re.compile(r'(\+?[\d][\d\s\-\(\)]{7,14}\d)')
                        for ph in phone_pattern.findall(page_text):
                            cleaned = re.sub(r"[^\d+]", "", ph)
                            if len(cleaned) >= 8 and cleaned not in result["phones"]:
                                result["phones"].append(cleaned)

                    # Extract external website
                    ext_links = await page.locator("a[href^='http']").all()
                    for link in ext_links:
                        href = await link.get_attribute("href") or ""
                        if "facebook.com" not in href and "instagram.com" not in href:
                            result["website"] = href.split("?")[0]
                            break

                    if result["emails"] or result["phones"]:
                        self.emit_fn("info", f"  📘 Facebook: found {len(result['emails'])} email(s), {len(result['phones'])} phone(s)")

                except Exception as exc:
                    self.emit_fn("warn", f"  Facebook page visit failed: {str(exc)[:80]}")

                await browser.close()

        except Exception as exc:
            self.emit_fn("warn", f"  Facebook search failed: {str(exc)[:80]}")

        return result if (result["emails"] or result["phones"]) else None

    async def _search_sulekha(self, company_name: str, location: str) -> Optional[Dict]:
        """Search Sulekha.com for company contact info."""
        result = {"emails": [], "phones": [], "website": ""}

        try:
            import requests
            from bs4 import BeautifulSoup
            import random
            from config.settings import USER_AGENTS

            city = location.split(",")[0].strip()
            query = quote_plus(f"{company_name} {city}")
            url = f"https://www.sulekha.com/search/result/?searchkey={query}&src=search"
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
            resp = requests.get(url, headers=headers, timeout=12)
            if resp.status_code != 200:
                return None

            soup = BeautifulSoup(resp.text, "html.parser")
            page_text = soup.get_text(" ", strip=True)

            email_pattern = re.compile(r'[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,7}')
            for email in email_pattern.findall(page_text):
                email = email.lower().strip()
                if email not in result["emails"] and self._is_valid_email(email):
                    result["emails"].append(email)

            phone_pattern = re.compile(r'(\+?[\d][\d\s\-]{7,14}\d)')
            for ph in phone_pattern.findall(page_text):
                cleaned = re.sub(r"[^\d+]", "", ph)
                if len(cleaned) >= 8 and cleaned not in result["phones"]:
                    result["phones"].append(cleaned)

        except Exception:
            pass

        return result if (result["emails"] or result["phones"]) else None

    def _is_valid_email(self, email: str) -> bool:
        """Basic email validation."""
        return bool(re.match(r'^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,7}$', email))
