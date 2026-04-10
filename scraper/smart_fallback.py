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
from typing import Dict, List, Optional, Callable
from urllib.parse import quote_plus, unquote

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

        # Try multiple sources in parallel
        sources = [
            ("google", self._search_google),
            ("justdial", self._search_justdial),
            ("indiamart", self._search_indiamart),
            ("facebook", self._search_facebook),
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
        """Search Google for company contact info."""
        result = {"emails": [], "phones": [], "website": ""}

        search_queries = [
            f"{company_name} {location} email contact phone",
            f"{company_name} {location} official website contact",
            f'"{company_name}" contact email',
        ]

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

            for query in search_queries:
                if result["emails"] and result["website"]:
                    break

                try:
                    encoded_query = quote_plus(query)
                    google_url = f"https://www.google.com/search?q={encoded_query}"

                    await page.goto(google_url, wait_until="domcontentloaded", timeout=15000)
                    await page.wait_for_timeout(2000)

                    # Extract emails from search results
                    page_text = await page.inner_text("body")
                    page_text = unquote(page_text)

                    # Find emails
                    email_pattern = re.compile(r'[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,7}')
                    emails = email_pattern.findall(page_text)
                    for email in emails:
                        email = email.lower().strip()
                        if email not in result["emails"] and self._is_valid_email(email):
                            result["emails"].append(email)

                    # Find website from first search result
                    if not result["website"]:
                        try:
                            links = await page.locator('div#search a[href*="http"]').all()
                            for link in links:
                                href = await link.get_attribute("href")
                                if href and "google.com" not in href and "google.co.in" not in href:
                                    clean_url = href.split("?")[0].split("&")[0]
                                    if len(clean_url) > 10:
                                        result["website"] = clean_url
                                        break
                        except:
                            pass

                except Exception:
                    continue

            await browser.close()

        return result if result["emails"] or result["website"] else None

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
        """Search Facebook for company contact info."""
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

                # Search Facebook via Google
                search_query = f'site:facebook.com "{company_name}" {location} contact'
                encoded_query = quote_plus(search_query)
                google_url = f"https://www.google.com/search?q={encoded_query}"

                await page.goto(google_url, wait_until="domcontentloaded", timeout=15000)
                await page.wait_for_timeout(2000)

                page_text = await page.inner_text("body")
                page_text = unquote(page_text)

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

        return result if result["emails"] else None

    def _is_valid_email(self, email: str) -> bool:
        """Basic email validation."""
        return bool(re.match(r'^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,7}$', email))
