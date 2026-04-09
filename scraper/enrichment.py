"""
scraper/enrichment.py
---------------------
Enrich missing company data using Google Search and other sources.

Functions:
- enrich_company(company_name, city, country, missing_fields) → enriched_data
- search_google_for_emails(company_name, city) → [emails]
- search_google_for_phones(company_name, city) → [phones]
- search_google_for_websites(company_name, city) → website_url

Strategy:
1. For missing emails: Google search "[Company] [City] email contact"
2. For missing phones: Google search "[Company] [City] phone number"
3. For missing websites: Google search "[Company] [City] official website"
4. Extract data from search results and visited pages
"""

import asyncio
import re
from typing import Dict, List, Callable, Optional
from urllib.parse import quote_plus


async def enrich_company(
    company_name: str,
    city: str = "",
    country: str = "",
    missing_fields: List[str] = None,
    emit_fn: Callable = None,
) -> Dict:
    """
    Enrich a company with missing data from multiple sources.

    Args:
        company_name: Company name
        city: City name
        country: Country name
        missing_fields: List of missing fields ['email', 'phone', 'website']
        emit_fn: Callback for logging

    Returns:
        {
            "emails": [],
            "phones": [],
            "website": "",
            "sources": {"email": "google", "phone": "maps", ...}
        }
    """
    if emit_fn is None:
        emit_fn = lambda level, msg: None

    if missing_fields is None:
        missing_fields = ["email", "phone", "website"]

    result = {
        "emails": [],
        "phones": [],
        "website": "",
        "sources": {}
    }

    location = f"{city}, {country}" if city else country
    search_base = f"{company_name} {location}".strip()

    emit_fn("info", f"  🔍 Enriching: {company_name} (missing: {', '.join(missing_fields)})")

    # Search for missing emails
    if "email" in missing_fields:
        emails = await search_google_for_emails(company_name, location, emit_fn)
        if emails:
            result["emails"] = emails
            result["sources"]["email"] = "google"

    # Search for missing phones
    if "phone" in missing_fields:
        phones = await search_google_for_phones(company_name, location, emit_fn)
        if phones:
            result["phones"] = phones
            result["sources"]["phone"] = "google"

    # Search for missing websites
    if "website" in missing_fields:
        website = await search_google_for_websites(company_name, location, emit_fn)
        if website:
            result["website"] = website
            result["sources"]["website"] = "google"

    return result


async def search_google_for_emails(
    company_name: str,
    location: str,
    emit_fn: Callable = None,
) -> List[str]:
    """Search Google for company emails."""
    if emit_fn is None:
        emit_fn = lambda level, msg: None

    emails = []

    # Multiple search queries to find emails
    search_queries = [
        f"{company_name} {location} email contact",
        f"{company_name} {location} contact email address",
        f"{company_name} {location} official email",
    ]

    from playwright.async_api import async_playwright

    for query in search_queries:
        if emails:  # Stop if we found emails
            break

        encoded_query = quote_plus(query)
        google_url = f"https://www.google.com/search?q={encoded_query}"

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

                await page.goto(google_url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(2000)

                # Extract emails from search result snippets
                page_text = await page.locator('body').inner_text(timeout=5000)
                email_pattern = re.compile(r'[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,7}')
                found_emails = email_pattern.findall(page_text)

                # Filter generic emails
                generic_prefixes = {'info', 'contact', 'admin', 'support', 'noreply', 'no-reply', 'sales', 'help'}
                for email in found_emails:
                    prefix = email.split('@')[0].lower()
                    if prefix not in generic_prefixes and email not in emails:
                        emails.append(email)

                if emails:
                    emit_fn("info", f"  📧 Found {len(emails)} email(s): {', '.join(emails[:2])}")

                await browser.close()
        except Exception as exc:
            emit_fn("warn", f"  Email search failed: {str(exc)[:80]}")

    return emails[:5]  # Limit to 5 emails


async def search_google_for_phones(
    company_name: str,
    location: str,
    emit_fn: Callable = None,
) -> List[str]:
    """Search Google for company phone numbers."""
    if emit_fn is None:
        emit_fn = lambda level, msg: None

    phones = []

    search_queries = [
        f"{company_name} {location} phone number contact",
        f"{company_name} {location} telephone",
        f"{company_name} {location} mobile",
    ]

    from playwright.async_api import async_playwright

    for query in search_queries:
        if phones:
            break

        encoded_query = quote_plus(query)
        google_url = f"https://www.google.com/search?q={encoded_query}"

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

                await page.goto(google_url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(2000)

                # Extract phone numbers from search results
                page_text = await page.locator('body').inner_text(timeout=5000)
                phone_pattern = re.compile(r'(\+?[\d\s\-\(\)]{8,})')
                found_phones = phone_pattern.findall(page_text)

                for phone in found_phones:
                    clean_phone = re.sub(r'[^\d+]', '', phone).strip()
                    if len(clean_phone) >= 8 and clean_phone not in phones:
                        phones.append(clean_phone)

                if phones:
                    emit_fn("info", f"  📞 Found {len(phones)} phone(s): {', '.join(phones[:2])}")

                await browser.close()
        except Exception as exc:
            emit_fn("warn", f"  Phone search failed: {str(exc)[:80]}")

    return phones[:3]  # Limit to 3 phones


async def search_google_for_websites(
    company_name: str,
    location: str,
    emit_fn: Callable = None,
) -> str:
    """Search Google for company website URL."""
    if emit_fn is None:
        emit_fn = lambda level, msg: None

    website = ""

    search_queries = [
        f"{company_name} {location} official website",
        f"{company_name} {location} site",
        f"{company_name} official website",
    ]

    from playwright.async_api import async_playwright

    for query in search_queries:
        if website:
            break

        encoded_query = quote_plus(query)
        google_url = f"https://www.google.com/search?q={encoded_query}"

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

                await page.goto(google_url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(2000)

                # Find website URL from first search result
                try:
                    # Look for the first result link
                    links = await page.locator('div#search a[href*="http"]').all()
                    for link in links:
                        href = await link.get_attribute("href")
                        if href and len(href) > 10:
                            # Skip Google internal links
                            if "google.com" not in href and "google.co.in" not in href:
                                # Clean up the URL
                                clean_url = href.split("?")[0].split("&")[0]
                                if "http" in clean_url and len(clean_url) < 200:
                                    website = clean_url
                                    emit_fn("info", f"  🌐 Found website: {website}")
                                    break
                except:
                    pass

                await browser.close()
        except Exception as exc:
            emit_fn("warn", f"  Website search failed: {str(exc)[:80]}")

    return website
