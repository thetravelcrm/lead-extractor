"""
scraper/linkedin_search.py
--------------------------
Search LinkedIn for company profiles and extract contact information.

Functions:
- search_linkedin(company_name, city) → {emails: [], phones: [], website: ""}

Strategy:
1. Search Google for: "LinkedIn [Company Name] [City]"
2. Open LinkedIn company page
3. Extract contact info (email, phone, website)
"""

import asyncio
import re
from typing import Dict, List, Callable
from urllib.parse import quote_plus


async def search_linkedin(
    company_name: str,
    city: str = "",
    emit_fn: Callable = None,
) -> Dict:
    """
    Search LinkedIn for a company and extract contact information.

    Returns:
        {
            "emails": ["email1@company.com"],
            "phones": ["+91-XXXXXXX"],
            "website": "https://company.com"
        }
    """
    if emit_fn is None:
        emit_fn = lambda level, msg: None

    result = {"emails": [], "phones": [], "website": ""}

    from playwright.async_api import async_playwright

    search_query = f"{company_name} {city} LinkedIn".strip()
    encoded_query = quote_plus(search_query)
    google_url = f"https://www.google.com/search?q={encoded_query}"

    emit_fn("info", f"  🔍 Searching LinkedIn for: {company_name}")

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

        try:
            # Step 1: Search Google for LinkedIn page
            emit_fn("info", f"  Searching Google for LinkedIn profile...")
            await page.goto(google_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)

            # Find LinkedIn URL from search results
            linkedin_url = ""
            try:
                # Look for linkedin.com links in search results
                links = await page.locator('a[href*="linkedin.com/company"]').all()
                for link in links:
                    href = await link.get_attribute("href")
                    if href and "linkedin.com/company" in href and "jobs" not in href:
                        linkedin_url = href.split("?")[0]
                        emit_fn("info", f"  Found LinkedIn: {linkedin_url[:60]}...")
                        break
            except:
                pass

            if not linkedin_url:
                emit_fn("info", "  No LinkedIn profile found")
                await browser.close()
                return result

            # Step 2: Visit LinkedIn company page
            emit_fn("info", f"  Visiting LinkedIn company page...")
            await page.goto(linkedin_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)

            # Step 3: Extract contact info
            # Extract website
            try:
                website_links = await page.locator('a[href*="http"]:not([href*="linkedin"])').all()
                for link in website_links:
                    href = await link.get_attribute("href")
                    if href and len(href) > 10 and "linkedin.com" not in href:
                        result["website"] = href.split("?")[0]
                        emit_fn("info", f"  Found website from LinkedIn: {result['website']}")
                        break
            except:
                pass

            # Extract email patterns from page text
            try:
                page_text = await page.locator('body').inner_text(timeout=5000)
                email_pattern = re.compile(r'[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,7}')
                found_emails = email_pattern.findall(page_text)

                # Filter generic emails
                generic_prefixes = {'info', 'contact', 'admin', 'support', 'noreply', 'no-reply'}
                for email in found_emails:
                    prefix = email.split('@')[0].lower()
                    if prefix not in generic_prefixes and email not in result["emails"]:
                        result["emails"].append(email)

                if result["emails"]:
                    emit_fn("info", f"  Found {len(result['emails'])} email(s) from LinkedIn")
            except:
                pass

            # Extract phone patterns from page text
            try:
                page_text = await page.locator('body').inner_text(timeout=5000)
                phone_pattern = re.compile(r'(\+?[\d\s\-\(\)]{8,})')
                found_phones = phone_pattern.findall(page_text)

                for phone in found_phones:
                    clean_phone = re.sub(r'[^\d+]', '', phone).strip()
                    if len(clean_phone) >= 8 and clean_phone not in result["phones"]:
                        result["phones"].append(clean_phone)

                if result["phones"]:
                    emit_fn("info", f"  Found {len(result['phones'])} phone(s) from LinkedIn")
            except:
                pass

        except Exception as exc:
            emit_fn("warn", f"LinkedIn search failed: {str(exc)[:100]}")
        finally:
            await browser.close()

    return result
