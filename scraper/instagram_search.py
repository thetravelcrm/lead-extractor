"""
scraper/instagram_search.py
---------------------------
Search Instagram for business profiles and extract contact information.

Functions:
- search_instagram(company_name, city) → {emails: [], phones: [], website: ""}

Strategy:
1. Search Google for: "Instagram [Company Name] [City]"
2. Open Instagram business profile
3. Extract contact info from bio (email, phone, website)
"""

import asyncio
import re
from typing import Dict, List, Callable
from urllib.parse import quote_plus


async def search_instagram(
    company_name: str,
    city: str = "",
    emit_fn: Callable = None,
) -> Dict:
    """
    Search Instagram for a business profile and extract contact information.

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

    search_query = f"{company_name} {city} Instagram".strip()
    encoded_query = quote_plus(search_query)
    google_url = f"https://www.google.com/search?q={encoded_query}"

    emit_fn("info", f"  📷 Searching Instagram for: {company_name}")

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
            # Step 1: Search Google for Instagram profile
            emit_fn("info", f"  Searching Google for Instagram profile...")
            await page.goto(google_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)

            # Find Instagram URL from search results
            instagram_url = ""
            try:
                # Look for instagram.com links in search results
                links = await page.locator('a[href*="instagram.com"]').all()
                for link in links:
                    href = await link.get_attribute("href")
                    if href and "instagram.com" in href:
                        instagram_url = href.split("?")[0]
                        emit_fn("info", f"  Found Instagram: {instagram_url[:60]}...")
                        break
            except:
                pass

            if not instagram_url:
                emit_fn("info", "  No Instagram profile found")
                await browser.close()
                return result

            # Step 2: Visit Instagram profile
            emit_fn("info", f"  Visiting Instagram profile...")
            await page.goto(instagram_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)

            # Step 3: Extract contact info from bio
            try:
                # Instagram bio text
                bio_text = ""
                try:
                    bio_elements = await page.locator('div[aria-label*="bio"], h1, span._ap3a').all()
                    for elem in bio_elements:
                        text = await elem.inner_text(timeout=1000)
                        if text:
                            bio_text += " " + text
                except:
                    pass

                # Also try to get page text
                try:
                    page_text = await page.locator('body').inner_text(timeout=5000)
                    bio_text += " " + page_text[:2000]
                except:
                    pass

                # Extract email patterns
                email_pattern = re.compile(r'[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,7}')
                found_emails = email_pattern.findall(bio_text)

                # Filter generic emails
                generic_prefixes = {'info', 'contact', 'admin', 'support', 'noreply', 'no-reply'}
                for email in found_emails:
                    prefix = email.split('@')[0].lower()
                    if prefix not in generic_prefixes and email not in result["emails"]:
                        result["emails"].append(email)

                if result["emails"]:
                    emit_fn("info", f"  Found {len(result['emails'])} email(s) from Instagram")

                # Extract website from bio
                try:
                    website_links = await page.locator('a[href*="http"]:not([href*="instagram"])').all()
                    for link in website_links:
                        href = await link.get_attribute("href")
                        if href and len(href) > 10 and "instagram.com" not in href:
                            result["website"] = href.split("?")[0]
                            emit_fn("info", f"  Found website from Instagram: {result['website']}")
                            break
                except:
                    pass

                # Extract phone patterns
                phone_pattern = re.compile(r'(\+?[\d\s\-\(\)]{8,})')
                found_phones = phone_pattern.findall(bio_text)

                for phone in found_phones:
                    clean_phone = re.sub(r'[^\d+]', '', phone).strip()
                    if len(clean_phone) >= 8 and clean_phone not in result["phones"]:
                        result["phones"].append(clean_phone)

                if result["phones"]:
                    emit_fn("info", f"  Found {len(result['phones'])} phone(s) from Instagram")

            except Exception as e:
                emit_fn("warn", f"Instagram extraction failed: {str(e)[:100]}")

        except Exception as exc:
            emit_fn("warn", f"Instagram search failed: {str(exc)[:100]}")
        finally:
            await browser.close()

    return result
