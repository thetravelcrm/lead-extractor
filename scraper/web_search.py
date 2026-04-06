"""
scraper/web_search.py
---------------------
Uses Google Search to find company emails across the entire web.

Strategy:
1. For each company, search Google for: "<company name>" + "email" OR "contact"
2. Extract emails from search result snippets, titles, and page content
3. This catches emails on Facebook, Justdial, IndiaMART, directories, etc.
"""

import asyncio
import random
from typing import Callable, List, Dict, Optional
from urllib.parse import quote_plus

from config.settings import USER_AGENTS


async def search_emails_for_company(
    company_name: str,
    website_url: str = "",
    emit_fn: Optional[Callable] = None,
) -> List[str]:
    """
    Search Google for email addresses associated with a company.
    
    Returns a list of found email addresses.
    """
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    
    emails_found = []
    
    # Build targeted search queries
    queries = []
    
    # Query 1: Company name + email keywords
    queries.append(f'"{company_name}" email OR contact OR "@gmail.com" OR "@yahoo.com"')
    
    # Query 2: If website exists, search by domain
    if website_url:
        from urllib.parse import urlparse
        domain = urlparse(website_url).netloc.replace("www.", "")
        queries.append(f'site:{domain} email OR contact')
        queries.append(f'"{company_name}" {domain} email')
    
    # Query 3: Social media + directories
    queries.append(f'"{company_name}" site:facebook.com OR site:instagram.com OR site:justdial.com OR site:indiamart.com')
    
    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            context = await browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                viewport={"width": 1400, "height": 900},
            )
            await context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            page = await context.new_page()
            
            for query in queries[:2]:  # Limit to 2 queries per company for speed
                if len(emails_found) >= 5:  # Already found enough
                    break
                    
                try:
                    encoded_query = quote_plus(query)
                    search_url = f"https://www.google.com/search?q={encoded_query}&num=10"
                    
                    await page.goto(search_url, wait_until="domcontentloaded", timeout=15_000)
                    await page.wait_for_timeout(2000)
                    
                    # Accept cookies if shown
                    for selector in ['button:has-text("Accept all")', 'button:has-text("Agree")']:
                        try:
                            btn = page.locator(selector).first
                            if await btn.is_visible(timeout=1000):
                                await btn.click()
                                await page.wait_for_timeout(1000)
                                break
                        except Exception:
                            pass
                    
                    await page.wait_for_timeout(1500)
                    
                    # Extract emails from the page content
                    page_content = await page.content()
                    
                    # Simple email regex extraction from page
                    import re
                    email_pattern = re.compile(
                        r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,7}\b'
                    )
                    
                    # Filter: only keep emails that mention the company or are generic
                    raw_emails = email_pattern.findall(page_content)
                    for email in raw_emails:
                        email_lower = email.lower()
                        # Skip common false positives
                        if any(skip in email_lower for skip in [
                            'google', 'example', 'test', 'noreply', 'donotreply',
                            'w3.org', 'schema.org'
                        ]):
                            continue
                        
                        # Keep if it's a Gmail/Yahoo (common for Indian SMBs) or contains company name hint
                        company_hint = company_name.lower().split()[0] if company_name else ""
                        if email_lower.endswith(('@gmail.com', '@yahoo.com', '@outlook.com')):
                            if email_lower not in emails_found:
                                emails_found.append(email_lower)
                        elif company_hint and company_hint in email_lower:
                            if email_lower not in emails_found:
                                emails_found.append(email_lower)
                    
                    if emit_fn and raw_emails:
                        emit_fn("info", f"  Web search: found {len(raw_emails)} potential emails for {company_name[:30]}")
                    
                except Exception as e:
                    if emit_fn:
                        emit_fn("info", f"  Web search failed for {company_name[:30]}: {str(e)[:50]}")
                    continue
            
            await browser.close()
            
        except Exception as e:
            if emit_fn:
                emit_fn("warn", f"Web search error for {company_name}: {e}")
    
    return emails_found[:3]  # Return max 3 emails per company
