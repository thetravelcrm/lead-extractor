"""
scraper/advanced_crawler.py
----------------------------
Advanced multi-page website crawler with intelligent page prioritization.

Features:
- Crawls up to 5 high-priority internal pages
- Priority order: /contact, /contact-us, /about, /support, /reach-us, /team
- Proper URL joining and domain validation
- Timeout + retry logic (3 attempts)
- Skips broken/slow websites
- Async Playwright-based for performance
"""

import asyncio
import re
import time
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# Priority contact-related paths (ordered by likelihood of containing emails)
PRIORITY_PATHS = [
    '/contact',
    '/contact-us',
    '/contactus',
    '/get-in-touch',
    '/getintouch',
    '/about',
    '/about-us',
    '/aboutus',
    '/team',
    '/our-team',
    '/support',
    '/help',
    '/reach-us',
    '/reachus',
    '/contact-info',
    '/contactinfo',
]

# Keywords to identify contact pages in link text
CONTACT_KEYWORDS = [
    'contact', 'about', 'team', 'support', 'help', 'reach',
    'get in touch', 'getintouch', 'contact us', 'contactus',
]


class AdvancedCrawler:
    """
    Advanced multi-page website crawler.
    
    Crawls homepage + priority internal pages to extract maximum contact information.
    """

    def __init__(
        self,
        max_pages: int = 5,
        timeout_per_page: int = 15000,  # 15 seconds per page
        max_retries: int = 2,
        user_agent: str = None,
    ):
        self.max_pages = max_pages
        self.timeout_per_page = timeout_per_page
        self.max_retries = max_retries
        self.user_agent = user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

    async def crawl_website(self, url: str) -> Dict:
        """
        Crawl a website and extract content from homepage + priority pages.

        Args:
            url: Website URL to crawl

        Returns:
            {
                "homepage": {"html": str, "text": str, "url": str},
                "contact_pages": [{"html": str, "text": str, "url": str}],
                "all_html": str,  # Combined HTML from all pages
                "all_text": str,  # Combined text from all pages
                "pages_crawled": int,
                "errors": [str],
            }
        """
        result = {
            "homepage": None,
            "contact_pages": [],
            "all_html": "",
            "all_text": "",
            "pages_crawled": 0,
            "errors": [],
        }

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
                )
                context = await browser.new_context(
                    user_agent=self.user_agent,
                    viewport={"width": 1400, "height": 900},
                    locale="en-US",
                )
                # Anti-detection: disable webdriver property
                await context.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
                )
                page = await context.new_page()

                # Step 1: Crawl homepage
                homepage_data = await self._fetch_page(page, url)
                if homepage_data:
                    result["homepage"] = homepage_data
                    result["all_html"] += homepage_data["html"]
                    result["all_text"] += homepage_data["text"]
                    result["pages_crawled"] += 1

                    # Step 2: Find and crawl priority contact pages
                    contact_urls = await self._find_contact_pages(page, url, homepage_data["html"])

                    for contact_url in contact_urls[:self.max_pages - 1]:  # -1 for homepage
                        if result["pages_crawled"] >= self.max_pages:
                            break

                        contact_data = await self._fetch_page(page, contact_url)
                        if contact_data:
                            result["contact_pages"].append(contact_data)
                            result["all_html"] += contact_data["html"]
                            result["all_text"] += contact_data["text"]
                            result["pages_crawled"] += 1
                        else:
                            result["errors"].append(f"Failed to load: {contact_url}")

                else:
                    result["errors"].append(f"Failed to load homepage: {url}")

                await browser.close()

        except Exception as exc:
            result["errors"].append(f"Crawler error: {str(exc)}")

        return result

    async def _fetch_page(self, page, url: str, retries: int = None) -> Optional[Dict]:
        """
        Fetch a single page with retry logic and timeout.

        Returns:
            {"html": str, "text": str, "url": str} or None
        """
        if retries is None:
            retries = self.max_retries

        for attempt in range(retries + 1):
            try:
                response = await page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=self.timeout_per_page
                )

                # Skip non-200 responses
                if response and response.status != 200:
                    return None

                # Wait for page to fully render (catch lazy-loaded content)
                await page.wait_for_timeout(3000)

                html = await page.content()
                text = await page.inner_text("body")

                return {
                    "html": html,
                    "text": text,
                    "url": page.url,
                }

            except PWTimeout:
                if attempt < retries:
                    await asyncio.sleep(1)
                    continue
                return None
            except Exception:
                if attempt < retries:
                    await asyncio.sleep(1)
                    continue
                return None

        return None

    async def _find_contact_pages(self, page, base_url: str, html: str) -> List[str]:
        """
        Find priority contact/about pages from the homepage.

        Returns:
            List of URLs to crawl, ordered by priority
        """
        found_urls = []
        domain = urlparse(base_url).netloc

        try:
            # Method 1: Try to find links matching priority paths
            for path in PRIORITY_PATHS:
                try:
                    # Look for links containing the priority path
                    links = await page.locator(f'a[href*="{path}"]').all()
                    for link in links:
                        try:
                            href = await link.get_attribute("href")
                            if href:
                                full_url = urljoin(base_url, href)
                                # Validate it's on the same domain
                                if urlparse(full_url).netloc == domain:
                                    if full_url not in found_urls:
                                        found_urls.append(full_url)
                        except:
                            pass
                except:
                    pass

            # Method 2: Look for links with contact-related text
            all_links = await page.locator('a[href]').all()
            for link in all_links:
                try:
                    text = await link.inner_text()
                    text_lower = text.lower().strip()

                    if any(keyword in text_lower for keyword in CONTACT_KEYWORDS):
                        href = await link.get_attribute("href")
                        if href:
                            full_url = urljoin(base_url, href)
                            # Validate it's on the same domain and not a fragment
                            if urlparse(full_url).netloc == domain and not full_url.endswith('#'):
                                if full_url not in found_urls:
                                    found_urls.append(full_url)
                except:
                    pass

        except Exception:
            pass

        # Prioritize exact contact pages first
        priority_urls = []
        other_urls = []

        for url in found_urls:
            path = urlparse(url).path.lower()
            if any(p in path for p in ['/contact', '/contact-us', '/contactus', '/get-in-touch']):
                priority_urls.append(url)
            else:
                other_urls.append(url)

        return priority_urls + other_urls
