"""
scraper/google_search.py
------------------------
Scrapes Google Maps for business listings using Playwright.
Uses domcontentloaded (not networkidle) to avoid timeout on Maps.
"""

import asyncio
import random
from typing import Callable, List, Dict
from urllib.parse import quote_plus

from config.settings import USER_AGENTS, DELAY_PAGE_LOAD


def build_query(country: str, business_type: str, city: str = "") -> str:
    """
    Build the search query.
    If city is provided: "plumbers in Dubai, UAE"
    Otherwise:           "plumbers in United Arab Emirates"
    """
    location = f"{city}, {country}" if city.strip() else country
    return f"{business_type} in {location}"


async def search_google_maps(
    query: str,
    max_results: int,
    emit_fn: Callable,
) -> List[Dict]:
    """
    Search Google Maps and return business listings.

    Each record: { "name": str, "category": str, "website_url": str }
    """
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout

    results: List[Dict] = []
    seen_names: set = set()
    encoded_query = quote_plus(query)
    maps_url = f"https://www.google.com/maps/search/{encoded_query}"

    emit_fn("info", f"Opening Google Maps: {query}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ],
        )
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1400, "height": 900},
            locale="en-US",
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        page = await context.new_page()

        try:
            # Use domcontentloaded — Maps never reaches networkidle
            await page.goto(maps_url, wait_until="domcontentloaded", timeout=45_000)
            await page.wait_for_timeout(3000)  # Give Maps time to render

            # Accept cookies / consent FIRST (before looking for results)
            emit_fn("info", "Checking for cookie consent...")
            for selector in [
                'button:has-text("Accept all")',
                'button:has-text("Agree")',
                'button:has-text("I agree")',
                'button:has-text("Accept")',
                'form[action*="consent"] button[type="submit"]',
            ]:
                try:
                    btn = page.locator(selector).first
                    if await btn.is_visible(timeout=2000):
                        await btn.click()
                        emit_fn("info", "Accepted cookies.")
                        await page.wait_for_timeout(2000)
                        break
                except Exception:
                    pass

            # Wait for results feed with multiple selector strategies
            emit_fn("info", "Waiting for Google Maps results to load...")
            feed_found = False
            
            # Try multiple selectors that Google Maps might use
            for selector in [
                'div[role="feed"]',
                'div[role="listbox"]',
                'div[aria-label*="Results"]',
                'div[jsaction*="scroll"]',
                'div.Nv2PK',  # Common Maps results container class
                'div.fontBodyMedium',  # Text content in results
            ]:
                try:
                    await page.wait_for_selector(selector, timeout=5000)
                    emit_fn("info", f"Found results container: {selector}")
                    feed_found = True
                    break
                except PWTimeout:
                    continue
            
            if not feed_found:
                emit_fn("warn", "No results feed found — Maps may have changed layout or blocked the request.")
                # Try to debug: check if page loaded at all
                try:
                    page_title = await page.title()
                    page_url = page.url
                    emit_fn("warn", f"Page title: {page_title}, URL: {page_url[:80]}")
                except Exception:
                    pass
                return results

            emit_fn("info", "Google Maps loaded. Scrolling for results...")
            await page.wait_for_timeout(int(random.uniform(*DELAY_PAGE_LOAD) * 1000))

            scroll_attempts = 0
            max_scrolls = max(10, max_results // 4)

            while len(results) < max_results and scroll_attempts < max_scrolls:

                # ── Extract visible listing cards ──────────────────────
                # Try multiple selectors for Google Maps listing cards
                cards = []
                for card_selector in [
                    'a[href*="/maps/place/"]',
                    'a[aria-label][href*="maps"]',
                    'div.Nv2PK a',
                    'div[role="article"] a',
                ]:
                    try:
                        found_cards = await page.locator(card_selector).all()
                        if found_cards:
                            cards = found_cards
                            emit_fn("info", f"Using card selector: {card_selector}, found {len(found_cards)} cards")
                            break
                    except Exception:
                        continue
                
                if not cards:
                    emit_fn("warn", "No listing cards found. Trying to scroll anyway...")

                for card in cards:
                    if len(results) >= max_results:
                        break
                    try:
                        name = (await card.get_attribute("aria-label") or "").strip()
                        if not name or name in seen_names:
                            continue
                        seen_names.add(name)

                        # Walk up to find the enclosing listing container
                        container = card.locator("xpath=ancestor::div[@data-result-index or contains(@class,'Nv2PK') or @jsaction][1]").first

                        # Category — small descriptive text in card
                        category = ""
                        for cat_sel in [
                            'span.fontBodyMedium > span:first-child',
                            'div.fontBodyMedium > span',
                            'span[jsan*="category"]',
                        ]:
                            try:
                                el = container.locator(cat_sel).first
                                txt = (await el.inner_text(timeout=400)).strip()
                                if txt and not any(c.isdigit() for c in txt[:3]):
                                    category = txt.split("·")[0].strip()
                                    break
                            except Exception:
                                pass

                        # Website URL — look for external link in card
                        website_url = ""
                        for web_selector in [
                            'a[data-value="Website"]',
                            'a[aria-label*="website" i]',
                            'a[href*="http"]:not([href*="google"]):not([href*="/maps"])',
                            'div[data-item-id*="authority"] a',
                        ]:
                            try:
                                web_links = await container.locator(web_selector).all()
                                for wl in web_links:
                                    href = (await wl.get_attribute("href") or "").strip()
                                    if href.startswith("http") and "google" not in href and "/maps" not in href:
                                        website_url = href.split("?")[0]
                                        break
                                if website_url:
                                    break
                            except Exception:
                                pass

                        # Fallback: click listing to open side panel and grab website
                        if not website_url:
                            try:
                                await card.click(timeout=3000)
                                await page.wait_for_timeout(2000)
                                
                                # Try multiple selectors for website link in side panel
                                for panel_selector in [
                                    'a[data-item-id="authority"]',
                                    'a[aria-label*="website" i]',
                                    'div[data-section-id="apiv3link"] a',
                                    'a[href*="http"]:not([href*="google"]):not([href*="/maps"])',
                                ]:
                                    try:
                                        web_el = page.locator(panel_selector).first
                                        if await web_el.is_visible(timeout=2000):
                                            href = (await web_el.get_attribute("href") or "").strip()
                                            if href.startswith("http") and "google" not in href and "/maps" not in href:
                                                website_url = href.split("?")[0]
                                                break
                                    except Exception:
                                        continue
                                
                                # Press Escape to close the side panel
                                await page.keyboard.press("Escape")
                                await page.wait_for_timeout(500)
                            except Exception:
                                try:
                                    await page.keyboard.press("Escape")
                                except Exception:
                                    pass

                        results.append({
                            "name":        name,
                            "category":    category,
                            "website_url": website_url,
                        })

                    except Exception:
                        continue

                emit_fn(
                    "info",
                    f"Collected {len(results)}/{max_results} listings...",
                    data={"current": len(results), "total": max_results},
                )

                # ── Scroll down to load more ───────────────────────────
                try:
                    feed = page.locator('div[role="feed"]')
                    await feed.evaluate("el => el.scrollBy(0, 800)")
                except Exception:
                    await page.evaluate("window.scrollBy(0, 800)")

                await page.wait_for_timeout(int(random.uniform(1800, 3000)))
                scroll_attempts += 1

                # Stop if "You've reached the end" message appears
                try:
                    end = await page.locator(
                        "text=You've reached the end, "
                        "text=end of the list"
                    ).count()
                    if end > 0:
                        emit_fn("info", "Reached end of Maps results.")
                        break
                except Exception:
                    pass

        except PWTimeout:
            emit_fn("warn", "Google Maps timed out. The server IP may be rate-limited. Retrying with fallback...")
        except Exception as exc:
            emit_fn("warn", f"Google Maps error: {exc}")
        finally:
            await browser.close()

    emit_fn("success", f"Google Maps: collected {len(results)} listings.")
    return results
