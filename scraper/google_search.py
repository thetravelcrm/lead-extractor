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

            # Wait for either the results feed or the consent form
            try:
                await page.wait_for_selector(
                    'div[role="feed"], form[action*="consent"], button[aria-label*="Accept"]',
                    timeout=15_000,
                )
            except PWTimeout:
                emit_fn("warn", "Maps page took too long to load results panel.")

            # Accept cookies / consent (EU regions)
            for selector in [
                'button:has-text("Accept all")',
                'button:has-text("Agree")',
                'button:has-text("I agree")',
                'form[action*="consent"] button[type="submit"]',
            ]:
                try:
                    btn = page.locator(selector).first
                    if await btn.is_visible(timeout=2000):
                        await btn.click()
                        await page.wait_for_timeout(1500)
                        break
                except Exception:
                    pass

            # Wait for results feed after consent
            try:
                await page.wait_for_selector('div[role="feed"]', timeout=12_000)
            except PWTimeout:
                emit_fn("warn", "No results feed found — Maps may have changed layout or blocked the request.")
                return results

            emit_fn("info", "Google Maps loaded. Scrolling for results...")
            await page.wait_for_timeout(int(random.uniform(*DELAY_PAGE_LOAD) * 1000))

            scroll_attempts = 0
            max_scrolls = max(10, max_results // 4)

            while len(results) < max_results and scroll_attempts < max_scrolls:

                # ── Extract visible listing cards ──────────────────────
                cards = await page.locator('a[href*="/maps/place/"]').all()

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
                        try:
                            web_links = await container.locator(
                                'a[data-value="Website"], a[aria-label*="website" i]'
                            ).all()
                            for wl in web_links:
                                href = (await wl.get_attribute("href") or "").strip()
                                if href.startswith("http") and "google" not in href:
                                    website_url = href.split("?")[0]
                                    break
                        except Exception:
                            pass

                        # Fallback: click listing to open side panel and grab website
                        if not website_url:
                            try:
                                await card.click(timeout=3000)
                                await page.wait_for_timeout(1500)
                                web_el = page.locator(
                                    'a[data-item-id="authority"], '
                                    'a[aria-label*="website" i], '
                                    'div[data-section-id="apiv3link"] a'
                                ).first
                                if await web_el.is_visible(timeout=3000):
                                    href = (await web_el.get_attribute("href") or "").strip()
                                    if href.startswith("http") and "google" not in href:
                                        website_url = href.split("?")[0]
                                # Press Escape to close the side panel
                                await page.keyboard.press("Escape")
                                await page.wait_for_timeout(500)
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
