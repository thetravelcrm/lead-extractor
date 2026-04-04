"""
scraper/google_search.py
------------------------
Uses Playwright (headless Chromium) to scrape Google Maps for business
listings, then returns their website URLs for further email extraction.

Why Google Maps instead of Google Search?
  - Far fewer CAPTCHAs than Google Search
  - Directly provides: company name, business category, website URL
  - Results are specifically businesses (not directories or social media)
  - Scrollable feed gives 20-60+ results per search

Flow:
  1. Navigate to maps.google.com/search/{business_type} in {country}
  2. Scroll the results panel to load more listings
  3. Extract: business name, category, website URL from each listing card
  4. Return list of dicts with {name, category, website_url}
"""

import asyncio
import random
import re
from typing import Callable, List, Dict
from urllib.parse import quote_plus

from config.settings import (
    USER_AGENTS,
    DELAY_BETWEEN_SEARCHES,
    DELAY_PAGE_LOAD,
)


def build_query(country: str, business_type: str) -> str:
    """Build a Google Maps search query."""
    return f"{business_type} in {country}"


async def search_google_maps(
    query: str,
    max_results: int,
    emit_fn: Callable,
) -> List[Dict]:
    """
    Search Google Maps and return a list of business records.

    Each record:
    {
        "name":         str,   # company name from Maps listing
        "category":     str,   # business category from Maps
        "website_url":  str,   # company website (empty string if none listed)
    }

    Parameters
    ----------
    query       : str      — search query e.g. "plumbers in United Kingdom"
    max_results : int      — how many listings to collect (scrolls until reached)
    emit_fn     : Callable — SSE emitter: emit_fn(level, message, data=None)
    """
    from playwright.async_api import async_playwright

    results: List[Dict] = []
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
            ],
        )

        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1400, "height": 900},
            locale="en-US",
            java_script_enabled=True,
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        page = await context.new_page()

        try:
            await page.goto(maps_url, wait_until="networkidle", timeout=30_000)
            await page.wait_for_timeout(int(random.uniform(2000, 4000)))

            # Accept cookies popup if shown (EU regions)
            try:
                accept_btn = page.locator('button:has-text("Accept all"), button:has-text("Reject all"), form[action*="consent"] button')
                if await accept_btn.first.is_visible(timeout=3000):
                    await accept_btn.first.click()
                    await page.wait_for_timeout(1000)
            except Exception:
                pass

            # The results panel on the left side
            results_panel = page.locator('div[role="feed"]')

            emit_fn("info", "Scrolling Google Maps results...")

            collected = 0
            scroll_attempts = 0
            max_scroll_attempts = int(max_results / 5) + 10  # roughly 5 results per scroll

            while collected < max_results and scroll_attempts < max_scroll_attempts:
                # Extract currently visible listing cards
                cards = await page.locator('a[href*="/maps/place/"]').all()

                for card in cards:
                    if collected >= max_results:
                        break
                    try:
                        # Get the parent div containing all info for this listing
                        parent = card.locator("xpath=ancestor::div[contains(@jsaction,'mouseover')]").first

                        # Company name — from aria-label on the main link
                        name = await card.get_attribute("aria-label") or ""
                        name = name.strip()

                        if not name or name in [r["name"] for r in results]:
                            continue

                        # Category — small grey text in the card
                        category = ""
                        try:
                            cat_el = parent.locator('div[jslog] span.fontBodyMedium').first
                            category = (await cat_el.inner_text(timeout=500)).strip()
                            # Clean up — category is usually first part before · or address
                            if "·" in category:
                                category = category.split("·")[0].strip()
                        except Exception:
                            pass

                        # Website URL — look for the website button/link in the card
                        website_url = ""
                        try:
                            # When a card is clicked, Maps shows detail panel with website link
                            # Instead, we get the website from the href of the "Website" button
                            web_links = await parent.locator('a[data-value="Website"], a[aria-label*="website" i], a[href^="http"]:not([href*="google"])').all()
                            for wl in web_links:
                                href = await wl.get_attribute("href") or ""
                                if href.startswith("http") and "google" not in href and "maps" not in href:
                                    website_url = href.split("?")[0]
                                    break
                        except Exception:
                            pass

                        results.append({
                            "name":        name,
                            "category":    category,
                            "website_url": website_url,
                        })
                        collected += 1

                    except Exception:
                        continue

                emit_fn(
                    "info",
                    f"Collected {collected}/{max_results} listings...",
                    data={"current": collected, "total": max_results},
                )

                # Scroll the results panel down to load more
                try:
                    await results_panel.evaluate("el => el.scrollBy(0, 600)")
                except Exception:
                    await page.keyboard.press("End")

                await page.wait_for_timeout(int(random.uniform(1500, 2500)))
                scroll_attempts += 1

                # Check for "end of results" message
                end_msg = await page.locator("text=You've reached the end of the list").count()
                if end_msg > 0:
                    emit_fn("info", "Reached end of Maps results.")
                    break

        except Exception as exc:
            emit_fn("warn", f"Google Maps error: {exc}")

        finally:
            await browser.close()

    emit_fn("success", f"Google Maps: collected {len(results)} business listings.")
    return results


async def fetch_website_from_maps_detail(
    place_name: str,
    country: str,
    emit_fn: Callable,
) -> str:
    """
    Fallback: if a listing had no website URL in the card, search Maps for
    the specific business and extract the website from the detail panel.
    Returns website URL string or empty string.
    """
    from playwright.async_api import async_playwright

    query = quote_plus(f"{place_name} {country}")
    url = f"https://www.google.com/maps/search/{query}"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context(user_agent=random.choice(USER_AGENTS))
        page = await context.new_page()

        try:
            await page.goto(url, wait_until="networkidle", timeout=20_000)
            await page.wait_for_timeout(2000)

            # Click the first result to open detail panel
            first = page.locator('a[href*="/maps/place/"]').first
            if await first.is_visible(timeout=3000):
                await first.click()
                await page.wait_for_timeout(2000)

            # Look for website link in detail panel
            web_el = page.locator('a[data-item-id="authority"], a[aria-label*="website" i]').first
            if await web_el.is_visible(timeout=3000):
                href = await web_el.get_attribute("href") or ""
                if href.startswith("http"):
                    return href.split("?")[0]
        except Exception:
            pass
        finally:
            await browser.close()

    return ""
