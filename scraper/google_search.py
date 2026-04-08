"""
scraper/google_search.py
------------------------
Scrapes Google Maps for business listings using Playwright.
Uses domcontentloaded (not networkidle) to avoid timeout on Maps.

Supports pagination/offsets to fetch more than 300 results by:
1. Using multiple search variations (adding area codes, nearby areas)
2. Multiple scroll sessions with different starting points
"""

import asyncio
import random
from typing import Callable, List, Dict
from urllib.parse import quote_plus

from config.settings import USER_AGENTS, DELAY_PAGE_LOAD


def build_query(country: str, business_type: str, city: str = "") -> str:
    """
    Build the search query with intelligent cleaning.

    Handles cases where user enters full query in business_type field:
    - "travel agency in lucknow" → extracts "travel agency"
    - "restaurants in dubai" → extracts "restaurants"

    Examples:
    - "travel agency in lucknow" + city="Lucknow" → "travel agency in Lucknow, India"
    - "travel agency" + city="Lucknow" → "travel agency in Lucknow, India"
    - "restaurants in dubai" + city="" → "restaurants in dubai, India"
    - "restaurants in dubai, UAE" + city="" → "restaurants in dubai, UAE"
    """
    import re

    # Clean business_type: remove location info if user accidentally included it
    cleaned_business = business_type.strip()
    extracted_location = None

    # Pattern: Detect "business_type in location" format
    # Matches: "travel agency in lucknow", "restaurants in dubai uae", etc.
    location_pattern = re.compile(
        r'^(.+?)\s+in\s+([a-zA-Z\s,]+)$',
        re.IGNORECASE
    )

    match = location_pattern.search(cleaned_business)
    if match:
        extracted_business = match.group(1).strip()
        extracted_location = match.group(2).strip()

        # Check if extracted location matches user's city/country input
        user_city_lower = city.lower().strip()
        user_country_lower = country.lower().strip()
        extracted_location_lower = extracted_location.lower()

        # If the extracted location contains the user's city or country, use cleaned business
        if (user_city_lower and user_city_lower in extracted_location_lower) or \
           (user_country_lower and user_country_lower in extracted_location_lower) or \
           extracted_location_lower in user_city_lower or \
           len(extracted_location.split()) <= 5:  # Reasonable location name
            cleaned_business = extracted_business

    # Build final query
    if city.strip():
        # User provided city, use it
        location = f"{city}, {country}"
    elif extracted_location:
        # User didn't provide city but included location in business_type
        # Use the extracted location with country as backup
        if ',' in extracted_location:
            # Already has city, country format
            location = extracted_location
        else:
            # Just city name, append country
            location = f"{extracted_location}, {country}"
    else:
        # No city provided anywhere, just use country
        location = country

    return f"{cleaned_business} in {location}"


def build_extended_queries(base_query: str, city: str = "") -> List[str]:
    """
    Build multiple query variations to bypass Google Maps 300-result limit.
    Returns a list of queries to try sequentially.
    """
    queries = [base_query]

    # Add area-specific variations if city is provided
    if city:
        area_modifiers = [
            "near me",
            "nearby",
            "top rated",
            "best",
            "popular",
            "local",
        ]
        for modifier in area_modifiers:
            queries.append(f"{base_query} {modifier}")

    return queries


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
            await page.wait_for_timeout(5000)  # Longer wait for full JS render

            scroll_attempts = 0
            max_scrolls = max(30, max_results // 2)
            last_count = 0
            no_new_results_count = 0

            while len(results) < max_results and scroll_attempts < max_scrolls:

                # Wait for content to stabilize after scroll
                await page.wait_for_timeout(1500)

                # ── Extract listing cards from sidebar ─────────────────
                # CRITICAL: Use selectors that target sidebar results ONLY, not map controls
                cards = []
                best_selector = None

                for card_selector in [
                    # Primary: Business listing containers in sidebar
                    'div[role="article"] div[tabindex="0"]',
                    # Secondary: Specific business card structure
                    'div.fontBodyMedium div[tabindex="0"]',
                    # Fallback: Any clickable div in the results feed
                    'div[role="feed"] div[tabindex="0"]',
                ]:
                    try:
                        found_cards = await page.locator(card_selector).all()
                        # Filter out non-business elements (map controls, instructions)
                        valid_cards = []
                        for card in found_cards:
                            try:
                                aria_label = await card.get_attribute("aria-label")
                                # Skip if it's map instructions or UI elements
                                if (aria_label and
                                    len(aria_label) > 5 and
                                    "arrow keys" not in aria_label.lower() and
                                    "pan the map" not in aria_label.lower() and
                                    "get details" not in aria_label.lower()):
                                    valid_cards.append(card)
                            except:
                                continue

                        if valid_cards and len(valid_cards) > len(cards):
                            cards = valid_cards
                            best_selector = card_selector
                    except Exception as e:
                        continue

                if cards:
                    emit_fn("info", f"Found {len(cards)} business cards using: {best_selector} (total unique: {len(results)})")
                else:
                    emit_fn("warn", "No valid business cards found. Scrolling to load more...")

                for card in cards:
                    if len(results) >= max_results:
                        break
                    try:
                        # Get business name from aria-label
                        name = (await card.get_attribute("aria-label") or "").strip()

                        # Skip if name is too short or looks like UI text
                        if (not name or
                            len(name) < 3 or
                            "arrow keys" in name.lower() or
                            "pan the map" in name.lower() or
                            name in seen_names):
                            continue

                        seen_names.add(name)

                        # Category - business type text
                        category = ""
                        try:
                            # Look for category text within the card
                            cat_elements = await card.locator('span.fontBodyMedium').all()
                            if cat_elements:
                                category = await cat_elements[0].inner_text(timeout=500)
                                category = category.strip().split('·')[0].strip()
                        except:
                            pass

                        # Website URL - check if listing has website
                        website_url = ""
                        try:
                            # Look for website button/link in the card
                            website_btn = card.locator('a[data-value="Website"], a[aria-label*="Website" i]').first
                            if await website_btn.count() > 0:
                                href = await website_btn.get_attribute("href")
                                if href and "google" not in href and "/maps" not in href:
                                    website_url = href.split("?")[0]
                        except:
                            pass

                        # Fallback: Click card to open side panel for website
                        if not website_url:
                            try:
                                await card.click(timeout=2000)
                                await page.wait_for_timeout(2000)

                                # Try to find website in side panel
                                for panel_selector in [
                                    'a[data-item-id="authority"]',
                                    'a[aria-label*="website" i]',
                                ]:
                                    try:
                                        web_el = page.locator(panel_selector).first
                                        if await web_el.is_visible(timeout=1500):
                                            href = await web_el.get_attribute("href")
                                            if href and "google" not in href and "/maps" not in href:
                                                website_url = href.split("?")[0]
                                                break
                                    except:
                                        continue

                                # Close side panel
                                await page.keyboard.press("Escape")
                                await page.wait_for_timeout(500)
                            except:
                                try:
                                    await page.keyboard.press("Escape")
                                except:
                                    pass

                        results.append({
                            "name": name,
                            "category": category,
                            "website_url": website_url,
                        })

                    except Exception as e:
                        continue

                emit_fn(
                    "info",
                    f"Collected {len(results)}/{max_results} listings...",
                    data={"current": len(results), "total": max_results},
                )

                # Check if we got new results in this scroll
                if len(results) == last_count:
                    no_new_results_count += 1
                    if no_new_results_count >= 8:  # Stop if 8 consecutive scrolls with no new results
                        emit_fn("info", f"No new results after 8 scrolls. Total collected: {len(results)}")
                        break
                else:
                    no_new_results_count = 0
                    last_count = len(results)

                # ── Scroll down to load more ───────────────────────────
                # Strategy: Scroll the results container progressively
                scrolled = False

                # Try scrolling the feed container first (most reliable)
                for scroll_selector in [
                    'div[role="feed"]',
                    'div[role="listbox"]',
                    'div[aria-label*="Results"]',
                ]:
                    try:
                        feed = page.locator(scroll_selector).first
                        if await feed.count() > 0:
                            # Get current scroll position and scroll further
                            current_scroll = await feed.evaluate("el => el.scrollTop")
                            await feed.evaluate("el => el.scrollTop += 1500")
                            new_scroll = await feed.evaluate("el => el.scrollTop")
                            if new_scroll > current_scroll:
                                scrolled = True
                                emit_fn("info", f"Scrolled {scroll_selector}: {current_scroll} → {new_scroll}")
                                break
                    except Exception as e:
                        continue

                if not scrolled:
                    # Fallback: Keyboard scroll (simulate user behavior)
                    await page.keyboard.press("End")
                    await page.wait_for_timeout(500)
                    scrolled = True
                    emit_fn("info", "Used keyboard End key to scroll")

                # Wait for new results to load after scroll
                await page.wait_for_timeout(3000)
                scroll_attempts += 1

                # Log progress every 10 scrolls
                if scroll_attempts % 10 == 0:
                    emit_fn("info", f"Scroll #{scroll_attempts}: {len(results)} results so far...")

                # Stop if "You've reached the end" message appears
                try:
                    end_text_patterns = [
                        "You've reached the end",
                        "end of the list",
                        "No more results",
                    ]
                    for pattern in end_text_patterns:
                        elements = await page.get_by_text(pattern, exact=False).all()
                        if len(elements) > 0:
                            emit_fn("info", f"Reached end of Maps results (found: '{pattern}'). Total: {len(results)}")
                            break
                    else:
                        continue
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


async def search_google_maps_extended(
    query: str,
    max_results: int,
    emit_fn: Callable,
    city: str = "",
) -> List[Dict]:
    """
    Extended search that tries multiple query variations to bypass
    the ~300 result limit on Google Maps.

    Strategy:
    1. First search with the base query
    2. If results < max_results, try extended queries
    3. Merge results, deduplicate by website_url
    """
    all_results = []
    seen_urls = set()

    # Get extended queries
    extended_queries = build_extended_queries(query, city)

    for idx, q in enumerate(extended_queries):
        if len(all_results) >= max_results:
            break

        if idx > 0:
            emit_fn("info", f"Trying extended query {idx+1}/{len(extended_queries)}: {q[:60]}...")
            await asyncio.sleep(random.uniform(2, 4))  # Delay between queries

        try:
            results = await search_google_maps(q, max_results - len(all_results), emit_fn)

            # Deduplicate by website_url
            for result in results:
                url = result.get("website_url", "").strip().rstrip("/")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_results.append(result)
                elif not url:  # Keep listings without website URLs
                    all_results.append(result)

            emit_fn("info", f"Extended query {idx+1}: found {len(results)} listings (total unique: {len(all_results)})")

        except Exception as exc:
            emit_fn("warn", f"Extended query {idx+1} failed: {exc}")
            continue

    emit_fn("success", f"Extended search complete: {len(all_results)} unique listings from {len(extended_queries)} queries.")
    return all_results
