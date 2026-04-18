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

    # Extract business type from query (e.g., "Travel Agency in Lucknow, India" → "Travel Agency")
    import re
    query_match = re.match(r'^(.+?)\s+in\s+', query)
    business_type = query_match.group(1).strip() if query_match else query

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
                await page.wait_for_timeout(2000)

                # ── Extract listing cards from sidebar ─────────────────
                # Google Maps structure: feed contains business listing items
                # Each listing is clickable and has business name in aria-label
                cards = []
                best_selector = None

                # Strategy 1: Get all clickable elements in feed with aria-label
                try:
                    feed = page.locator('div[role="feed"]').first
                    if await feed.count() > 0:
                        # Get all elements with tabindex (clickable) inside feed
                        all_items = await feed.locator('div[tabindex="0"]').all()

                        valid_cards = []
                        for item in all_items:
                            try:
                                aria_label = await item.get_attribute("aria-label")
                                # Must have a meaningful aria-label (business name)
                                if (aria_label and
                                    len(aria_label) >= 3 and
                                    len(aria_label) < 200 and
                                    "arrow keys" not in aria_label.lower() and
                                    "pan the map" not in aria_label.lower() and
                                    "get details" not in aria_label.lower() and
                                    "scroll" not in aria_label.lower() and
                                    "zoom" not in aria_label.lower()):
                                    valid_cards.append(item)
                            except:
                                continue

                        if valid_cards:
                            cards = valid_cards
                            best_selector = "div[role='feed'] div[tabindex='0']"
                            emit_fn("info", f"Strategy 1: Found {len(valid_cards)} business listings")
                except Exception as e:
                    emit_fn("warn", f"Strategy 1 failed: {str(e)[:100]}")

                # Strategy 2: Try role="article" elements
                if not cards:
                    try:
                        articles = await page.locator('div[role="feed"] div[role="article"]').all()
                        valid = []
                        for article in articles:
                            try:
                                label = await article.get_attribute("aria-label")
                                if (label and len(label) >= 3 and len(label) < 200 and
                                    "arrow keys" not in label.lower() and
                                    "pan the map" not in label.lower()):
                                    valid.append(article)
                            except:
                                continue
                        if valid:
                            cards = valid
                            best_selector = "div[role='feed'] div[role='article']"
                            emit_fn("info", f"Strategy 2: Found {len(valid)} listings")
                    except Exception as e:
                        emit_fn("warn", f"Strategy 2 failed: {str(e)[:100]}")

                # Strategy 3: Try Nv2PK class (Google Maps listing class)
                if not cards:
                    try:
                        nv2pk = await page.locator('div[role="feed"] div.Nv2PK').all()
                        if nv2pk:
                            cards = nv2pk
                            best_selector = "div.Nv2PK"
                            emit_fn("info", f"Strategy 3: Found {len(nv2pk)} listings via Nv2PK class")
                    except Exception as e:
                        emit_fn("warn", f"Strategy 3 failed: {str(e)[:100]}")

                # Strategy 4: Try fontBodyMedium (text container class)
                if not cards:
                    try:
                        font_medium = await page.locator('div[role="feed"] div.fontBodyMedium').all()
                        if font_medium:
                            cards = font_medium
                            best_selector = "div.fontBodyMedium"
                            emit_fn("info", f"Strategy 4: Found {len(font_medium)} listings via fontBodyMedium")
                    except Exception as e:
                        emit_fn("warn", f"Strategy 4 failed: {str(e)[:100]}")

                if cards:
                    emit_fn("info", f"Using: {best_selector}. Total unique results: {len(results)}")
                else:
                    emit_fn("warn", "No valid business cards found. Scrolling to load more...")
                    # DEBUG: Log what's actually in the feed to understand structure
                    try:
                        feed_debug = page.locator('div[role="feed"]').first
                        child_count = await feed_debug.locator('div').count()
                        emit_fn("info", f"DEBUG: Feed has {child_count} div elements")
                        # Sample first 15 elements to see their structure
                        first_few = await feed_debug.locator('div').all()
                        for i, elem in enumerate(first_few[:15]):
                            try:
                                lbl = await elem.get_attribute("aria-label")
                                tabindex = await elem.get_attribute("tabindex")
                                role = await elem.get_attribute("role")
                                classes = await elem.get_attribute("class")
                                jsaction = await elem.get_attribute("jsaction")
                                # Log all elements that might be listings
                                if lbl or tabindex == "0" or role == "article":
                                    emit_fn("info", f"  [{i}] label='{lbl[:80] if lbl else 'None'}', tabindex={tabindex}, role={role}, class='{(classes or '')[:50]}', jsaction='{(jsaction or '')[:30]}'")
                            except:
                                pass
                    except:
                        pass

                # Process each card - extract business data
                for card in cards:
                    if len(results) >= max_results:
                        break
                    try:
                        # Get name from card's aria-label
                        name = (await card.get_attribute("aria-label") or "").strip()

                        # Clean up name - remove UI elements
                        for suffix in [' · Share', ' · Save', ' · More options', 'Share', 'Save']:
                            if name.endswith(suffix):
                                name = name[:-len(suffix)].strip()

                        # Validate name - must be a reasonable business name
                        if not name or len(name) < 3 or len(name) > 150 or name in seen_names:
                            continue

                        # Skip if name looks like UI text (not a business)
                        skip_keywords = ['results', 'search', 'nearby', 'map', 'navigation', 'directions', 'your location']
                        if any(keyword in name.lower() for keyword in skip_keywords):
                            continue

                        seen_names.add(name)

                        # Click card to open side panel for additional data
                        await card.click(timeout=3000)
                        await page.wait_for_timeout(3000)

                        # Category / Business Type - from card or side panel
                        category = business_type
                        try:
                            # Try to get from side panel first
                            for cat_selector in [
                                'div[aria-label*="Category"] span',
                                'span.fontBodyMedium',
                                'div.fontBodyMedium span',
                            ]:
                                cat_el = page.locator(cat_selector).first
                                if await cat_el.count() > 0:
                                    cat_text = (await cat_el.inner_text(timeout=1000) or "").strip()
                                    if not cat_text:
                                        continue
                                    # Reject pure numbers — these are ratings (4.7, 4.9), not categories
                                    if re.match(r'^\d+\.?\d*$', cat_text):
                                        continue
                                    if '·' in cat_text and len(cat_text) < 100:
                                        candidate = cat_text.split('·')[0].strip()
                                        if candidate and not re.match(r'^\d+\.?\d*$', candidate):
                                            category = candidate
                                            break
                                    elif len(cat_text) < 50:
                                        category = cat_text
                                        break
                        except:
                            pass

                        # Rating
                        rating = ""
                        try:
                            rating_el = page.locator('div[aria-label*="star"] span, span[aria-label*="star"]').first
                            if await rating_el.count() > 0:
                                rating_text = await rating_el.inner_text(timeout=1000)
                                import re
                                rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                                if rating_match:
                                    rating = rating_match.group(1)
                        except:
                            pass

                        # Review Count
                        review_count = ""
                        try:
                            review_el = page.locator('div[aria-label*="review"], span[aria-label*="review"]').first
                            if await review_el.count() > 0:
                                review_text = await review_el.inner_text(timeout=1000)
                                import re
                                review_match = re.search(r'(\d[\d,]*)', review_text)
                                if review_match:
                                    review_count = review_match.group(1).replace(',', '')
                        except:
                            pass

                        # Phone Number - try multiple approaches
                        phone = ""
                        try:
                            # Method 1: Look for tel: links
                            phone_links = await page.locator('a[href^="tel:"]').all()
                            for plink in phone_links:
                                href = await plink.get_attribute("href")
                                if href and href.startswith("tel:"):
                                    phone = href.replace("tel:", "").strip()
                                    break

                            # Method 2: Look for phone button
                            if not phone:
                                phone_btn = page.locator('button[data-item-id*="phone"]').first
                                if await phone_btn.count() > 0:
                                    phone_text = await phone_btn.inner_text(timeout=1000)
                                    import re
                                    phone_match = re.search(r'([\d\s\-\+\(\)]{8,})', phone_text)
                                    if phone_match:
                                        phone = phone_match.group(1).strip()

                            # Method 3: Look for phone in text
                            if not phone:
                                phone_div = page.locator('div[data-item-id*="phone"]').first
                                if await phone_div.count() > 0:
                                    phone_text = await phone_div.inner_text(timeout=1000)
                                    import re
                                    phone_match = re.search(r'([\d\s\-\+\(\)]{8,})', phone_text)
                                    if phone_match:
                                        phone = phone_match.group(1).strip()
                        except Exception as e:
                            emit_fn("warn", f"  Phone extraction failed: {str(e)[:50]}")
                            pass

                        # Website URL
                        website_url = ""
                        try:
                            # Look for website link
                            web_link = page.locator('a[data-item-id="authority"], a[aria-label*="Website" i], a[aria-label*="website" i]').first
                            if await web_link.count() > 0:
                                href = await web_link.get_attribute("href")
                                if href and href.startswith("http"):
                                    website_url = href.split("?")[0]
                        except:
                            pass

                        # Address
                        address = ""
                        try:
                            addr_btn = page.locator('button[data-item-id="address"]').first
                            if await addr_btn.count() > 0:
                                address = await addr_btn.inner_text(timeout=1000)
                                if address:
                                    address = address.strip()
                                    # Strip leading emoji / map-pin icon (📍 and similar)
                                    address = re.sub(r'^[^\w\(]+', '', address).strip()
                        except:
                            pass

                        # Plus Code
                        plus_code = ""
                        try:
                            plus_el = page.locator('div[data-item-id*="plus_code"]').first
                            if await plus_el.count() > 0:
                                plus_code = await plus_el.inner_text(timeout=1000)
                                plus_code = plus_code.strip() if plus_code else ""
                        except:
                            pass

                        # Close side panel
                        try:
                            await page.keyboard.press("Escape")
                            await page.wait_for_timeout(500)
                        except:
                            pass

                        results.append({
                            "name": name,
                            "category": category or business_type,
                            "website_url": website_url,
                            "phone": phone,
                            "address": address,
                            "rating": rating,
                            "review_count": review_count,
                            "plus_code": plus_code,
                        })

                        emit_fn("info", f"  ✅ Extracted: {name[:50]}")

                    except Exception as e:
                        emit_fn("warn", f"  Card processing failed: {str(e)[:50]}")
                        try:
                            await page.keyboard.press("Escape")
                            await page.wait_for_timeout(500)
                        except:
                            pass
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
