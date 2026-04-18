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
            matched_feed_selector = 'div[role="feed"]'  # default

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
                    matched_feed_selector = selector
                    break
                except PWTimeout:
                    continue
            
            if not feed_found:
                emit_fn("warn", "No results feed found — Maps may have changed layout or blocked the request.")
                try:
                    page_title = await page.title()
                    page_url = page.url
                    emit_fn("warn", f"Page title: {page_title}, URL: {page_url[:80]}")
                except Exception:
                    pass
                # Signal to caller that Maps was blocked (empty list)
                return []

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
                    feed = page.locator(matched_feed_selector).first
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
                            best_selector = f"{matched_feed_selector} div[tabindex='0']"
                            emit_fn("info", f"Strategy 1: Found {len(valid_cards)} business listings")
                except Exception as e:
                    emit_fn("warn", f"Strategy 1 failed: {str(e)[:100]}")

                # Strategy 2: Try role="article" elements
                if not cards:
                    try:
                        articles = await page.locator(f'{matched_feed_selector} div[role="article"]').all()
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
                            best_selector = f"{matched_feed_selector} div[role='article']"
                            emit_fn("info", f"Strategy 2: Found {len(valid)} listings")
                    except Exception as e:
                        emit_fn("warn", f"Strategy 2 failed: {str(e)[:100]}")

                # Strategy 3: Try Nv2PK class (Google Maps listing class)
                if not cards:
                    try:
                        nv2pk = await page.locator(f'{matched_feed_selector} div.Nv2PK').all()
                        if nv2pk:
                            cards = nv2pk
                            best_selector = f"{matched_feed_selector} div.Nv2PK"
                            emit_fn("info", f"Strategy 3: Found {len(nv2pk)} listings via Nv2PK class")
                    except Exception as e:
                        emit_fn("warn", f"Strategy 3 failed: {str(e)[:100]}")

                # Strategy 4: Try fontBodyMedium (text container class)
                if not cards:
                    try:
                        font_medium = await page.locator(f'{matched_feed_selector} div.fontBodyMedium').all()
                        if font_medium:
                            cards = font_medium
                            best_selector = f"{matched_feed_selector} div.fontBodyMedium"
                            emit_fn("info", f"Strategy 4: Found {len(font_medium)} listings via fontBodyMedium")
                    except Exception as e:
                        emit_fn("warn", f"Strategy 4 failed: {str(e)[:100]}")

                if cards:
                    emit_fn("info", f"Using: {best_selector}. Total unique results: {len(results)}")
                else:
                    emit_fn("warn", "No valid business cards found. Scrolling to load more...")
                    # DEBUG: Log what's actually in the feed to understand structure
                    try:
                        feed_debug = page.locator(matched_feed_selector).first
                        child_count = await feed_debug.locator('div').count()
                        emit_fn("info", f"DEBUG: Feed ({matched_feed_selector}) has {child_count} div elements")
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

                        # Extract website from the card's Website button BEFORE clicking
                        # (avoids depending on side panel async load)
                        card_website_url = ""
                        try:
                            card_web_btn = card.locator(
                                'a[aria-label*="website" i], '
                                'a[aria-label*="Visit" i], '
                                'a[data-item-id="authority"]'
                            ).first
                            if await card_web_btn.count() > 0:
                                href = await card_web_btn.get_attribute("href") or ""
                                # Decode Google redirect: google.com/url?q=https://real-site.com
                                if "google.com/url" in href:
                                    from urllib.parse import parse_qs, urlparse as _urlparse
                                    qs = parse_qs(_urlparse(href).query)
                                    href = qs.get("q", qs.get("url", [href]))[0]
                                if href.startswith("http"):
                                    _CARD_SKIP = {"google.com", "google.co", "goo.gl", "wa.me",
                                                  "wa.link", "facebook.com", "instagram.com",
                                                  "youtube.com", "c.petrol"}
                                    if not any(d in href for d in _CARD_SKIP):
                                        card_website_url = href.split("?")[0]
                        except Exception:
                            pass

                        # Track previous result's website to detect stale panel reads
                        _prev_website = results[-1]["website_url"] if results else ""

                        # Click card — scroll into view first, then click
                        prev_url = page.url
                        try:
                            await card.scroll_into_view_if_needed(timeout=2000)
                        except Exception:
                            pass
                        try:
                            await card.click(timeout=5000)
                        except Exception:
                            await card.click(force=True, timeout=5000)
                        try:
                            # Wait for URL to change (step 1: navigation started)
                            await page.wait_for_url(lambda u: u != prev_url, timeout=5000)
                            # Wait for h1 to contain this company's name (step 2: content rendered)
                            name_check = re.sub(r'[^a-zA-Z0-9 ]', '', name)[:15].strip().lower()
                            if name_check:
                                try:
                                    await page.wait_for_function(
                                        f"document.querySelector('h1') && "
                                        f"document.querySelector('h1').textContent.toLowerCase().includes('{name_check}')",
                                        timeout=4000
                                    )
                                except Exception:
                                    await page.wait_for_timeout(1500)
                            else:
                                await page.wait_for_timeout(2000)
                            # Step 3: if previous company had a website, wait for that link
                            # to update so we don't read stale data
                            if _prev_website:
                                escaped = _prev_website.replace("'", "\\'").replace("\\", "\\\\")
                                try:
                                    await page.wait_for_function(
                                        "(function(){"
                                        "var el=document.querySelector('a[data-item-id=\"authority\"]');"
                                        f"return !el || el.href!=='{escaped}';"
                                        "})();",
                                        timeout=3000
                                    )
                                except Exception:
                                    await page.wait_for_timeout(800)
                        except Exception:
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

                        # Website URL — multiple strategies to handle Google Maps' varied rendering
                        website_url = ""
                        _SKIP_DOMAINS = {
                            "google.com", "google.co", "goo.gl", "wa.me",
                            "wa.link", "facebook.com", "instagram.com", "twitter.com",
                            "youtube.com", "maps.app", "c.petrol",
                        }
                        # Match any element type — Google Maps uses a, button, div for website links
                        _web_sel = (
                            '[data-item-id="authority"], '
                            'a[aria-label*="website" i], '
                            'button[aria-label*="website" i], '
                            'a[aria-label*="Open website" i], '
                            'a[aria-label*="Visit" i]'
                        )
                        try:
                            await page.wait_for_selector(_web_sel, timeout=3000)
                        except Exception:
                            pass

                        try:
                            web_el = page.locator(_web_sel).first
                            if await web_el.count() > 0:
                                # Strategy A: standard href attribute
                                href = await web_el.get_attribute("href") or ""
                                if href.startswith("http") and not any(d in href for d in _SKIP_DOMAINS):
                                    website_url = href.split("?")[0]

                                # Strategy B: data-url / data-href attribute
                                if not website_url:
                                    for attr in ("data-url", "data-href", "data-value"):
                                        val = await web_el.get_attribute(attr) or ""
                                        if val.startswith("http") and not any(d in val for d in _SKIP_DOMAINS):
                                            website_url = val.split("?")[0]
                                            break

                                # Strategy C: inner text looks like a domain (e.g. "sulekhaholidays.com")
                                if not website_url:
                                    txt = (await web_el.inner_text(timeout=1000) or "").strip()
                                    if txt and "." in txt and " " not in txt and len(txt) < 80:
                                        if not any(d in txt for d in _SKIP_DOMAINS):
                                            website_url = ("https://" + txt) if not txt.startswith("http") else txt
                        except Exception:
                            pass

                        # Broader fallback: scan any external <a> in the side panel
                        if not website_url:
                            try:
                                panel_links = await page.locator(
                                    'div[role="main"] a[href^="http"]'
                                ).all()
                                for pl in panel_links:
                                    href = await pl.get_attribute("href") or ""
                                    # Decode Google redirect URLs
                                    if "google.com/url" in href:
                                        from urllib.parse import parse_qs, urlparse as _up
                                        qs = parse_qs(_up(href).query)
                                        href = qs.get("q", qs.get("url", [href]))[0]
                                    if href.startswith("http") and not any(d in href for d in _SKIP_DOMAINS):
                                        website_url = href.split("?")[0]
                                        break
                            except Exception:
                                pass

                        # Fallback: use website found directly on the card button
                        if not website_url and card_website_url:
                            website_url = card_website_url

                        # Last resort: scan panel text for domain-like patterns
                        if not website_url:
                            try:
                                panel = page.locator('div[role="main"]').first
                                if await panel.count() > 0:
                                    panel_text = await panel.inner_text(timeout=2000)
                                    _dom_pat = re.compile(
                                        r'\b([a-z0-9][a-z0-9\-]*\.[a-z]{2,}(?:\.[a-z]{2})?)\b',
                                        re.IGNORECASE
                                    )
                                    for m in _dom_pat.finditer(panel_text):
                                        dom = m.group(1).lower()
                                        if (dom not in _SKIP_DOMAINS and
                                                not any(s in dom for s in _SKIP_DOMAINS) and
                                                len(dom) > 4):
                                            website_url = f"https://{dom}"
                                            break
                            except Exception:
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
                    matched_feed_selector,
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
            emit_fn("warn", "Google Maps timed out (IP may be rate-limited or blocked).")
        except Exception as exc:
            emit_fn("warn", f"Google Maps error: {exc}")
        finally:
            await browser.close()

    # Clear website URLs that appear for multiple companies — stale panel data.
    # The FIRST company with a given URL keeps it (that's the one whose panel actually loaded).
    # All subsequent companies sharing the same URL get cleared.
    seen_urls: set = set()
    for r in results:
        url = r.get("website_url", "")
        if url:
            if url in seen_urls:
                r["website_url"] = ""
            else:
                seen_urls.add(url)

    emit_fn("success", f"Google Maps: collected {len(results)} listings.")
    return results


async def search_overpass_fallback(
    business_type: str,
    city: str,
    country: str,
    max_results: int,
    emit_fn: Callable,
) -> List[Dict]:
    """
    Fallback business search using Overpass API (OpenStreetMap).
    Free, no bot detection, returns real business data.
    Used when Google Maps is blocked (e.g. HF Spaces datacenter IP).
    Runs synchronous requests in a thread executor to stay async-compatible.
    """
    import asyncio
    import requests as _requests
    import functools

    emit_fn("info", f"Trying Overpass API fallback for: {business_type} in {city}, {country}")

    OSM_TAG_MAP = {
        "travel agency":  [("shop", "travel_agency"),   ("office", "travel_agent")],
        "travel":         [("shop", "travel_agency"),   ("office", "travel_agent")],
        "packers":        [("office", "moving_company"), ("amenity", "storage")],
        "movers":         [("office", "moving_company")],
        "courier":        [("amenity", "post_office"),  ("shop", "courier")],
        "restaurant":     [("amenity", "restaurant"),   ("amenity", "fast_food")],
        "hotel":          [("tourism", "hotel"),        ("tourism", "guest_house")],
        "hospital":       [("amenity", "hospital"),     ("amenity", "clinic")],
        "pharmacy":       [("amenity", "pharmacy")],
        "bank":           [("amenity", "bank")],
        "school":         [("amenity", "school")],
        "gym":            [("leisure", "fitness_centre")],
        "salon":          [("shop", "hairdresser"),     ("shop", "beauty")],
        "cafe":           [("amenity", "cafe")],
        "bar":            [("amenity", "bar"),          ("amenity", "pub")],
        "supermarket":    [("shop", "supermarket")],
        "clinic":         [("amenity", "clinic"),       ("amenity", "doctors")],
        "lawyer":         [("office", "lawyer")],
        "accountant":     [("office", "accountant")],
        "real estate":    [("office", "estate_agent")],
        "insurance":      [("office", "insurance")],
        "catering":       [("amenity", "restaurant"),   ("shop", "catering")],
        "security":       [("office", "security")],
        "it company":     [("office", "company")],
        "software":       [("office", "company")],
        "consultancy":    [("office", "consulting")],
    }

    btype_lower = business_type.lower()
    tags = None
    for key, val in OSM_TAG_MAP.items():
        if key in btype_lower:
            tags = val
            break

    _regex_fallback = tags is None
    if _regex_fallback:
        keyword = btype_lower.split()[0]
        tags = [("__regex__", keyword)]

    headers = {"User-Agent": "LeadExtractorBot/1.0 (lead-extraction-tool)"}

    def _run_overpass() -> List[Dict]:
        # Step 1: Geocode city via Nominatim
        geo_url = (
            "https://nominatim.openstreetmap.org/search"
            f"?q={quote_plus(city + ', ' + country)}&format=json&limit=1"
        )
        try:
            geo_resp = _requests.get(geo_url, headers=headers, timeout=10)
            geo_data = geo_resp.json()
        except Exception as e:
            emit_fn("warn", f"Overpass geocode failed: {e}")
            return []

        if not geo_data:
            emit_fn("warn", "Overpass: could not geocode city.")
            return []

        bbox = geo_data[0].get("boundingbox", [])
        if len(bbox) < 4:
            emit_fn("warn", "Overpass: no bounding box returned.")
            return []

        south, north, west, east = bbox[0], bbox[1], bbox[2], bbox[3]

        tag_queries = ""
        for k, v in tags:
            if k == "__regex__":
                tag_queries += f'  node["name"~"{v}",i]({south},{west},{north},{east});\n'
                tag_queries += f'  way["name"~"{v}",i]({south},{west},{north},{east});\n'
            else:
                tag_queries += f'  node["{k}"="{v}"]({south},{west},{north},{east});\n'
                tag_queries += f'  way["{k}"="{v}"]({south},{west},{north},{east});\n'

        overpass_query = (
            "[out:json][timeout:25];\n"
            "(\n"
            f"{tag_queries}"
            ");\n"
            f"out center {max_results * 3};\n"
        )

        try:
            ov_resp = _requests.post(
                "https://overpass-api.de/api/interpreter",
                data={"data": overpass_query},
                headers=headers,
                timeout=35,
            )
            if ov_resp.status_code != 200:
                emit_fn("warn", f"Overpass HTTP {ov_resp.status_code}")
                return []
            raw_text = ov_resp.text.strip()
            if not raw_text or not raw_text.startswith("{"):
                emit_fn("warn", f"Overpass returned non-JSON response (len={len(raw_text)})")
                return []
            data = ov_resp.json()
        except Exception as e:
            emit_fn("warn", f"Overpass query failed: {e}")
            return []

        elements = data.get("elements", [])
        emit_fn("info", f"Overpass returned {len(elements)} raw elements.")

        results: List[Dict] = []
        seen: set = set()
        for el in elements:
            t = el.get("tags", {})
            name = t.get("name") or t.get("name:en", "")
            if not name or name.lower() in seen:
                continue
            seen.add(name.lower())

            website = t.get("website") or t.get("contact:website") or t.get("url", "")
            phone = t.get("phone") or t.get("contact:phone") or t.get("contact:mobile", "")
            addr_parts = [
                t.get("addr:housenumber", ""),
                t.get("addr:street", ""),
                t.get("addr:city", city),
            ]
            address = ", ".join(p for p in addr_parts if p)

            results.append({
                "name": name,
                "category": business_type,
                "website_url": website,
                "phone": phone,
                "address": address,
                "city": t.get("addr:city", city),
                "country": country,
                "source": "overpass",
            })
            if len(results) >= max_results:
                break
        return results

    loop = asyncio.get_event_loop()
    results = await loop.run_in_executor(None, _run_overpass)
    emit_fn("info", f"Overpass fallback: {len(results)} businesses found.")
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
