"""
scraper/extractor.py
--------------------
Pure-function extraction layer — takes raw HTML / text and returns structured
data using regex patterns.

Functions:
- extract_emails()        — find all valid-looking email addresses
- extract_phones()        — find all phone/mobile numbers
- extract_whatsapp()      — find WhatsApp deep-links (wa.me/...)
- extract_company_name()  — infer company name from HTML metadata
"""

import re
from typing import List
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from config.settings import (
    EMAIL_PATTERN,
    PHONE_PATTERN,
    WHATSAPP_HREF_PATTERN,
    BLOCKED_EMAIL_PREFIXES,
    BLOCKED_EMAIL_DOMAINS,
    BLOCKED_EMAIL_EXTENSIONS,
)


# ---------------------------------------------------------------------------
# Email extraction
# ---------------------------------------------------------------------------

def extract_emails(text: str) -> List[str]:
    """
    Find all email addresses in the given text.

    Applies multiple filter layers:
    1. Regex structural match
    2. Extension blacklist (e.g. noreply@domain.png)
    3. Prefix blacklist (noreply, postmaster, …)
    4. Domain blacklist (example.com, schema.org, …)
    """
    raw_matches = EMAIL_PATTERN.findall(text)
    cleaned: List[str] = []
    seen: set = set()

    for email in raw_matches:
        email = email.strip().lower()

        # Skip if it looks like a file extension rather than a real TLD
        if any(email.endswith(ext) for ext in BLOCKED_EMAIL_EXTENSIONS):
            continue

        try:
            local, domain = email.rsplit("@", 1)
        except ValueError:
            continue

        # Skip blocked local-parts (prefixes)
        if local in BLOCKED_EMAIL_PREFIXES:
            continue

        # Skip blocked domains
        if domain in BLOCKED_EMAIL_DOMAINS:
            continue

        # Skip obviously fake / placeholder addresses
        if "example" in domain or "yourdomain" in domain or "placeholder" in domain:
            continue

        # Deduplicate
        if email in seen:
            continue
        seen.add(email)
        cleaned.append(email)

    return cleaned


# ---------------------------------------------------------------------------
# Phone extraction
# ---------------------------------------------------------------------------

# Minimum number of digits a valid phone number should have
_MIN_DIGITS = 7

def _count_digits(s: str) -> int:
    return sum(1 for c in s if c.isdigit())


def extract_phones(text: str) -> List[str]:
    """
    Find all phone numbers in the given text.

    Filters out:
    - Strings with fewer than 7 digits (years, zip codes, etc.)
    - Pure repeating sequences like 0000000
    - Strings that are clearly not phone numbers
    """
    raw_matches = PHONE_PATTERN.findall(text)
    cleaned: List[str] = []
    seen: set = set()

    for match in raw_matches:
        phone = match.strip()

        if _count_digits(phone) < _MIN_DIGITS:
            continue

        # Filter out pure-repeating sequences (0000000, 1111111…)
        digits_only = re.sub(r"\D", "", phone)
        if len(set(digits_only)) == 1:
            continue

        # Normalise whitespace and deduplicate
        phone_norm = re.sub(r"\s+", " ", phone).strip()
        if phone_norm in seen:
            continue
        seen.add(phone_norm)
        cleaned.append(phone_norm)

    return cleaned


# ---------------------------------------------------------------------------
# WhatsApp extraction
# ---------------------------------------------------------------------------

def extract_whatsapp(html: str) -> List[str]:
    """
    Extract phone numbers embedded in WhatsApp deep-links.

    Matches:
    - https://wa.me/1234567890
    - https://api.whatsapp.com/send?phone=1234567890
    """
    matches = WHATSAPP_HREF_PATTERN.findall(html)
    cleaned: List[str] = []
    seen: set = set()

    for number in matches:
        number = number.strip()
        if _count_digits(number) < _MIN_DIGITS:
            continue
        if number not in seen:
            seen.add(number)
            cleaned.append(number)

    return cleaned


# ---------------------------------------------------------------------------
# Company name extraction
# ---------------------------------------------------------------------------

def extract_company_name(soup: BeautifulSoup, url: str) -> str:
    """
    Try to determine the company name using the following priority:

    1. <meta property="og:site_name"> (most reliable)
    2. <title> tag — stripped of common suffixes like '| Company | UK'
    3. Domain name — humanised (strip www., replace hyphens with spaces, title-case)
    """
    # 1. Open Graph site name
    og_site = soup.find("meta", property="og:site_name")
    if og_site and og_site.get("content", "").strip():
        return og_site["content"].strip()

    # 2. Title tag
    title_tag = soup.find("title")
    if title_tag:
        title = title_tag.get_text(strip=True)
        # Remove common trailing separators and their content
        # e.g. "Acme Plumbing | London | UK" → "Acme Plumbing"
        for sep in [" | ", " - ", " – ", " — ", " :: ", " / "]:
            if sep in title:
                title = title.split(sep)[0].strip()
        if title:
            return title

    # 3. Domain name humanised
    try:
        host = urlparse(url).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        # Drop TLD
        name = host.rsplit(".", 1)[0]
        # Replace separators
        name = name.replace("-", " ").replace("_", " ")
        return name.title()
    except Exception:
        pass

    return ""
