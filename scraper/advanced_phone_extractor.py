"""
scraper/advanced_phone_extractor.py
------------------------------------
Advanced phone and WhatsApp extraction with normalization.

Features:
- Extract phones from:
    - tel: links
    - WhatsApp deep links (wa.me, api.whatsapp.com)
    - Labeled patterns: "Phone:", "Mobile:", "WhatsApp:", "Contact:"
    - Standalone international patterns
- Normalize to E.164 format
- Detect WhatsApp-specific numbers
- Remove duplicates
- Separate mobile vs landline detection
"""

import re
from typing import List, Dict, Tuple, Optional
from urllib.parse import unquote


# WhatsApp patterns
WHATSAPP_PATTERNS = [
    r'https?://wa\.me/(\+?[\d]+)',
    r'https?://api\.whatsapp\.com/send\?.*?phone=(\+?[\d]+)',
    r'whatsapp.*?(\+?[\d\s\-\(\)]{8,})',
]

# Phone label patterns
PHONE_LABEL_PATTERNS = [
    r'(?:phone|tel|telephone|mobile|cell|call|contact|whatsapp)[:\s]+(\+?[\d\s\-\(\)]{8,})',
    r'(?:m|p|t|tel|mob)[:\s]+(\+?[\d\s\-\(\)]{8,})',
]

# Standalone international phone pattern
INTL_PHONE_PATTERN = re.compile(
    r'(\+\d{1,4}[\s\-\(\)]?\d{2,4}[\s\-\(\)]?\d{3,4}[\s\-\(\)]?\d{3,4})'
)


def extract_all_phones(html: str, text: str = "") -> Dict:
    """
    Extract ALL phone numbers and WhatsApp links from content.

    Returns:
        {
            "phones": ["+919876543210", "+919876543211"],
            "whatsapp": ["+919876543210"],
            "count": int,
            "sources": {"tel": [], "whatsapp": [], "labels": [], "standalone": []},
        }
    """
    phones = []
    whatsapp_numbers = []
    sources = {
        "tel": [],
        "whatsapp": [],
        "labels": [],
        "standalone": [],
    }

    # Decode URL encoding
    html_decoded = unquote(html)
    text_decoded = unquote(text) if text else ""

    # 1. Extract from tel: links
    tel_phones = extract_tel_links(html_decoded)
    phones.extend(tel_phones)
    sources["tel"].extend(tel_phones)

    # 2. Extract from WhatsApp links
    wa_phones = extract_whatsapp_links(html_decoded)
    phones.extend(wa_phones)
    whatsapp_numbers.extend(wa_phones)
    sources["whatsapp"].extend(wa_phones)

    # 3. Extract from labeled patterns
    label_phones = extract_labeled_phones(html_decoded + " " + text_decoded)
    phones.extend(label_phones)
    sources["labels"].extend(label_phones)

    # 4. Extract standalone international numbers
    standalone_phones = extract_standalone_phones(html_decoded + " " + text_decoded)
    phones.extend(standalone_phones)
    sources["standalone"].extend(standalone_phones)

    # Normalize and deduplicate
    normalized_phones = []
    normalized_whatsapp = []

    for phone in phones:
        normalized = normalize_phone(phone)
        if normalized and normalized not in normalized_phones:
            normalized_phones.append(normalized)

    for phone in whatsapp_numbers:
        normalized = normalize_phone(phone)
        if normalized and normalized not in normalized_whatsapp:
            normalized_whatsapp.append(normalized)

    # If no explicit WhatsApp numbers found, try to detect mobile numbers
    if not normalized_whatsapp and normalized_phones:
        # Assume first phone could be WhatsApp
        for phone in normalized_phones:
            if is_possible_mobile(phone):
                normalized_whatsapp.append(phone)
                break

    return {
        "phones": normalized_phones,
        "whatsapp": normalized_whatsapp,
        "count": len(normalized_phones),
        "sources": sources,
    }


def extract_tel_links(html: str) -> List[str]:
    """Extract phone numbers from tel: links."""
    tel_pattern = re.compile(
        r'href=["\']tel:(\+?[\d\s\-\(\)]{8,})["\']',
        re.IGNORECASE
    )
    return [m.strip() for m in tel_pattern.findall(html)]


def extract_whatsapp_links(html: str) -> List[str]:
    """Extract phone numbers from WhatsApp deep links."""
    phones = []

    for pattern in WHATSAPP_PATTERNS:
        matches = re.findall(pattern, html, re.IGNORECASE)
        for match in matches:
            # Clean up the number
            cleaned = re.sub(r'[^\d+]', '', match).strip()
            if cleaned and len(cleaned) >= 8:
                # Add + if missing
                if not cleaned.startswith('+'):
                    cleaned = '+' + cleaned
                phones.append(cleaned)

    return list(set(phones))


def extract_labeled_phones(text: str) -> List[str]:
    """Extract phone numbers from labeled patterns."""
    phones = []

    for pattern in PHONE_LABEL_PATTERNS:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            cleaned = match.strip()
            if len(cleaned) >= 8 and has_min_digits(cleaned, 7):
                phones.append(cleaned)

    return list(set(phones))


def extract_standalone_phones(text: str) -> List[str]:
    """Extract standalone international phone numbers."""
    matches = INTL_PHONE_PATTERN.findall(text)
    return [m.strip() for m in matches if has_min_digits(m, 7)]


def normalize_phone(phone: str) -> Optional[str]:
    """
    Normalize phone number to clean format.

    - Removes spaces, dashes, parentheses
    - Keeps + prefix
    - Validates minimum length
    """
    if not phone:
        return None

    # Clean up
    cleaned = phone.strip()

    # Check if it has + prefix
    has_plus = cleaned.startswith('+')

    # Remove all non-digit characters except +
    cleaned = re.sub(r'[^\d+]', '', cleaned)

    # If no + but has country code, add it
    if not has_plus and len(cleaned) >= 10:
        # Assume Indian number if 10 digits
        if len(cleaned) == 10:
            cleaned = '+91' + cleaned
        # Could be US/Canada
        elif len(cleaned) == 11 and cleaned.startswith('1'):
            cleaned = '+' + cleaned
        else:
            cleaned = '+' + cleaned

    # Validate minimum length (country code + number)
    digits_only = re.sub(r'[^\d]', '', cleaned)
    if len(digits_only) < 7:
        return None

    # Check for pure repeating sequences (junk)
    if len(set(digits_only)) <= 1:
        return None

    return cleaned


def is_possible_mobile(phone: str) -> bool:
    """
    Check if phone number is likely a mobile number (WhatsApp-capable).

    Heuristics:
    - Indian mobile: +91 followed by 10 digits starting with 6-9
    - US mobile: +1 followed by 10 digits
    - General: 10-15 digits total
    """
    digits = re.sub(r'[^\d]', '', phone)

    # Indian mobile
    if phone.startswith('+91') and len(digits) == 12:
        mobile_part = digits[2:3]  # Third digit
        return mobile_part in '6789'

    # US/Canada
    if phone.startswith('+1') and len(digits) == 11:
        return True

    # General case: 10-15 digits
    return 10 <= len(digits) <= 15


def has_min_digits(phone: str, min_digits: int) -> bool:
    """Check if phone has minimum number of digits."""
    digits = re.sub(r'[^\d]', '', phone)
    return len(digits) >= min_digits


def score_phone_quality(phone: str) -> int:
    """
    Score phone quality for prioritization.

    Mobile/WhatsApp → +50
    Landline → +20
    Invalid → 0
    """
    if not phone:
        return 0

    if is_possible_mobile(phone):
        return 50
    else:
        return 20
