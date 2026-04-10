"""
scraper/advanced_email_extractor.py
------------------------------------
Advanced email extraction with obfuscation detection and validation.

Features:
- Extract ALL emails from HTML (not just first one)
- Detect obfuscated emails:
    - info [at] domain.com
    - info at domain dot com
    - info (at) domain.com
    - info[at]domain.com
- Remove duplicates and invalid emails
- Filter junk (image filenames, CSS, JS files)
- Email quality scoring
"""

import re
from typing import List, Set, Dict, Tuple
from urllib.parse import unquote


# Obfuscated email patterns
OBFUSCATION_PATTERNS = [
    # info [at] domain.com
    r'([a-zA-Z0-9._%+\-]+)\s*\[at\]\s*([a-zA-Z0-9.\-]+)\s*\.\s*([a-zA-Z]{2,7})',
    # info at domain dot com
    r'([a-zA-Z0-9._%+\-]+)\s+at\s+([a-zA-Z0-9.\-]+)\s+dot\s+([a-zA-Z]{2,7})',
    # info (at) domain.com
    r'([a-zA-Z0-9._%+\-]+)\s*\(at\)\s*([a-zA-Z0-9.\-]+)\s*\.\s*([a-zA-Z]{2,7})',
    # info[at]domain.com
    r'([a-zA-Z0-9._%+\-]+)\[at\]([a-zA-Z0-9.\-]+)\.([a-zA-Z]{2,7})',
]

# Standard email pattern
STANDARD_EMAIL_PATTERN = re.compile(
    r'[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,7}'
)

# Junk email patterns to filter out
JUNK_PATTERNS = [
    r'\.(png|jpg|jpeg|gif|svg|webp|ico|css|js|woff|ttf)$',  # File extensions
    r'example\.com$',  # Example domains
    r'domain\.com$',
    r'test\.com$',
]

# Generic email prefixes to filter (optional - keep if you want more leads)
GENERIC_PREFIXES = frozenset({
    "noreply", "no-reply", "donotreply", "mailer-daemon",
    "postmaster", "bounce",
})

# High-value email prefixes (always keep)
HIGH_VALUE_PREFIXES = frozenset({
    "info", "contact", "admin", "support", "sales",
    "hello", "team", "help", "marketing",
    "service", "services", "manager", "reception",
})


def extract_all_emails(html: str, text: str = "") -> Dict:
    """
    Extract ALL emails from HTML and text content.

    Handles:
    - Standard emails: info@domain.com
    - Obfuscated emails: info [at] domain.com
    - mailto: links
    - URL-encoded emails
    - Emails in data attributes
    - Emails in JavaScript variables

    Returns:
        {
            "emails": ["email1@domain.com", "email2@domain.com"],
            "count": int,
            "sources": {"mailto": [], "text": [], "html": [], "obfuscated": []},
        }
    """
    emails = []
    sources = {
        "mailto": [],
        "text": [],
        "html": [],
        "obfuscated": [],
    }

    # Decode URL encoding first
    html_decoded = unquote(html)
    text_decoded = unquote(text) if text else ""

    # 1. Extract from mailto: links
    mailto_emails = extract_mailto_links(html_decoded)
    emails.extend(mailto_emails)
    sources["mailto"].extend(mailto_emails)

    # 2. Extract standard emails from HTML
    html_emails = extract_standard_emails(html_decoded)
    emails.extend(html_emails)
    sources["html"].extend(html_emails)

    # 3. Extract standard emails from text
    text_emails = extract_standard_emails(text_decoded) if text_decoded else []
    emails.extend(text_emails)
    sources["text"].extend(text_emails)

    # 4. Extract obfuscated emails
    obfuscated_emails = extract_obfuscated_emails(html_decoded + " " + text_decoded)
    emails.extend(obfuscated_emails)
    sources["obfuscated"].extend(obfuscated_emails)

    # 5. Extract from data attributes
    data_attr_emails = extract_from_data_attributes(html_decoded)
    emails.extend(data_attr_emails)

    # 6. Extract from JavaScript variables
    js_emails = extract_from_javascript(html_decoded)
    emails.extend(js_emails)

    # Deduplicate and validate
    unique_emails = list(dict.fromkeys(emails))  # Preserve order
    valid_emails = []

    for email in unique_emails:
        email = email.lower().strip()
        if is_valid_email(email) and not is_junk_email(email):
            if email not in valid_emails:
                valid_emails.append(email)

    return {
        "emails": valid_emails,
        "count": len(valid_emails),
        "sources": sources,
    }


def extract_mailto_links(html: str) -> List[str]:
    """Extract emails from mailto: links."""
    mailto_pattern = re.compile(
        r'mailto:([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,7})',
        re.IGNORECASE
    )
    return list(set(m.lower() for m in mailto_pattern.findall(html)))


def extract_standard_emails(text: str) -> List[str]:
    """Extract standard emails from text."""
    return list(set(
        e.lower() for e in STANDARD_EMAIL_PATTERN.findall(text)
        if is_valid_email(e)
    ))


def extract_obfuscated_emails(text: str) -> List[str]:
    """Extract obfuscated emails and convert to standard format."""
    emails = []

    for pattern in OBFUSCATION_PATTERNS:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            if len(match) == 3:
                local, domain, tld = match
                email = f"{local.strip()}@{domain.strip()}.{tld.strip()}".lower()
                if is_valid_email(email):
                    emails.append(email)

    return list(set(emails))


def extract_from_data_attributes(html: str) -> List[str]:
    """Extract emails from data-* attributes."""
    # Match data-email, data-contact, data-mail, etc.
    data_pattern = re.compile(
        r'data[-_](?:email|contact|mail)["\']?\s*=\s*["\']([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,7})["\']',
        re.IGNORECASE
    )
    return list(set(m.lower() for m in data_pattern.findall(html)))


def extract_from_javascript(html: str) -> List[str]:
    """Extract emails from JavaScript variables."""
    # Match common JS email patterns
    js_patterns = [
        r'["\']([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,7})["\']',
        r'var\s+\w*\s*=?\s*["\']([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,7})["\']',
    ]

    emails = []
    for pattern in js_patterns:
        matches = re.findall(pattern, html)
        emails.extend(matches)

    return list(set(e.lower() for e in emails if is_valid_email(e)))


def is_valid_email(email: str) -> bool:
    """Validate email format."""
    # Basic format check
    if not re.match(r'^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,7}$', email):
        return False

    # Domain must have at least one dot
    parts = email.split('@')
    if len(parts) != 2:
        return False

    domain = parts[1]
    if '.' not in domain:
        return False

    # TLD must be at least 2 chars
    tld = domain.split('.')[-1]
    if len(tld) < 2:
        return False

    return True


def is_junk_email(email: str) -> bool:
    """Check if email is junk (image filenames, CSS, JS, etc.)."""
    email_lower = email.lower()

    # Check against junk patterns
    for pattern in JUNK_PATTERNS:
        if re.search(pattern, email_lower):
            return True

    return False


def score_email_quality(email: str) -> int:
    """
    Score email quality for prioritization.

    High-value: info@, contact@, sales@ → +30
    Personal: john@, jane@ → +50
    Generic: noreply@ → 0
    """
    local = email.split('@')[0].lower()

    if local in HIGH_VALUE_PREFIXES:
        return 30
    elif local in GENERIC_PREFIXES:
        return 0
    else:
        # Likely personal/specific email
        return 50
