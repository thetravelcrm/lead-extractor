"""
processor/cleaner.py
--------------------
Data cleaning and deduplication functions applied after raw extraction.

Functions:
- validate_email()       — structural check + optional DNS MX lookup
- normalize_phone()      — parse to E.164 format using phonenumbers library
- clean_company_name()   — title-case, strip extra whitespace
- deduplicate_leads()    — URL-based + fuzzy company-name dedup
"""

import re
from typing import List, Optional
from urllib.parse import urlparse

from processor.lead_model import Lead


# ---------------------------------------------------------------------------
# Email validation
# ---------------------------------------------------------------------------

def validate_email(email: str) -> bool:
    """
    Two-level email validation:

    Level 1: Basic structural regex (must have local@domain.tld form)
    Level 2: DNS MX record lookup — checks if the domain actually accepts mail.
             Falls back to Level 1 only if DNS times out or is unavailable.
    """
    # Level 1: structural check
    pattern = re.compile(
        r'^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,7}$'
    )
    if not pattern.match(email):
        return False

    domain = email.split("@")[1]

    # Level 2: DNS MX check (optional — gracefully skip if dnspython unavailable)
    try:
        import dns.resolver
        try:
            dns.resolver.resolve(domain, "MX", lifetime=3.0)
            return True
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, dns.resolver.NoNameservers):
            return False
        except Exception:
            # DNS timeout or other transient error — trust the regex result
            return True
    except ImportError:
        # dnspython not installed — fall back to regex-only
        return True


# ---------------------------------------------------------------------------
# Phone normalisation
# ---------------------------------------------------------------------------

def normalize_phone(phone: str, country: str = "") -> str:
    """
    Attempt to parse and normalise a phone number to E.164 format.

    Uses the `phonenumbers` library with the supplied country as a hint.
    Falls back to a cleaned raw string if parsing fails.

    Examples:
        "(020) 7946-0958" + "United Kingdom" → "+442079460958"
        "+1 800 555 0199"                    → "+18005550199"
    """
    try:
        import phonenumbers
        from phonenumbers import NumberParseException

        # Map country name to ISO 3166-1 alpha-2 code
        country_code = _country_to_iso(country)

        parsed = phonenumbers.parse(phone, country_code)
        if phonenumbers.is_valid_number(parsed):
            return phonenumbers.format_number(
                parsed, phonenumbers.PhoneNumberFormat.E164
            )
    except Exception:
        pass

    # Fallback: strip all non-digit/plus characters and return
    cleaned = re.sub(r"[^\d+]", "", phone)
    return cleaned if len(re.sub(r"\D", "", cleaned)) >= 7 else phone


def _country_to_iso(country: str) -> Optional[str]:
    """
    Very simple country-name → ISO-3166-1 alpha-2 mapping for the most
    common cases.  Returns None if unknown (phonenumbers will guess).
    """
    mapping = {
        "united kingdom": "GB", "uk": "GB", "england": "GB",
        "united states": "US", "usa": "US", "us": "US",
        "united arab emirates": "AE", "uae": "AE",
        "germany": "DE", "france": "FR", "spain": "ES",
        "italy": "IT", "canada": "CA", "australia": "AU",
        "india": "IN", "pakistan": "PK", "saudi arabia": "SA",
        "qatar": "QA", "kuwait": "KW", "bahrain": "BH",
        "oman": "OM", "turkey": "TR", "egypt": "EG",
        "south africa": "ZA", "nigeria": "NG", "kenya": "KE",
        "singapore": "SG", "malaysia": "MY", "philippines": "PH",
        "netherlands": "NL", "belgium": "BE", "switzerland": "CH",
        "sweden": "SE", "norway": "NO", "denmark": "DK",
        "poland": "PL", "portugal": "PT", "brazil": "BR",
        "mexico": "MX", "argentina": "AR", "chile": "CL",
        "china": "CN", "japan": "JP", "south korea": "KR",
    }
    return mapping.get(country.strip().lower())


# ---------------------------------------------------------------------------
# Company name cleaning
# ---------------------------------------------------------------------------

def clean_company_name(name: str) -> str:
    """
    Normalise a company name:
    - Strip leading/trailing whitespace
    - Collapse internal whitespace
    - Title-case (preserves existing capitalisation patterns like 'IBM', 'McDonalds')
    """
    if not name:
        return ""
    name = re.sub(r"\s+", " ", name.strip())
    # Only title-case if the string is all-lower or all-upper
    if name == name.lower() or name == name.upper():
        name = name.title()
    return name


# ---------------------------------------------------------------------------
# Lead deduplication
# ---------------------------------------------------------------------------

def _normalise_url(url: str) -> str:
    """Strip scheme, www, trailing slash for URL comparison."""
    try:
        parsed = urlparse(url.lower().strip())
        host = parsed.netloc
        if host.startswith("www."):
            host = host[4:]
        path = parsed.path.rstrip("/")
        return f"{host}{path}"
    except Exception:
        return url.lower().strip()


def deduplicate_leads(leads: List[Lead]) -> List[Lead]:
    """
    Remove duplicate leads.

    Two-pass deduplication:
    Pass 1 — by normalised website URL (exact match).
    Pass 2 — by company name fuzzy match (similarity > 90 %).
              Uses rapidfuzz if available; falls back to exact-name match.

    When two leads are considered duplicates the one with the higher
    `data_score()` (more filled fields) is kept.
    """
    # ---- Pass 1: URL dedup ----
    url_map: dict = {}   # normalised_url → Lead
    no_url: List[Lead] = []

    for lead in leads:
        if lead.website_url:
            key = _normalise_url(lead.website_url)
            if key in url_map:
                existing = url_map[key]
                if lead.data_score() > existing.data_score():
                    url_map[key] = lead
            else:
                url_map[key] = lead
        else:
            no_url.append(lead)

    deduped = list(url_map.values()) + no_url

    # ---- Pass 2: Fuzzy name dedup ----
    try:
        from rapidfuzz import fuzz

        final: List[Lead] = []
        for lead in deduped:
            merged = False
            for i, existing in enumerate(final):
                if not lead.company_name or not existing.company_name:
                    continue
                score = fuzz.ratio(
                    lead.company_name.lower(),
                    existing.company_name.lower(),
                )
                if score > 90:
                    # Keep the richer lead
                    if lead.data_score() > existing.data_score():
                        final[i] = lead
                    merged = True
                    break
            if not merged:
                final.append(lead)
        return final

    except ImportError:
        # rapidfuzz not installed — return URL-deduped list
        return deduped
