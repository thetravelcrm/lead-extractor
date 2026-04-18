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
    Pass 1 — by (normalised URL + email) compound key.
              This preserves multiple email rows for the same company (one-email-per-row
              design) while still removing exact duplicates.
              No-email rows are deduped by URL alone (keep richer by data_score).
    Pass 2 — by company name fuzzy match (similarity > 90 %).
              Uses rapidfuzz if available; falls back to exact-name match.
    """
    # ---- Pass 1: (URL + email) compound dedup ----
    seen_url_email: set = set()
    url_only_map: dict = {}   # normalised_url → Lead (no-email rows)
    unique: List[Lead] = []

    for lead in leads:
        email_key = lead.email[0].lower() if lead.email else ""
        url_key   = _normalise_url(lead.website_url) if lead.website_url else ""

        if url_key and email_key:
            compound = (url_key, email_key)
            if compound not in seen_url_email:
                seen_url_email.add(compound)
                unique.append(lead)
        elif url_key and not email_key:
            if url_key not in url_only_map:
                url_only_map[url_key] = lead
            elif lead.data_score() > url_only_map[url_key].data_score():
                url_only_map[url_key] = lead
        else:
            unique.append(lead)

    unique.extend(url_only_map.values())

    # ---- Pass 2: Fuzzy name dedup ----
    try:
        from rapidfuzz import fuzz

        final: List[Lead] = []
        for lead in unique:
            merged = False
            for i, existing in enumerate(final):
                if not lead.company_name or not existing.company_name:
                    continue
                score = fuzz.ratio(
                    lead.company_name.lower(),
                    existing.company_name.lower(),
                )
                if score > 95:
                    if lead.data_score() > existing.data_score():
                        final[i] = lead
                    merged = True
                    break
            if not merged:
                final.append(lead)
    except ImportError:
        final = unique

    # ---- Pass 3: (normalized_name + city + phone) cross-source dedup ----
    def _norm(s: str) -> str:
        return re.sub(r'[^a-z0-9]', '', s.lower()) if s else ""

    seen_ncp: set = set()
    deduped_final: List[Lead] = []
    for lead in final:
        phone_key = re.sub(r'\D', '', lead.phone or lead.whatsapp_phone or "")[-10:]
        name_key  = _norm(lead.company_name)[:20]
        city_key  = _norm(lead.city)[:10]
        key = (name_key, city_key, phone_key) if phone_key else None
        if key and key in seen_ncp:
            continue
        if key:
            seen_ncp.add(key)
        deduped_final.append(lead)

    return deduped_final


def add_validation_flags(leads: List[Lead]) -> List[Lead]:
    """
    Set lead.validation_flag for suspicious data:
    - EMAIL_SHARED: same email in 3+ different companies
    - WEBSITE_SHARED: same website for 3+ different companies
    - NO_EMAIL: lead has no email address
    Modifies in-place and returns the list.
    """
    from collections import Counter

    email_counts: Counter = Counter()
    website_counts: Counter = Counter()

    for lead in leads:
        for e in lead.email:
            email_counts[e.lower()] += 1
        if lead.website_url:
            website_counts[_normalise_url(lead.website_url)] += 1

    for lead in leads:
        flags = []
        for e in lead.email:
            if email_counts[e.lower()] >= 3:
                flags.append("EMAIL_SHARED")
                break
        if lead.website_url and website_counts[_normalise_url(lead.website_url)] >= 3:
            flags.append("WEBSITE_SHARED")
        if not lead.email:
            flags.append("NO_EMAIL")
        lead.validation_flag = "|".join(flags) if flags else "OK"

    return leads
