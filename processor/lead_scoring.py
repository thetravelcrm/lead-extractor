"""
processor/lead_scoring.py
--------------------------
Lead quality scoring and data cleaning engine.

Features:
- Lead quality scoring based on data completeness
- Data cleaning: remove duplicates, junk data, normalize fields
- Company name normalization
- Email deduplication and validation
- Phone normalization
"""

from typing import List, Dict, Optional
import re


def calculate_lead_score(lead_data: Dict) -> int:
    """
    Calculate lead quality score based on data completeness.

    Scoring:
    - Email found → +50 (per email, max 100)
    - Phone found → +30
    - WhatsApp found → +20
    - Website → +10
    - Address → +10
    - City + Country → +10

    Max score: 180
    """
    score = 0

    # Email scoring: +50 per email, max 100
    emails = lead_data.get("emails", [])
    if emails:
        score += min(len(emails) * 50, 100)

    # Phone scoring: +30
    phones = lead_data.get("phones", [])
    if phones:
        score += 30

    # WhatsApp scoring: +20
    whatsapp = lead_data.get("whatsapp", "")
    if whatsapp:
        score += 20

    # Website scoring: +10
    website = lead_data.get("website_url", "") or lead_data.get("website", "")
    if website:
        score += 10

    # Address scoring: +10
    address = lead_data.get("address", "")
    if address:
        score += 10

    # City + Country scoring: +10
    city = lead_data.get("city", "")
    country = lead_data.get("country", "")
    if city and country:
        score += 10

    return min(score, 180)


def get_lead_grade(score: int) -> str:
    """
    Get lead grade based on score.

    A: 130-180 (Excellent)
    B: 90-129 (Good)
    C: 50-89 (Average)
    D: 0-49 (Poor)
    """
    if score >= 130:
        return "A"
    elif score >= 90:
        return "B"
    elif score >= 50:
        return "C"
    else:
        return "D"


def clean_and_normalize_lead(lead_data: Dict) -> Dict:
    """
    Clean and normalize lead data.

    - Remove duplicate emails
    - Remove duplicate phones
    - Normalize phone numbers
    - Clean company name
    - Separate city and country from address
    """
    cleaned = lead_data.copy()

    # Clean emails
    emails = cleaned.get("emails", [])
    if emails:
        # Lowercase and deduplicate
        cleaned_emails = []
        seen = set()
        for email in emails:
            email = email.lower().strip()
            if email and email not in seen and is_valid_email(email):
                cleaned_emails.append(email)
                seen.add(email)
        cleaned["emails"] = cleaned_emails

    # Clean phones
    phones = cleaned.get("phones", [])
    if phones:
        cleaned_phones = []
        seen = set()
        for phone in phones:
            normalized = normalize_phone(phone)
            if normalized and normalized not in seen:
                cleaned_phones.append(normalized)
                seen.add(normalized)
        cleaned["phones"] = cleaned_phones

    # Clean WhatsApp
    whatsapp = cleaned.get("whatsapp", "")
    if whatsapp:
        cleaned["whatsapp"] = normalize_phone(whatsapp)

    # Clean company name
    name = cleaned.get("name", "") or cleaned.get("company_name", "")
    if name:
        cleaned["name"] = clean_company_name(name)

    # Clean website URL
    website = cleaned.get("website_url", "") or cleaned.get("website", "")
    if website:
        cleaned["website_url"] = clean_url(website)

    # Separate city and country from address if needed
    address = cleaned.get("address", "")
    if address and not cleaned.get("city"):
        city, country = extract_city_country_from_address(address)
        cleaned["city"] = city
        cleaned["country"] = country

    # Calculate lead score
    cleaned["lead_score"] = calculate_lead_score(cleaned)
    cleaned["lead_grade"] = get_lead_grade(cleaned["lead_score"])

    return cleaned


def is_valid_email(email: str) -> bool:
    """Validate email format."""
    return bool(re.match(r'^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,7}$', email))


def normalize_phone(phone: str) -> Optional[str]:
    """
    Normalize phone number.

    - Remove spaces, dashes, parentheses
    - Keep + prefix
    - Validate minimum length
    """
    if not phone:
        return None

    phone = phone.strip()
    has_plus = phone.startswith('+')

    # Remove non-digit characters except +
    cleaned = re.sub(r'[^\d+]', '', phone)

    # Add + if missing and length is reasonable
    if not has_plus and len(cleaned) >= 10:
        if len(cleaned) == 10:  # Indian number
            cleaned = '+91' + cleaned
        else:
            cleaned = '+' + cleaned

    # Validate
    digits = re.sub(r'[^\d]', '', cleaned)
    if len(digits) < 7 or len(set(digits)) <= 1:
        return None

    return cleaned


def clean_company_name(name: str) -> str:
    """
    Clean company name.

    - Remove extra whitespace
    - Title case
    - Remove common suffixes like " - Google Maps"
    """
    if not name:
        return ""

    # Remove Google Maps artifacts
    for suffix in [' - Google Maps', ' | Google Maps', ' - Home | Facebook', ' | Facebook']:
        if name.endswith(suffix):
            name = name[:-len(suffix)]

    # Clean whitespace
    name = re.sub(r'\s+', ' ', name).strip()

    # Title case (but preserve acronyms)
    words = name.split()
    cleaned_words = []
    for word in words:
        if word.isupper() and len(word) <= 4:
            # Keep acronyms uppercase
            cleaned_words.append(word)
        else:
            cleaned_words.append(word.title())

    return ' '.join(cleaned_words)


def clean_url(url: str) -> str:
    """Clean and normalize URL."""
    if not url:
        return ""

    # Remove query parameters
    url = url.split('?')[0].split('&')[0]

    # Ensure https://
    if url.startswith('http://'):
        url = url.replace('http://', 'https://')
    elif not url.startswith('https://'):
        url = 'https://' + url

    return url.strip()


def extract_city_country_from_address(address: str) -> tuple:
    """
    Extract city and country from address string.

    Heuristic: Last part after comma is country/city
    """
    if not address:
        return "", ""

    parts = [p.strip() for p in address.split(',')]

    if len(parts) >= 2:
        # Last part is likely country, second-to-last is city
        country = parts[-1]
        city = parts[-2]
        return city, country
    elif len(parts) == 1:
        # Could be just city or just country
        return parts[0], ""

    return "", ""


def remove_duplicate_companies(leads: List[Dict]) -> List[Dict]:
    """
    Remove duplicate companies based on name similarity.

    Uses fuzzy matching if rapidfuzz is available, otherwise exact match.
    """
    try:
        from rapidfuzz import fuzz
        use_fuzzy = True
    except ImportError:
        use_fuzzy = False

    unique_leads = []
    seen_names = set()

    for lead in leads:
        name = lead.get("name", "").lower().strip()

        if use_fuzzy:
            # Check for fuzzy duplicates
            is_duplicate = False
            for seen_name in seen_names:
                similarity = fuzz.ratio(name, seen_name)
                if similarity > 90:
                    # Merge data: keep the richer lead
                    existing = next((l for l in unique_leads if l.get("name", "").lower() == seen_name), None)
                    if existing:
                        merge_leads(existing, lead)
                    is_duplicate = True
                    break

            if not is_duplicate:
                unique_leads.append(lead)
                seen_names.add(name)
        else:
            # Exact match
            if name not in seen_names:
                unique_leads.append(lead)
                seen_names.add(name)

    return unique_leads


def merge_leads(existing: Dict, new: Dict) -> None:
    """Merge new lead data into existing lead (enriches existing)."""
    # Merge emails
    existing_emails = set(existing.get("emails", []))
    new_emails = set(new.get("emails", []))
    existing["emails"] = list(existing_emails | new_emails)

    # Merge phones
    existing_phones = set(existing.get("phones", []))
    new_phones = set(new.get("phones", []))
    existing["phones"] = list(existing_phones | new_phones)

    # Fill missing fields
    if not existing.get("website_url") and new.get("website_url"):
        existing["website_url"] = new["website_url"]

    if not existing.get("whatsapp") and new.get("whatsapp"):
        existing["whatsapp"] = new["whatsapp"]

    if not existing.get("address") and new.get("address"):
        existing["address"] = new["address"]

    # Recalculate score
    existing["lead_score"] = calculate_lead_score(existing)
    existing["lead_grade"] = get_lead_grade(existing["lead_score"])
