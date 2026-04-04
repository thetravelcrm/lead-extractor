"""
config/settings.py
------------------
Central configuration file. All tunable constants live here so you can
adjust behaviour without hunting through the codebase.
"""

import os

# ---------------------------------------------------------------------------
# Flask
# ---------------------------------------------------------------------------
FLASK_SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev-secret-change-me")
FLASK_DEBUG = os.getenv("FLASK_DEBUG", "false").lower() == "true"
MAX_CONCURRENT_JOBS = int(os.getenv("MAX_CONCURRENT_JOBS", "3"))

# ---------------------------------------------------------------------------
# Google Sheets
# ---------------------------------------------------------------------------
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "./credentials.json")
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

# ---------------------------------------------------------------------------
# Anti-bot timing (seconds) — random range (min, max)
# ---------------------------------------------------------------------------
DELAY_BETWEEN_SEARCHES = (3.0, 7.0)   # between Google search pages
DELAY_BETWEEN_VISITS   = (2.0, 5.0)   # between website visits
DELAY_PAGE_LOAD        = (1.5, 3.5)   # after page load before parsing

# ---------------------------------------------------------------------------
# HTTP request settings
# ---------------------------------------------------------------------------
REQUEST_TIMEOUT  = 15   # seconds per request
MAX_RETRIES      = 2
RATE_LIMIT_RPM   = 20   # max requests per minute to external sites

# ---------------------------------------------------------------------------
# User-Agent rotation pool (15 real browser UA strings)
# ---------------------------------------------------------------------------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.144 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 OPR/104.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
]

# ---------------------------------------------------------------------------
# Regex patterns (compiled once, reused everywhere)
# ---------------------------------------------------------------------------
import re

# Email: strict but not overly narrow; handles subdomains
EMAIL_PATTERN = re.compile(
    r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,7}\b'
)

# Phone: international-aware — captures +44 020 style and (020) 1234-5678 style
PHONE_PATTERN = re.compile(
    r'(?<!\d)(\+?(?:[\d]{1,4}[\s\-\.])?(?:\([\d]{1,4}\)[\s\-\.])?(?:[\d]{2,4}[\s\-\.]){2,4}[\d]{2,4})(?!\d)'
)

# WhatsApp deep-links in HTML source
WHATSAPP_HREF_PATTERN = re.compile(
    r'href=["\']https?://(?:wa\.me|api\.whatsapp\.com/send)[/\?](?:phone=)?(\+?[\d]+)',
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Email false-positive filters
# ---------------------------------------------------------------------------
BLOCKED_EMAIL_PREFIXES = frozenset({
    "noreply", "no-reply", "donotreply", "mailer-daemon",
    "postmaster", "bounce", "support", "admin", "webmaster",
    "info",  # too generic — keep if you want more leads
})

BLOCKED_EMAIL_DOMAINS = frozenset({
    "example.com", "example.org", "test.com", "test.org",
    "sentry.io", "schema.org", "w3.org", "wixpress.com",
    "placeholder.com", "yourdomain.com", "domain.com",
})

BLOCKED_EMAIL_EXTENSIONS = frozenset({
    ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp",
    ".css", ".js", ".ico", ".ttf", ".woff",
})

# ---------------------------------------------------------------------------
# Domains excluded from search results (directories, social media, etc.)
# ---------------------------------------------------------------------------
EXCLUDED_DOMAINS = frozenset({
    "facebook.com", "instagram.com", "twitter.com", "x.com",
    "linkedin.com", "youtube.com", "tiktok.com", "pinterest.com",
    "yelp.com", "tripadvisor.com", "yellowpages.com", "whitepages.com",
    "bing.com", "google.com", "wikipedia.org", "wikimedia.org",
    "amazon.com", "amazon.co.uk", "ebay.com",
    "trustpilot.com", "glassdoor.com", "indeed.com",
    "bbb.org", "manta.com", "angieslist.com", "houzz.com",
    "bark.com", "checkatrade.com", "yell.com",
})

# ---------------------------------------------------------------------------
# Business category keyword map
# Used by classifier.py for keyword-frequency scoring
# ---------------------------------------------------------------------------
BUSINESS_KEYWORDS = {
    "Construction": [
        "construction", "contractor", "builder", "building", "renovation",
        "remodel", "carpentry", "roofing", "plumbing", "electrical",
        "civil", "structural", "architect", "excavation",
    ],
    "IT / Technology": [
        "software", "technology", "tech", "it services", "digital",
        "web development", "app development", "cybersecurity", "cloud",
        "networking", "devops", "saas", "programming", "coding",
    ],
    "Tourism / Travel": [
        "tourism", "travel", "tour", "holiday", "vacation", "resort",
        "adventure", "excursion", "sightseeing", "booking", "cruise",
        "safari", "trekking", "hospitality",
    ],
    "Recruitment / HR": [
        "recruitment", "staffing", "hiring", "hr", "human resources",
        "talent", "headhunting", "placement", "workforce", "manpower",
        "careers", "jobs", "employment agency",
    ],
    "Real Estate": [
        "real estate", "realty", "property", "homes", "estate agent",
        "mortgage", "letting", "rental", "landlord", "housing",
        "commercial property", "residential",
    ],
    "Restaurant / Food": [
        "restaurant", "cafe", "bistro", "dining", "eatery", "food",
        "catering", "bakery", "cuisine", "takeaway", "delivery",
        "menu", "chef", "grill",
    ],
    "Hotel / Accommodation": [
        "hotel", "motel", "inn", "lodge", "resort", "bed and breakfast",
        "b&b", "hostel", "accommodation", "guesthouse", "suites",
    ],
    "Law / Legal": [
        "law", "legal", "attorney", "solicitor", "counsel", "barrister",
        "lawyer", "litigation", "compliance", "notary", "advocate",
    ],
    "Healthcare / Medical": [
        "clinic", "medical", "health", "dental", "dentist", "doctor",
        "hospital", "pharmacy", "physiotherapy", "nursing", "care",
        "specialist", "surgeon",
    ],
    "Education": [
        "school", "college", "university", "education", "training",
        "academy", "institute", "tutoring", "learning", "courses",
        "certification",
    ],
    "Finance / Accounting": [
        "finance", "accounting", "accountant", "audit", "tax",
        "bookkeeping", "investment", "banking", "insurance", "wealth",
        "financial advisor",
    ],
    "Manufacturing": [
        "manufacturing", "factory", "production", "industrial",
        "fabrication", "assembly", "machinery", "plant", "processing",
    ],
    "Logistics / Transport": [
        "logistics", "transport", "shipping", "freight", "courier",
        "delivery", "warehouse", "supply chain", "cargo", "fleet",
    ],
    "Marketing / Advertising": [
        "marketing", "advertising", "branding", "seo", "social media",
        "pr", "public relations", "agency", "media", "creative",
    ],
    "Retail": [
        "retail", "shop", "store", "boutique", "outlet", "merchandise",
        "wholesale", "e-commerce", "ecommerce",
    ],
}
