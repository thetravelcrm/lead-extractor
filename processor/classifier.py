"""
processor/classifier.py
-----------------------
Classifies a company into a business category using keyword-frequency scoring.

The primary source of truth is the user's own input (they told us what type
of business they're looking for).  We canonicalise that input and use it as
the category unless the website text strongly suggests something different.

Functions:
- classify_business()       — main classification entry point
- get_canonical_category()  — map user input to a known category name
"""

from config.settings import BUSINESS_KEYWORDS


# ---------------------------------------------------------------------------
# Canonical category lookup
# ---------------------------------------------------------------------------

def get_canonical_category(input_type: str) -> str:
    """
    Map a user-supplied business type string to a canonical category name
    from BUSINESS_KEYWORDS.

    Case-insensitive partial match.  Returns the canonical name if found,
    or the original input (title-cased) if no match.

    Examples:
        "restaurants"  → "Restaurant / Food"
        "it company"   → "IT / Technology"
        "law"          → "Law / Legal"
    """
    input_lower = input_type.strip().lower()

    for category, keywords in BUSINESS_KEYWORDS.items():
        # Direct match against category name
        if input_lower in category.lower():
            return category
        # Match against any keyword in the category
        for kw in keywords:
            if kw in input_lower or input_lower in kw:
                return category

    # No match — return the input cleaned up
    return input_type.strip().title()


# ---------------------------------------------------------------------------
# Business classification by keyword frequency
# ---------------------------------------------------------------------------

def classify_business(
    company_name: str,
    page_text: str,
    input_business_type: str,
) -> str:
    """
    Determine the nature of a company's business.

    Algorithm:
    1. Try to canonicalise the user's input_business_type → if it maps to a
       known category, use that directly (user already told us what we're
       looking for).
    2. Score the page text against all keyword lists.  If there's a clear
       winner (top score >= 3 hits), use it to confirm / override.
    3. Fall back to the canonicalised input_business_type.

    Parameters
    ----------
    company_name       : str — company name (included in scored text)
    page_text          : str — visible text from the company's website
    input_business_type: str — the business type the user searched for

    Returns
    -------
    str — canonical category name (e.g. "Construction", "IT / Technology")
    """
    # Step 1: canonicalise user input
    canonical = get_canonical_category(input_business_type)

    # Step 2: score the page text
    combined = (company_name + " " + page_text).lower()

    scores: dict = {}
    for category, keywords in BUSINESS_KEYWORDS.items():
        score = sum(combined.count(kw.lower()) for kw in keywords)
        if score > 0:
            scores[category] = score

    if scores:
        top_category = max(scores, key=scores.get)
        top_score = scores[top_category]

        # Only override the user's intent if the evidence is very strong
        if top_score >= 5 and top_category != canonical:
            return top_category

    return canonical
