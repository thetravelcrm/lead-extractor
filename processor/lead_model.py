"""
processor/lead_model.py
-----------------------
Defines the Lead dataclass — the single data structure that flows through the
entire pipeline from extraction to storage.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List

# Column order used for both CSV and Google Sheets output
CSV_HEADERS = [
    "Company Name",
    "Email(s)",
    "Phone(s)",
    "WhatsApp",
    "Business Type",
    "Website URL",
    "Country",
    "Scraped At",
]


@dataclass
class Lead:
    """Represents a single extracted company lead."""

    company_name: str = ""
    email: List[str] = field(default_factory=list)
    phone: List[str] = field(default_factory=list)
    whatsapp: List[str] = field(default_factory=list)
    website_url: str = ""
    business_type: str = ""
    country: str = ""
    source_query: str = ""
    scraped_at: str = field(default_factory=lambda: datetime.utcnow().isoformat(timespec="seconds") + "Z")

    # -----------------------------------------------------------------------
    # Serialisation helpers
    # -----------------------------------------------------------------------

    def to_csv_row(self) -> dict:
        """Return a dict keyed by CSV_HEADERS, with lists flattened to '; ' strings."""
        return {
            "Company Name":  self.company_name,
            "Email(s)":      "; ".join(self.email),
            "Phone(s)":      "; ".join(self.phone),
            "WhatsApp":      "; ".join(self.whatsapp),
            "Business Type": self.business_type,
            "Website URL":   self.website_url,
            "Country":       self.country,
            "Scraped At":    self.scraped_at,
        }

    def to_sheets_row(self) -> list:
        """Return a list in the same column order as CSV_HEADERS."""
        row = self.to_csv_row()
        return [row[h] for h in CSV_HEADERS]

    def data_score(self) -> int:
        """
        Returns a simple quality score used by the deduplicator to keep
        the 'richest' version of a lead when merging duplicates.
        """
        return (
            len(self.email) * 3
            + len(self.phone) * 2
            + len(self.whatsapp) * 2
            + (1 if self.company_name else 0)
            + (1 if self.website_url else 0)
        )
