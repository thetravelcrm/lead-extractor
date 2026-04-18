"""
processor/lead_model.py
-----------------------
Columns: Company Name | Email(s) | WhatsApp/Phone | Business Type | Website URL | City | Country | Phone | Address | Rating | Review Count | Plus Code | Scraped At
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List

CSV_HEADERS = [
    "Company Name",
    "Email(s)",
    "WhatsApp/Phone",
    "Business Type",
    "Website URL",
    "City",
    "Country",
    "Phone",
    "Address",
    "Rating",
    "Review Count",
    "Plus Code",
    "Scraped At",
]


@dataclass
class Lead:
    company_name:  str       = ""
    email:         List[str] = field(default_factory=list)
    whatsapp_phone: str      = ""
    business_type: str       = ""
    website_url:   str       = ""
    city:          str       = ""
    country:       str       = ""
    phone:         str       = ""  # Phone from Google Maps
    address:       str       = ""  # Full address from Google Maps
    rating:        str       = ""  # Rating (e.g., 4.8)
    review_count:  str       = ""  # Number of reviews
    plus_code:     str       = ""  # Google Plus Code
    source_query:  str       = ""
    scraped_at:    str       = field(
        default_factory=lambda: datetime.utcnow().isoformat(timespec="seconds") + "Z"
    )

    def to_csv_row(self) -> dict:
        return {
            "Company Name":  self.company_name,
            "Email(s)":      "; ".join(self.email),
            "WhatsApp/Phone": str(self.whatsapp_phone or ""),
            "Business Type": self.business_type,
            "Website URL":   self.website_url,
            "City":          self.city,
            "Country":       self.country,
            "Phone":         str(self.phone or ""),
            "Address":       self.address,
            "Rating":        str(self.rating or ""),
            "Review Count":  str(self.review_count or ""),
            "Plus Code":     str(self.plus_code or ""),
            "Scraped At":    self.scraped_at,
        }

    def to_sheets_row(self) -> list:
        row = self.to_csv_row()
        return [row[h] for h in CSV_HEADERS]

    def data_score(self) -> int:
        return (
            len(self.email) * 3
            + (1 if self.company_name else 0)
            + (1 if self.website_url else 0)
            + (1 if self.business_type else 0)
            + (1 if self.whatsapp_phone else 0)
            + (1 if self.phone else 0)
            + (1 if self.address else 0)
        )
