"""
processor/lead_model.py
-----------------------
Columns: Company Name | Email(s) | Business Type | Website URL | City | Country | Scraped At
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List

CSV_HEADERS = [
    "Company Name",
    "Email(s)",
    "Business Type",
    "Website URL",
    "City",
    "Country",
    "Scraped At",
]


@dataclass
class Lead:
    company_name:  str       = ""
    email:         List[str] = field(default_factory=list)
    business_type: str       = ""
    website_url:   str       = ""
    city:          str       = ""
    country:       str       = ""
    source_query:  str       = ""
    scraped_at:    str       = field(
        default_factory=lambda: datetime.utcnow().isoformat(timespec="seconds") + "Z"
    )

    def to_csv_row(self) -> dict:
        return {
            "Company Name":  self.company_name,
            "Email(s)":      "; ".join(self.email),
            "Business Type": self.business_type,
            "Website URL":   self.website_url,
            "City":          self.city,
            "Country":       self.country,
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
        )
