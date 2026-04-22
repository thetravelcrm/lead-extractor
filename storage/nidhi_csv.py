"""
storage/nidhi_csv.py
--------------------
Write NIDHI portal records to CSV.
Columns: Name, Email, WhatsApp, Website, Location, Category
"""

import csv
import os
import tempfile
from typing import List, Dict

NIDHI_CSV_HEADERS = ["Name", "Email", "WhatsApp", "Website", "Location", "Category"]


def get_nidhi_csv_path(job_id: str) -> str:
    return os.path.join(tempfile.gettempdir(), f"nidhi_{job_id}.csv")


def write_nidhi_csv(records: List[Dict], job_id: str) -> str:
    """Write all records at once; returns file path."""
    path = get_nidhi_csv_path(job_id)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=NIDHI_CSV_HEADERS,
                                quoting=csv.QUOTE_ALL,
                                extrasaction="ignore")
        writer.writeheader()
        for rec in records:
            writer.writerow({
                "Name":     rec.get("name", ""),
                "Email":    rec.get("email", ""),
                "WhatsApp": rec.get("whatsapp", ""),
                "Website":  rec.get("website", ""),
                "Location": rec.get("location", ""),
                "Category": rec.get("category", ""),
            })
    return path
