"""
storage/csv_writer.py
---------------------
Write lead data to CSV files.

Two write modes:
1. Incremental / streaming  — append_lead_csv() writes one row at a time
   as each site is processed.  Protects against data loss if the job is
   interrupted.
2. Batch                    — write_leads_csv() writes all leads at once
   at the end of a job (used for a final clean output).

CSV files are written with UTF-8-sig BOM so they open correctly in Excel
on Windows without needing to change encoding settings.
"""

import csv
import os
import tempfile
from typing import List

from processor.lead_model import Lead, CSV_HEADERS


# ---------------------------------------------------------------------------
# File path helper
# ---------------------------------------------------------------------------

def get_csv_path(job_id: str) -> str:
    """Return the path to the CSV file for the given job ID."""
    tmp_dir = tempfile.gettempdir()
    return os.path.join(tmp_dir, f"leads_{job_id}.csv")


# ---------------------------------------------------------------------------
# Incremental write (one row at a time)
# ---------------------------------------------------------------------------

def append_lead_csv(lead: Lead, job_id: str) -> None:
    """
    Append a single lead row to the CSV file.

    Creates the file with a header row if it doesn't exist yet.
    Uses 'a' mode so existing data is never overwritten.
    """
    path = get_csv_path(job_id)
    file_exists = os.path.exists(path)

    with open(path, mode="a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS, quoting=csv.QUOTE_ALL)
        if not file_exists:
            writer.writeheader()
        writer.writerow(lead.to_csv_row())


# ---------------------------------------------------------------------------
# Batch write (all leads at once)
# ---------------------------------------------------------------------------

def write_leads_csv(leads: List[Lead], job_id: str) -> str:
    """
    Write all leads to a CSV file, overwriting any existing file.

    Returns the path to the written file.
    """
    path = get_csv_path(job_id)

    with open(path, mode="w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for lead in leads:
            writer.writerow(lead.to_csv_row())

    return path
