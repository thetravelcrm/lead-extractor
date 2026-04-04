"""
storage/sheets_writer.py
------------------------
Optional Google Sheets integration.

Uses a Google Service Account (free tier) so the app can write to Sheets
without OAuth browser prompts.  The user must:
  1. Create a Google Cloud project and enable Sheets + Drive APIs (free).
  2. Create a Service Account and download the JSON key as credentials.json.
  3. Share their target spreadsheet with the service account's email address.

Free tier limits:
  - 300 Sheets API requests/minute
  - No daily cost

All writes use worksheet.append_rows() (batch) to minimise API calls.
Exponential back-off handles transient APIError responses.
"""

import os
import time
from typing import List, Optional

from processor.lead_model import Lead, CSV_HEADERS
from config.settings import GOOGLE_CREDENTIALS_PATH, GOOGLE_SCOPES


# ---------------------------------------------------------------------------
# Credentials check
# ---------------------------------------------------------------------------

def check_sheets_credentials() -> bool:
    """
    Return True if a credentials.json file exists at the configured path
    and appears to be a valid JSON file.
    """
    path = GOOGLE_CREDENTIALS_PATH
    if not os.path.isfile(path):
        return False
    try:
        import json
        with open(path, "r") as f:
            data = json.load(f)
        return "client_email" in data and "private_key" in data
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Client factory
# ---------------------------------------------------------------------------

def get_sheets_client():
    """
    Return an authenticated gspread client using a service account.

    Raises ImportError if gspread / google-auth are not installed.
    Raises FileNotFoundError if credentials.json is missing.
    Raises ValueError if credentials are invalid.
    """
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError as e:
        raise ImportError(
            "gspread and google-auth are required for Sheets integration. "
            "Run: pip install gspread google-auth"
        ) from e

    creds_path = GOOGLE_CREDENTIALS_PATH
    if not os.path.isfile(creds_path):
        raise FileNotFoundError(
            f"Google credentials file not found: {creds_path}. "
            "Download your service account JSON key and save it as credentials.json."
        )

    creds = Credentials.from_service_account_file(creds_path, scopes=GOOGLE_SCOPES)
    return gspread.authorize(creds)


# ---------------------------------------------------------------------------
# Sheet helper
# ---------------------------------------------------------------------------

def get_or_create_sheet(client, spreadsheet_id: str, sheet_name: str = "Leads"):
    """
    Open the spreadsheet by ID and return the named worksheet.
    Creates the worksheet if it doesn't exist.
    Writes the header row if the sheet is empty.
    """
    import gspread

    spreadsheet = client.open_by_key(spreadsheet_id)

    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=sheet_name, rows=1000, cols=len(CSV_HEADERS)
        )

    # Write header if the sheet is empty
    if worksheet.row_count == 0 or not worksheet.row_values(1):
        worksheet.append_row(CSV_HEADERS, value_input_option="USER_ENTERED")

    return worksheet


# ---------------------------------------------------------------------------
# Batch append with retry
# ---------------------------------------------------------------------------

def append_leads_to_sheet(
    leads: List[Lead],
    spreadsheet_id: str,
    sheet_name: str = "Leads",
) -> None:
    """
    Append all leads to the Google Sheets spreadsheet in one batch call.

    Uses exponential back-off (up to 3 retries) on API errors to handle
    transient rate-limiting from the free tier.
    """
    if not leads:
        return

    client = get_sheets_client()
    worksheet = get_or_create_sheet(client, spreadsheet_id, sheet_name)

    rows = [lead.to_sheets_row() for lead in leads]

    max_retries = 3
    for attempt in range(max_retries):
        try:
            worksheet.append_rows(rows, value_input_option="USER_ENTERED")
            return
        except Exception as exc:
            if attempt < max_retries - 1:
                wait = 2 ** attempt * 5  # 5s, 10s, 20s
                time.sleep(wait)
            else:
                raise RuntimeError(
                    f"Failed to write to Google Sheets after {max_retries} attempts: {exc}"
                ) from exc
