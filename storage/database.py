"""
storage/database.py
-------------------
SQLite database layer for persistent search history and listing tracking.

Tables:
  - searches: Stores unique search queries with metadata
  - listings: Stores individual business listings from Google Maps
  - extracted_leads: Stores extracted email leads linked to listings

Workflow:
  1. User searches "travel agency in Lucknow" → stored in `searches`
  2. Google Maps results (~300+) → stored in `listings` (status: pending)
  3. User requests 50 emails → extract from 50 pending listings → status: extracted
  4. Next time: shows "250 remaining" → user can extract more
"""

import os
import tempfile
import sqlite3
import json
import random
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from contextlib import contextmanager

# Database file path - use /tmp for Docker/HF Spaces compatibility
# Falls back to local data/ directory if writable
def _get_db_path():
    """Get database path, preferring /tmp for Docker containers."""
    # Try /tmp first (always writable in Docker/HF Spaces)
    tmp_path = os.path.join(tempfile.gettempdir(), "lead_extractor.db")
    return tmp_path

DB_PATH = _get_db_path()


def _ensure_db_dir():
    """Ensure the database directory exists (not needed for /tmp)."""
    global DB_PATH
    db_dir = os.path.dirname(DB_PATH)
    try:
        os.makedirs(db_dir, exist_ok=True)
    except PermissionError:
        # If can't create directory, use /tmp as fallback
        DB_PATH = os.path.join(tempfile.gettempdir(), "lead_extractor.db")


@contextmanager
def get_db():
    """Context manager for database connections."""
    _ensure_db_dir()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")  # Better concurrency
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Initialize database tables if they don't exist."""
    _ensure_db_dir()
    with get_db() as conn:
        conn.executescript("""
            -- Search queries with metadata
            CREATE TABLE IF NOT EXISTS searches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query TEXT NOT NULL UNIQUE,
                country TEXT NOT NULL,
                city TEXT DEFAULT '',
                business_type TEXT NOT NULL,
                total_listings INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            -- Individual business listings from Google Maps
            CREATE TABLE IF NOT EXISTS listings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                search_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                category TEXT DEFAULT '',
                website_url TEXT DEFAULT '',
                phone TEXT DEFAULT '',
                address TEXT DEFAULT '',
                rating TEXT DEFAULT '',
                review_count TEXT DEFAULT '',
                plus_code TEXT DEFAULT '',
                status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'extracted', 'enriching', 'enriched', 'failed', 'no_website')),
                extracted_at TEXT,
                lead_data TEXT,  -- JSON: {emails, company_name, business_type, etc.}
                enrichment_data TEXT,  -- JSON: {linkedin_urls: [], instagram_urls: [], ...}
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (search_id) REFERENCES searches(id) ON DELETE CASCADE
            );

            -- Index for fast queries
            CREATE INDEX IF NOT EXISTS idx_listings_search_id ON listings(search_id);
            CREATE INDEX IF NOT EXISTS idx_listings_status ON listings(status);
            CREATE INDEX IF NOT EXISTS idx_searches_query ON searches(query);
        """)


# ---------------------------------------------------------------------------
# Search Operations
# ---------------------------------------------------------------------------

def upsert_search(
    query: str,
    country: str,
    business_type: str,
    city: str = "",
    total_listings: int = 0,
) -> int:
    """
    Insert or update a search query.
    Returns the search ID.
    If query exists, updates total_listings and updated_at.
    """
    now = datetime.utcnow().isoformat()
    with get_db() as conn:
        cursor = conn.execute("""
            INSERT INTO searches (query, country, city, business_type, total_listings, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(query) DO UPDATE SET
                total_listings = excluded.total_listings,
                updated_at = excluded.updated_at
        """, (query, country, city, business_type, total_listings, now, now))

        # Get the ID (whether inserted or existing)
        row = conn.execute("SELECT id FROM searches WHERE query = ?", (query,)).fetchone()
        return row["id"]


def get_search_by_query(query: str) -> Optional[Dict]:
    """Get search details by query string."""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM searches WHERE query = ?", (query,)).fetchone()
        return dict(row) if row else None


def get_search_stats(query: str) -> Dict:
    """
    Get statistics for a search query:
    - total_listings: Total found on Google Maps
    - extracted: Count of extracted listings
    - pending: Count of pending listings
    - failed: Count of failed listings
    - remaining: Pending + failed (can retry)
    """
    with get_db() as conn:
        search = conn.execute("SELECT * FROM searches WHERE query = ?", (query,)).fetchone()
        if not search:
            return None

        stats = conn.execute("""
            SELECT
                status,
                COUNT(*) as count
            FROM listings
            WHERE search_id = ?
            GROUP BY status
        """, (search["id"],)).fetchall()

        result = {
            "id": search["id"],
            "query": search["query"],
            "country": search["country"],
            "city": search["city"],
            "business_type": search["business_type"],
            "total_listings": search["total_listings"],
            "created_at": search["created_at"],
            "updated_at": search["updated_at"],
            "extracted": 0,
            "pending": 0,
            "failed": 0,
            "no_website": 0,
        }

        for row in stats:
            result[row["status"]] = row["count"]

        result["remaining"] = result["pending"] + result["failed"]
        return result


def get_all_searches() -> List[Dict]:
    """Get all searches with their stats (for sidebar)."""
    with get_db() as conn:
        searches = conn.execute("""
            SELECT * FROM searches
            ORDER BY updated_at DESC
        """).fetchall()

        result = []
        for search in searches:
            stats = conn.execute("""
                SELECT status, COUNT(*) as count
                FROM listings
                WHERE search_id = ?
                GROUP BY status
            """, (search["id"],)).fetchall()

            search_dict = dict(search)
            search_dict["extracted"] = 0
            search_dict["pending"] = 0
            search_dict["failed"] = 0
            search_dict["no_website"] = 0

            for row in stats:
                search_dict[row["status"]] = row["count"]

            search_dict["remaining"] = search_dict["pending"] + search_dict["failed"]
            result.append(search_dict)

        return result


def delete_search(query: str):
    """Delete a search and all its listings."""
    with get_db() as conn:
        conn.execute("DELETE FROM searches WHERE query = ?", (query,))


# ---------------------------------------------------------------------------
# Listing Operations
# ---------------------------------------------------------------------------

def bulk_insert_listings(search_id: int, listings: List[Dict]) -> int:
    """
    Insert multiple listings for a search.
    Returns count of inserted listings.
    If listings already exist (same search_id + website_url), skip them.
    """
    now = datetime.utcnow().isoformat()
    inserted = 0

    with get_db() as conn:
        for listing in listings:
            # Check if listing already exists for this search
            existing = conn.execute("""
                SELECT id FROM listings
                WHERE search_id = ? AND (website_url = ? OR name = ?)
            """, (search_id, listing.get("website_url", ""), listing.get("name", ""))).fetchone()

            if existing:
                # Update existing record with new data if available
                conn.execute("""
                    UPDATE listings
                    SET phone = ?, address = ?, rating = ?, review_count = ?, plus_code = ?, updated_at = ?
                    WHERE id = ?
                """, (
                    listing.get("phone", ""),
                    listing.get("address", ""),
                    listing.get("rating", ""),
                    listing.get("review_count", ""),
                    listing.get("plus_code", ""),
                    now,
                    existing["id"]
                ))
                inserted += 1
                continue

            conn.execute("""
                INSERT INTO listings (search_id, name, category, website_url, phone, address, rating, review_count, plus_code, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                search_id,
                listing.get("name", ""),
                listing.get("category", ""),
                listing.get("website_url", ""),
                listing.get("phone", ""),
                listing.get("address", ""),
                listing.get("rating", ""),
                listing.get("review_count", ""),
                listing.get("plus_code", ""),
                "pending" if listing.get("website_url") else "no_website",
                now,
                now,
            ))
            inserted += 1

    return inserted


def get_pending_listings(search_id: int, limit: int = 50) -> List[Dict]:
    """
    Get random pending (or failed) listings for extraction.
    Returns up to `limit` listings.
    """
    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM listings
            WHERE search_id = ? AND status IN ('pending', 'failed')
            ORDER BY RANDOM()
            LIMIT ?
        """, (search_id, limit)).fetchall()

        return [dict(row) for row in rows]


def get_extracted_listings(search_id: int) -> List[Dict]:
    """Get all extracted listings for a search."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM listings
            WHERE search_id = ? AND status = 'extracted' AND lead_data IS NOT NULL
        """, (search_id,)).fetchall()

        result = []
        for row in rows:
            listing = dict(row)
            if listing["lead_data"]:
                listing["lead_data"] = json.loads(listing["lead_data"])
            result.append(listing)

        return result


def update_listing_status(listing_id: int, status: str, lead_data: Optional[Dict] = None):
    """Update listing status and optionally save extracted lead data."""
    now = datetime.utcnow().isoformat()
    with get_db() as conn:
        conn.execute("""
            UPDATE listings
            SET status = ?, extracted_at = ?, lead_data = ?
            WHERE id = ?
        """, (status, now, json.dumps(lead_data) if lead_data else None, listing_id))


def bulk_update_listing_status(listing_ids: List[int], status: str):
    """Update status for multiple listings."""
    now = datetime.utcnow().isoformat()
    with get_db() as conn:
        for listing_id in listing_ids:
            conn.execute("""
                UPDATE listings
                SET status = ?, extracted_at = ?
                WHERE id = ?
            """, (status, now, listing_id))


def get_all_extracted_leads(search_id: int) -> List[Dict]:
    """Get all extracted lead data for a search (for CSV export)."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM listings
            WHERE search_id = ? AND status = 'extracted' AND lead_data IS NOT NULL
            ORDER BY extracted_at ASC
        """, (search_id,)).fetchall()

        result = []
        for row in rows:
            if row["lead_data"]:
                lead = json.loads(row["lead_data"])
                lead["_listing_id"] = row["id"]
                lead["_extracted_at"] = row["extracted_at"]
                result.append(lead)

        return result


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def clear_all_data():
    """Delete all data (for testing/reset)."""
    with get_db() as conn:
        conn.execute("DELETE FROM listings")
        conn.execute("DELETE FROM searches")
        # Reset autoincrement
        conn.execute("DELETE FROM sqlite_sequence")
