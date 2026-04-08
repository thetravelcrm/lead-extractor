"""
app.py
------
Flask application — orchestrates the Google Maps → email extraction pipeline.

Pipeline:
  1. Search Google Maps for businesses (company name + category + website URL)
  2. For each listing that has a website: visit it and extract email addresses
  3. For listings without a website: optionally do a detail-panel lookup
  4. Clean and deduplicate leads
  5. Export to CSV (and optionally Google Sheets)

Output columns: Company Name | Email(s) | Business Type | Website URL | Country | Scraped At
"""

import asyncio
import os
import threading
import time
import uuid
from datetime import datetime

from dotenv import load_dotenv
from flask import (Flask, Response, jsonify, render_template,
                   request, send_file, stream_with_context)

load_dotenv()

from config.settings import FLASK_SECRET_KEY, FLASK_DEBUG, MAX_CONCURRENT_JOBS, JOB_TIMEOUT_MINUTES
from sse.event_stream import create_job_queue, emit, event_generator, cancel_job, is_cancelled
from processor.lead_model import Lead
from processor.cleaner import validate_email, clean_company_name, deduplicate_leads
from processor.classifier import classify_business
from scraper.anti_bot import build_session, random_delay, RateLimiter
from scraper.extractor import extract_emails, extract_emails_from_html, extract_company_name
from scraper.website_visitor import visit_website
from scraper.google_search import build_query, search_google_maps
from scraper.web_search import search_emails_for_company
from storage.csv_writer import append_lead_csv, write_leads_csv, get_csv_path
from storage.sheets_writer import check_sheets_credentials, append_leads_to_sheet
from storage.database import (
    init_db, upsert_search, get_search_by_query, get_search_stats,
    get_all_searches, bulk_insert_listings, get_pending_listings,
    get_extracted_listings, update_listing_status, get_all_extracted_leads,
    delete_search
)

# Initialize database on startup
init_db()

# ---------------------------------------------------------------------------
app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY

_jobs: dict = {}
_jobs_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/start", methods=["POST"])
def start_job():
    country       = request.form.get("country", "").strip()
    city          = request.form.get("city", "").strip()
    business_type = request.form.get("business_type", "").strip()
    max_results   = request.form.get("max_results", "50").strip()
    use_sheets    = request.form.get("use_sheets") == "on"
    sheets_id     = request.form.get("sheets_id", "").strip()

    if not country:
        return jsonify({"error": "Country is required."}), 400
    if not business_type:
        return jsonify({"error": "Business type is required."}), 400

    try:
        max_results = max(5, min(int(max_results), 200))
    except ValueError:
        max_results = 50

    if use_sheets and not sheets_id:
        return jsonify({"error": "Spreadsheet ID is required when Google Sheets is enabled."}), 400
    if use_sheets and not check_sheets_credentials():
        return jsonify({"error": "credentials.json not found or invalid."}), 400

    with _jobs_lock:
        active = sum(1 for j in _jobs.values() if j["status"] == "running")
        if active >= MAX_CONCURRENT_JOBS:
            return jsonify({"error": f"Too many concurrent jobs ({active}/{MAX_CONCURRENT_JOBS})."}), 429

    job_id = str(uuid.uuid4())
    params = {
        "country":       country,
        "city":          city,
        "business_type": business_type,
        "max_results":   max_results,
        "use_sheets":    use_sheets,
        "sheets_id":     sheets_id,
    }
    create_job_queue(job_id)

    with _jobs_lock:
        _jobs[job_id] = {
            "status":     "running",
            "params":     params,
            "lead_count": 0,
            "started_at": datetime.utcnow().isoformat(),
        }

    thread = threading.Thread(target=_run_pipeline, args=(job_id, params), daemon=True)
    thread.start()
    return jsonify({"job_id": job_id, "error": None})


@app.route("/stream/<job_id>")
def stream(job_id):
    def generate():
        yield from event_generator(job_id)
    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


@app.route("/download/<job_id>")
def download(job_id):
    path = get_csv_path(job_id)
    if not os.path.isfile(path):
        return jsonify({"error": "CSV not ready yet."}), 404
    job    = _jobs.get(job_id, {})
    params = job.get("params", {})
    fname  = f"leads_{params.get('business_type','leads')}_{params.get('country','')}.csv"
    fname  = fname.replace(" ", "_").lower()
    return send_file(path, as_attachment=True, download_name=fname, mimetype="text/csv")


@app.route("/abort/<job_id>", methods=["POST"])
def abort(job_id):
    cancel_job(job_id)
    with _jobs_lock:
        if job_id in _jobs:
            _jobs[job_id]["status"] = "cancelled"
    return jsonify({"ok": True})


@app.route("/status/<job_id>")
def status(job_id):
    job = _jobs.get(job_id)
    if not job:
        return jsonify({"error": "Unknown job ID"}), 404
    # Also tell the frontend whether a CSV file exists to download
    csv_ready = os.path.isfile(get_csv_path(job_id))
    return jsonify({**job, "csv_ready": csv_ready})


@app.route("/resume/<job_id>")
def resume(job_id):
    """
    Called when the user clicks Resume after an SSE drop.
    Returns the current job status so the frontend knows whether to
    re-open the stream (still running) or just enable Download (done/cancelled).
    """
    job = _jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found. It may have expired (server restarted)."}), 404
    csv_ready = os.path.isfile(get_csv_path(job_id))
    return jsonify({**job, "csv_ready": csv_ready})


# ---------------------------------------------------------------------------
# New Routes for 2-Phase System
# ---------------------------------------------------------------------------

@app.route("/api/search_history", methods=["GET"])
def search_history():
    """Get all previous searches with stats (for sidebar)."""
    try:
        searches = get_all_searches()
        return jsonify({"searches": searches})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/search_status", methods=["POST"])
def search_status():
    """Check if a query already exists and get its stats."""
    query = request.form.get("query", "").strip()
    if not query:
        return jsonify({"error": "Query is required"}), 400

    stats = get_search_stats(query)
    if not stats:
        return jsonify({"exists": False})

    return jsonify({
        "exists": True,
        "stats": stats,
        "message": f"You already have {stats['remaining']} records remaining for this search."
    })


@app.route("/api/search_maps", methods=["POST"])
def search_maps_only():
    """
    Phase 1 only: Search Google Maps and save all listings to database.
    Does NOT extract emails yet.
    """
    country       = request.form.get("country", "").strip()
    city          = request.form.get("city", "").strip()
    business_type = request.form.get("business_type", "").strip()
    max_results   = request.form.get("max_results", "300").strip()

    if not country or not business_type:
        return jsonify({"error": "Country and Business Type are required."}), 400

    try:
        max_results = max(50, min(int(max_results), 500))
    except ValueError:
        max_results = 300

    query = build_query(country, business_type, city)
    job_id = str(uuid.uuid4())

    # Check if query already exists
    existing = get_search_stats(query)
    if existing and request.form.get("force_search") != "true":
        return jsonify({
            "exists": True,
            "stats": existing,
            "message": f"You already have {existing['remaining']} records remaining. Search anyway?"
        }), 409

    # Run Maps search in background thread
    thread = threading.Thread(
        target=_run_maps_search,
        args=(job_id, query, country, city, business_type, max_results),
        daemon=True
    )
    thread.start()

    return jsonify({"job_id": job_id, "query": query})


@app.route("/api/extract_batch", methods=["POST"])
def extract_batch():
    """
    Phase 2: Extract emails from pending listings in batches.
    User specifies how many records they want.
    """
    query = request.form.get("query", "").strip()
    batch_size = request.form.get("batch_size", "50").strip()
    use_sheets = request.form.get("use_sheets") == "on"
    sheets_id = request.form.get("sheets_id", "").strip()

    if not query:
        return jsonify({"error": "Query is required"}), 400

    try:
        batch_size = max(5, min(int(batch_size), 200))
    except ValueError:
        batch_size = 50

    stats = get_search_stats(query)
    if not stats:
        return jsonify({"error": "Search not found. Please search Google Maps first."}), 404

    if stats["remaining"] == 0:
        return jsonify({"error": "No pending listings found. All records already extracted."}), 400

    # Adjust batch size to available listings
    actual_batch_size = min(batch_size, stats["remaining"])

    job_id = str(uuid.uuid4())

    # Run batch extraction in background thread
    thread = threading.Thread(
        target=_run_batch_extraction,
        args=(job_id, query, actual_batch_size, use_sheets, sheets_id),
        daemon=True
    )
    thread.start()

    return jsonify({
        "job_id": job_id,
        "query": query,
        "batch_size": actual_batch_size,
        "remaining_after": stats["remaining"] - actual_batch_size
    })


@app.route("/api/download_all/<query>", methods=["GET"])
def download_all_extracted(query):
    """Download CSV with ALL extracted data for a specific query."""
    from urllib.parse import unquote
    query = unquote(query)

    leads = get_all_extracted_leads_by_query(query)
    if not leads:
        return jsonify({"error": "No extracted data found for this query."}), 404

    # Write to temporary CSV
    job_id = f"export_{hash(query) % 10000}"
    write_leads_csv(leads, job_id)

    path = get_csv_path(job_id)
    if not os.path.isfile(path):
        return jsonify({"error": "CSV generation failed."}), 500

    fname = f"leads_{query.replace(' ', '_').lower()}_all.csv"
    return send_file(path, as_attachment=True, download_name=fname, mimetype="text/csv")


@app.route("/api/delete_search", methods=["POST"])
def delete_search_route():
    """Delete a search and all its data."""
    query = request.form.get("query", "").strip()
    if not query:
        return jsonify({"error": "Query is required"}), 400

    try:
        delete_search(query)
        return jsonify({"ok": True})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _emit(job_id, level, message, data=None):
    emit(job_id, level, message, data)


def get_all_extracted_leads_by_query(query: str) -> list:
    """Get all extracted leads for a query as Lead objects."""
    from storage.database import get_search_by_query, get_all_extracted_leads

    search = get_search_by_query(query)
    if not search:
        return []

    extracted = get_all_extracted_leads(search["id"])
    leads = []

    for item in extracted:
        lead_data = item.get("lead_data", {})
        if lead_data:
            lead = Lead(
                company_name=lead_data.get("company_name", ""),
                email=lead_data.get("email", []),
                whatsapp_phone=lead_data.get("whatsapp_phone", ""),
                business_type=lead_data.get("business_type", ""),
                website_url=lead_data.get("website_url", ""),
                city=lead_data.get("city", ""),
                country=lead_data.get("country", ""),
                source_query=lead_data.get("source_query", query),
                scraped_at=lead_data.get("scraped_at", item.get("_extracted_at", "")),
            )
            leads.append(lead)

    return leads


# Generic email local-parts that don't represent a company name
_GENERIC_LOCAL = frozenset({
    "info", "contact", "hello", "admin", "support", "sales", "mail",
    "enquiry", "enquiries", "office", "team", "help", "marketing",
    "service", "services", "manager", "reception", "accounts", "billing",
    "hr", "jobs", "careers", "media", "pr", "news", "web", "website",
    "hello", "hi", "general", "query", "queries", "feedback",
})


def _company_name_from_email(email: str) -> str:
    """
    Derive a company name from an email address when no other name is found.

    Rules:
    - Generic local part (info@, contact@, etc.) → use domain name
      e.g.  info@abccorp.com       → "Abccorp"
    - Specific local part           → use local part
      e.g.  abccorp@gmail.com      → "Abccorp"
    """
    try:
        local, domain = email.lower().split("@", 1)
        if local in _GENERIC_LOCAL:
            # Use domain without TLD
            name = domain.rsplit(".", 1)[0]
        else:
            name = local
        # Clean separators and title-case
        name = name.replace("-", " ").replace("_", " ").replace(".", " ")
        return name.strip().title()
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


def _run_pipeline(job_id: str, params: dict) -> None:
    """
    Main pipeline:
      Stage 1 — Google Maps search → list of {name, category, website_url}
      Stage 2 — Visit each website → extract emails
      Stage 3 — Deduplicate
      Stage 4 — Save CSV + optional Sheets
    """
    start_time = time.time()
    country       = params["country"]
    city          = params.get("city", "")
    business_type = params["business_type"]
    max_results   = params["max_results"]
    use_sheets    = params["use_sheets"]
    sheets_id     = params["sheets_id"]

    all_leads: list = []
    rate_limiter = RateLimiter(rpm=15)
    session = build_session()

    try:
        # ----------------------------------------------------------------
        # STAGE 1 — Google Maps
        # ----------------------------------------------------------------
        query = build_query(country, business_type, city)
        _emit(job_id, "info", f"Searching Google Maps: {query}")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        def emit_fn(level, message, data=None):
            _emit(job_id, level, message, data)

        listings = loop.run_until_complete(
            search_google_maps(query, max_results, emit_fn)
        )
        loop.close()

        if is_cancelled(job_id):
            _finish_job(job_id, all_leads)
            return

        if not listings:
            _emit(job_id, "warn", "No listings found on Google Maps. Try a different search.")
            _finish_job(job_id, all_leads)
            return

        total = len(listings)
        _emit(job_id, "info", f"Found {total} businesses on Maps. Extracting emails...",
              data={"current": 0, "total": total})

        # ----------------------------------------------------------------
        # STAGE 2 — Visit websites & extract emails
        # ----------------------------------------------------------------
        # Deduplicate by URL to avoid visiting the same website multiple times
        # (common with Google Maps where multiple listings share one website)
        visited_urls = {}  # url -> lead_data
        unique_listings = []
        skipped_dupes = 0
        
        for listing in listings:
            url = listing.get("website_url", "").strip().rstrip("/")
            if url and url in visited_urls:
                skipped_dupes += 1
                continue
            unique_listings.append(listing)
            if url:
                visited_urls[url] = listing
        
        if skipped_dupes > 0:
            _emit(job_id, "info", f"Skipped {skipped_dupes} duplicate URLs. Processing {len(unique_listings)} unique websites...")
        
        total_unique = len(unique_listings)
        processed_count = 0
        
        for listing in unique_listings:
            # Check job timeout
            elapsed_minutes = (time.time() - start_time) / 60
            if elapsed_minutes > JOB_TIMEOUT_MINUTES:
                _emit(job_id, "warn", 
                      f"Job timeout reached ({JOB_TIMEOUT_MINUTES} min). Saving {len(all_leads)} leads collected so far.")
                break
            
            if is_cancelled(job_id):
                _emit(job_id, "warn", "Job cancelled.")
                break
            
            processed_count += 1
            name         = listing.get("name", "")
            category     = listing.get("category", "") or business_type
            website_url  = listing.get("website_url", "")

            _emit(job_id, "info",
                  f"[{processed_count}/{total_unique}] {name or 'Unknown'} — {website_url[:50] or 'no website'}",
                  data={"current": processed_count, "total": total_unique})

            emails = []
            page_data = None

            if website_url:
                rate_limiter.acquire()
                page_data = visit_website(website_url, session)

                if page_data:
                    from bs4 import BeautifulSoup
                    try:
                        soup = BeautifulSoup(page_data["html"][:200_000], "lxml")
                    except Exception:
                        soup = None

                    # Use name from page if Maps name is empty
                    if not name and soup:
                        name = extract_company_name(soup, website_url)

                    # BULLETPROOF: Extract emails from entire HTML source (not just visible text)
                    from scraper.website_visitor import extract_emails_from_source, extract_phones_from_source
                    source_emails = extract_emails_from_source(page_data["html"])
                    text_emails  = extract_emails(page_data["text"])
                    mailto_emails = extract_emails_from_html(page_data["html"])
                    # Merge all sources, keep order, validate
                    all_raw = list(dict.fromkeys(
                        [e for e in source_emails if validate_email(e)] +
                        [e for e in text_emails if validate_email(e)] +
                        [e for e in mailto_emails if validate_email(e)]
                    ))
                    emails = all_raw

                    # Extract WhatsApp/Phone numbers
                    whatsapp_phone = ""
                    found_phones = page_data.get("found_phones", [])
                    source_phones = extract_phones_from_source(page_data["html"])
                    all_phones = list(dict.fromkeys(found_phones + list(source_phones)))
                    # Prefer WhatsApp number if available, otherwise first phone
                    if all_phones:
                        whatsapp_phone = all_phones[0]

                    # STAGE 2.5: Google Search for additional emails (Facebook, directories, etc.)
                    if len(emails) < 3 and name:  # Only search if we have <3 emails
                        _emit(job_id, "info", f"  🔍 Web search for {name[:30]}...")
                        rate_limiter.acquire()
                        loop2 = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop2)
                        try:
                            search_emails_list = loop2.run_until_complete(
                                search_emails_for_company(name, website_url, lambda lvl, msg: None)
                            )
                        except Exception:
                            search_emails_list = []
                        finally:
                            loop2.close()

                        # Validate and merge web search emails
                        for se in search_emails_list:
                            if validate_email(se) and se not in emails:
                                emails.append(se)
                                _emit(job_id, "info", f"  🌐 Web search found: {se}")

            # Classify business type
            page_text = page_data["text"] if page_data else ""
            final_category = classify_business(name, page_text[:3000], category or business_type)
            clean_name = clean_company_name(name)

            # Derive company name from email if still missing
            if not clean_name and emails:
                clean_name = _company_name_from_email(emails[0])

            if emails:
                # ── ONE ROW PER EMAIL with numbered naming ────────────────────────
                for idx, email in enumerate(emails, start=1):
                    # Numbered naming: "Company Name 1", "Company Name 2", etc.
                    row_name = f"{clean_name} {idx}" if clean_name else f"Unknown {idx}"

                    lead = Lead(
                        company_name  = row_name,
                        email         = [email],
                        whatsapp_phone = whatsapp_phone if idx == 1 else "",  # Only first row gets phone
                        business_type = final_category,
                        website_url   = website_url,
                        city          = city,
                        country       = country,
                        source_query  = query,
                    )
                    all_leads.append(lead)
                    append_lead_csv(lead, job_id)

                with _jobs_lock:
                    _jobs[job_id]["lead_count"] = len(all_leads)

                _emit(job_id, "success",
                      f"  {clean_name or '(no name)'} — {len(emails)} row(s): {', '.join(emails[:2])}"
                      + (" ..." if len(emails) > 2 else ""),
                      data={
                          "company":       clean_name,
                          "emails":        emails,
                          "whatsapp_phone": whatsapp_phone,
                          "category":      final_category,
                          "website":       website_url,
                          "city":          city,
                          "country":       country,
                          "current":       processed_count,
                          "total":         total_unique,
                      })
            else:
                # No emails — still save the company row (without email)
                # But extract phone/WhatsApp if available
                whatsapp_phone = ""
                if page_data:
                    from scraper.website_visitor import extract_phones_from_source
                    found_phones = page_data.get("found_phones", [])
                    source_phones = extract_phones_from_source(page_data["html"])
                    all_phones = list(dict.fromkeys(found_phones + list(source_phones)))
                    if all_phones:
                        whatsapp_phone = all_phones[0]

                lead = Lead(
                    company_name  = clean_name,
                    email         = [],
                    whatsapp_phone = whatsapp_phone,
                    business_type = final_category,
                    website_url   = website_url,
                    city          = city,
                    country       = country,
                    source_query  = query,
                )
                all_leads.append(lead)
                append_lead_csv(lead, job_id)

                with _jobs_lock:
                    _jobs[job_id]["lead_count"] = len(all_leads)

                _emit(job_id, "info", f"  {clean_name or '(no name)'} — no email found",
                      data={"current": processed_count, "total": total_unique})

            random_delay((1.0, 2.5))

        # ----------------------------------------------------------------
        # STAGE 3 — Deduplicate
        # ----------------------------------------------------------------
        _emit(job_id, "info", f"Deduplicating {len(all_leads)} leads...")
        all_leads = deduplicate_leads(all_leads)
        _emit(job_id, "info", f"{len(all_leads)} unique leads after dedup")

        # ----------------------------------------------------------------
        # STAGE 4 — Save
        # ----------------------------------------------------------------
        write_leads_csv(all_leads, job_id)

        if use_sheets and sheets_id:
            _emit(job_id, "info", "Pushing to Google Sheets...")
            try:
                append_leads_to_sheet(all_leads, sheets_id)
                _emit(job_id, "success", "Saved to Google Sheets.")
            except Exception as exc:
                _emit(job_id, "warn", f"Sheets error: {exc}")

        _finish_job(job_id, all_leads)

    except Exception as exc:
        _emit(job_id, "error", f"Unexpected error: {exc}")
        with _jobs_lock:
            if job_id in _jobs:
                _jobs[job_id]["status"] = "error"
        emit(job_id, "error", f"Job failed: {exc}")


def _finish_job(job_id: str, leads: list) -> None:
    count        = len(leads)
    emails_found = sum(len(l.email) for l in leads)

    with _jobs_lock:
        if job_id in _jobs:
            _jobs[job_id]["status"]     = "done"
            _jobs[job_id]["lead_count"] = count

    _emit(job_id, "done",
          f"Complete! {count} companies | {emails_found} emails found.",
          data={"count": count, "emails": emails_found})


# ---------------------------------------------------------------------------
# Phase 1: Maps Search Only (Background Worker)
# ---------------------------------------------------------------------------

def _run_maps_search(job_id: str, query: str, country: str, city: str, business_type: str, max_results: int) -> None:
    """
    Phase 1 worker: Search Google Maps, save all listings to database.
    Does NOT extract emails.
    """
    from scraper.google_search import search_google_maps_extended

    create_job_queue(job_id)

    with _jobs_lock:
        _jobs[job_id] = {
            "status": "running",
            "params": {"query": query, "country": country, "city": city, "business_type": business_type},
            "lead_count": 0,
            "started_at": datetime.utcnow().isoformat(),
        }

    try:
        _emit(job_id, "info", f"Phase 1: Searching Google Maps for '{query}'...")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        def emit_fn(level, message, data=None):
            _emit(job_id, level, message, data)

        # Use extended search to get more than 300 results
        listings = loop.run_until_complete(
            search_google_maps_extended(query, max_results, emit_fn, city)
        )
        loop.close()

        if is_cancelled(job_id):
            _emit(job_id, "warn", "Search cancelled.")
            with _jobs_lock:
                _jobs[job_id]["status"] = "cancelled"
            return

        if not listings:
            _emit(job_id, "warn", "No listings found on Google Maps.")
            with _jobs_lock:
                _jobs[job_id]["status"] = "done"
            return

        # Save to database
        _emit(job_id, "info", f"Saving {len(listings)} listings to database...")
        search_id = upsert_search(query, country, business_type, city, len(listings))
        inserted = bulk_insert_listings(search_id, listings)

        _emit(job_id, "success", f"Phase 1 complete! Saved {inserted} listings ({len(listings)} total).")
        _emit(job_id, "info", f"Next: Request batch extraction for emails.")

        with _jobs_lock:
            _jobs[job_id]["status"] = "done"
            _jobs[job_id]["lead_count"] = inserted

        _emit(job_id, "done", f"Maps search complete: {inserted} listings saved.", data={"count": inserted})

    except Exception as exc:
        _emit(job_id, "error", f"Maps search failed: {exc}")
        with _jobs_lock:
            if job_id in _jobs:
                _jobs[job_id]["status"] = "error"
        emit(job_id, "error", f"Job failed: {exc}")


# ---------------------------------------------------------------------------
# Phase 2: Batch Email Extraction (Background Worker)
# ---------------------------------------------------------------------------

def _run_batch_extraction(job_id: str, query: str, batch_size: int, use_sheets: bool, sheets_id: str) -> None:
    """
    Phase 2 worker: Extract emails from pending listings in batches.
    """
    from processor.classifier import classify_business
    from scraper.anti_bot import build_session, random_delay, RateLimiter
    from scraper.website_visitor import (
        visit_website,
        extract_emails_from_source,
        extract_phones_from_source
    )
    from scraper.web_search import search_emails_for_company
    from scraper.extractor import extract_emails, extract_emails_from_html, extract_company_name
    from bs4 import BeautifulSoup

    create_job_queue(job_id)

    with _jobs_lock:
        _jobs[job_id] = {
            "status": "running",
            "params": {"query": query, "batch_size": batch_size},
            "lead_count": 0,
            "started_at": datetime.utcnow().isoformat(),
        }

    all_leads = []
    rate_limiter = RateLimiter(rpm=15)
    session = build_session()

    try:
        # Get pending listings from database
        search = get_search_by_query(query)
        if not search:
            _emit(job_id, "error", "Search not found in database.")
            with _jobs_lock:
                _jobs[job_id]["status"] = "error"
            return

        pending = get_pending_listings(search["id"], batch_size)
        if not pending:
            _emit(job_id, "warn", "No pending listings found.")
            with _jobs_lock:
                _jobs[job_id]["status"] = "done"
            return

        total = len(pending)
        _emit(job_id, "info", f"Phase 2: Extracting emails from {total} listings...")

        processed_count = 0

        for listing in pending:
            if is_cancelled(job_id):
                _emit(job_id, "warn", "Batch extraction cancelled.")
                break

            processed_count += 1
            name = listing["name"]
            category = listing.get("category", "") or search["business_type"]
            website_url = listing.get("website_url", "")
            listing_id = listing["id"]

            _emit(job_id, "info",
                  f"[{processed_count}/{total}] {name or 'Unknown'} — {website_url[:50] or 'no website'}",
                  data={"current": processed_count, "total": total})

            emails = []
            page_data = None

            if website_url:
                rate_limiter.acquire()
                page_data = visit_website(website_url, session)

                if page_data:
                    try:
                        soup = BeautifulSoup(page_data["html"][:200_000], "lxml")
                    except Exception:
                        soup = None

                    if not name and soup:
                        name = extract_company_name(soup, website_url)

                    # Extract emails from entire HTML source
                    source_emails = extract_emails_from_source(page_data["html"])
                    text_emails = extract_emails(page_data["text"])
                    mailto_emails = extract_emails_from_html(page_data["html"])

                    all_raw = list(dict.fromkeys(
                        [e for e in source_emails if validate_email(e)] +
                        [e for e in text_emails if validate_email(e)] +
                        [e for e in mailto_emails if validate_email(e)]
                    ))
                    emails = all_raw

                    # Extract WhatsApp/Phone
                    whatsapp_phone = ""
                    found_phones = page_data.get("found_phones", [])
                    source_phones = extract_phones_from_source(page_data["html"])
                    all_phones = list(dict.fromkeys(found_phones + list(source_phones)))
                    if all_phones:
                        whatsapp_phone = all_phones[0]

                    # Web search for additional emails
                    if len(emails) < 3 and name:
                        _emit(job_id, "info", f"  🔍 Web search for {name[:30]}...")
                        rate_limiter.acquire()
                        loop2 = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop2)
                        try:
                            search_emails_list = loop2.run_until_complete(
                                search_emails_for_company(name, website_url, lambda lvl, msg: None)
                            )
                        except Exception:
                            search_emails_list = []
                        finally:
                            loop2.close()

                        for se in search_emails_list:
                            if validate_email(se) and se not in emails:
                                emails.append(se)

            # Classify business type
            page_text = page_data["text"] if page_data else ""
            final_category = classify_business(name, page_text[:3000], category)
            clean_name = clean_company_name(name)

            # Derive company name from email if still missing
            if not clean_name and emails:
                clean_name = _company_name_from_email(emails[0])

            if emails:
                # One row per email with numbered naming
                for idx, email in enumerate(emails, start=1):
                    row_name = f"{clean_name} {idx}" if clean_name else f"Unknown {idx}"

                    lead = Lead(
                        company_name=row_name,
                        email=[email],
                        whatsapp_phone=whatsapp_phone if idx == 1 else "",
                        business_type=final_category,
                        website_url=website_url,
                        city=search.get("city", ""),
                        country=search["country"],
                        source_query=query,
                    )
                    all_leads.append(lead)
                    append_lead_csv(lead, job_id)

                # Update listing status in database
                lead_data = {
                    "company_name": clean_name,
                    "email": emails,
                    "whatsapp_phone": whatsapp_phone,
                    "business_type": final_category,
                    "website_url": website_url,
                    "city": search.get("city", ""),
                    "country": search["country"],
                    "source_query": query,
                    "scraped_at": datetime.utcnow().isoformat(),
                }
                update_listing_status(listing_id, "extracted", lead_data)

                with _jobs_lock:
                    _jobs[job_id]["lead_count"] = len(all_leads)

                _emit(job_id, "success",
                      f"  {clean_name or '(no name)'} — {len(emails)} email(s): {', '.join(emails[:2])}"
                      + (" ..." if len(emails) > 2 else ""),
                      data={
                          "company":        clean_name,
                          "emails":         emails,
                          "whatsapp_phone": whatsapp_phone,
                          "category":       final_category,
                          "website":        website_url,
                          "city":           search.get("city", ""),
                          "country":        search["country"],
                          "current":        processed_count,
                          "total":          total,
                      })
            else:
                # No emails - mark as failed
                update_listing_status(listing_id, "failed")

                with _jobs_lock:
                    _jobs[job_id]["lead_count"] = len(all_leads)

                _emit(job_id, "info", f"  {clean_name or '(no name)'} — no email found",
                      data={"current": processed_count, "total": total})

            random_delay((1.0, 2.5))

        # Deduplicate
        _emit(job_id, "info", f"Deduplicating {len(all_leads)} leads...")
        all_leads = deduplicate_leads(all_leads)
        _emit(job_id, "info", f"{len(all_leads)} unique leads after dedup")

        # Save CSV
        write_leads_csv(all_leads, job_id)

        if use_sheets and sheets_id:
            _emit(job_id, "info", "Pushing to Google Sheets...")
            try:
                append_leads_to_sheet(all_leads, sheets_id)
                _emit(job_id, "success", "Saved to Google Sheets.")
            except Exception as exc:
                _emit(job_id, "warn", f"Sheets error: {exc}")

        # Get updated stats
        stats = get_search_stats(query)
        _finish_job(job_id, all_leads)
        _emit(job_id, "done",
              f"Batch complete! {len(all_leads)} leads extracted. {stats['remaining']} listings remaining.",
              data={"count": len(all_leads), "remaining": stats["remaining"]})

    except Exception as exc:
        _emit(job_id, "error", f"Batch extraction failed: {exc}")
        with _jobs_lock:
            if job_id in _jobs:
                _jobs[job_id]["status"] = "error"
        emit(job_id, "error", f"Job failed: {exc}")


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7860))
    app.run(host="0.0.0.0", port=port, debug=FLASK_DEBUG, threaded=True)
