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
# Helpers
# ---------------------------------------------------------------------------

def _emit(job_id, level, message, data=None):
    emit(job_id, level, message, data)


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

                    # Extract from visible text AND from mailto: links in HTML
                    text_emails  = extract_emails(page_data["text"])
                    mailto_emails = extract_emails_from_html(page_data["html"])
                    all_raw = list(dict.fromkeys(text_emails + mailto_emails))  # merge, keep order
                    emails = [e for e in all_raw if validate_email(e)]

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
                # ── ONE ROW PER EMAIL ────────────────────────────────
                for email in emails:
                    local_part = email.split("@")[0].lower()
                    # Append local part to company name only when it's meaningful
                    if local_part in _GENERIC_LOCAL:
                        row_name = clean_name          # info@... → keep plain name
                    else:
                        row_name = f"{clean_name} {local_part}".strip() if clean_name else local_part.title()

                    lead = Lead(
                        company_name  = row_name,
                        email         = [email],
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
                          "company":    clean_name,
                          "emails":     emails,
                          "category":   final_category,
                          "website":    website_url,
                          "city":       city,
                          "current":    processed_count,
                          "total":      total_unique,
                      })
            else:
                # No emails — still save the company row (without email)
                lead = Lead(
                    company_name  = clean_name,
                    email         = [],
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
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7860))
    app.run(host="0.0.0.0", port=port, debug=FLASK_DEBUG, threaded=True)
