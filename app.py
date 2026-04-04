"""
app.py
------
Flask application entry point and pipeline orchestrator.

Routes:
  GET  /                   — serve the main UI
  POST /start              — validate inputs, spawn scraping job, return job_id
  GET  /stream/<job_id>    — SSE stream of real-time progress events
  GET  /download/<job_id>  — download the finished CSV file
  POST /abort/<job_id>     — request cancellation of a running job
  GET  /status/<job_id>    — JSON status of a job

The heavy lifting (Google search + website scraping) runs in a background
daemon thread so Flask can keep serving requests (SSE stream, downloads, etc.)
while the job runs.
"""

import asyncio
import os
import threading
import uuid
from datetime import datetime

from dotenv import load_dotenv
from flask import Flask, Response, jsonify, render_template, request, send_file, stream_with_context

# Load .env file (if present) before importing settings
load_dotenv()

from config.settings import FLASK_SECRET_KEY, FLASK_DEBUG, MAX_CONCURRENT_JOBS
from sse.event_stream import create_job_queue, emit, event_generator, cancel_job, is_cancelled
from processor.lead_model import Lead
from processor.cleaner import validate_email, normalize_phone, clean_company_name, deduplicate_leads
from processor.classifier import classify_business
from scraper.anti_bot import build_session, random_delay, RateLimiter
from scraper.extractor import extract_emails, extract_phones, extract_whatsapp, extract_company_name
from scraper.website_visitor import visit_website
from scraper.google_search import build_query, search_google
from storage.csv_writer import append_lead_csv, write_leads_csv, get_csv_path
from storage.sheets_writer import check_sheets_credentials, append_leads_to_sheet

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY

# In-memory job registry: job_id → {status, params, lead_count, started_at}
_jobs: dict = {}
_jobs_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    """Serve the main UI page."""
    return render_template("index.html")


@app.route("/start", methods=["POST"])
def start_job():
    """
    Validate the form input, create a new job, and spawn a background thread.
    Returns JSON: {"job_id": "...", "error": null}
    """
    country       = request.form.get("country", "").strip()
    business_type = request.form.get("business_type", "").strip()
    max_results   = request.form.get("max_results", "50").strip()
    use_sheets    = request.form.get("use_sheets") == "on"
    sheets_id     = request.form.get("sheets_id", "").strip()

    # Validation
    if not country:
        return jsonify({"error": "Country is required."}), 400
    if not business_type:
        return jsonify({"error": "Business type is required."}), 400

    try:
        max_results = max(1, min(int(max_results), 500))
    except ValueError:
        max_results = 50

    if use_sheets and not sheets_id:
        return jsonify({"error": "Spreadsheet ID is required when Google Sheets is enabled."}), 400

    if use_sheets and not check_sheets_credentials():
        return jsonify({
            "error": "credentials.json not found or invalid. "
                     "Please set up your Google Service Account first."
        }), 400

    # Cap concurrent jobs
    with _jobs_lock:
        active = sum(1 for j in _jobs.values() if j["status"] == "running")
        if active >= MAX_CONCURRENT_JOBS:
            return jsonify({"error": f"Too many concurrent jobs ({active}/{MAX_CONCURRENT_JOBS}). "
                                     "Please wait for an existing job to finish."}), 429

    job_id = str(uuid.uuid4())
    params = {
        "country":       country,
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
    """SSE endpoint — yields real-time progress events as the job runs."""
    def generate():
        yield from event_generator(job_id)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":    "no-cache",
            "X-Accel-Buffering":"no",    # important for nginx reverse proxies
            "Connection":       "keep-alive",
        },
    )


@app.route("/download/<job_id>")
def download(job_id):
    """Stream the completed CSV file to the browser as a download."""
    path = get_csv_path(job_id)
    if not os.path.isfile(path):
        return jsonify({"error": "CSV file not found. The job may not have completed yet."}), 404

    job = _jobs.get(job_id, {})
    country       = job.get("params", {}).get("country", "leads")
    business_type = job.get("params", {}).get("business_type", "")
    filename      = f"leads_{business_type}_{country}.csv".replace(" ", "_").lower()

    return send_file(path, as_attachment=True, download_name=filename, mimetype="text/csv")


@app.route("/abort/<job_id>", methods=["POST"])
def abort(job_id):
    """Signal the pipeline thread to stop after the current site."""
    cancel_job(job_id)
    with _jobs_lock:
        if job_id in _jobs:
            _jobs[job_id]["status"] = "cancelled"
    return jsonify({"ok": True})


@app.route("/status/<job_id>")
def status(job_id):
    """Return JSON status of a job."""
    job = _jobs.get(job_id)
    if not job:
        return jsonify({"error": "Unknown job ID"}), 404
    return jsonify(job)


# ---------------------------------------------------------------------------
# Pipeline (runs in a background thread)
# ---------------------------------------------------------------------------

def _emit(job_id: str, level: str, message: str, data: dict = None):
    """Thin wrapper so we don't repeat job_id everywhere."""
    emit(job_id, level, message, data)


def _run_pipeline(job_id: str, params: dict) -> None:
    """
    Main scraping pipeline.  Runs in a daemon background thread.

    Stages:
    1. Build Google search query and collect company website URLs.
    2. Visit each URL, extract contact data.
    3. Clean and classify each lead.
    4. Deduplicate the full lead list.
    5. Write final CSV (and optionally push to Google Sheets).
    """
    country       = params["country"]
    business_type = params["business_type"]
    max_results   = params["max_results"]
    use_sheets    = params["use_sheets"]
    sheets_id     = params["sheets_id"]

    # Derive max_pages from max_results (10 results per page)
    max_pages = max(1, min((max_results + 9) // 10, 10))

    all_leads: list = []
    rate_limiter = RateLimiter(rpm=15)  # be gentle with external sites
    session = build_session()

    try:
        # ----------------------------------------------------------------
        # STAGE 1 — Google search
        # ----------------------------------------------------------------
        _emit(job_id, "info", f"Building search query for: {business_type} in {country}")
        query = build_query(country, business_type)
        _emit(job_id, "info", f"Query: {query[:100]}")

        # run_until_complete in a thread-local event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        def emit_fn(level, message, data=None):
            _emit(job_id, level, message, data)

        urls = loop.run_until_complete(
            search_google(query, max_pages, emit_fn)
        )
        loop.close()

        if is_cancelled(job_id):
            _emit(job_id, "warn", "Job cancelled by user.")
            _finish_job(job_id, all_leads, 0)
            return

        if not urls:
            _emit(job_id, "warn",
                  "No URLs found. Google may have blocked the search. "
                  "Try again in a few minutes, or reduce the number of pages.")
            _finish_job(job_id, all_leads, 0)
            return

        total_urls = len(urls)
        _emit(job_id, "info", f"Total URLs to visit: {total_urls}",
              data={"current": 0, "total": total_urls})

        # ----------------------------------------------------------------
        # STAGE 2 — Visit each website & extract data
        # ----------------------------------------------------------------
        for i, url in enumerate(urls, start=1):
            if is_cancelled(job_id):
                _emit(job_id, "warn", "Job cancelled by user.")
                break

            _emit(job_id, "info", f"[{i}/{total_urls}] Visiting: {url[:60]}",
                  data={"current": i, "total": total_urls})

            # Rate-limit outgoing requests
            rate_limiter.acquire()

            page_data = visit_website(url, session)
            if not page_data:
                _emit(job_id, "warn", f"  Skipped — could not fetch page")
                continue

            html = page_data["html"]
            text = page_data["text"]

            # ---- Extract ----
            from bs4 import BeautifulSoup
            try:
                soup = BeautifulSoup(html[:200_000], "lxml")  # cap to 200KB
            except Exception:
                soup = None

            emails    = extract_emails(text)
            phones    = extract_phones(text)
            whatsapps = extract_whatsapp(html)
            name      = extract_company_name(soup, url) if soup else ""

            # ---- Clean ----
            valid_emails = [e for e in emails if validate_email(e)]
            norm_phones  = [normalize_phone(p, country) for p in phones]
            norm_wa      = [normalize_phone(w, country) for w in whatsapps]
            clean_name   = clean_company_name(name)

            # ---- Classify ----
            category = classify_business(clean_name, text[:5000], business_type)

            # ---- Build lead ----
            lead = Lead(
                company_name  = clean_name,
                email         = valid_emails,
                phone         = norm_phones,
                whatsapp      = norm_wa,
                website_url   = url,
                business_type = category,
                country       = country,
                source_query  = query,
            )
            all_leads.append(lead)

            # Write to CSV immediately (streaming — survives job abort)
            append_lead_csv(lead, job_id)

            # Update job registry
            with _jobs_lock:
                _jobs[job_id]["lead_count"] = len(all_leads)

            # Log result
            if valid_emails:
                _emit(job_id, "success",
                      f"  {clean_name or url[:40]} — {len(valid_emails)} email(s): "
                      + ", ".join(valid_emails[:2])
                      + (" ..." if len(valid_emails) > 2 else ""))
            else:
                _emit(job_id, "info", f"  {clean_name or url[:40]} — no emails found")

            random_delay((1.0, 3.0))

        # ----------------------------------------------------------------
        # STAGE 3 — Deduplicate
        # ----------------------------------------------------------------
        _emit(job_id, "info", f"Deduplicating {len(all_leads)} leads...")
        all_leads = deduplicate_leads(all_leads)
        _emit(job_id, "info", f"After dedup: {len(all_leads)} unique leads")

        # ----------------------------------------------------------------
        # STAGE 4 — Final CSV write
        # ----------------------------------------------------------------
        write_leads_csv(all_leads, job_id)

        # ----------------------------------------------------------------
        # STAGE 5 — Google Sheets (optional)
        # ----------------------------------------------------------------
        if use_sheets and sheets_id:
            _emit(job_id, "info", "Pushing leads to Google Sheets...")
            try:
                append_leads_to_sheet(all_leads, sheets_id)
                _emit(job_id, "success", "Data saved to Google Sheets.")
            except Exception as exc:
                _emit(job_id, "warn", f"Google Sheets error: {exc}")

        _finish_job(job_id, all_leads, len(all_leads))

    except Exception as exc:
        _emit(job_id, "error", f"Unexpected error: {exc}")
        with _jobs_lock:
            if job_id in _jobs:
                _jobs[job_id]["status"] = "error"
        emit(job_id, "error", f"Job failed: {exc}")


def _finish_job(job_id: str, leads: list, count: int) -> None:
    """Emit the final 'done' event and update job state."""
    with _jobs_lock:
        if job_id in _jobs:
            _jobs[job_id]["status"] = "done"
            _jobs[job_id]["lead_count"] = count

    emails_found = sum(len(l.email) for l in leads)
    _emit(
        job_id, "done",
        f"Complete! {count} leads saved ({emails_found} email addresses found).",
        data={"count": count, "emails": emails_found},
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # PORT env var is set by Hugging Face Spaces (7860), Render, Railway, etc.
    port = int(os.environ.get("PORT", 7860))
    app.run(
        host="0.0.0.0",
        port=port,
        debug=FLASK_DEBUG,
        threaded=True,   # required for SSE + background threads
    )
