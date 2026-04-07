# Email Extractor Tool — Project Context & Reference

> **Purpose:** This document summarizes the entire Email Extractor Tool project. When starting a new session or switching AI models/agents, read this file first to understand the codebase, features, and where to continue development.

---

## 📋 Project Overview

**Name:** Email Extractor Tool  
**Purpose:** Free, self-hosted B2B lead generation tool that extracts emails from Google Maps listings.  
**Deployment:** Hugging Face Spaces (port 7860)  
**Tech Stack:** Python 3.11 + Flask + Playwright + Gunicorn

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Flask (app.py)                           │
│  Routes: /, /start, /stream/<id>, /download/<id>, /abort   │
│  Orchestrates: Maps search → Email extraction → CSV export │
└─────────────────────────┬───────────────────────────────────┘
                          │
          ┌───────────────┼───────────────┐
          │               │               │
          ▼               ▼               ▼
    ┌───────────┐  ┌───────────┐  ┌───────────┐
    │ scraper/  │  │processor/ │  │ storage/  │
    │ website_  │→ │lead_model │→ │csv_writer │
    │ visitor.py│  │cleaner.py │  │sheets_    │
    │ extractor.│  │classifier.│  │writer.py  │
    │ google_   │  └───────────┘  └───────────┘
    │ search.py │
    │ web_search│
    └─────┬─────┘
          │ emit()
          ▼
    ┌───────────┐
    │ sse/      │
    │event_stream│→ Frontend (SSE real-time updates)
    └───────────┘
```

---

## 📁 File Structure & Functions

### `app.py` — Main Flask Application
**Role:** Pipeline orchestration, route handling, job management

| Function | Purpose |
|----------|---------|
| `index()` | Renders homepage UI |
| `start_job()` | Validates input, creates job, spawns background thread |
| `stream(job_id)` | SSE endpoint for real-time progress updates |
| `download(job_id)` | Serves CSV file download |
| `abort(job_id)` | Cancels running job |
| `status(job_id)` | Returns job status + CSV readiness |
| `resume(job_id)` | Reconnects to lost SSE stream |
| `_run_pipeline()` | **Main pipeline:** Stage 1 (Maps) → Stage 2 (Website visit) → Stage 2.5 (Web search) → Stage 3 (Dedup) → Stage 4 (Save) |
| `_finish_job()` | Marks job complete, emits final stats |

**Key Features:**
- URL deduplication (skips duplicate websites from Maps)
- Job timeout protection (configurable, default 60 min)
- Rate limiting (15 req/min)
- Multiple emails per company → separate rows with "Company Name 1", "Company Name 2" naming

---

### `scraper/website_visitor.py` — BULLETPROOF Website Visitor
**Role:** Fetches websites with full JS rendering, extracts emails + phones

| Function | Purpose |
|----------|---------|
| `extract_emails_from_source(html)` | Aggressive email extraction from raw HTML (mailto links, data attrs, JS vars, hidden text, meta tags) |
| `extract_phones_from_source(html)` | Extracts phone/WhatsApp from tel: links, wa.me links, visible text patterns |
| `_fetch_with_playwright(url)` | Async Playwright fetch with 5-second wait for full JS rendering |
| `_fetch_with_playwright_sync(url)` | Sync wrapper for Playwright |
| `get_contact_page_urls(base_url, soup)` | Finds internal contact/about links (up to 5) |
| `get_all_internal_urls(base_url, soup)` | Gets ALL internal page URLs (up to 15) |
| `visit_website(url, session)` | **Main function:** Playwright fetch → extract emails + phones → visit contact pages → visit internal pages → return combined data |

**BULLETPROOF Features:**
- **ALWAYS uses Playwright** (catches React/Next.js/SPA sites)
- **5-second wait** for lazy-loaded footers/popups
- **Aggressive email extraction** from entire HTML source (not just visible text)
- **Phone/WhatsApp extraction** from tel: links, wa.me links, and visible text
- **Visits up to 15 internal pages** per site (not just /contact or /about)
- Returns `found_emails` + `found_phones` lists

---

### `scraper/extractor.py` — Email/Phone Extraction Utilities
**Role:** Pure-function extraction layer with regex patterns

| Function | Purpose |
|----------|---------|
| `extract_emails_from_html(html)` | Extracts emails from `mailto:` links in HTML |
| `extract_emails(text)` | Extracts emails from visible text with filtering (blacklists, domain checks) |
| `extract_phones(text)` | Extracts phone numbers with digit count filtering |
| `extract_whatsapp(html)` | Extracts WhatsApp deep-links (`wa.me`, `api.whatsapp.com`) |
| `extract_company_name(soup, url)` | Infers company name from OG meta tag, title, or domain |

---

### `scraper/google_search.py` — Google Maps Scraper
**Role:** Searches Google Maps via Playwright, collects business listings

| Function | Purpose |
|----------|---------|
| `build_query(country, business_type, city)` | Builds search query string |
| `search_google_maps(query, max_results, emit_fn)` | **Main function:** Opens Maps → accepts cookies → scrolls results → extracts listings |

**Features:**
- Multiple selector fallbacks for results feed
- Cookie consent handling
- Scroll-to-load-more logic
- Returns list of `{name, category, website_url}`

---

### `scraper/web_search.py` — Google Search Email Finder
**Role:** Searches Google for emails on Facebook, Justdial, IndiaMART, directories

| Function | Purpose |
|----------|---------|
| `search_emails_for_company(name, website_url, emit_fn)` | Searches Google for company emails, returns list of found emails |

---

### `scraper/anti_bot.py` — Anti-Detection Utilities
**Role:** Rate limiting, UA rotation, delay helpers

| Function/Class | Purpose |
|----------------|---------|
| `random_delay(range)` | Sleeps for random duration |
| `get_random_ua()` | Returns random User-Agent string |
| `build_session(ua)` | Creates requests.Session with browser-like headers + retry adapter |
| `RateLimiter(rpm)` | Token-bucket rate limiter (thread-safe) |

---

### `processor/lead_model.py` — Lead Data Model
**Role:** Dataclass for lead records, CSV/Sheets serialization

| Field | Type | Purpose |
|-------|------|---------|
| `company_name` | str | Company name (numbered: "Company 1", "Company 2") |
| `email` | List[str] | List of email addresses (single per row) |
| `whatsapp_phone` | str | WhatsApp/Phone number (only on first row per company) |
| `business_type` | str | Classified category |
| `website_url` | str | Website URL |
| `city` | str | City |
| `country` | str | Country |
| `source_query` | str | Original search query |
| `scraped_at` | str | ISO timestamp |

**Methods:**
- `to_csv_row()` → dict for CSV writer
- `to_sheets_row()` → list for Google Sheets
- `data_score()` → quality score for deduplication

---

### `processor/cleaner.py` — Data Cleaning
**Role:** Email validation, phone normalization, deduplication

| Function | Purpose |
|----------|---------|
| `validate_email(email)` | Structural regex + DNS MX lookup validation |
| `clean_company_name(name)` | Removes separators, title-cases |
| `deduplicate_leads(leads)` | URL exact-match + fuzzy name dedup (rapidfuzz >90%) |

---

### `processor/classifier.py` — Business Type Classifier
**Role:** Classifies business type via keyword-frequency scoring

| Function | Purpose |
|----------|---------|
| `classify_business(name, text, default)` | Scores text against BUSINESS_KEYWORDS map, returns best match |

---

### `sse/event_stream.py` — SSE Infrastructure
**Role:** Real-time progress streaming to frontend

| Function | Purpose |
|----------|---------|
| `create_job_queue(job_id)` | Creates event queue for job |
| `cancel_job(job_id)` | Signals pipeline to stop |
| `is_cancelled(job_id)` | Checks if job should stop |
| `cleanup_job(job_id)` | Removes job state (only after done/error) |
| `emit(job_id, level, message, data)` | Puts event onto queue |
| `event_generator(job_id)` | Yields SSE-formatted strings (60-min timeout) |

**Key Fix:** Job queue preserved on SSE disconnect for reconnection support.

---

### `storage/csv_writer.py` — CSV Export
**Role:** Writes leads to CSV with incremental + batch modes

| Function | Purpose |
|----------|---------|
| `get_csv_path(job_id)` | Returns `/tmp/leads_{job_id}.csv` |
| `append_lead_csv(lead, job_id)` | Incremental write per lead |
| `write_leads_csv(leads, job_id)` | Batch rewrite (final save) |

---

### `storage/sheets_writer.py` — Google Sheets Export
**Role:** Optional Google Sheets push via service account API

| Function | Purpose |
|----------|---------|
| `check_sheets_credentials()` | Validates credentials.json exists |
| `append_leads_to_sheet(leads, sheets_id)` | Batch append to Google Sheets |

---

### `templates/index.html` — Frontend UI
**Role:** Vanilla JS frontend with SSE streaming

**Features:**
- Form: country, city, business type, max results, Google Sheets toggle
- Real-time progress bar + stats (companies, emails, success rate)
- Live log console with color-coded badges
- Auto-reconnect on SSE drop (3-second delay)
- Resume banner for recovered jobs
- Download CSV button
- Abort button

---

## 🚀 Key Features

### 1. **Google Maps Search**
- Playwright-based scraping with cookie consent handling
- Scrolls to load more results
- Returns up to 200 listings

### 2. **Bulletproof Email Extraction**
- **ALWAYS uses Playwright** (full JS rendering)
- **5-second wait** for lazy-loaded content
- **Aggressive extraction** from entire HTML source:
  - `mailto:` links
  - Direct text in body/footer/header
  - `data-email`, `data-contact`, `data-mail` attributes
  - JavaScript variables and hidden text
  - Meta tags
- **Visits up to 15 internal pages** per site
- **Google Search fallback** for emails on directories (Facebook, Justdial, IndiaMART)

### 3. **Phone/WhatsApp Extraction**
- Extracts from `tel:` links
- WhatsApp deep links (`wa.me`, `api.whatsapp.com`)
- Visible text patterns ("Phone:", "Mobile:", "WhatsApp:")
- International phone number patterns

### 4. **Multiple Emails Per Company**
- Creates separate row for each email
- Numbered naming: "Company Name 1", "Company Name 2", etc.
- WhatsApp/Phone only on first row per company

### 5. **Deduplication**
- URL exact-match dedup (keeps lead with more data)
- Fuzzy company-name dedup via `rapidfuzz` (similarity >90%)

### 6. **SSE Real-Time Updates**
- 60-minute timeout (supports long jobs)
- Auto-reconnect on disconnect (3-second delay)
- Job queue preserved for reconnection

### 7. **Job Management**
- Max 3 concurrent jobs
- Job timeout protection (configurable, default 60 min)
- Abort/cancel support
- Status checking

---

## 📦 Dependencies

| Package | Purpose |
|---------|---------|
| `flask>=2.3.0` | Web framework |
| `gunicorn>=21.2.0` | Production WSGI server |
| `playwright>=1.40.0` | Headless Chromium |
| `requests>=2.31.0` | HTTP client |
| `beautifulsoup4>=4.12.0` | HTML parsing |
| `lxml>=4.9.0` | Fast HTML parser |
| `gspread>=5.12.0` | Google Sheets API |
| `google-auth>=2.23.0` | Google auth |
| `phonenumbers>=8.13.0` | Phone parsing |
| `dnspython>=2.4.0` | DNS MX lookup |
| `python-dotenv>=1.0.0` | Env vars |
| `rapidfuzz>=3.5.0` | Fuzzy matching |

---

## 🐛 Known Issues & Fixes Applied

### Fixed Issues:
1. ✅ **Google Maps not finding results** → Updated selectors, added fallback chain
2. ✅ **SSE timeout after 10 min** → Increased to 60 min + auto-reconnect
3. ✅ **SSE "Unknown job ID" on reconnect** → Job queue preserved on disconnect
4. ✅ **Low email extraction rate (10%)** → Bulletproof extraction with Playwright + aggressive HTML scanning
5. ✅ **Missed homepage/footer emails** → 5-second Playwright wait + entire HTML source scanning
6. ✅ **Duplicate URLs wasting requests** → URL deduplication before visiting
7. ✅ **Jobs running indefinitely** → Job timeout protection (60 min)
8. ✅ **30-min Hugging Face startup timeout** → Pre-installed Chromium in Docker build
9. ✅ **Single row per company with multiple emails** → Separate rows with numbered naming
10. ✅ **No WhatsApp/Phone column** → Added column with extraction from tel:/wa.me links

### Current Limitations:
- Some sites use CAPTCHA or bot detection (Cloudflare, etc.)
- Very large sites (>15 pages) may not be fully crawled
- Google Search fallback may be rate-limited

---

## 🔧 Configuration (`config/settings.py`)

| Setting | Default | Description |
|---------|---------|-------------|
| `MAX_CONCURRENT_JOBS` | 3 | Max parallel jobs |
| `JOB_TIMEOUT_MINUTES` | 60 | Job timeout |
| `REQUEST_TIMEOUT` | 15 | HTTP request timeout (sec) |
| `RATE_LIMIT_RPM` | 20 | Max requests per minute |
| `DELAY_BETWEEN_VISITS` | (2.0, 5.0) | Delay between website visits |
| `BUSINESS_KEYWORDS` | dict | Category classification keywords |
| `BLOCKED_EMAIL_PREFIXES` | frozenset | Email prefixes to skip (noreply, info, etc.) |
| `BLOCKED_EMAIL_DOMAINS` | frozenset | Domains to skip (example.com, etc.) |

---

## 🚦 Deployment

### Hugging Face Spaces:
1. Push to GitHub (`main` branch)
2. GitHub Actions auto-deploys to HF Spaces
3. Dockerfile pre-installs Chromium during build

### Local Docker:
```bash
docker build -t lead-extractor .
docker run -p 7860:7860 lead-extractor
```

### Local Development:
```bash
pip install -r requirements.txt
playwright install chromium
python app.py
```

---

## 📝 CSV Output Format

| Column | Description |
|--------|-------------|
| Company Name | Numbered: "Company Name 1", "Company Name 2" |
| Email(s) | Single email per row |
| WhatsApp/Phone | Phone/WhatsApp number (only on first row) |
| Business Type | Classified category |
| Website URL | Website URL |
| City | City |
| Country | Country |
| Scraped At | ISO timestamp |

---

## 🔮 Future Improvements (Not Yet Implemented)

1. **CAPTCHA solving** for blocked sites
2. **Email verification** (SMTP ping)
3. **LinkedIn profile extraction**
4. **Social media email extraction** (Twitter, Instagram, Facebook)
5. **Batch URL input** (upload CSV of websites)
6. **Multi-language support**
7. **API endpoint** for programmatic access
8. **Database storage** (SQLite/PostgreSQL) instead of CSV
9. **Email template generator** for outreach
10. **Scheduled scraping jobs** (cron-like)

---

## 📖 How to Continue Development

1. **Read this file** to understand the codebase
2. **Check `app.py`** for pipeline logic
3. **Check `scraper/website_visitor.py`** for email/phone extraction
4. **Check `processor/lead_model.py`** for data model
5. **Test changes locally** before pushing to GitHub
6. **Monitor Hugging Face logs** after deployment

### Common Tasks:
- **Add new email extraction patterns** → Edit `extract_emails_from_source()` in `scraper/website_visitor.py`
- **Add new phone extraction patterns** → Edit `extract_phones_from_source()` in `scraper/website_visitor.py`
- **Add new CSV columns** → Update `Lead` dataclass in `processor/lead_model.py` + `CSV_HEADERS`
- **Add new routes** → Add to `app.py`
- **Fix extraction issues** → Increase Playwright wait time, add new selectors, or visit more pages

---

**Last Updated:** 2026-04-07  
**Current Version:** `02cdeb7`  
**Status:** Production-ready on Hugging Face Spaces
