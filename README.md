---
title: Company Lead Extractor
emoji: 🔍
colorFrom: indigo
colorTo: purple
sdk: docker
pinned: false
app_port: 7860
---

# Company Lead Extractor Tool

A free, self-hosted tool that searches Google for companies by **Country** and **Business Type**, visits each website, and extracts:

- **Company Name**
- **Email Addresses**
- **Phone / Mobile Numbers**
- **WhatsApp Numbers**
- **Business Category**

Exports all data to **CSV** or **Google Sheets** (free API).

## Features

- Headless Chrome automation via Playwright
- Regex-based email & phone extraction
- Contact page discovery (visits `/contact`, `/about` pages automatically)
- Real-time progress logs via Server-Sent Events
- Anti-bot: random delays, user-agent rotation
- Data cleaning: email DNS validation, E.164 phone formatting, fuzzy dedup
- Optional Google Sheets push (free service account)

## Tech Stack

| Layer | Technology |
|---|---|
| Backend | Python 3.11 + Flask |
| Search | Playwright (headless Chromium) |
| Scraping | requests + BeautifulSoup4 |
| Extraction | Regex |
| Storage | CSV export / Google Sheets API |
| UI | Vanilla JS + SSE |

## Quick Start (Local)

```bash
# 1. Install dependencies
pip install -r requirements.txt
playwright install chromium

# 2. Run
python app.py

# 3. Open
# http://localhost:7860
```

## Google Sheets Setup (Optional)

1. Go to [Google Cloud Console](https://console.cloud.google.com) → create a free project
2. Enable **Google Sheets API** + **Google Drive API**
3. Create a **Service Account** → download JSON key → save as `credentials.json` in project root
4. Share your target spreadsheet with the service account email as **Editor**
5. In the UI, check "Save to Google Sheets" and paste the Spreadsheet ID

> **Note:** Never commit `credentials.json` — it is in `.gitignore`

## Output Format

| Company Name | Email(s) | Phone(s) | WhatsApp | Business Type | Website URL | Country | Scraped At |
|---|---|---|---|---|---|---|---|

## Deploying to Hugging Face Spaces

1. Fork / clone this repo
2. Go to [huggingface.co/spaces](https://huggingface.co/spaces) → **New Space**
3. Select **Docker** as the SDK
4. Connect your GitHub repo **or** push directly to the Space's git remote
5. The Space builds automatically — takes ~3–5 minutes for the first build (Chromium download)
6. Access your live app at `https://huggingface.co/spaces/YOUR_USERNAME/YOUR_SPACE_NAME`

> Free tier: ~1 GB RAM, persistent process, 24/7 uptime (may sleep after inactivity on free tier)

## Environment Variables

Copy `.env.example` to `.env` and fill in:

| Variable | Default | Description |
|---|---|---|
| `GOOGLE_CREDENTIALS_PATH` | `./credentials.json` | Path to Google service account JSON |
| `FLASK_SECRET_KEY` | `dev-secret-change-me` | Flask secret (change in production!) |
| `FLASK_DEBUG` | `false` | Enable Flask debug mode |
| `MAX_CONCURRENT_JOBS` | `3` | Max simultaneous scraping jobs |
| `PORT` | `7860` | Server port (set automatically by HF Spaces) |

## Important Notes

- This tool scrapes public web pages only
- Google may show CAPTCHAs — the tool detects and skips them automatically
- Respect websites' `robots.txt` and terms of service
- Use responsibly and in compliance with applicable laws
