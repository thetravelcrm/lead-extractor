# Email Extractor Tool - Version History

## Version Tracking
Each release updates this file with version number and changes.

---

### V2.5 - 2026-04-08
**Fix: Google Maps scraper selecting wrong elements**
- Fixed critical bug where scraper was selecting map control elements instead of business listings
- Updated selectors to target `div[role="article"] div[tabindex="0"]` for business cards
- Added filtering to skip map instructions ("arrow keys", "pan the map", "get details")
- Increased initial wait to 5000ms for full JS render
- Improved card validation: skip names < 3 characters
- Better website extraction from sidebar and side panel
- Simplified category extraction logic
- Progress logging every 10 scrolls

### V2.4 - 2026-04-08
**Improved: Google Maps scroll mechanism**
- Use keyboard End key as fallback scroll method
- Track scroll position changes to verify scrolling works
- Increase wait time to 3000ms after scroll for results to load
- Stop after 8 consecutive scrolls with no new results
- Add progress logging every 10 scrolls
- Better end-of-results detection
- Scroll 1500px per scroll

### V2.3 - 2026-04-08
**Improved: Google Maps scraper efficiency**
- Increase scroll aggressiveness: 1200px per scroll
- Increase max scrolls: max_results//3
- Better card selectors: prioritize div[role='article']
- Wait 2500ms after scroll for results to load
- Track consecutive scrolls with no new results
- Multiple scroll container fallbacks
- Better 'end of results' detection
- Try extracting name from headings if aria-label missing
- Longer initial wait (3000ms)

### V2.2 - 2026-04-08
**Fix: Intelligently clean business_type**
- Detect and remove 'in [location]' patterns from business_type field
- Handles: 'travel agency in lucknow' → 'travel agency'
- Prevents duplicate location in query
- Builds correct query: 'travel agency in Lucknow, India'

### V2.1 - 2026-04-08
**Fix: Database permission error in Docker**
- Changed database path from /app/data to /tmp/lead_extractor.db
- Added fallback logic for PermissionError
- Fixed global declaration in _ensure_db_dir()

### V2.0 - 2026-04-07
**Major: 2-Phase Extraction System**
- Added SQLite database for persistent search tracking
- Phase 1: Search Google Maps and save ALL listings (~300-500)
- Phase 2: Extract emails in batches (e.g., 50 at a time)
- Left sidebar with search history and stats
- Modals for existing search detection and batch extraction
- Extended Google Maps scraping with multiple query variations
- 6 new API routes for search management
- Track extracted/remaining counts per keyword
- Download all extracted data for a keyword
- Delete search and its data

### V1.0 - 2026-04-06
**Initial Release**
- Google Maps search and email extraction
- CSV export and Google Sheets integration
- Real-time progress via SSE
- Anti-bot protection with random delays
- WhatsApp/Phone number extraction
- Multi-page website crawling
