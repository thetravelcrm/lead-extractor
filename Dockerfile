# ── Dockerfile ────────────────────────────────────────────────────────────
# Targets Hugging Face Spaces (port 7860) but also works on any Docker host.
# Build: docker build -t lead-extractor .
# Run:   docker run -p 7860:7860 lead-extractor
# ──────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim

# System dependencies required by Playwright's Chromium browser
# (same list that `playwright install-deps chromium` would install)
RUN apt-get update && apt-get install -y --no-install-recommends \
      # Chromium runtime libraries
      libnss3 \
      libatk1.0-0 \
      libatk-bridge2.0-0 \
      libcups2 \
      libdrm2 \
      libxkbcommon0 \
      libxcomposite1 \
      libxdamage1 \
      libxrandr2 \
      libgbm1 \
      libasound2 \
      libxss1 \
      libpango-1.0-0 \
      libpangocairo-1.0-0 \
      libcairo2 \
      libatspi2.0-0 \
      libgtk-3-0 \
      libx11-xcb1 \
      libxcb-dri3-0 \
      libxfixes3 \
      fonts-liberation \
      # Needed to download Playwright browsers
      wget \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user (HF Spaces runs as UID 1000)
RUN useradd -m -u 1000 appuser

WORKDIR /app

# Copy and install Python dependencies first (layer-cached)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright's Chromium browser
# PLAYWRIGHT_BROWSERS_PATH keeps the binary inside /app so it is owned by appuser
ENV PLAYWRIGHT_BROWSERS_PATH=/app/.playwright-browsers
RUN playwright install chromium && chmod -R 777 /app/.playwright-browsers

# Copy the rest of the source code
COPY --chown=appuser:appuser . .

# Switch to non-root user
USER appuser

# HF Spaces requires port 7860
EXPOSE 7860
ENV PORT=7860

# Use gunicorn for production (threaded worker handles SSE + background threads)
CMD ["gunicorn", \
     "--bind", "0.0.0.0:7860", \
     "--workers", "1", \
     "--threads", "8", \
     "--worker-class", "gthread", \
     "--timeout", "600", \
     "--keep-alive", "120", \
     "app:app"]
