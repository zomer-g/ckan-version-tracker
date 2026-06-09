/**
 * Single source of truth for the "is this a
 * practitioners.health.gov.il URL we know how to scrape?" check on
 * the frontend. Used by HomePage and SearchPage to gate the
 * "auto-treat as scraper" code path when the user pastes a URL
 * instead of typing a search query.
 *
 * Mirrors ``HEALTH_PRACTITIONERS_RE`` in ``app/api/health.py``: only
 * per-registry URLs ``/Practitioners/{numeric id}`` are in scope. The
 * bare ``/Practitioners`` index is intentionally rejected — each
 * registry id is tracked as its own dataset (see the OVER/GOVSCRAPER
 * decision documented in ``app/api/health.py``).
 *
 * The backend is the authoritative validator; this regex is only a
 * quick UI hint so the search bar treats the pasted URL as a scraper
 * candidate instead of falling through to keyword search.
 */

export const HEALTH_PRACTITIONERS_PATTERN =
  /^https?:\/\/practitioners\.health\.gov\.il\/Practitioners\/\d+\/?$/i;
