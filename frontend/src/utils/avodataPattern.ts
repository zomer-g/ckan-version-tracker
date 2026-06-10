/**
 * Single source of truth for "is this an avodata.labor.gov.il scope
 * URL?" on the frontend. Used by HomePage and SearchPage to gate the
 * auto-treat-as-scraper code path when the user pastes a URL instead
 * of typing a search query.
 *
 * Mirrors ``AVODATA_SEARCH_RE`` + the AVODATA_SCOPES_HE allowlist in
 * ``app/api/avodata.py``. The backend is the authoritative validator;
 * this regex is only a quick UI hint. We deliberately do NOT enforce
 * the 22-scope allowlist here — the backend has it, and duplicating
 * the Hebrew list in two places risks them drifting. Any
 * ``/search?scope=<non-empty>`` URL on the right host passes the UI
 * check; the backend's /api/avodata/validate is what rejects unknown
 * scope names with a helpful message.
 */

export const AVODATA_SEARCH_PATTERN =
  /^https?:\/\/avodata\.labor\.gov\.il\/search\/?\?[^#]*\bscope=[^&#]+/i;
