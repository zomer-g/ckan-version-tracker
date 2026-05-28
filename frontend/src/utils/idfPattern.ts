/**
 * Single source of truth for the "is this an idf.il URL we know how
 * to scrape?" check on the frontend. Used by HomePage and SearchPage
 * to gate the "auto-treat as scraper" code path when the user pastes
 * a URL instead of typing a search query.
 *
 * Mirrors ``IDF_ALLOWED_SECTIONS`` in ``app/api/idf.py``. When a new
 * section is added there, append it here too — the entries must
 * agree or the frontend will accept URLs the backend rejects (or
 * vice-versa). The backend is the authoritative validator; this
 * regex is only a quick UI hint.
 *
 * The regex matches both forms users can paste:
 *   - raw Hebrew (modern browsers leave the address bar in this form)
 *   - percent-encoded Hebrew (copy from old browsers or scripts)
 */

interface IdfSection {
  /** Raw Hebrew slug as it appears in the URL path. */
  raw: string;
  /** Same string percent-encoded (UTF-8 bytes → %XX%XX…). */
  encoded: string;
}

// Pre-computed pairs so we don't recompute encodeURIComponent on
// every keystroke. Keep aligned with IDF_ALLOWED_SECTIONS on the
// backend.
export const IDF_SECTIONS: IdfSection[] = [
  {
    raw: "הפרקליטות-הצבאית",
    encoded:
      "%D7%94%D7%A4%D7%A8%D7%A7%D7%9C%D7%99%D7%98%D7%95%D7%AA-%D7%94%D7%A6%D7%91%D7%90%D7%99%D7%AA",
  },
  {
    raw: "אתר-הפקודות",
    encoded:
      "%D7%90%D7%AA%D7%A8-%D7%94%D7%A4%D7%A7%D7%95%D7%93%D7%95%D7%AA",
  },
];

const UNIT_SITES_RAW = "אתרי-יחידות";
const UNIT_SITES_ENC =
  "%D7%90%D7%AA%D7%A8%D7%99-%D7%99%D7%97%D7%99%D7%93%D7%95%D7%AA";

const sectionAlt = IDF_SECTIONS
  .map((s) => `${s.raw}|${s.encoded}`)
  .join("|");

/**
 * RegExp matching any idf.il URL that lives under a whitelisted
 * ``/אתרי-יחידות/<section>/`` prefix. Trailing path content is
 * required (a bare section root isn't an actionable scraper target).
 */
export const IDF_PATTERN = new RegExp(
  `^https?://(www\\.)?idf\\.il/(?:${UNIT_SITES_RAW}|${UNIT_SITES_ENC})/(?:${sectionAlt})/`,
  "i",
);
