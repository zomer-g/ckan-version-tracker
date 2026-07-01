/**
 * Single source of truth for "is this a trackable חוזרי מנכ"ל URL?" on the
 * frontend. Used by HomePage and SearchPage to gate the
 * auto-treat-as-scraper code path when the user pastes a URL instead of
 * typing a search query.
 *
 * Mirrors ``_corpus_of_url`` in ``app/api/mankal.py``. Four trackable
 * URLs, each its own dataset (like avodata's /occupations vs /education):
 *   default.aspx / the /Mankal root / EtzNosim.aspx → all
 *   Horaa.aspx  (no ?siduri=)  → הוראות
 *   Hodaa.aspx  (no ?siduri=)  → הודעות
 *   Chozer.aspx (no ?siduri=)  → חוזרים
 * A type path WITH ?siduri= is a naked item page and is NOT trackable — the
 * negative lookahead on the query string rejects it (the backend is
 * authoritative; this is only a UI hint).
 */

export const MANKAL_PATTERN =
  /^https?:\/\/apps\.education\.gov\.il\/mankal(?:\/(?:default\.aspx|etznosim\.aspx|(?:horaa|hodaa|chozer)\.aspx(?!\?[^#]*siduri=))?)?\/?(?:[?#].*)?$/i;
