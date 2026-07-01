/**
 * Single source of truth for "is this a trackable חוזרי מנכ"ל index page?"
 * on the frontend. Used by HomePage and SearchPage to gate the
 * auto-treat-as-scraper code path when the user pastes a URL instead of
 * typing a search query.
 *
 * Mirrors ``MANKAL_INDEX_PATHS`` in ``app/api/mankal.py``. The whole
 * corpus is one dataset: the portal index (default.aspx / the /Mankal
 * root / EtzNosim.aspx). Naked ?siduri= item pages are NOT trackable
 * individually — the scraper walks them internally.
 */

export const MANKAL_PATTERN =
  /^https?:\/\/apps\.education\.gov\.il\/mankal(?:\/(?:default\.aspx|etznosim\.aspx)?)?\/?(?:[?#].*)?$/i;
