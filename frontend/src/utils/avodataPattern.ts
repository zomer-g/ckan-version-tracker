/**
 * Single source of truth for "is this the avodata.labor.gov.il
 * occupations index?" on the frontend. Used by HomePage and SearchPage
 * to gate the auto-treat-as-scraper code path when the user pastes a
 * URL instead of typing a search query.
 *
 * Mirrors ``AVODATA_OCCUPATIONS_RE`` in ``app/api/avodata.py``. We
 * track the whole occupation corpus as ONE dataset (the per-scope
 * search is backed by a blocked Elasticsearch endpoint), so the only
 * accepted URL is the occupations index.
 */

export const AVODATA_OCCUPATIONS_PATTERN =
  /^https?:\/\/avodata\.labor\.gov\.il\/occupations\/?(?:[?#].*)?$/i;
