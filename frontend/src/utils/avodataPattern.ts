/**
 * Single source of truth for "is this a trackable avodata.labor.gov.il
 * index page?" on the frontend. Used by HomePage and SearchPage to gate
 * the auto-treat-as-scraper code path when the user pastes a URL instead
 * of typing a search query.
 *
 * Mirrors ``AVODATA_OCCUPATIONS_RE`` / ``AVODATA_EDUCATION_RE`` in
 * ``app/api/avodata.py``. Two trackable index pages, each its own
 * dataset: /occupations (occupation corpus) and /education (studies &
 * training corpus). Per-scope search pages are NOT trackable (backed by
 * a blocked Elasticsearch endpoint).
 */

export const AVODATA_OCCUPATIONS_PATTERN =
  /^https?:\/\/avodata\.labor\.gov\.il\/(?:occupations|education)\/?(?:[?#].*)?$/i;
