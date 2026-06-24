/**
 * Single source of truth for "is this a trackable geo.mot.gov.il (חצב)
 * portal URL?" on the frontend. Used by HomePage and SearchPage to gate
 * the auto-treat-as-scraper code path when the user pastes a URL instead
 * of typing a search query.
 *
 * Mirrors ``HATZAV_ROOT_RE`` in ``app/api/hatzav.py``. חצב is a GovMap
 * map viewer with no per-layer URLs, so the whole layer catalog is
 * tracked as ONE dataset, registered via the portal root.
 */

export const HATZAV_PATTERN =
  /^https?:\/\/geo\.mot\.gov\.il\/?(?:index\.html?)?\/?(?:[?#].*)?$/i;
