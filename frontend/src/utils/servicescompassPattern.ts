/**
 * Single source of truth for "is this the trackable gov.il services-compass
 * page?" on the frontend. Used by HomePage and SearchPage to gate the
 * auto-treat-as-scraper code path when the user pastes a URL instead of
 * typing a search query.
 *
 * Mirrors ``SERVICESCOMPASS_PATH_RE`` in ``app/api/servicescompass.py``.
 * One trackable URL, one dataset: the National Digital Agency's weekly
 * "מצפן השירותים הממשלתיים" dashboard at gov.il/apps/servicescompass.
 */

export const SERVICESCOMPASS_PATTERN =
  /^https?:\/\/(?:www\.)?gov\.il\/apps\/servicescompass\/?(?:[?#].*)?$/i;
