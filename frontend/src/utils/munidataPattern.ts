/**
 * Single source of truth for "is this a trackable municipal-data.org
 * per-metric page?" on the frontend. Used by HomePage and SearchPage to
 * gate the auto-treat-as-scraper code path when the user pastes a URL
 * instead of typing a search query.
 *
 * Mirrors ``_parse_munidata_url`` in ``app/api/munidata.py`` (the backend
 * is authoritative — this is only a permissive UI hint). municipal-data.org
 * ("מצב השלטון המקומי", Ministry of Interior) tracks one dataset per metric;
 * the trackable URL is a clean screen slug (demographics | budget |
 * governance | human-capital), a screen id, or ?screen=, plus a
 * ``?metric=<id>`` query param. A bare screen page (no ?metric) is not
 * trackable on its own.
 */

export const MUNIDATA_METRIC_PATTERN =
  /^https?:\/\/(?:www\.)?municipal-data\.org\/[^?#]*\?(?:[^#]*&)?metric=[^&#]+/i;
