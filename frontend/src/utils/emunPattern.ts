/**
 * Single source of truth for "is this a trackable מערכת אמו״ן page?" on the
 * frontend. Used by HomePage and SearchPage to gate the auto-treat-as-scraper
 * code path when the user pastes a URL instead of typing a search query.
 *
 * Mirrors ``_parse_emun_url`` in ``app/api/emun.py`` (the backend is
 * authoritative — this is only a permissive UI hint). govextra.gov.il/pmo/emun
 * ("מערכת אמו״ן", Prime Minister's Office) is a single dashboard — one
 * embedded Looker Studio report — so every path under /pmo/emun is the same,
 * single trackable dataset. govextra.gov.il hosts other, unrelated PMO
 * mini-sites, so the /pmo/emun path prefix is what makes a URL ours.
 */

export const EMUN_DASHBOARD_PATTERN =
  /^https?:\/\/(?:www\.)?govextra\.gov\.il\/pmo\/emun(?:[/?#]|$)/i;
