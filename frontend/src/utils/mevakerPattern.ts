/**
 * Single source of truth for "is this a trackable mevaker.gov.il reports
 * index page?" on the frontend. Used by HomePage and SearchPage to gate
 * the auto-treat-as-scraper code path when the user pastes a URL instead
 * of typing a search query.
 *
 * Mirrors ``MEVAKER_SUBJECTS_RE`` in ``app/api/mevaker.py``. The whole
 * State Comptroller report corpus is tracked as ONE dataset, registered
 * via the public "דוחות לפי נושאים" landing page at /subjects.
 */

export const MEVAKER_SUBJECTS_PATTERN =
  /^https?:\/\/(?:www\.)?mevaker\.gov\.il\/subjects\/?(?:[?#].*)?$/i;
