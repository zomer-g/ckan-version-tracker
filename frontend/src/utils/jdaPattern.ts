/**
 * Single source of truth for "is this a trackable jda.gov.il URL?" on the
 * frontend. Used by HomePage and SearchPage to gate the
 * auto-treat-as-scraper code path when the user pastes a URL instead of
 * typing a search query.
 *
 * Mirrors ``corpus_of`` in ``app/api/jda.py``. Three trackable index
 * pages, each its own dataset (like mankal's horaot/hodaot/chozarim
 * split / avodata's occupations/education split): מכרזים (tenders),
 * הודעות לפי תקנות חובת המכרזים (notices), החלטות ועדת המכרזים
 * (decisions). The three paths are Hebrew WordPress permalink slugs, so
 * this pattern only matches the host — the backend ``/api/jda/validate``
 * is authoritative on the exact path.
 */

export const JDA_PATTERN =
  /^https?:\/\/(www\.)?jda\.gov\.il(?:\/[^?#]*)?(?:[?#].*)?$/i;
