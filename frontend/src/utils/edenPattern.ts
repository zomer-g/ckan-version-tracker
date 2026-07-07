/**
 * Single source of truth for "is this a trackable jeden.co.il URL?" on the
 * frontend. Used by HomePage and SearchPage to gate the
 * auto-treat-as-scraper code path when the user pastes a URL instead of
 * typing a search query.
 *
 * Mirrors the host check in ``app/api/eden.py`` (חברת עדן / Eden, the
 * Jerusalem municipal development company). Two corpora — מכרזים
 * (tenders) and החלטות ועדת מכרזים (decisions) — but unlike jda they
 * live on ONE shared page, so the corpus is chosen by a ``?category=``
 * marker (``?category=tenders`` / ``?category=decisions``). This pattern
 * only matches the host with any path/query/hash (so it also matches URLs
 * carrying ``?category=``) — the backend ``/api/eden/validate`` is
 * authoritative on the corpus and rejects a bare URL with no category.
 */

export const EDEN_PATTERN =
  /^https?:\/\/(www\.)?jeden\.co\.il(?:\/[^?#]*)?(?:[?#].*)?$/i;
