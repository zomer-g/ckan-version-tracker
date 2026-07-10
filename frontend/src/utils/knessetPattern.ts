/**
 * Single source of truth for "is this a trackable knesset.gov.il committee
 * URL?" on the frontend. Used by HomePage and SearchPage to gate the
 * auto-treat-as-scraper code path when the user pastes a URL instead of
 * typing a search query.
 *
 * Mirrors the host + path check in ``app/api/knesset.py``. Each committee is
 * tracked as its own dataset via an ODATA ``KNS_Committee`` query carrying a
 * committee scope — ``?$filter=CategoryID eq N`` (the persistent committee
 * across all Knessets, e.g. ועדת הכספים = cat 2) or ``?$filter=Id eq N`` (a
 * single committee). This pattern matches the KNS_Committee entity set on the
 * host with any query; the backend ``/api/knesset/validate`` is authoritative
 * and rejects a query that carries no committee scope.
 */

export const KNESSET_PATTERN =
  /^https?:\/\/knesset\.gov\.il\/OdataV4\/ParliamentInfo\/KNS_Committee(?:[?#].*)?$/i;
