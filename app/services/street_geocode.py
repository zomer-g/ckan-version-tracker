"""Place a street that only the official register knows, using GovMap.

27,963 streets in ``over_re_streets`` come from רשות האוכלוסין's register alone:
real streets that the address list, the postal file and the gazetteer all omit,
so nothing in the crosswalk can put them on a map. הר הצופים in Dimona is one,
and the reader who reported it lives there.

GovMap can place them — but only if you ask the right index. Its autocomplete
takes a ``filterType``, and the one the batch geocoder uses is ``address``,
which searches house-level points. Asked for "הר הצופים דימונה" under that
filter it returns sixteen results, **every one of them in Kiryat Shmona, 240 km
away**, because it ignores the locality term and answers from the street name
alone. Under ``filterType: "street"`` the same query returns the street itself,
at ITM 202667/554833, which is the middle of Dimona.

So this module does two things, and the second is not optional:

1. Asks the STREET index rather than the address index.
2. Refuses an answer that is not near the settlement that was asked for. The
   caller supplies that test, because it needs the parcel table; this module
   hands back the candidate and the caller decides. Without it the Dimona
   lookup would confidently return a street in Kiryat Shmona — which is the
   same failure ``geocode_queue.merge_into_addresses`` already guards against
   ("asked for גדרה, GovMap answers חדרה").

Live, in a request path, so it is bounded hard: one call, a short timeout, a
small memo, and any failure returns None rather than raising. A street we
cannot place is the status quo, not an error.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import time
import uuid

logger = logging.getLogger(__name__)

ENDPOINT = "https://www.govmap.gov.il/api/search-service/autocomplete"
# A request path, not a batch: short enough that a slow GovMap costs the reader
# a moment rather than the whole answer.
TIMEOUT_SECONDS = 6.0
_TTL_SECONDS = 3600.0
_MAX_CACHE = 2000

_cache: dict[str, tuple[float, dict | None]] = {}
_lock = asyncio.Lock()


def _web_mercator_to_wgs84(x: float, y: float) -> tuple[float, float]:
    """GovMap returns EPSG:3857. Verified against the coordinates its own UI
    puts in a share link: this point round-trips to ITM 202667/554833, which is
    exactly what govmap.gov.il/?c=202667.17,554833.51 shows."""
    lon = x / 20037508.34 * 180.0
    lat = y / 20037508.34 * 180.0
    lat = 180.0 / math.pi * (2.0 * math.atan(math.exp(lat * math.pi / 180.0)) - math.pi / 2.0)
    return lat, lon


def _parse_point(shape: str | None) -> tuple[float, float] | None:
    if not shape or not shape.startswith("POINT("):
        return None
    try:
        x, y = shape[6:].rstrip(")").split()
        return _web_mercator_to_wgs84(float(x), float(y))
    except (ValueError, TypeError):
        return None


async def locate_street(settlement_name: str, street: str) -> dict | None:
    """GovMap's best guess for ``street`` in ``settlement_name``, unguarded.

    Returns ``{"lat", "lon", "label"}`` or None. The result is NOT verified to
    be in the right settlement — see the module docstring; the caller must do
    that against the parcel table before showing it to anyone.
    """
    q = f"{street} {settlement_name}".strip()
    if not q:
        return None

    now = time.monotonic()
    hit = _cache.get(q)
    if hit and now - hit[0] < _TTL_SECONDS:
        return hit[1]

    async with _lock:
        hit = _cache.get(q)
        if hit and time.monotonic() - hit[0] < _TTL_SECONDS:
            return hit[1]
        found = await _ask(q)
        if len(_cache) >= _MAX_CACHE:
            _cache.clear()
        _cache[q] = (time.monotonic(), found)
        return found


async def _ask(q: str) -> dict | None:
    import httpx
    body = {"searchText": q, "language": "he", "filterType": "street",
            "isAccurate": True, "maxResults": 5}
    headers = {"Referer": "https://www.govmap.gov.il/",
               "User-Agent": "Mozilla/5.0",
               "x-fingerprint-id": str(uuid.uuid4()),
               "x-user-id": str(uuid.uuid4()),
               "x-trace-id": str(uuid.uuid4())}
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_SECONDS) as client:
            r = await client.post(ENDPOINT, json=body, headers=headers)
        if r.status_code != 200:
            return None
        results = (r.json() or {}).get("results") or []
    except Exception as e:  # noqa: BLE001 — an unplaceable street is the status quo
        logger.info("street_geocode: %s unavailable (%s)", q, str(e)[:120])
        return None

    for it in results:
        if not isinstance(it, dict) or it.get("type") != "street":
            continue
        point = _parse_point(it.get("shape"))
        if point:
            return {"lat": point[0], "lon": point[1],
                    "label": (it.get("text") or "").strip() or None}
    return None


def to_json(value) -> str:            # pragma: no cover - debugging aid
    return json.dumps(value, ensure_ascii=False)
