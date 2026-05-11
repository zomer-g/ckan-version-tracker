"""Local collector for raw gov.il CollectorsWebApi / ContentPageWebApi URLs.

The external GOV SCRAPER worker only understands SPA URLs
(``/he/departments/dynamiccollectors/...`` and friends). When an admin
pastes the underlying JSON API URL — e.g.
``https://www.gov.il/CollectorsWebApi/api/DataCollector/GetResults?...``
— the scraper falls through to a default fetch path and the gov.il CDN
serves the SPA shell as HTML (no ``Accept: application/json``), which
shows up in admin as "returned HTML instead of JSON".

This module collects those URLs directly: it pages through the
endpoint, flattens every record into a CSV-friendly row, and returns a
list of rows + a stable field schema. The poll job hands the result to
the regular snapshot pipeline so the dataset versions like any other.
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import httpx

logger = logging.getLogger(__name__)

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; over.org.il)",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.gov.il/",
}
TIMEOUT = httpx.Timeout(connect=15.0, read=60.0, write=15.0, pool=10.0)

# Page size used for our paginated fetches. The SPA defaults to 20; we
# go larger so we round-trip less. Gov.il's API has been observed to
# accept up to a few hundred — anything beyond ~200 risks 500s.
PAGE_SIZE = 100
# Hard cap on pages walked per poll. The largest collectors run in the
# low thousands of items; 200 pages * 100 items = 20k rows is plenty
# while still bounding worst-case poll duration.
MAX_PAGES = 200


def _envelope_results(data: Any) -> list | None:
    """Find the row list inside a collector response envelope."""
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return None
    for key in ("Results", "results", "Items", "items", "Records", "records", "Data", "data"):
        v = data.get(key)
        if isinstance(v, list):
            return v
    return None


def _envelope_total(data: Any) -> int | None:
    """Try to pull a total-count from the response envelope."""
    if not isinstance(data, dict):
        return None
    for key in ("TotalResults", "totalResults", "Total", "total", "Count", "count"):
        v = data.get(key)
        if isinstance(v, int):
            return v
    return None


def _flatten(value: Any) -> str:
    """Render any JSON value as a CSV-friendly string.

    Nested dicts/lists are serialised as JSON. Scalars become their
    string repr; ``None`` becomes the empty string so blank cells round
    -trip cleanly through CSV → datastore.
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float, str)):
        return str(value)
    import json as _json
    try:
        return _json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        return str(value)


def _flatten_record(rec: dict) -> dict:
    """Stringify every value in a row so it survives the CSV path."""
    return {k: _flatten(v) for k, v in rec.items()}


def _with_paging(url: str, skip: int, limit: int) -> str:
    """Return ``url`` with skip/limit set to the given values.

    Preserves all other query params verbatim. Param names are matched
    case-insensitively so an ``officeId=...`` style URL keeps its
    capitalisation. ``skip``/``limit`` are added when missing.
    """
    parsed = urlparse(url)
    pairs = parse_qsl(parsed.query, keep_blank_values=True)
    found_skip = False
    found_limit = False
    new_pairs: list[tuple[str, str]] = []
    for k, v in pairs:
        lk = k.lower()
        if lk == "skip":
            new_pairs.append((k, str(skip)))
            found_skip = True
        elif lk == "limit":
            new_pairs.append((k, str(limit)))
            found_limit = True
        else:
            new_pairs.append((k, v))
    if not found_skip:
        new_pairs.append(("skip", str(skip)))
    if not found_limit:
        new_pairs.append(("limit", str(limit)))
    return urlunparse(parsed._replace(query=urlencode(new_pairs, doseq=True)))


async def fetch_all_records(url: str) -> tuple[list[dict], list[dict], dict]:
    """Page through a gov.il collector API URL.

    Returns ``(records, fields, meta)`` where ``records`` is a list of
    flat dicts (every value is a string), ``fields`` is the inferred
    column schema (``[{"id": "...", "type": "text"}, ...]``), and
    ``meta`` carries the reported total and the number of pages walked.

    Raises ``ValueError`` when the endpoint responds with HTML or with a
    body shape we can't interpret — surfaces a usable error message to
    the caller / admin UI without leaking the full 8KB SPA shell.
    """
    records: list[dict] = []
    total_from_server: int | None = None
    pages = 0

    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        follow_redirects=True,
        headers=REQUEST_HEADERS,
    ) as client:
        for page_idx in range(MAX_PAGES):
            page_url = _with_paging(url, skip=page_idx * PAGE_SIZE, limit=PAGE_SIZE)
            resp = await client.get(page_url)
            if resp.status_code != 200:
                raise ValueError(
                    f"collector API returned HTTP {resp.status_code} (page {page_idx + 1})"
                )
            ctype = (resp.headers.get("content-type") or "").lower()
            text = resp.text
            if "json" not in ctype and not text.lstrip().startswith(("{", "[")):
                sample = text[:200].replace("\n", " ")
                raise ValueError(
                    f"collector API returned non-JSON (content-type={ctype!r}, "
                    f"sample={sample!r}) — URL is likely not a collector endpoint"
                )
            try:
                data = resp.json()
            except Exception as e:
                raise ValueError(f"collector API returned malformed JSON: {e}") from e

            page_rows = _envelope_results(data)
            if page_rows is None:
                raise ValueError(
                    "collector API response had no recognised results array "
                    "(expected one of Results/Items/Records/Data)"
                )

            if total_from_server is None:
                total_from_server = _envelope_total(data)

            # Each row gets stringified to avoid pushing nested JSON
            # objects into a CKAN text column.
            for rec in page_rows:
                if isinstance(rec, dict):
                    records.append(_flatten_record(rec))
                else:
                    # Defensive: a list of scalars/strings — wrap as
                    # single-column rows so we still keep the data.
                    records.append({"value": _flatten(rec)})

            pages = page_idx + 1

            # Termination: fewer rows than requested → last page. Or we
            # already collected at least ``total_from_server`` items.
            if len(page_rows) < PAGE_SIZE:
                break
            if total_from_server is not None and len(records) >= total_from_server:
                break

    # Build the field schema from the union of keys we observed. We use
    # the first record's key order as the "primary" order and append
    # any keys that only appeared in later records — this keeps the
    # column order stable across versions when the source is well
    # -behaved while still tolerating optional fields.
    primary_order: list[str] = []
    seen: set[str] = set()
    if records:
        for k in records[0].keys():
            primary_order.append(k)
            seen.add(k)
        for rec in records[1:]:
            for k in rec.keys():
                if k not in seen:
                    primary_order.append(k)
                    seen.add(k)

    fields = [{"id": k, "type": "text"} for k in primary_order]
    meta = {
        "total_from_server": total_from_server,
        "pages_walked": pages,
        "row_count": len(records),
        "page_size": PAGE_SIZE,
    }
    logger.info(
        "Collector API %s yielded %d rows over %d pages (server-total=%s)",
        url, len(records), pages, total_from_server,
    )
    return records, fields, meta
