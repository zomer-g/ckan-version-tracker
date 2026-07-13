"""Outbound client for TAG-IT's MCP server — powers the ממ״מ "deep search".

The ממ״מ tab's default search hits our fast SQL metadata mirror
(``knesset.mmm_documents``). This module implements the OPTIONAL slow path:
a full-text search INSIDE the converted document bodies, run remotely on
TAG-IT (tag-it.biz) via its machine-to-machine *service-token bypass*.

Transport (see docs/service-integration.md): a stateless Streamable-HTTP
JSON-RPC 2.0 endpoint at ``settings.tagit_mcp_url``. We authenticate with a
single ``Authorization: Bearer <tagit_mcp_token>`` and call the
``search_documents`` tool against ``settings.tagit_mmm_scope`` (the ממ״מ
workspace on TAG-IT) with a ``text_query``.

We do NOT hard-code TAG-IT's scope-14 field names — every workspace exposes a
different ``meta.*`` catalog — so ``_normalize`` maps each hit best-effort
(title / date / doc_type / snippet / link) and always carries the raw field
dict through as ``fields`` so the caller can render whatever came back.
"""
from __future__ import annotations

import json
import logging

import httpx

from app.config import settings

logger = logging.getLogger(__name__)


class DeepSearchUnavailable(RuntimeError):
    """Deep search isn't configured (no service token) — surfaced as 503."""


class DeepSearchError(RuntimeError):
    """TAG-IT returned an error / unexpected payload — surfaced as 502."""


def is_configured() -> bool:
    return bool((settings.tagit_mcp_token or "").strip())


async def _rpc(method: str, params: dict) -> dict:
    """One stateless JSON-RPC 2.0 round-trip to the TAG-IT MCP endpoint."""
    if not is_configured():
        raise DeepSearchUnavailable("TAGIT_MCP_TOKEN is not set")
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    headers = {
        "Authorization": f"Bearer {settings.tagit_mcp_token.strip()}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=20.0),
                                 follow_redirects=True) as client:
        resp = await client.post(settings.tagit_mcp_url, headers=headers,
                                 json=payload)
    if resp.status_code == 401:
        raise DeepSearchError("TAG-IT rejected the service token (401)")
    if resp.status_code >= 400:
        raise DeepSearchError(f"TAG-IT MCP HTTP {resp.status_code}")
    # Streamable-HTTP may answer as application/json or as an SSE frame; both
    # carry a single JSON-RPC object. Pull the JSON out either way.
    body = _extract_json(resp)
    if "error" in body:
        err = body["error"]
        raise DeepSearchError(f"TAG-IT MCP error: {err.get('message') or err}")
    return body.get("result") or {}


def _extract_json(resp: httpx.Response) -> dict:
    ctype = resp.headers.get("content-type", "")
    if "text/event-stream" in ctype:
        # Concatenate `data:` lines of the last event and parse.
        data = "".join(line[5:].strip() for line in resp.text.splitlines()
                       if line.startswith("data:"))
        return json.loads(data) if data else {}
    return resp.json()


def _tool_payload(result: dict) -> dict | list:
    """A tools/call result wraps the tool's real return as JSON text inside
    ``content[0].text`` (per the MCP spec). Unwrap and parse it."""
    if isinstance(result.get("structuredContent"), (dict, list)):
        return result["structuredContent"]
    for part in result.get("content") or []:
        if part.get("type") == "text" and part.get("text"):
            try:
                return json.loads(part["text"])
            except (ValueError, TypeError):
                return {"text": part["text"]}
    return {}


# Field-name candidates, checked in order, against the FLATTENED hit. TAG-IT's
# scope-14 (ממ״מ) hit carries an English ``meta.*`` block AND a Hebrew-keyed
# ``ai.*`` block, so each list mixes both. ``meta.document_title`` etc. are the
# stable primary; the Hebrew ``ai`` keys are the fallback.
_TITLE_KEYS = ("document_title", "כותרת_המסמך", "title", "doc_title",
               "case_name", "שם_התיק", "name", "subject", "heading", "filename")
_DATE_KEYS = ("document_date", "תאריך_המסמך", "date", "doc_date", "published_at")
_ABSTRACT_KEYS = ("תקציר", "abstract", "summary", "description")
_TYPE_KEYS = ("doc_type", "document_type", "type", "category", "report_group")
_LINK_KEYS = ("pdf_url", "source_url", "url", "link", "original_pdf_url",
              "file_url", "document_url", "incident_url")


def _flatten(hit: dict) -> dict:
    """TAG-IT groups a hit's fields as ``{id, rank, snippet, ai:{…}, sql:{…},
    meta:{…}}``. Merge ONE level of nesting into a single flat lookup so field
    resolution doesn't depend on which block a value lives in. Scalars and the
    first-seen key win, so top-level values shadow the sub-blocks."""
    flat: dict = {}
    for k, v in hit.items():
        if isinstance(v, dict):
            for kk, vv in v.items():
                flat.setdefault(kk, vv)
        else:
            flat.setdefault(k, v)
    return flat


def _first(flat: dict, keys: tuple[str, ...]) -> str | None:
    for k in keys:
        v = flat.get(k)
        if v not in (None, "", [], {}):
            return str(v)
    return None


def _normalize(hit: dict) -> dict:
    """Best-effort projection of one opaque TAG-IT hit onto the shape the ממ״מ
    deep-results cards render, preserving the raw grouped fields under
    ``fields`` so the UI can fall back to anything we didn't map."""
    raw = hit.get("fields") if isinstance(hit.get("fields"), dict) else hit
    flat = _flatten(raw)
    return {
        "doc_id": hit.get("doc_id") or flat.get("id"),
        "title": _first(flat, _TITLE_KEYS),
        "date": _first(flat, _DATE_KEYS),
        "abstract": _first(flat, _ABSTRACT_KEYS),
        "doc_type": _first(flat, _TYPE_KEYS),
        "link": _first(flat, _LINK_KEYS),
        "snippet": hit.get("snippet") or flat.get("snippet"),
        "rank": hit.get("rank") or flat.get("rank"),
        "fields": raw,
    }


def _as_items_and_total(payload) -> tuple[list[dict], int | None]:
    """Locate the hit list + total in TAG-IT's paginated payload without
    assuming its exact key names."""
    if isinstance(payload, list):
        return payload, None
    if not isinstance(payload, dict):
        return [], None
    items = None
    for k in ("items", "results", "documents", "hits", "data", "rows"):
        if isinstance(payload.get(k), list):
            items = payload[k]
            break
    if items is None:
        items = []
    total = None
    for k in ("total", "total_count", "count", "totalHits", "num_results"):
        v = payload.get(k)
        if isinstance(v, int):
            total = v
            break
    return items, total


async def deep_search(text_query: str, page: int = 1, size: int = 20) -> dict:
    """Full-text search inside the ממ״מ document bodies on TAG-IT.

    Returns ``{items, total, page, size}`` where each item is normalized by
    ``_normalize`` (title/date/doc_type/snippet/link + raw ``fields``).
    """
    text_query = (text_query or "").strip()
    if not text_query:
        return {"items": [], "total": 0, "page": 1, "size": size}
    size = max(1, min(int(size or 20), 50))
    page = max(1, int(page or 1))

    result = await _rpc("tools/call", {
        "name": "search_documents",
        "arguments": {
            "scope": settings.tagit_mmm_scope,
            "text_query": text_query,
            "page": page,
            "size": size,
        },
    })
    payload = _tool_payload(result)
    items, total = _as_items_and_total(payload)
    norm = [_normalize(h) for h in items if isinstance(h, dict)]
    return {
        "items": norm,
        "total": total if total is not None else len(norm),
        "total_exact": total is not None,
        "page": page,
        "size": size,
    }
