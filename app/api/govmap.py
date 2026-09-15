"""GovMap layer URL parsing and validation.

Parses URLs from https://www.govmap.gov.il/ that point to a single layer
(via the ``lay`` query parameter). The actual scraping is done by the
external GOV SCRAPER worker — this module only validates the URL shape
on the request side and provides a typed parser for storing the layer
metadata on TrackedDataset.scraper_config.

A link can also point at a layer GROUP (``?g=397``): a published bundle of
layers that GovMap draws as one (``govmap:group_397``). A group is not a
dataset — it has no features of its own — so validation expands it to its
member layers from the catalog's ``userGroups`` and the request is made for
those.
"""

import logging
import re
import time

import httpx
from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.rate_limit import limiter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/govmap", tags=["govmap"])

GOVMAP_HOST_RE = re.compile(r"^https?://(www\.)?govmap\.gov\.il/?\?", re.IGNORECASE)
LAY_RE = re.compile(r"[?&]lay(?:er|ers)?=(\d+)", re.IGNORECASE)
GROUP_RE = re.compile(r"[?&]g=(\d+)", re.IGNORECASE)
CENTER_RE = re.compile(r"[?&]c=([-\d.]+),([-\d.]+)", re.IGNORECASE)

# The catalog is ~730 KB and a group's membership changes rarely, so one fetch
# serves every group lookup for a while. Failures are never cached.
_CATALOG_TTL_S = 3600
_catalog_cache: tuple[float, dict] | None = None


class ParsedGovmap(BaseModel):
    layer_id: str
    center_itm: dict | None = None  # {"x": float, "y": float}


def parse_govmap_url(url: str) -> ParsedGovmap | None:
    """Parse a GovMap layer URL.

    Returns None if it isn't a recognised GovMap URL with a numeric ``lay`` id.
    """
    s = (url or "").strip()
    if not GOVMAP_HOST_RE.match(s):
        return None
    lay_match = LAY_RE.search(s)
    if not lay_match:
        return None

    center: dict | None = None
    cm = CENTER_RE.search(s)
    if cm:
        try:
            center = {"x": float(cm.group(1)), "y": float(cm.group(2))}
        except ValueError:
            center = None

    return ParsedGovmap(layer_id=lay_match.group(1), center_itm=center)


def parse_govmap_group_id(url: str) -> str | None:
    """The ``g=`` layer-group id of a GovMap link that names no single layer."""
    s = (url or "").strip()
    if not GOVMAP_HOST_RE.match(s) or LAY_RE.search(s):
        return None
    m = GROUP_RE.search(s)
    return m.group(1) if m else None


def build_govmap_title(layer_id: str) -> str:
    return f"GovMap layer {layer_id}"


def build_govmap_layer_url(layer_id: str) -> str:
    return f"https://www.govmap.gov.il/?lay={layer_id}"


def group_layers(catalog: dict, group_id: str) -> tuple[str, list[dict]] | None:
    """``(group name, member layers)`` for a group id, or None if the catalog
    has no such group. Members keep the group's display order; a node that is
    not a layer (or not in the catalog) still gets its id, so nothing the group
    holds is silently dropped."""
    groups = catalog.get("userGroups") if isinstance(catalog, dict) else None
    if not isinstance(groups, list):
        return None
    group = next((g for g in groups if str(g.get("id")) == str(group_id)), None)
    if group is None:
        return None
    by_id = {
        str(item.get("id")): item
        for item in (catalog.get("catalog") or [])
        if isinstance(item, dict)
    }
    nodes = sorted(
        (n for n in group.get("nodes") or [] if n.get("nodeType", "layer") == "layer"),
        key=lambda n: n.get("displayOrder", 0),
    )
    layers = []
    for node in nodes:
        layer_id = str(node.get("nodeId"))
        entry = by_id.get(layer_id) or {}
        caption = (
            entry.get("caption")
            or (node.get("layerIcon") or {}).get("layerName")
            or build_govmap_title(layer_id)
        )
        layers.append({
            "layer_id": layer_id,
            "caption": str(caption).strip(),
            "url": build_govmap_layer_url(layer_id),
        })
    return (group.get("groupName") or f"GovMap group {group_id}"), layers


async def _fetch_catalog() -> dict:
    global _catalog_cache
    now = time.monotonic()
    if _catalog_cache and now - _catalog_cache[0] < _CATALOG_TTL_S:
        return _catalog_cache[1]
    from app.services.govmap_coverage import CATALOG_URL, _catalog_headers

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        r = await client.get(CATALOG_URL, headers=_catalog_headers())
        r.raise_for_status()
        catalog = r.json()
    if not isinstance(catalog, dict) or "catalog" not in catalog:
        raise ValueError("GovMap catalog response has no catalog")
    _catalog_cache = (now, catalog)
    return catalog


class ValidateRequest(BaseModel):
    url: str


class GroupLayer(BaseModel):
    layer_id: str
    caption: str
    url: str


class ValidateResponse(BaseModel):
    valid: bool
    layer_id: str | None = None
    center_itm: dict | None = None
    url: str | None = None
    title: str | None = None
    error: str | None = None
    # Set for a layer-group link: the group, and the layers to request instead.
    group_id: str | None = None
    layers: list[GroupLayer] = []


@router.post("/validate", response_model=ValidateResponse)
@limiter.limit("20/minute")
async def validate_govmap_url(request: Request, body: ValidateRequest):
    """Validate a govmap.gov.il layer URL and return a normalised parse."""
    parsed = parse_govmap_url(body.url)
    if not parsed:
        group_id = parse_govmap_group_id(body.url)
        if group_id:
            return await _validate_group(body.url.strip(), group_id)
        return ValidateResponse(
            valid=False,
            error="URL must be of the form "
                  "https://www.govmap.gov.il/?...&lay=<numeric layer id>",
        )
    return ValidateResponse(
        valid=True,
        layer_id=parsed.layer_id,
        center_itm=parsed.center_itm,
        url=body.url.strip(),
        title=build_govmap_title(parsed.layer_id),
    )


async def _validate_group(url: str, group_id: str) -> ValidateResponse:
    try:
        found = group_layers(await _fetch_catalog(), group_id)
    except Exception:  # noqa: BLE001 — a lookup, not a reason to 500
        logger.warning("govmap: catalog fetch failed for group %s", group_id, exc_info=True)
        return ValidateResponse(
            valid=False, group_id=group_id,
            error="קישור לקבוצת שכבות ב-GovMap, ולא ניתן היה לקרוא כרגע אילו שכבות יש בה. "
                  "פתחו את הקבוצה ב-GovMap והעתיקו קישור לשכבה עצמה (lay=).",
        )
    if found is None or not found[1]:
        return ValidateResponse(
            valid=False, group_id=group_id,
            error=f"קבוצת השכבות g={group_id} לא נמצאה בקטלוג של GovMap.",
        )
    name, layers = found
    return ValidateResponse(
        valid=True,
        url=url,
        title=name,
        group_id=group_id,
        layers=[GroupLayer(**layer) for layer in layers],
    )
