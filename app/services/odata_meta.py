"""What מידע לעם knows about its own publishers.

The odata catalog is mostly freedom-of-information responses, and the single
most useful way to narrow it is by WHO published — התנועה לחופש המידע, עמותת
הצלחה, a municipality, an individual requester. There are 44 such bodies and
the list is theirs, not ours, so it is read from the catalog rather than
written down here.

It was written down here, briefly: the filter shipped with eight organizations
picked off one facet query, which silently made the other 36 unreachable. Same
failure as every hardcoded coverage label on this site — right on the day it
was typed.

Never on the critical path: this feeds /api/deep-search/sources, the request
that draws the search UI, so it is read through a BoundedRefreshCache.
"""
from __future__ import annotations

import logging

from app.services.refresh_cache import BoundedRefreshCache

logger = logging.getLogger(__name__)

# The catalog gains a publisher rarely; a stale read costs one missing option.
_TTL_SECONDS = 3600.0
MAX_OPTIONS = 60


async def _fetch() -> list[dict]:
    """``[{id, title, datasets}]``, busiest publisher first."""
    from app.mcp import odata_server

    data, _ = await odata_server._tool_list_organizations(None, None, None, {})
    return [o for o in (data.get("organizations") or [])
            if isinstance(o, dict) and o.get("id") and o.get("title")]


_cache = BoundedRefreshCache(
    "odata organizations", _fetch, ttl_seconds=_TTL_SECONDS, empty=[],
    default_max_wait=1.0)


async def organizations(max_wait: float | None = -1.0) -> list[dict]:
    return await _cache.get(max_wait=max_wait)


def reset_for_tests() -> None:
    _cache.reset()


def options(orgs: list[dict], fallback: tuple[dict, ...]) -> list[dict]:
    """The select options for the filter: "all", then one per publisher.

    Falls back to the seeded shortlist when the catalog cannot be reached — a
    smaller menu is a degraded filter, while an empty one is a broken control.
    The count rides along in the label because it is the thing that tells a
    reader whether narrowing to this body will leave them anything.
    """
    out = [{"value": "", "label": "כל הגופים"}]
    if not orgs:
        return out + [dict(o) for o in fallback]
    for o in orgs[:MAX_OPTIONS]:
        n = o.get("datasets")
        label = o["title"]
        if isinstance(n, int) and n:
            label = f"{label} ({n:,})"
        out.append({"value": str(o["id"]), "label": label})
    return out
