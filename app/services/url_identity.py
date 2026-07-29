"""What a pasted SOURCE URL identifies, so it can be matched to the dataset
that already tracks it.

The problem this solves: every source URL a user might paste carries noise
that does not change WHAT is being tracked. A GovMap layer link is the worst
case — the map writes the current viewport into the URL, so the same layer 11
appears as::

    https://www.govmap.gov.il/?lay=11                      (what we store)
    https://www.govmap.gov.il/?c=219143.61,618345.06&lay=11 (what a user copies)

An exact ``source_url`` comparison says "not tracked" for the second one, which
is how the public request path used to invite a request for a layer OVER
already had (and then create a duplicate dataset — see migration 035).

So instead of comparing URLs we compare IDENTITIES:

  ``("govmap", "11")``      — a GovMap layer is its layer id, nothing else
  ``("ckan", "<name>")``    — a data.gov.il dataset is its package name
  ``("url", "<canonical>")``— everything else: host (no ``www.``) + path +
                              sorted query, minus tracking params

Two URLs with the same identity are the same thing. ``path_key()`` is a
deliberately looser second chance used only when no identity matched.
"""

from __future__ import annotations

import re
from urllib.parse import parse_qsl, unquote, urlsplit

# Query params that never change what is being tracked — they describe the
# visit, not the resource. ``utm_*`` is handled by prefix.
_NOISE_PARAMS = {
    "fbclid", "gclid", "msclkid", "mc_cid", "mc_eid",
    "_ga", "_gl", "ref", "source",
}

_GOVMAP_LAY_RE = re.compile(r"[?&]lay(?:er|ers)?=(\d+)", re.IGNORECASE)
# data.gov.il package permalinks: /dataset/<name> and the newer
# /datasets/<org>/<name>[/<resource-uuid>].
_CKAN_FULL_RE = re.compile(r"/datasets/[^/]+/([^/?#]+)")
_CKAN_SIMPLE_RE = re.compile(r"/dataset/([^/?#]+)")

Identity = tuple[str, str]


def host_of(url: str | None) -> str | None:
    """Registrable-ish host of a URL, lower-cased and without ``www.``.

    Used as the candidate filter: ``source_url ILIKE '%<host>%'`` matches
    stored URLs whether or not they carry the ``www.`` the user pasted.
    """
    parts = _split(url)
    if parts is None:
        return None
    host = (parts.hostname or "").lower()
    if not host:
        return None
    return host[4:] if host.startswith("www.") else host


def _split(url: str | None):
    s = (url or "").strip()
    if not s:
        return None
    if not re.match(r"^https?://", s, re.IGNORECASE):
        s = "https://" + s
    try:
        parts = urlsplit(s)
    except ValueError:
        return None
    host = parts.hostname or ""
    # Guard the scheme we just prepended: "some search phrase" would otherwise
    # parse as a host and get an identity, so an ordinary keyword query could
    # match a dataset. A real host has a dot and no spaces.
    if not host or " " in host or "." not in host:
        return None
    return parts


def canonical_url(url: str | None) -> str | None:
    """``host/path?sorted-query`` with scheme, ``www.``, trailing slashes and
    tracking params removed. Returns None for anything that isn't a URL."""
    parts = _split(url)
    if parts is None:
        return None
    host = (parts.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    path = re.sub(r"/+$", "", unquote(parts.path or "")) or "/"
    pairs = [
        (k, v)
        for k, v in parse_qsl(parts.query, keep_blank_values=True)
        if k.lower() not in _NOISE_PARAMS and not k.lower().startswith("utm_")
    ]
    pairs.sort()
    query = "&".join(f"{k}={v}" for k, v in pairs)
    return f"{host}{path}?{query}" if query else f"{host}{path}"


def path_key(url: str | None) -> str | None:
    """``host/path`` — the same page regardless of query string.

    Looser than an identity on purpose: two datasets can share a page and be
    told apart only by a query param (jeden.co.il's ``?category=`` serves both
    its corpora from one page), so a path match may legitimately return more
    than one dataset. Callers must treat multiple hits as "ambiguous, show
    them all", never as "found it".
    """
    parts = _split(url)
    if parts is None:
        return None
    host = (parts.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    path = re.sub(r"/+$", "", unquote(parts.path or "")) or "/"
    return f"{host}{path}"


def url_identity(
    url: str | None,
    source_type: str | None = None,
    scraper_config: dict | None = None,
) -> Identity | None:
    """Identity of a source URL.

    ``source_type``/``scraper_config`` are passed when the URL comes from a
    stored dataset — the config's ``layer_id`` is authoritative for GovMap and
    survives a source_url that was written in some other shape.
    """
    host = host_of(url)
    if host is None:
        return None

    # --- GovMap: the layer id IS the dataset ---
    if source_type == "govmap" or host.endswith("govmap.gov.il"):
        layer_id = None
        if isinstance(scraper_config, dict):
            raw = scraper_config.get("layer_id")
            if raw is not None and str(raw).strip():
                layer_id = str(raw).strip()
        if layer_id is None:
            m = _GOVMAP_LAY_RE.search(url or "")
            if m:
                layer_id = m.group(1)
        if layer_id is not None:
            return ("govmap", layer_id)
        # A govmap URL with no layer — fall through to the generic identity.

    # --- data.gov.il: the package name, whichever permalink shape ---
    if host.endswith("data.gov.il"):
        m = _CKAN_FULL_RE.search(url or "") or _CKAN_SIMPLE_RE.search(url or "")
        if m:
            return ("ckan", m.group(1).lower())

    canonical = canonical_url(url)
    return ("url", canonical) if canonical else None
