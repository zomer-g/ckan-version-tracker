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
                              sorted query, minus tracking params, plus the
                              fragment WHEN IT IS A ROUTE (see below)

Two URLs with the same identity are the same thing. ``path_key()`` is a
deliberately looser second chance used only when no identity matched.

ROUTE FRAGMENTS. A fragment is normally an anchor — ``/about#contact`` is the
same page as ``/about`` — so canonicalisation drops it. But a hash-routed SPA
puts the whole route there, and ykpubdata.jerusalem.muni.il is one:

    …/#/?SystemCode=26400046                     the register
    …/#/documents                                a second corpus
    …/#/Details?TikNum=2004/0196.00&…            one building file

Dropping the fragment made all three the SAME identity, so the second dataset
of that site could never be requested ("400 Already tracked") and a per-file
dataset could never exist at all. So the fragment is kept when it LOOKS LIKE A
ROUTE — it starts with ``/`` (or the hashbang ``!/``), which is the SPA
convention and is never how an anchor is written — and dropped otherwise. It is
then canonicalised exactly like a path+query, so param order and tracking params
are noise inside the route too. A bare ``#/`` adds nothing to the path it
follows and is treated as absent.
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


def _sorted_query(query: str | None) -> str:
    """``a=1&b=2`` — the query with tracking params dropped and keys sorted, so
    the order a link happened to be written in is not part of the identity."""
    pairs = [
        (k, v)
        for k, v in parse_qsl(query or "", keep_blank_values=True)
        if k.lower() not in _NOISE_PARAMS and not k.lower().startswith("utm_")
    ]
    pairs.sort()
    return "&".join(f"{k}={v}" for k, v in pairs)


def route_fragment(fragment: str | None) -> str | None:
    """The ROUTE a hash-routed SPA carries in its fragment, canonicalised the
    same way a path+query is — or None when the fragment is an ordinary anchor.

    The test is the leading slash (``#/…``, or the hashbang ``#!/…``): that is
    how every hash router writes a route, and no anchor is written that way.
    ``#contact``, ``#top`` and Chrome's ``#:~:text=…`` scroll-to-text therefore
    all stay noise, which is what keeps ``…/about#contact`` and ``…/about`` one
    dataset. A fragment that canonicalises to a bare ``/`` with no query adds
    nothing to the URL it follows, so it is treated as absent — ``site/#/`` and
    ``site/`` are the same page."""
    frag = (fragment or "").strip()
    if frag.startswith("!"):  # hashbang routes (#!/path) are the same convention
        frag = frag[1:]
    if not frag.startswith("/"):
        return None
    raw_path, _, raw_query = frag.partition("?")
    path = re.sub(r"/+$", "", unquote(raw_path))
    query = _sorted_query(raw_query)
    if not path and not query:
        return None
    return f"{path or '/'}?{query}" if query else path


def canonical_url(url: str | None) -> str | None:
    """``host/path?sorted-query[#route]`` with scheme, ``www.``, trailing
    slashes and tracking params removed. The fragment is included only when it
    is a route (see route_fragment). Returns None for anything that isn't a
    URL."""
    parts = _split(url)
    if parts is None:
        return None
    host = (parts.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    path = re.sub(r"/+$", "", unquote(parts.path or "")) or "/"
    query = _sorted_query(parts.query)
    canonical = f"{host}{path}?{query}" if query else f"{host}{path}"
    route = route_fragment(parts.fragment)
    return f"{canonical}#{route}" if route else canonical


def path_key(url: str | None) -> str | None:
    """``host/path`` — the same page regardless of query string.

    Looser than an identity on purpose: two datasets can share a page and be
    told apart only by a query param (jeden.co.il's ``?category=`` serves both
    its corpora from one page), so a path match may legitimately return more
    than one dataset. Callers must treat multiple hits as "ambiguous, show
    them all", never as "found it".

    Deliberately fragment-BLIND, even though the identity is not: every route of
    a hash-routed SPA is served from the same path, so all of them collapsing to
    one key is this function doing its job. That is what makes a bare
    ``ykpubdata.jerusalem.muni.il/`` link offer every dataset cut from that site
    as a "did you mean", exactly as a bare ``jeden.co.il/`` link does.
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
