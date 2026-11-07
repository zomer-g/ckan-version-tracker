"""Browser security headers: CSP, HSTS, Permissions-Policy.

The site keeps its sign-in token in localStorage, so anything that runs script
in the page can take a session. Until now nothing told the browser which
scripts are ours. The Content-Security-Policy below does: our own origin, the
Google tag, and the two small inline scripts of index.html by hash (computed
from the built file at startup, so a rebuild cannot leave a stale hash).

It ships as **Report-Only** (``CSP_ENFORCE=false``): browsers report what it
would have blocked to /api/csp-report and block nothing. Once the reports show
only noise, set CSP_ENFORCE=true. The maps, the GovMap layers and the story
decks pull from many places, which is why it starts this way and not enforced.
"""
from __future__ import annotations

import base64
import hashlib
import re

from app.config import settings

# Inline <script> blocks without src= in the SPA shell, hashed for script-src.
_script_hashes: list[str] = []

_INLINE_SCRIPT_RE = re.compile(r"<script(?![^>]*\bsrc=)(?P<attrs>[^>]*)>(?P<body>.*?)</script>",
                               re.S | re.I)


def register_shell(html: str) -> None:
    """Record the hashes of the shell's inline scripts (JSON-LD is data, not
    script, and is skipped)."""
    hashes = []
    for m in _INLINE_SCRIPT_RE.finditer(html):
        if "application/ld+json" in m.group("attrs").lower():
            continue
        digest = hashlib.sha256(m.group("body").encode("utf-8")).digest()
        hashes.append("'sha256-" + base64.b64encode(digest).decode() + "'")
    _script_hashes[:] = sorted(set(hashes))


def csp_value() -> str:
    script_src = " ".join(["'self'", *_script_hashes, "https://www.googletagmanager.com"])
    return "; ".join([
        "default-src 'self'",
        f"script-src {script_src}",
        # Leaflet and the charts set inline styles; Google Fonts serves the CSS.
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
        "font-src 'self' data: https://fonts.gstatic.com",
        # Map tiles and layer previews come from several public tile servers.
        "img-src 'self' data: blob: https:",
        "connect-src 'self' https://www.gov.il https://www.googletagmanager.com "
        "https://*.google-analytics.com https://*.analytics.google.com",
        "worker-src 'self' blob:",
        "frame-src 'self'",
        "frame-ancestors 'self'",
        "object-src 'none'",
        "base-uri 'self'",
        "form-action 'self' https://accounts.google.com",
        "report-uri /api/csp-report",
    ])


PERMISSIONS_POLICY = "camera=(), microphone=(), geolocation=(), payment=(), usb=()"
HSTS = "max-age=31536000"


def apply(request, response) -> None:
    """Add the headers to ``response``. setdefault semantics: a route that
    sets its own wins."""
    h = response.headers
    h.setdefault("Permissions-Policy", PERMISSIONS_POLICY)
    if request.url.scheme == "https":
        # No includeSubDomains: not every subdomain is known to be HTTPS-only.
        h.setdefault("Strict-Transport-Security", HSTS)
    ctype = h.get("content-type", "")
    # The policy is for documents. The story decks are standalone pages with
    # their own CDN scripts and are left out rather than reported on forever.
    if ctype.startswith("text/html") and not request.url.path.startswith("/story/"):
        name = ("Content-Security-Policy" if getattr(settings, "csp_enforce", False)
                else "Content-Security-Policy-Report-Only")
        h.setdefault(name, csp_value())
