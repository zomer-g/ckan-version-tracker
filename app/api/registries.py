"""registries.health.gov.il URL validation endpoint.

Mirror of ``app/api/health.py``. The Ministry of Health "מאגרי מידע"
portal (https://registries.health.gov.il) is a module-federation Angular
SPA whose registry selector lists separate registries (ambulances, medic
training, food importers/manufacturers/factories, radiation devices,
cosmetics, medical devices, licensed institutions, ...). Each registry is
backed by an open same-origin JSON API and is tracked as its own dataset.

Supported URLs: ``https://registries.health.gov.il/<Path>`` where
``<Path>`` is one of the known registry paths (e.g. ``/Ambulances``,
``/FoodImporters``, ``/MedicalDevices``). The bare root (the registry
selector) and unknown paths are rejected — each registry is its own
tracked dataset.

Registries that redirect to ``practitioners.health.gov.il`` (Nurses,
Hypnotists, Practitioners — handled by the ``health`` source) and the
external Medicines registry are intentionally not supported here.

We don't fetch the page — the title comes from the static catalog below,
which mirrors the worker's ``govscraper.scrapers.registries`` catalog.
Actual scraping runs in the external govil-scraper worker.
"""

import logging
import re
from urllib.parse import urlparse

from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.rate_limit import limiter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/registries", tags=["registries"])


REGISTRIES_HOSTS = {"registries.health.gov.il"}

# Registry catalog: key -> (URL path, Hebrew title). Keep in lockstep with
# ``govscraper/scrapers/registries/_engine.py::_REGISTRIES`` on the worker.
REGISTRIES: dict[str, tuple[str, str]] = {
    "ambulances": ("Ambulances", "אמבולנסים"),
    "paramedics": ("Paramedics", "הכשרת חובשים"),
    "foodimporters": ("FoodImporters", "יבואני מזון/משווקים"),
    "foodmanufacturers": ("FoodManufacturers", "יצרני מזון"),
    "radiationinstitutes": ("RadiationInstitutes", "מכשירי קרינה ברישוי"),
    "cosmeticsbusinesses": ("CosmeticsBusinesses", "עוסקים בתמרוקים"),
    "foodfactories": ("FoodFactories", "עסקי מזון מן החי"),
    "medicaldevices": ("MedicalDevices", 'ציוד רפואי (אמ"ר)'),
    "institutions": ("Institutions", "רישוי מוסדות"),
    "cosmetics": ("Cosmetics", "תמרוקים"),
}

# path (lowercased) -> key
_PATH_TO_KEY = {path.lower(): key for key, (path, _title) in REGISTRIES.items()}


class ValidateRequest(BaseModel):
    url: str


class ValidateResponse(BaseModel):
    valid: bool
    page_type: str | None = None
    collector_name: str | None = None
    title: str | None = None
    url: str | None = None
    error: str | None = None


def _registry_key_of(url: str) -> str | None:
    """Return the catalog key for a single-registry URL, or None."""
    parsed = urlparse(url.strip())
    if (parsed.hostname or "").lower() not in REGISTRIES_HOSTS:
        return None
    segments = [s for s in (parsed.path or "").split("/") if s]
    if len(segments) != 1:
        return None
    return _PATH_TO_KEY.get(segments[0].lower())


def _parse_registries_url(url: str) -> tuple[str | None, str | None]:
    """Parse a registries.health.gov.il URL.

    Returns ``("registries_<key>", "registries-<key>")`` for an in-scope
    per-registry URL; ``(None, None)`` otherwise. The registry key is
    embedded in the page_type so the dispatch switch in
    ``app/api/datasets.py`` can attach registries-aware scraper config.

    ``"registries_<key>"`` matches ``startswith("registries_")`` so the
    dispatch works the same way it does for ``health_`` / ``idf_``.
    """
    key = _registry_key_of(url)
    if not key:
        return None, None
    return f"registries_{key}", f"registries-{key}"


# Nominal per-registry limits. The largest registry (Cosmetics) holds
# ~60k entities; keep max_docs generous — the worker logs a truncation
# warning if it hits the cap. max_depth is nominal (the engine paginates
# a listing API rather than doing BFS); kept for parity with health/idf.
REGISTRIES_DEFAULT_LIMITS: tuple[int, int] = (3, 200000)


def get_registries_limits(page_type: str) -> tuple[int, int]:
    """Return ``(max_depth, max_docs)`` for a ``registries_<key>`` page_type."""
    return REGISTRIES_DEFAULT_LIMITS


def _format_registries_title(key: str) -> str:
    """Human title for a registry key, from the static catalog."""
    _path, title = REGISTRIES.get(key, ("", ""))
    if title:
        return f"מאגרי מידע — משרד הבריאות — {title}"
    return "מאגרי מידע — משרד הבריאות"


@router.post("/validate", response_model=ValidateResponse)
@limiter.limit("10/minute")
async def validate_registries_url(request: Request, body: ValidateRequest):
    """Validate a registries.health.gov.il URL and surface a title.

    We don't fetch the page — the SPA shell carries no readable title and
    the catalog gives us the Hebrew registry name directly.
    """
    url = body.url.strip()

    page_type, slug = _parse_registries_url(url)
    if not page_type or not slug:
        valid_paths = ", ".join(f"/{path}" for path, _t in REGISTRIES.values())
        return ValidateResponse(
            valid=False,
            error=(
                "URL is not a supported registries.health.gov.il registry. "
                f"Expected https://registries.health.gov.il/<registry> where "
                f"<registry> is one of: {valid_paths}. The bare site root "
                "(the registry selector) is not supported — register each "
                "registry separately."
            ),
        )

    key = page_type.split("_", 1)[1]
    return ValidateResponse(
        valid=True,
        page_type=page_type,
        collector_name=slug,
        title=_format_registries_title(key),
        url=url,
    )
