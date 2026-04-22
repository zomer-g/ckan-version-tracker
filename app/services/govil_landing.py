"""Client for the gov.il landing-page JSON API.

The /he/departments/govil-landing-page SPA calls
``https://www.gov.il/govil-landing-page-api/he`` which returns the full
list of ministries/units with title, url_name, logo, etc.

Logo path pattern (when logo.name is present):
    https://www.gov.il/BlobFolder/office/{url_name}/he/{logo.name}
The SPA falls back to ``OfficeLogo/menora.png`` — we skip that default
so rows without a real logo stay null.
"""

import logging
from dataclasses import dataclass

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://www.gov.il/govil-landing-page-api/he"
LOGO_BASE = "https://www.gov.il/BlobFolder/office"
TIMEOUT = httpx.Timeout(connect=15.0, read=30.0, write=15.0, pool=10.0)
UA = "Mozilla/5.0 (compatible; over.org.il)"


@dataclass
class GovIlOffice:
    url_name: str
    title: str
    logo_url: str | None
    external_website: str | None
    org_type: int | None


def _logo_url(url_name: str, logo: dict | None) -> str | None:
    if not logo:
        return None
    name = (logo.get("name") or "").strip()
    if not name:
        return None
    return f"{LOGO_BASE}/{url_name}/he/{name}"


async def fetch_offices() -> list[GovIlOffice]:
    """Fetch top-level ministries/offices from gov.il landing page.

    Returns only the top-level entries (each has a url_name you can
    navigate to on gov.il). Nested ``unitsList`` entries are ignored
    here; add them later if we want to track sub-units.
    """
    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        follow_redirects=True,
        headers={"User-Agent": UA, "Accept": "application/json"},
    ) as client:
        resp = await client.get(BASE_URL)
        resp.raise_for_status()
        data = resp.json()

    results = data.get("results") or []
    offices: list[GovIlOffice] = []
    for r in results:
        url_name = (r.get("urlName") or "").strip()
        title = (r.get("title") or "").strip()
        if not url_name or not title:
            continue
        offices.append(GovIlOffice(
            url_name=url_name,
            title=title,
            logo_url=_logo_url(url_name, r.get("logo")),
            external_website=(r.get("externalWebsite") or None),
            org_type=r.get("orgType"),
        ))
    logger.info("Fetched %d offices from gov.il landing page", len(offices))
    return offices
