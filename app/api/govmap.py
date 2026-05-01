"""GovMap layer URL parsing and validation.

Parses URLs from https://www.govmap.gov.il/ that point to a single layer
(via the ``lay`` query parameter). The actual scraping is done by the
external GOV SCRAPER worker — this module only validates the URL shape
on the request side and provides a typed parser for storing the layer
metadata on TrackedDataset.scraper_config.
"""

import re

from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.rate_limit import limiter

router = APIRouter(prefix="/api/govmap", tags=["govmap"])

GOVMAP_HOST_RE = re.compile(r"^https?://(www\.)?govmap\.gov\.il/?\?", re.IGNORECASE)
LAY_RE = re.compile(r"[?&]lay(?:er|ers)?=(\d+)", re.IGNORECASE)
CENTER_RE = re.compile(r"[?&]c=([-\d.]+),([-\d.]+)", re.IGNORECASE)


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


def build_govmap_title(layer_id: str) -> str:
    return f"GovMap layer {layer_id}"


class ValidateRequest(BaseModel):
    url: str


class ValidateResponse(BaseModel):
    valid: bool
    layer_id: str | None = None
    center_itm: dict | None = None
    url: str | None = None
    title: str | None = None
    error: str | None = None


@router.post("/validate", response_model=ValidateResponse)
@limiter.limit("20/minute")
async def validate_govmap_url(request: Request, body: ValidateRequest):
    """Validate a govmap.gov.il layer URL and return a normalised parse."""
    parsed = parse_govmap_url(body.url)
    if not parsed:
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
