"""Declarative source registry — onboard a scraper source with no OVER deploy.

Every source OVER tracks used to need code here: a URL parser, a /validate
endpoint, four branches in datasets.py, a router include, and a matching set
of frontend files. That coupling meant a new site could not ship from the
GOVSCRAPER repo alone.

A *manifest* replaces all of it. The worker declares one JSON document per
source and pushes it to POST /api/worker/sources/sync; OVER stores it in the
``source_registry`` table and reads it whenever a pasted URL doesn't match any
hardcoded parser. The manifest carries the URL regexes, the title template,
the scraper_config the worker will receive back, the poll cadence, whether the
source is NEON-eligible, and the display badge.

Ordering matters: the hardcoded parsers in datasets.py run FIRST and the
registry is the fallback. A manifest can therefore never change how one of the
fifteen existing sources behaves, however sloppy its regex.

Titles are a template, not a live fetch — validation must answer instantly
while the user is typing. The real title lands on the first successful scrape
via push_version's ``scrape_metadata.dataset_title_he``.
"""
import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urlparse

from pydantic import BaseModel, Field, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.source_registry import SourceRegistry

logger = logging.getLogger(__name__)

CACHE_TTL_SECONDS = 60

# Manifest ids that would collide with a hardcoded parser, a source_type, or a
# scraper_config kind the worker already dispatches on. Registering one of
# these would create a dataset the hardcoded path also claims — the two would
# disagree about which engine runs it.
RESERVED_SOURCE_IDS = frozenset({
    "govil", "datagovil", "datacollector_api", "govmap", "nadlan", "ckan",
    "idf", "health", "health_practitioners", "registries", "avodata",
    "munidata", "emun", "servicescompass", "mevaker", "hatzav", "mankal",
    "jda", "eden", "knesset", "knesset_mmm", "cbs", "drive", "gov",
})

_ID_RE = re.compile(r"^[a-z][a-z0-9_]{1,39}$")
_HEX_COLOR_RE = re.compile(r"^#[0-9a-fA-F]{3,8}$")

# A manifest author is trusted (they can already run arbitrary scrape code),
# but a runaway regex would hang the request thread for every visitor, so the
# inputs stay bounded.
MAX_REGEX_LENGTH = 600
MAX_URL_LENGTH = 2000
MAX_PATTERNS_PER_SOURCE = 40


class UrlPattern(BaseModel):
    """One recognisable URL shape for a source.

    Named groups in ``regex`` (Python ``(?P<name>…)`` syntax) are substituted
    into ``title_he`` and into any string value of ``config``, so a single
    pattern can cover a whole family of URLs (per-year, per-committee, …).
    """

    regex: str
    page_type: str | None = None
    title_he: str | None = None
    title_en: str | None = None
    # Merged over the manifest's default_config for URLs matching this pattern.
    config: dict[str, Any] = Field(default_factory=dict)

    @field_validator("regex")
    @classmethod
    def _check_regex(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("regex must not be empty")
        if len(v) > MAX_REGEX_LENGTH:
            raise ValueError(f"regex must be at most {MAX_REGEX_LENGTH} characters")
        if not v.startswith("^"):
            raise ValueError("regex must be anchored at the start (^)")
        try:
            re.compile(v)
        except re.error as e:
            raise ValueError(f"regex does not compile: {e}") from e
        return v

    @field_validator("page_type")
    @classmethod
    def _check_page_type(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = v.strip()
        if v and not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", v):
            raise ValueError("page_type must be lower-case snake_case")
        return v or None


class Badge(BaseModel):
    """Chip colours for the source, mirroring frontend/src/utils/sourceBadge.ts."""

    bg: str
    fg: str
    accent: str
    label: str | None = None

    @field_validator("bg", "fg", "accent")
    @classmethod
    def _check_color(cls, v: str) -> str:
        v = (v or "").strip()
        if not _HEX_COLOR_RE.fullmatch(v):
            raise ValueError("badge colours must be hex, e.g. '#fae8ff'")
        return v


class SourceManifest(BaseModel):
    """The contract between GOVSCRAPER and OVER for a declarative source."""

    manifest_version: int = 1
    id: str
    label_he: str
    label_en: str
    site_url: str
    badge: Badge
    # Falls back to the site_url host; used as the dataset's organization.
    origin: str | None = None
    source_link_he: str | None = None
    source_link_en: str | None = None
    default_poll_interval: int = 86400
    # True when the engine emits row-level tabular data (so the dataset can be
    # loaded into the NEON append DB and queried in the SQL console), rather
    # than files or a catalog index. See datasets.dataset_is_neon_eligible.
    neon_eligible: bool = False
    # Display hint: the source produces geographic layers.
    spatial: bool = False
    # The scraper_config every dataset of this source starts with. Travels
    # verbatim to the worker via /api/worker/poll.
    default_config: dict[str, Any] = Field(default_factory=dict)
    url_patterns: list[UrlPattern]

    @field_validator("manifest_version")
    @classmethod
    def _check_version(cls, v: int) -> int:
        if v != 1:
            raise ValueError("unsupported manifest_version (this OVER speaks v1)")
        return v

    @field_validator("id")
    @classmethod
    def _check_id(cls, v: str) -> str:
        # Not lower-cased for the author: the id becomes scraper_config["kind"],
        # which the worker looks its engine up by. Silently normalizing here
        # would make the two sides disagree about the engine's name.
        v = (v or "").strip()
        if not _ID_RE.fullmatch(v):
            raise ValueError(
                "id must be lower-case snake_case, 2-40 chars, starting with a letter"
            )
        if v in RESERVED_SOURCE_IDS:
            raise ValueError(f"id '{v}' is reserved by a built-in source")
        return v

    @field_validator("site_url")
    @classmethod
    def _check_site_url(cls, v: str) -> str:
        v = (v or "").strip()
        if not urlparse(v).scheme.startswith("http") or not urlparse(v).hostname:
            raise ValueError("site_url must be an absolute http(s) URL")
        return v

    @field_validator("url_patterns")
    @classmethod
    def _check_patterns(cls, v: list[UrlPattern]) -> list[UrlPattern]:
        if not v:
            raise ValueError("at least one url_pattern is required")
        if len(v) > MAX_PATTERNS_PER_SOURCE:
            raise ValueError(f"at most {MAX_PATTERNS_PER_SOURCE} url_patterns")
        return v

    # --- convention-derived accessors (never stored in the manifest) ---

    @property
    def resolved_origin(self) -> str:
        return self.origin or (urlparse(self.site_url).hostname or self.id)

    @property
    def ckan_id_prefix(self) -> str:
        return f"{self.id}-scraper-"

    @property
    def slug_prefix(self) -> str:
        return f"{self.id}-scraper"

    @property
    def mirror_prefix(self) -> str:
        return f"gov-versions-{self.id}"


def validate_manifest(raw: dict) -> SourceManifest:
    """Parse+validate a manifest dict, raising ValueError with a readable
    message. Pydantic's own errors are verbose; the sync endpoint reports
    them per-manifest so one bad source can't reject the whole payload."""
    return SourceManifest.model_validate(raw)


def manifest_hash(raw: dict) -> str:
    """Stable hash of a manifest so sync can skip unchanged rows."""
    canonical = json.dumps(raw, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# --- cache -----------------------------------------------------------------
#
# Process-local, same shape as worker_version.py. classify_url runs on every
# pasted URL that misses the hardcoded parsers, so it must not hit the DB each
# time. The sync endpoint invalidates explicitly; the TTL covers multi-dyno
# deployments where another process did the sync.

_cache: tuple[float, list[SourceManifest]] | None = None


def invalidate_cache() -> None:
    global _cache
    _cache = None


async def load_enabled(db: AsyncSession, *, force: bool = False) -> list[SourceManifest]:
    """All enabled manifests, newest-registered last. Cached for CACHE_TTL_SECONDS."""
    global _cache
    now = time.time()
    if not force and _cache and (now - _cache[0]) < CACHE_TTL_SECONDS:
        return _cache[1]

    rows = (
        await db.execute(
            select(SourceRegistry)
            .where(SourceRegistry.enabled.is_(True))
            .order_by(SourceRegistry.created_at)
        )
    ).scalars().all()

    manifests: list[SourceManifest] = []
    for row in rows:
        try:
            manifests.append(validate_manifest(row.manifest))
        except Exception as e:
            # A stored manifest that no longer validates (e.g. this OVER is
            # older than the manifest_version that wrote it) is skipped, not
            # fatal — the other sources must keep working.
            logger.warning("Skipping invalid manifest %s: %s", row.id, e)

    _cache = (now, manifests)
    return manifests


def cached_manifests() -> list[SourceManifest]:
    """Whatever is in the cache right now, without touching the DB.

    For synchronous callers (dataset_is_neon_eligible runs inside response
    serialization). Returns [] before the first load; main.py warms the cache
    at startup so that window is a few hundred milliseconds after boot.
    """
    return list(_cache[1]) if _cache else []


def neon_kinds() -> frozenset[str]:
    """Registry source ids whose engines emit tabular rows."""
    return frozenset(m.id for m in cached_manifests() if m.neon_eligible)


def registry_source_names() -> list[str]:
    """Human-readable names for the "unsupported URL" error message."""
    return [f"{m.label_he} ({m.resolved_origin})" for m in cached_manifests()]


async def warm_cache(session_factory) -> None:
    """Populate the cache at startup so the first request is already fast."""
    try:
        async with session_factory() as db:
            await load_enabled(db, force=True)
    except Exception as e:
        logger.warning("Could not warm the source registry cache: %s", e)


# --- classification --------------------------------------------------------


@dataclass
class RegistryMatch:
    source_id: str
    page_type: str
    collector_name: str
    title: str
    scraper_config: dict[str, Any]
    manifest: SourceManifest


def _render(template: str, groups: dict[str, str]) -> str:
    """Substitute {name} placeholders from the regex's named groups.

    Unknown placeholders are left as-is rather than raising — a title with a
    literal brace is a cosmetic problem, a 500 on paste is not. Double spaces
    left by an empty optional group are collapsed.
    """
    out = template
    for key, value in groups.items():
        out = out.replace("{" + key + "}", value)
    return re.sub(r"\s{2,}", " ", out).strip(" -—–,")


def _apply_groups(value: Any, groups: dict[str, str]) -> Any:
    if isinstance(value, str):
        return _render(value, groups)
    if isinstance(value, list):
        return [_apply_groups(v, groups) for v in value]
    if isinstance(value, dict):
        return {k: _apply_groups(v, groups) for k, v in value.items()}
    return value


def match_manifests(url: str, manifests: list[SourceManifest]) -> RegistryMatch | None:
    """First manifest+pattern that matches ``url``, or None.

    The URL is tried both raw and percent-decoded: Hebrew path segments arrive
    encoded from a browser copy-paste and decoded when typed into a JSON body,
    and a manifest author will only have written one of the two.
    """
    url = (url or "").strip()
    if not url or len(url) > MAX_URL_LENGTH:
        return None
    candidates = [url]
    decoded = unquote(url)
    if decoded != url:
        candidates.append(decoded)

    for man in manifests:
        for pattern in man.url_patterns:
            try:
                rx = re.compile(pattern.regex)
            except re.error:
                continue
            match = next((m for c in candidates if (m := rx.match(c))), None)
            if not match:
                continue

            groups = {k: (v or "") for k, v in match.groupdict().items()}
            page_type = pattern.page_type or f"{man.id}_main"
            config = dict(man.default_config)
            for key, value in pattern.config.items():
                rendered = _apply_groups(value, groups)
                # A placeholder fed by an optional group that didn't
                # participate renders empty — drop the key rather than send
                # the worker a blank string it would treat as a real value.
                if isinstance(value, str) and "{" in value and rendered == "":
                    continue
                config[key] = rendered
            config["kind"] = man.id
            title = _render(pattern.title_he or man.label_he, groups)
            return RegistryMatch(
                source_id=man.id,
                page_type=page_type,
                collector_name=page_type.replace("_", "-"),
                title=title,
                scraper_config=config,
                manifest=man,
            )
    return None


async def classify_url(db: AsyncSession, url: str) -> RegistryMatch | None:
    """Match a pasted URL against every enabled manifest."""
    return match_manifests(url, await load_enabled(db))


def display_view(man: SourceManifest) -> dict[str, Any]:
    """The projection the frontend needs: badges and labels, no regexes.

    Python and JavaScript disagree on named-group syntax ((?P<n>…) vs (?<n>…)),
    so the browser never sees a pattern — it calls POST /api/sources/validate
    when a pasted URL misses every hardcoded detector.
    """
    return {
        "id": man.id,
        "label_he": man.label_he,
        "label_en": man.label_en,
        "site_url": man.site_url,
        "origin": man.resolved_origin,
        "ckan_id_prefix": man.ckan_id_prefix,
        "badge": {
            "bg": man.badge.bg,
            "fg": man.badge.fg,
            "accent": man.badge.accent,
            "label": man.badge.label or man.label_en,
        },
        "source_link_he": man.source_link_he or f"לצפייה באתר {man.label_he}",
        "source_link_en": man.source_link_en or f"View on {man.resolved_origin}",
        "default_poll_interval": man.default_poll_interval,
        "neon_eligible": man.neon_eligible,
        "spatial": man.spatial,
    }
