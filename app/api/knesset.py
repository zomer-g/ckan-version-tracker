"""knesset.gov.il committee-protocols URL validation endpoint.

Each OVER dataset is one *committee*, tracked from the Knesset's open
ODATA-v4 feed (``https://knesset.gov.il/OdataV4/ParliamentInfo``). Committee
protocols live across three tables — ``KNS_Committee`` (one row per committee
per Knesset; ``CategoryID`` is the persistent identity), ``KNS_CommitteeSession``
and ``KNS_DocumentCommitteeSession`` (protocols = ``GroupTypeID 23``, direct
files on ``fs.knesset.gov.il``).

The trackable URL is an honest ODATA query against ``KNS_Committee`` that
returns exactly the committee being tracked:

    https://knesset.gov.il/OdataV4/ParliamentInfo/KNS_Committee?$filter=Id eq 4186          # ועדת הכספים (כנסת 25)
    https://knesset.gov.il/OdataV4/ParliamentInfo/KNS_Committee?$filter=CategoryID eq 2      # ועדת הכספים (all Knessets)

``Id eq N`` → a single committee instance, tracked exactly and nothing else
(an ``Id`` is a *per-Knesset* committee, so this pins the dataset to one
Knesset). ``CategoryID eq N`` → the persistent committee across all Knessets
(incl. its sub-committees sharing the category) — the historical model.

The **Knesset-25 rollout** registers every one of the ~90 Knesset-25
committees — main, special, joint, House, *and* every ועדת משנה — as its own
dataset by ``Id eq N`` (see ``/bootstrap-knesset25`` below). Single scope pulls
no sub-committee children, so the 90 datasets never double-count each other.

Unlike practitioners/idf, the feed is fully open, so ``/validate`` probes it
live for the real committee name.

The actual scrape runs in the external govil-scraper worker
(``govscraper.scrapers.knesset``), which dispatches on
``scraper_config.kind == "knesset"``.
"""

import logging
import re
from urllib.parse import unquote, urlparse

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, Request
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.dependencies import get_admin_user
from app.database import get_db
from app.models.tracked_dataset import TrackedDataset
from app.models.user import User
from app.rate_limit import limiter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/knesset", tags=["knesset"])


KNESSET_HOSTS = {"knesset.gov.il"}
# Anchored at the KNS_Committee entity set (lower-cased for the compare).
_ODATA_PATH = "/odatav4/parliamentinfo/kns_committee"
KNESSET_ODATA_BASE = "https://knesset.gov.il/OdataV4/ParliamentInfo"

# Tolerant of surrounding whitespace / other clauses / percent-encoding
# (the query is unquoted before matching). ``\bId`` avoids false-matching
# the ``ID`` tail of ``CommitteeTypeID`` / ``CategoryID``.
_CATEGORY_RE = re.compile(r"CategoryID\s+eq\s+(\d+)", re.IGNORECASE)
_ID_RE = re.compile(r"\bId\s+eq\s+(\d+)", re.IGNORECASE)

# MMM (מרכז המחקר והמידע) — the Knesset Research & Information Center. Its
# research documents are NOT in the ODATA feed; they live in a SharePoint
# search app on main.knesset.gov.il (behind a Radware challenge). The whole
# corpus is one OVER dataset, scraped by the worker's ``knesset_mmm`` engine.
MMM_HOSTS = {"main.knesset.gov.il"}
_MMM_PATH_PREFIX = "/activity/info/research"


def is_mmm_url(url: str) -> bool:
    """True for any main.knesset.gov.il Research-center page (the landing page
    the user pastes, or the search page)."""
    if not url:
        return False
    try:
        parsed = urlparse(url.strip())
    except Exception:
        return False
    if (parsed.hostname or "").lower() not in MMM_HOSTS:
        return False
    return (parsed.path or "").lower().startswith(_MMM_PATH_PREFIX)


class ValidateRequest(BaseModel):
    url: str


class ValidateResponse(BaseModel):
    valid: bool
    page_type: str | None = None
    collector_name: str | None = None
    title: str | None = None
    url: str | None = None
    error: str | None = None


def committee_scope_of(url: str) -> tuple[str, int] | None:
    """Return ``("category", N)`` or ``("single", N)`` for an in-scope
    ``KNS_Committee`` query URL, else ``None``. Mirrors the worker engine's
    classifier (``govscraper.scrapers.knesset._engine.committee_scope_of``);
    the backend copy is authoritative for what OVER accepts."""
    if not url:
        return None
    parsed = urlparse(url.strip())
    if (parsed.hostname or "").lower() not in KNESSET_HOSTS:
        return None
    if (parsed.path or "").lower().rstrip("/") != _ODATA_PATH:
        return None
    query = unquote(parsed.query or "")
    m = _CATEGORY_RE.search(query)
    if m:
        return ("category", int(m.group(1)))
    m = _ID_RE.search(query)
    if m:
        return ("single", int(m.group(1)))
    return None


def _parse_knesset_url(url: str) -> tuple[str | None, str | None]:
    """Parse a knesset.gov.il committee URL.

    Returns ``("knesset_committee:<N>", "knesset-committee-cat-<N>")`` for a
    ``CategoryID`` scope, ``("knesset_committee_single:<N>",
    "knesset-committee-single-<N>")`` for an ``Id`` scope, or ``(None, None)``.

    Both page_types match ``startswith("knesset_")`` so the dispatch switch in
    ``datasets.py`` works the same way it does for ``idf_`` / ``health_``.

    The MMM Research-center page maps to ``("knesset_mmm", "knesset-mmm")`` —
    one whole-corpus dataset (also ``startswith("knesset_")``).
    """
    if is_mmm_url(url):
        return "knesset_mmm", "knesset-mmm"
    scope = committee_scope_of(url)
    if scope is None:
        return None, None
    kind, n = scope
    if kind == "category":
        return f"knesset_committee:{n}", f"knesset-committee-cat-{n}"
    return f"knesset_committee_single:{n}", f"knesset-committee-single-{n}"


def scope_of_page_type(page_type: str) -> tuple[str, int] | None:
    """Recover the committee scope from a ``knesset_committee[...]:<N>``
    page_type — used by ``datasets.py`` to stamp category/committee ids into
    scraper_config so the worker needn't re-parse the URL."""
    if not page_type or ":" not in page_type:
        return None
    head, _, tail = page_type.partition(":")
    try:
        n = int(tail)
    except ValueError:
        return None
    if head == "knesset_committee":
        return ("category", n)
    if head == "knesset_committee_single":
        return ("single", n)
    return None


# (max_depth, max_docs). max_depth is nominal (the engine paginates ODATA, it
# doesn't recurse). ועדת הכספים — the largest committee — has ~13k protocols
# across all Knessets, so 100k is a generous cap that still surfaces a
# truncation marker if a committee somehow exceeds it.
KNESSET_DEFAULT_LIMITS: tuple[int, int] = (3, 100_000)


# The MMM corpus (every research doc since Dec 1999) is ~6,500 and growing;
# 20k is a generous cap that still surfaces a truncation marker.
MMM_DEFAULT_LIMITS: tuple[int, int] = (3, 20_000)


def get_knesset_limits(page_type: str) -> tuple[int, int]:
    """Return ``(max_depth, max_docs)`` for a knesset page_type. Single cap for
    all committees; kept as a function for symmetry with the other parsers."""
    if page_type == "knesset_mmm":
        return MMM_DEFAULT_LIMITS
    return KNESSET_DEFAULT_LIMITS


def mmm_max_docs_override(url: str) -> int | None:
    """Optional ``?max_docs=N`` (or ``?smoke=N``) on an MMM source URL — lets a
    bounded smoke run be registered without editing config, before the full
    ~6,500-doc backfill. Returns a positive int or ``None`` (use the default)."""
    from urllib.parse import parse_qs

    try:
        q = parse_qs(urlparse(url).query)
        raw = q.get("max_docs") or q.get("smoke")
        if raw:
            n = int(raw[0])
            if n > 0:
                return n
    except Exception:  # noqa: BLE001
        pass
    return None


async def _probe_committee_name(scope: tuple[str, int]) -> str | None:
    """Live-probe the ODATA feed for the committee's display name (the Name of
    its most-recent instance). Best-effort: any failure returns ``None`` and
    the caller falls back to a generic title. The feed is open (no WAF), so
    this is a cheap single request."""
    kind, n = scope
    field = "CategoryID" if kind == "category" else "Id"
    url = (
        f"{KNESSET_ODATA_BASE}/KNS_Committee"
        f"?$filter={field} eq {n}"
        f"&$select=Name,KnessetNum&$orderby=KnessetNum desc&$top=1"
    )
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, headers={"Accept": "application/json"})
            resp.raise_for_status()
            rows = (resp.json() or {}).get("value") or []
            if rows:
                name = re.sub(r"\s+", " ", (rows[0].get("Name") or "").strip())
                return name or None
    except Exception as e:  # noqa: BLE001
        logger.info("knesset committee-name probe failed for %s=%s: %s", kind, n, e)
    return None


@router.post("/validate", response_model=ValidateResponse)
@limiter.limit("10/minute")
async def validate_knesset_url(request: Request, body: ValidateRequest):
    """Validate a knesset.gov.il committee URL and surface the committee name.

    The ODATA feed is open, so we probe it for the real committee name; if the
    probe fails we still validate the URL and return a generic Hebrew title.
    """
    url = body.url.strip()

    # MMM Research-center corpus (whole-corpus, one dataset).
    if is_mmm_url(url):
        page_type, slug = _parse_knesset_url(url)
        return ValidateResponse(
            valid=True,
            page_type=page_type,
            collector_name=slug,
            title='מסמכי מרכז המחקר והמידע של הכנסת (ממ"מ)',
            url=url,
        )

    scope = committee_scope_of(url)
    if scope is None:
        return ValidateResponse(
            valid=False,
            error=(
                "URL is not a supported knesset.gov.il committee page. Expected "
                "an ODATA KNS_Committee query with a committee scope, e.g. "
                "https://knesset.gov.il/OdataV4/ParliamentInfo/KNS_Committee?$filter=CategoryID eq 2 "
                "(a committee across all Knessets — כאן ועדת הכספים) or "
                "?$filter=Id eq 4187 (a single committee). Each committee is "
                "tracked as its own dataset."
            ),
        )

    page_type, slug = _parse_knesset_url(url)
    name = await _probe_committee_name(scope)
    if name:
        title = f"{name} — פרוטוקולי ועדה"
    else:
        kind, n = scope
        title = (
            f"פרוטוקולי ועדת הכנסת (קטגוריה {n})"
            if kind == "category"
            else f"פרוטוקולי ועדת הכנסת (ועדה {n})"
        )

    return ValidateResponse(
        valid=True,
        page_type=page_type,
        collector_name=slug,
        title=title,
        url=url,
    )


# --------------------------------------------------------------------------
# Bulk bootstrap: register every committee of a given Knesset in one call.
# --------------------------------------------------------------------------

# The rollout target. Every committee active in this Knesset — main, special,
# joint, House, and every ועדת משנה — becomes its own dataset (Id eq N).
BOOTSTRAP_KNESSET = 25


class BootstrapCommitteeResult(BaseModel):
    committee_id: int
    name: str
    committee_type: str | None = None
    source_url: str
    status: str  # "created" | "skipped" | "failed" | "would_create"
    dataset_id: str | None = None
    error: str | None = None


class BootstrapResponse(BaseModel):
    knesset: int
    total_committees: int
    created: int
    skipped: int
    failed: int
    would_create: int
    dry_run: bool
    results: list[BootstrapCommitteeResult]


async def _fetch_committees_for_knesset(knesset_num: int) -> list[dict]:
    """Page ``KNS_Committee`` for every committee row of one Knesset.

    Drives ``$skip`` manually (the feed's nextLink is unreliable — see the
    engine). Each row is a distinct committee instance whose ``Id`` uniquely
    identifies it within that Knesset."""
    rows: list[dict] = []
    skip = 0
    select_fields = "Id,Name,CommitteeTypeID,CommitteeTypeDesc"
    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            url = (
                f"{KNESSET_ODATA_BASE}/KNS_Committee"
                f"?$filter=KnessetNum eq {knesset_num}"
                f"&$select={select_fields}&$orderby=Id&$top=100&$skip={skip}"
            )
            resp = await client.get(url, headers={"Accept": "application/json"})
            resp.raise_for_status()
            page = (resp.json() or {}).get("value") or []
            if not page:
                break
            rows.extend(page)
            if len(page) < 100:
                break
            skip += 100
    return rows


@router.post("/bootstrap-knesset25", response_model=BootstrapResponse)
@limiter.limit("4/hour")
async def bootstrap_knesset25(
    request: Request,
    background_tasks: BackgroundTasks,
    dry_run: bool = False,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Register **every** committee of Knesset 25 as its own tracked dataset.

    Enumerates all Knesset-25 committees from the open ODATA feed and, for each,
    registers a dataset scoped to that committee instance (``Id eq N``) by
    reusing the normal ``track_dataset`` admin flow — so the ckan_id / slug /
    mirror / scraper_config wiring is identical to a hand-registered committee.
    Already-tracked committees (e.g. ועדת הכספים from the trial) are skipped, so
    the call is idempotent and safe to re-run.

    Pass ``?dry_run=true`` to preview the committee list (and which would be
    created vs already exist) without registering anything.
    """
    # Deferred import: datasets.py lazily imports this module, so importing it
    # at module top would risk a circular import at startup.
    from fastapi import HTTPException

    from app.api.datasets import TrackRequest, track_dataset

    committees = await _fetch_committees_for_knesset(BOOTSTRAP_KNESSET)

    # Pre-load already-tracked committee URLs so we skip without provoking (and
    # having to roll back) a duplicate error inside track_dataset.
    existing_rows = await db.execute(
        select(TrackedDataset.source_url).where(
            TrackedDataset.source_url.like("%KNS_Committee%")
        )
    )
    existing_urls = {u for (u,) in existing_rows.all() if u}

    results: list[BootstrapCommitteeResult] = []
    created = skipped = failed = would_create = 0

    for c in committees:
        cid = c.get("Id")
        name = re.sub(r"\s+", " ", (c.get("Name") or "").strip())
        ctype = (c.get("CommitteeTypeDesc") or "").strip() or None
        if not cid:
            failed += 1
            results.append(BootstrapCommitteeResult(
                committee_id=cid or 0, name=name, committee_type=ctype,
                source_url="", status="failed", error="committee row missing Id",
            ))
            continue

        source_url = f"{KNESSET_ODATA_BASE}/KNS_Committee?$filter=Id eq {cid}"
        title = (
            f"{name} — פרוטוקולי ועדה (כנסת {BOOTSTRAP_KNESSET})"
            if name
            else f"פרוטוקולי ועדת הכנסת (ועדה {cid}) (כנסת {BOOTSTRAP_KNESSET})"
        )

        if source_url in existing_urls:
            skipped += 1
            results.append(BootstrapCommitteeResult(
                committee_id=cid, name=name, committee_type=ctype,
                source_url=source_url, status="skipped",
            ))
            continue

        if dry_run:
            would_create += 1
            results.append(BootstrapCommitteeResult(
                committee_id=cid, name=name, committee_type=ctype,
                source_url=source_url, status="would_create",
            ))
            continue

        try:
            resp = await track_dataset(
                TrackRequest(
                    source_type="scraper",
                    source_url=source_url,
                    title=title,
                ),
                background_tasks,
                user=user,
                db=db,
            )
            created += 1
            existing_urls.add(source_url)
            results.append(BootstrapCommitteeResult(
                committee_id=cid, name=name, committee_type=ctype,
                source_url=source_url, status="created", dataset_id=resp.id,
            ))
        except HTTPException as e:
            await db.rollback()
            detail = str(e.detail)
            if e.status_code == 400 and "already tracked" in detail.lower():
                skipped += 1
                results.append(BootstrapCommitteeResult(
                    committee_id=cid, name=name, committee_type=ctype,
                    source_url=source_url, status="skipped",
                ))
            else:
                failed += 1
                results.append(BootstrapCommitteeResult(
                    committee_id=cid, name=name, committee_type=ctype,
                    source_url=source_url, status="failed", error=detail,
                ))
        except Exception as e:  # noqa: BLE001
            await db.rollback()
            failed += 1
            logger.exception("bootstrap: failed to register committee %s", cid)
            results.append(BootstrapCommitteeResult(
                committee_id=cid, name=name, committee_type=ctype,
                source_url=source_url, status="failed", error=str(e),
            ))

    logger.info(
        "knesset bootstrap K%d: %d committees → created=%d skipped=%d failed=%d "
        "(dry_run=%s)",
        BOOTSTRAP_KNESSET, len(committees), created, skipped, failed, dry_run,
    )

    return BootstrapResponse(
        knesset=BOOTSTRAP_KNESSET,
        total_committees=len(committees),
        created=created,
        skipped=skipped,
        failed=failed,
        would_create=would_create,
        dry_run=dry_run,
        results=results,
    )


# --------------------------------------------------------------------------
# MMM (ממ״מ) incremental archive-mode enable.
# --------------------------------------------------------------------------
# The MMM corpus is ONE dataset of ~6,500 documents behind the Radware WAF; a
# full re-scrape takes ~2.7h, so it can't be re-walked daily. This flips the
# dataset to INCREMENTAL archive mode (archive_type "mmm"): each daily poll asks
# the worker for a cheap delta (new rids only). We SEED the checkpoint from the
# rids already mirrored into knesset.mmm_documents, so the first poll finds only
# genuinely-new documents instead of re-downloading the whole corpus. Each new
# delta version is then auto-pulled into TAG-IT (scope 14) for MD extraction.

class MmmArchiveEnableResponse(BaseModel):
    dataset_id: str
    ckan_name: str
    known_rids_seeded: int
    poll_interval: int
    is_active: bool
    archive: bool
    previous_poll_interval: int | None = None
    was_archive: bool = False
    dry_run: bool


@router.post("/enable-mmm-archive", response_model=MmmArchiveEnableResponse)
@limiter.limit("6/hour")
async def enable_mmm_archive(
    request: Request,
    dry_run: bool = False,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Switch the tracked MMM dataset to daily INCREMENTAL archive mode.

    A deploy already auto-activates this once (see ``app.services.mmm_activate``
    + the startup task) — this endpoint is the manual override / re-seed, and
    (with ``?dry_run=true``) a preview of what the flip changes. Unlike the
    guarded startup task, calling this DOES re-seed even when already active, so
    use it deliberately (a re-seed resets ``checkpoint.known_rids`` to the
    current catalogue). ⚠ the poll job is (re)built at scheduler init, so a new
    daily interval takes effect on the next OVER restart/deploy.
    """
    from fastapi import HTTPException

    from app.services import mmm_activate

    ds = await mmm_activate.get_mmm_dataset(db)
    if ds is None:
        raise HTTPException(
            404, f"MMM dataset {mmm_activate.MMM_OVER_DATASET_ID} not found")

    known_rids = await mmm_activate.all_mmm_rids()
    was_archive = bool((ds.scraper_config or {}).get("archive"))
    prev_interval = ds.poll_interval

    if not dry_run:
        mmm_activate.apply_mmm_archive(ds, known_rids)
        await db.commit()
        logger.info(
            "MMM archive enabled (manual): ds=%s known_rids=%d (was archive=%s)",
            ds.id, len(known_rids), was_archive,
        )

    return MmmArchiveEnableResponse(
        dataset_id=str(ds.id), ckan_name=ds.ckan_name,
        known_rids_seeded=len(known_rids),
        poll_interval=mmm_activate.MMM_DAILY_INTERVAL, is_active=True, archive=True,
        previous_poll_interval=prev_interval, was_archive=was_archive,
        dry_run=dry_run,
    )
