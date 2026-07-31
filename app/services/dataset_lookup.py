"""Resolve a pasted source URL to the tracked dataset(s) that already cover it.

One matcher, used by three callers so they can never disagree:

  * ``GET /api/resolve``            — the public deep link (/lookup) and the
                                      home search box, so a URL that is already
                                      tracked opens its versions page instead of
                                      a request form.
  * ``POST /api/datasets/requests`` — duplicate detection when a request comes in.
  * ``POST /api/datasets`` (admin)  — the same, on the admin track path.

Matching is by identity (see app/services/url_identity.py), not by string
equality, so viewport/tracking params in a copied link don't hide an existing
dataset.

TWO IDENTITIES, not one. A URL's identity answers "is this the same LINK?", and
for a site that spells one corpus several ways it answers no when the truth is
yes: ykpubdata's search screen, its address-results grid and its bare root are
four canonical URLs for one corpus, and tracking two of them means running the
same ~10-hour sweep twice. So a URL that a MANIFEST classifies also carries a
registry identity — (source, page_type, resolved config) — and two URLs sharing
it are the same dataset however differently they are written. See
source_registry.identity_of for why that is sound for manifest sources and
wrong for the hardcoded ones.
"""

from __future__ import annotations

import logging

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.tracked_dataset import TrackedDataset
from app.services import source_registry
from app.services.url_identity import host_of, path_key, url_identity

logger = logging.getLogger(__name__)

# Candidates are filtered by host first, so this only bites on a host with a
# genuinely huge catalog. Rows are 7 short columns; the cap is a safety net
# against a pathological ILIKE, not an expected limit.
_CANDIDATE_LIMIT = 5000
# A path match can be ambiguous by design; past a handful it's noise, not a
# helpful "did you mean".
_LOOSE_LIMIT = 25


def _serialize(row, match: str) -> dict:
    return {
        "id": str(row.id),
        "title": row.title,
        "organization": row.organization,
        "source_type": row.source_type,
        "source_url": row.source_url,
        "status": row.status,
        "is_active": bool(row.is_active),
        "match": match,
    }


async def find_datasets_for_url(
    db: AsyncSession, url: str, *, strict: bool = False
) -> list[dict]:
    """Datasets that already track ``url``, best match first.

    Returns ``[]`` when nothing matches — the caller's cue to offer the
    collection request form. A list longer than one means the URL is ambiguous
    (it identifies a page that several datasets are cut from); callers must
    show the choice rather than pick one.

    ``strict=True`` returns identity matches only. Duplicate checks must use
    it: a path match is a "did you mean", and two datasets can legitimately be
    cut from one page (jeden.co.il's ``?category=tenders`` / ``=decisions``),
    so treating one as a duplicate of the other would block a valid request.
    A registry match counts as an identity match and is reported as one — it
    answers the same question the URL identity does, with better information.
    """
    target = url_identity(url)
    if target is None:
        return []
    host = host_of(url)
    if not host:
        return []

    # The registry identity, for the URL shapes that have one. Only consulted
    # for a generic ("url") identity: a GovMap layer and a CKAN package are
    # claimed by hardcoded parsers, which the registry runs behind and can never
    # shadow (see RESERVED_SOURCE_IDS), so those paths pay nothing for this.
    #
    # Best-effort: this axis only ever ADDS matches, so when the registry can't
    # be read the answer degrades to the URL identity — what every caller got
    # before it existed — instead of failing a public lookup. Logged, because
    # the failure mode of losing it silently is the duplicate it was added to
    # prevent.
    target_registry = None
    manifests: list = []
    if target[0] == "url":
        try:
            manifests = await source_registry.load_enabled(db)   # process-cached
            target_registry = source_registry.identity_of(
                source_registry.match_manifests(url, manifests)
            )
        except Exception:  # noqa: BLE001 — never break a lookup over this
            logger.warning(
                "dataset_lookup: source registry unavailable; falling back to "
                "URL identity alone", exc_info=True,
            )
            manifests = []

    def _registry_identity(source_url: str | None):
        if target_registry is None or not source_url:
            return None
        return source_registry.identity_of(
            source_registry.match_manifests(source_url, manifests)
        )

    # A CKAN dataset is tracked per RESOURCE and carries no source_url — its
    # link back to data.gov.il is the package name in ``ckan_name``. So the
    # candidate set is "same host in source_url" OR "same CKAN package", and a
    # data.gov.il link legitimately resolves to several datasets (one per
    # mirrored resource), which the caller shows as a choice.
    candidate_filter = TrackedDataset.source_url.ilike(f"%{host}%")
    if target[0] == "ckan":
        candidate_filter = candidate_filter | (
            func.lower(TrackedDataset.ckan_name) == target[1]
        )

    rows = (
        await db.execute(
            select(
                TrackedDataset.id,
                TrackedDataset.title,
                TrackedDataset.organization,
                TrackedDataset.source_type,
                TrackedDataset.source_url,
                TrackedDataset.status,
                TrackedDataset.is_active,
                TrackedDataset.scraper_config,
                TrackedDataset.ckan_name,
            )
            .where(
                candidate_filter,
                # Demoted duplicates (migration 035) are not somewhere to send
                # anyone; the surviving dataset is in the same result set.
                TrackedDataset.status != "duplicate",
                # ~2,900 auto-generated one-per-committee-meeting rows are
                # bulk-managed and excluded from the public catalog — same
                # exclusion as GET /api/datasets.
                TrackedDataset.ckan_name.notlike("knesset-committee-single-%"),
            )
            .order_by(TrackedDataset.is_active.desc(), TrackedDataset.created_at.asc())
            .limit(_CANDIDATE_LIMIT)
        )
    ).all()

    target_path = path_key(url)
    exact: list = []
    loose: list = []
    for row in rows:
        if target[0] == "ckan" and (row.ckan_name or "").lower() == target[1]:
            exact.append((row, "identity"))
            continue
        identity = url_identity(row.source_url, row.source_type, row.scraper_config)
        if identity is not None and identity == target:
            exact.append((row, "identity"))
        elif (
            target_registry is not None
            and _registry_identity(row.source_url) == target_registry
        ):
            # Same source, same page_type, same resolved config — the same
            # corpus written a different way.
            exact.append((row, "identity"))
        elif (
            target[0] == "url"
            and target_path is not None
            and path_key(row.source_url) == target_path
        ):
            loose.append((row, "path"))

    hits = exact if strict else (exact or loose[:_LOOSE_LIMIT])
    if not hits:
        return []

    counts = await _version_counts(db, [row.id for row, _ in hits])
    return [
        {**_serialize(row, match), "version_count": counts.get(row.id, 0)}
        for row, match in hits
    ]


async def _version_counts(db: AsyncSession, ids: list) -> dict:
    from app.models.version_index import VersionIndex

    return dict(
        (
            await db.execute(
                select(VersionIndex.tracked_dataset_id, func.count(VersionIndex.id))
                .where(VersionIndex.tracked_dataset_id.in_(ids))
                .group_by(VersionIndex.tracked_dataset_id)
            )
        ).all()
    )
