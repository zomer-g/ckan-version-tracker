"""Per-dataset content field-flags: boolean metadata about what KINDS of columns
a tracked dataset's table holds (a locality name, a local-authority name, a
government-ministry name, a date). Stored in ``tracked_datasets.field_flags``
(JSONB) as dataset METADATA — alongside title/source — not as rows and not as
tag objects.

Design:
  * One regex per flag, matched against COLUMN NAMES (Hebrew + English), with a
    small exclusion list per flag to kill the recurring false-friends
    (capacity→city, עירוני→עיר, "עומד לרשותם"→רשות, OfficeLineId→office).
  * ``recompute`` reads the live column list per dataset from data_catalog
    (the same source the /data console uses, covering the public, idx and
    knesset-backed dataset tables) and MERGES the computed keys into field_flags
    — it never clobbers keys it did not compute, so callers can enable one flag
    at a time (start with has_locality, expand later).

To add a new flag: add an entry to FLAG_PATTERNS and pass its key in ``fields``.
Nothing else changes — no migration, no clobber.
"""
from __future__ import annotations

import logging
import re

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.tracked_dataset import TrackedDataset
from app.services import data_catalog

logger = logging.getLogger(__name__)


class _Flag:
    """A column-name classifier: match if any pattern hits and no exclusion does."""

    def __init__(self, include: str, exclude: str | None = None):
        self.include = re.compile(include)
        self.exclude = re.compile(exclude) if exclude else None

    def matches(self, col: str) -> bool:
        if not self.include.search(col):
            return False
        if self.exclude and self.exclude.search(col):
            return False
        return True


# key -> classifier. Hebrew is case-flat; the English alternatives are lowercased
# before matching (see _col_has). Mirrors the audited /data detection query.
FLAG_PATTERNS: dict[str, _Flag] = {
    "has_locality": _Flag(
        r"יישוב|ישוב|עיר|(^|[^a-z])(city|town|locality)",
        exclude=r"עירוני|capacity",
    ),
    "has_authority": _Flag(
        r"רשות|מועצה|(^|[^a-z])(authority|municipal)",
        exclude=r"לרשות|ברשות|רשותם",
    ),
    "has_ministry": _Flag(
        r"משרד|ministry|(^|[^a-z])office($|[^a-z])",
    ),
    "has_date": _Flag(
        r"תאריך|שנה|שנת|date|datetime|(^|_)dt($|_)|(^|_)yr($|_)|year",
        exclude=r"אחוז",
    ),
    # A CADASTRAL key — גוש/חלקה — which is what lets a dataset be joined to the
    # parcel map, and through it to every other dataset carrying the same key.
    # Jerusalem's building-licensing register holds it on 88% of its ~100k files;
    # מבא"ת holds none at all (its layers publish no cadastral field), which is
    # exactly the kind of thing this flag exists to make visible without opening
    # each table.
    #
    # The exclusions are measured, not imagined. A bare גוש matches three place
    # names in one CBS file — אבו גוש, גוש עציון, ג'ש (גוש חלב) — and the KKL
    # layer's forest blocks (גוש יער / הגוש היערני), none of which is cadastral.
    # מחלקת/המחלקה is the department sense; the from-parcel column that reads
    # "מחלקה" survives on its table's גוש column anyway, which is the one a join
    # actually starts from.
    "has_parcel": _Flag(
        r"גוש|חלקה|חלקות|(^|[^a-z])(gush|parcel|helka)",
        exclude=r"אבו גוש|גוש עציון|גוש חלב|גוש יער|היערני|מחלקת|המחלקה",
    ),
    # A SHAPE, or the coordinates of one. Separate from has_parcel because the
    # two answer different questions: this one is "can I draw it", that one is
    # "can I join it to something I can draw".
    "has_geometry": _Flag(
        # (^|_)geom($|_) rather than ^geom$: PostGIS's own default column is
        # `the_geom`, and a layer that renames it usually keeps the word.
        r"(^|_)geom($|_)|geometry|_wkt$|קואורדינט|נ\.צ|"
        r"(^|[^a-z])(lat|lon|lng|latitude|longitude|easting|northing)($|[^a-z])",
    ),
}

DEFAULT_FIELDS = ("has_locality",)


def _col_has(columns: list, flag: _Flag) -> bool:
    """True if any column name in this table matches the flag."""
    for c in columns:
        name = c["name"] if isinstance(c, dict) else str(c)
        # `first_seen` is the ingest timestamp, not source data — never a date flag.
        if name == "first_seen":
            continue
        if flag.matches(name) or flag.matches(name.lower()):
            return True
    return False


async def compute_for_columns(columns_by_dataset: dict[str, list], fields) -> dict[str, dict]:
    """Pure classification step: {dataset_id: {flag: bool}} for the given fields.

    Kept separate from the DB write so it is unit-testable without a database.
    A dataset spanning several tables is flagged True if ANY of its tables match.
    """
    out: dict[str, dict] = {}
    for did, cols in columns_by_dataset.items():
        out[did] = {k: _col_has(cols, FLAG_PATTERNS[k]) for k in fields}
    return out


async def recompute(db: AsyncSession, fields=DEFAULT_FIELDS) -> dict:
    """Recompute the given flag keys for every dataset and MERGE them into
    field_flags (existing keys outside ``fields`` are preserved). Returns stats.

    ``fields`` defaults to just ('has_locality',) — the first flag we enable —
    so this can be rolled out one field at a time.
    """
    for k in fields:
        if k not in FLAG_PATTERNS:
            raise ValueError(f"unknown field flag: {k}")

    # Gather live columns per dataset from the same catalog the console uses.
    catalog = await data_catalog.build_catalog(db, use_cache=False)
    cols_by_ds: dict[str, list] = {}
    for rec in catalog:
        did = rec.get("dataset_id")
        if not did:
            continue  # knesset/odata helper tables aren't datasets
        cols_by_ds.setdefault(did, []).extend(rec.get("columns") or [])

    computed = await compute_for_columns(cols_by_ds, fields)

    datasets = (await db.execute(select(TrackedDataset))).scalars().all()
    changed = 0
    true_counts = {k: 0 for k in fields}
    for ds in datasets:
        flags = computed.get(str(ds.id))
        if flags is None:
            # No physical table yet → the flags are definitively False for now.
            flags = {k: False for k in fields}
        current = dict(ds.field_flags or {})
        merged = {**current, **flags}          # additive: only touch `fields` keys
        for k in fields:
            if flags[k]:
                true_counts[k] += 1
        if merged != current:
            ds.field_flags = merged            # reassign so SQLAlchemy tracks the JSONB change
            changed += 1

    await db.commit()
    # The /data catalog caches its records (incl. field_flags) for ~5 min per
    # process; drop this process's copy so the change shows at once. Other worker
    # processes converge on their own TTL — that's the documented backstop.
    data_catalog.invalidate_catalog_cache()
    stats = {
        "fields": list(fields),
        "datasets_total": len(datasets),
        "datasets_updated": changed,
        "true_counts": true_counts,
    }
    logger.info("field_flags.recompute: %s", stats)
    return stats
