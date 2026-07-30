"""What a dataset's archive ACTUALLY holds — as opposed to what its plan says.

The storage PLAN (``storage_target_of``: local / odata / r2 / neon / r2+neon) is a
declaration: an admin picked it, and it describes intent. What a dataset really
holds is decided later, at poll time, by whichever archiving path the poll
happened to take — and the two can disagree completely:

  * A CKAN dataset with more than ``large_dataset_threshold`` rows that was NOT
    opted into a NEON archive gets ``_poll_large_dataset``'s metadata stub: a row
    count, the field names and 200 sample rows. Its plan still reads
    ``r2`` — "R2 — סנפשוט מלא" in the admin — while not one row of the source was
    ever archived. Measured 2026-07-30: 16 of 67 CKAN datasets were in exactly
    this state, ~13.4M rows behind stubs, and the coverage audit counted every one
    of them as covered because it only asks ``version_count > 0``.
  * The plan belongs to the DATASET, but the bytes belong to each VERSION. A
    dataset re-pointed at R2 keeps historical versions whose mappings hold bare
    ODATA resource ids, and every UI showed the whole history as "R2".

So this module answers the question the plan cannot: **what is in there?** It reads
the LATEST version's ``resource_mappings`` — the record of what was actually
written — and classifies it. Nothing here mutates anything; it is a pure
derivation, and the plan stays the single place a human configures storage.

``resource_mappings`` is the authority rather than ``change_summary.type`` because
``metadata_only`` versions copy the previous version's mappings verbatim (see
conditional_archiver): a normal R2 dataset whose latest poll confirmed "bytes
unchanged" still carries its real ``r2:`` values, and a stub still carries
``_large_dataset_info``. Reading the mappings gets both right; reading the type
string would report the first as having no data.
"""
from __future__ import annotations

import re
from typing import Iterator

# Mapping keys that are bookkeeping, not archived content. Anything else in a
# version's mappings is a candidate file reference.
#
# ``_appendonly_seen`` is the reason this list has to be explicit rather than a
# "long string ⇒ file" heuristic: it holds the legacy seen-set, which for a
# keyless dataset is a list of 64-char row hashes. A length test counts every one
# of them as an archived file.
_BOOKKEEPING_KEYS = frozenset({
    "_hashes",
    "_resource_ids",
    "_appendonly_seen",
    "_names",
    "_filedates",
    "_probes",
    "_large_dataset_info",
    "append_table",
    "_append_tables",
})

# The NEON row-archive markers a version can carry: one table (``append_table``)
# or one per datastore resource (``_append_tables``). See append_store.
_ROW_KEYS = ("append_table", "_append_tables")

# The stub marker. Written ONLY by poll_job._poll_large_dataset, and carried
# forward by metadata_only versions — so its presence is an exact signal that the
# latest version records counts rather than data.
_STUB_KEY = "_large_dataset_info"

# The stub's OWN downloadable resource: the 200-row sample CSV that
# create_lightweight_snapshot pushes to the ODATA mirror. It is a real, fetchable
# resource, which is exactly the trap — counting it makes a stub look like a
# dataset with archived files. Verified on prod 2026-07-30: all 16 stub datasets
# reported ``file_store="odata"`` from this one key, which put an "ODATA" chip
# next to "מטא־דאטה בלבד" (implying downloadable data) and fired a redundant
# file-store mismatch on top of the sample_only one. Ignored while the stub
# marker is present, and only then — a resource legitimately NAMED "sample" on a
# normal dataset still counts.
_STUB_OWN_SAMPLE_KEY = "sample"

_R2_PREFIX = "r2:"
# A bare ODATA resource id is a canonical UUID. Matched strictly (not by length)
# so a row hash or a NEON table name can never be mistaken for a stored file.
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I
)

# ── fidelity: what the archive actually preserves ────────────────────────────
FULL = "full"          # downloadable files AND queryable rows
ROWS = "rows"          # queryable rows only (NEON-only plan)
FILES = "files"        # downloadable files only
SAMPLE = "sample"      # a metadata stub — counts + field names + sample rows
NONE = "none"          # nothing archived (no versions, or an empty version)

# ── mismatch codes: plan-vs-reality flags, for the admin only ────────────────
# Each is independently actionable; nothing is flagged that a human can't act on.
MISMATCH_NO_VERSION = "no_version"    # tracked, never archived
MISMATCH_SAMPLE_ONLY = "sample_only"  # plan promises data, archive holds a stub
MISMATCH_FILE_STORE = "file_store"    # files sit somewhere the plan doesn't name
# ^ In practice this almost always means "still on the legacy ODATA mirror under a
# plan of r2" — and note that plan is usually r2 only because r2 is the GLOBAL
# default, not because anyone pinned it. So it reads as "this dataset's bytes
# haven't been moved off ODATA yet" (18 CKAN datasets on prod 2026-07-30), which
# is actionable: there is an admin /migrate-r2 endpoint for exactly that.


def _iter_file_values(mappings: dict | None) -> Iterator[str]:
    """Every value in ``mappings`` that references an archived file.

    Walks named resource keys and the underscore aggregates (``_zip_parts``,
    ``_geojson``, ``_gpkg``, ``_parquet``, …), skipping bookkeeping keys, and
    yields only values that are recognizably a storage reference — an ``r2:``
    marker or a bare ODATA resource UUID.

    On a metadata stub the ``sample`` resource is skipped: it is the stub's own
    200-row excerpt, not an archive of the data (see _STUB_OWN_SAMPLE_KEY).
    """
    m = mappings or {}
    skip = _BOOKKEEPING_KEYS
    if m.get(_STUB_KEY):
        skip = skip | {_STUB_OWN_SAMPLE_KEY}
    for key, value in m.items():
        if key in skip:
            continue
        for v in (value if isinstance(value, list) else [value]):
            if not isinstance(v, str) or not v:
                continue
            if v.startswith(_R2_PREFIX) or _UUID_RE.match(v):
                yield v


def actual_file_store(mappings: dict | None) -> str:
    """Where this version's files physically are: ``r2`` | ``odata`` | ``mixed``
    | ``none``. Derived from the mapping VALUES, so it reports the truth even
    when the dataset's plan says something else."""
    seen_r2 = seen_odata = False
    for v in _iter_file_values(mappings):
        if v.startswith(_R2_PREFIX):
            seen_r2 = True
        else:
            seen_odata = True
    if seen_r2 and seen_odata:
        return "mixed"
    if seen_r2:
        return "r2"
    if seen_odata:
        return "odata"
    return "none"


def _has_rows(mappings: dict | None, archives_neon: bool) -> bool:
    """Whether the dataset's rows are in the NEON append DB.

    A version that streamed rows records the table in its mappings. The scraper
    dual-write (``archive_neon`` on registries / munidata / מצפן / אמו"ן) loads
    NEON from the push payload and does NOT stamp a table key, so for those the
    declared plan is the only signal available in the OVER DB — hence the
    ``archives_neon`` fallback. It's a declaration, not an observation, so it is
    deliberately never used to CLEAR a flag, only to set one.

    KNOWN BOUNDARY: this reads only the LATEST version, while the NEON table is a
    property of the DATASET — it persists once created. So a dataset that streamed
    rows in v10 and then fell back to a metadata stub in v11 would under-report
    ``row_store="none"`` even though its table still holds the rows.
    ``append_tables.resolve_tables`` handles that shape correctly (it walks
    versions newest-first) at the cost of a query per dataset. Measured across
    every CKAN dataset on prod 2026-07-30: ZERO datasets are in that state, so the
    extra query buys nothing today. If it ever appears, widen this to the same
    newest-first walk rather than trusting the latest version alone.
    """
    m = mappings or {}
    if any(m.get(k) for k in _ROW_KEYS):
        return True
    return archives_neon


def describe(
    mappings: dict | None,
    *,
    has_versions: bool,
    plan: str,
    archives_neon: bool,
    stores_files: bool,
) -> dict:
    """Classify one dataset's archive from its LATEST version's mappings.

    ``plan`` is the declared storage target (``storage_target_of``);
    ``archives_neon`` / ``stores_files`` are the routing predicates from
    ``storage_client``. They're passed in — rather than the dataset row, from
    which this could recompute them — so this module stays free of the
    config-parsing rules and ``scraper_config`` keeps exactly one reader.

    Returns a dict shaped for ``ArchiveState`` (see app/api/datasets.py).
    """
    file_store = actual_file_store(mappings)
    rows = _has_rows(mappings, archives_neon)
    is_stub = bool((mappings or {}).get(_STUB_KEY))

    # Order matters. Rows first: a dual-write version carries BOTH a NEON table
    # and an R2 snapshot, and that is the fullest state there is. The stub check
    # comes before files because a stub's `sample` mapping IS a real ODATA
    # resource (the 200-row sample CSV) — counting it as "files" would report a
    # dataset holding 200 of 640,460 rows as fully archived.
    if not has_versions:
        fidelity = NONE
    elif rows:
        fidelity = FULL if file_store != "none" else ROWS
    elif is_stub:
        fidelity = SAMPLE
    elif file_store != "none":
        fidelity = FILES
    else:
        fidelity = NONE

    sample_of = None
    if is_stub:
        rc = (mappings or {}).get(_STUB_KEY, {})
        if isinstance(rc, dict):
            try:
                sample_of = int(rc.get("record_count") or 0) or None
            except (TypeError, ValueError):
                sample_of = None

    # Plan-vs-reality. Only flag what a human can act on, and never flag a plan
    # that is honestly doing what it says (a `neon` plan stores no files by
    # design, so "no files" is not a finding for it).
    mismatch: list[str] = []
    if not has_versions:
        mismatch.append(MISMATCH_NO_VERSION)
    elif fidelity == SAMPLE:
        mismatch.append(MISMATCH_SAMPLE_ONLY)
    if (
        stores_files
        and file_store not in ("none", plan.split("+", 1)[0])
        and plan != "local"
    ):
        mismatch.append(MISMATCH_FILE_STORE)

    return {
        "fidelity": fidelity,
        "file_store": file_store,
        "row_store": "neon" if rows else "none",
        "sample_of": sample_of,
        "mismatch": mismatch,
    }
