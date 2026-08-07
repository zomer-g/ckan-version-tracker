"""What data.gov.il refuses to serve a server, remembered between polls.

data.gov.il puts some resource FILES behind a bot wall: a headless download
gets an HTML challenge page under the file's name instead of the file, which
``ckan_client`` raises as "Got HTML" and ``version_detector`` turns into a
blocked id. Tabular resources are unaffected — their rows still come through
the datastore API — so a blocked file is a hole in a dataset that otherwise
looks healthy.

Why this is stored rather than recomputed
-----------------------------------------
Being blocked is a property of the RESOURCE, not of one poll. The wall does not
come down because the package's ``metadata_modified`` stood still. But the
warning used to be built only after ``detect_resource_changes``, which two
shortcuts earlier in ``poll_dataset`` return before ever reaching:

  * nothing-changed (the metadata revision we already hold), and
  * a version already exists for this revision.

So a dataset whose metadata has not moved since the detection shipped kept
taking a shortcut past the only code that could notice its files were missing,
and lost them in total silence. Measured 2026-08-07 across the 13 CKAN datasets
holding blocked files: only 6 carried any warning at all. רמזורים was quietly
missing 11 resources, תושבים בישראל לפי ישובים another 11, with nothing in the
UI to say so.

Remembering the finding on the dataset fixes both halves: the shortcuts can
render the warning without repeating the work, and the entries are already the
structured work-list — resource id, name, format, URL — that a worker capable
of getting past the wall needs, instead of it having to parse a Hebrew
sentence out of an error field.

``assessed()`` is the third piece. A dataset that has never once run detection
has nothing stored, and would keep taking the shortcut forever — so the caller
treats "never assessed" as work to do, exactly as it already treats a NEON
archive that is behind its plan. That costs one full poll per dataset, once.
"""
from __future__ import annotations

CONFIG_KEY = "blocked_resources"


def describe(resources: list[dict], blocked_ids) -> list[dict]:
    """Structured entries for the resources this poll found blocked.

    Ordered by the dataset's own resource order rather than by the id set, so
    the stored list is stable between polls and does not churn the config
    (and the warning does not reshuffle) just because a set iterated
    differently.
    """
    ids = set(blocked_ids or ())
    return [
        {
            "id": r["id"],
            "name": r.get("name") or r["id"][:8],
            "format": (r.get("format") or "?").upper(),
            "url": r.get("url") or "",
        }
        for r in resources
        if r.get("id") in ids
    ]


def note_for(entries: list[dict] | None) -> str | None:
    """The standing user-facing indication, or None when nothing is blocked."""
    if not entries:
        return None
    listed = ", ".join(f"{e.get('name')} ({e.get('format')})" for e in entries)
    return (
        "ℹ ממתין לגירוד בדפדפן — "
        f"{len(entries)} קבצים חסומים להורדה שרתית ב-data.gov.il: {listed}"
    )


def stored(ds) -> list[dict]:
    """What the last assessment found. [] both when nothing is blocked and
    when nothing has been assessed — use `assessed()` to tell those apart."""
    entries = (ds.scraper_config or {}).get(CONFIG_KEY)
    return list(entries) if isinstance(entries, list) else []


def assessed(ds) -> bool:
    """Has detection ever run against this dataset?

    An empty list means "checked, nothing blocked" and is a real answer; a
    missing key means we have never looked.
    """
    return isinstance((ds.scraper_config or {}).get(CONFIG_KEY), list)


def remember(ds, entries: list[dict]) -> bool:
    """Persist this poll's finding. Returns True when it actually changed.

    The column is plain JSONB, not a MutableDict, so an in-place edit would not
    be flagged dirty and would never reach the database — the config is
    REPLACED, matching how every other writer here does it.

    A no-op write is skipped so an unchanged dataset does not dirty its row on
    every poll.
    """
    current = (ds.scraper_config or {}).get(CONFIG_KEY)
    if isinstance(current, list) and current == entries:
        return False
    ds.scraper_config = {**(ds.scraper_config or {}), CONFIG_KEY: entries}
    return True
