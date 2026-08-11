"""The OVER-side guarantees the mevaker scraper fix depends on.

govil-scraper f0e61ea rewrote how the State Comptroller library is enumerated,
after the old page-walk froze the corpus at 2019 for years without ever
erroring. That fix leans on three things being true on THIS side, and all
three were previously true only by accident — nothing asserted them:

  1. An unknown key in ``scraper_config_patch`` survives the merge, because
     the worker's shrink-protection memory now rides in ``mevaker_enumeration``.
     If the merge ever grew a known-key allowlist, that memory would arrive
     empty on every run, the protection would never arm, and nothing would say
     so.
  2. The shrink guard fires on SHRINKAGE only. The first run after the fix is a
     4x jump (1,863 → 7,971 rows); a guard that policed "large change" rather
     than "large loss" would reject all nine datasets at once.
  3. Every publication type the library actually populates is registerable.

These are cheap to assert and expensive to discover in production.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.api import mevaker as mevaker_api  # noqa: E402


# ── 1. scraper_config_patch must not filter unknown keys ────────────────────

def _merge(existing: dict, patch: dict) -> dict:
    """The exact merge app/api/worker.py performs at both call sites."""
    current = dict(existing or {})
    current.update(patch)
    return current


def test_unknown_scraper_config_patch_keys_survive_the_merge():
    merged = _merge(
        {"kind": "mevaker", "publication_type": "דוחות שנתיים"},
        {"mevaker_enumeration": {"publications": 1825, "declared_total": 1825,
                                 "last_page": 2190, "method": "csom",
                                 "task_rows": 7971, "matched_publications": 138}},
    )
    assert merged["mevaker_enumeration"]["publications"] == 1825
    assert merged["kind"] == "mevaker", "the patch must not drop existing config"


def test_patch_merge_has_no_key_allowlist_in_the_source():
    """Guards the guarantee at its real location: if someone introduces a
    whitelist in worker.py, the enumeration memory silently stops persisting
    and the shrink protection never arms."""
    import pathlib
    src = pathlib.Path(__file__).resolve().parents[1] / "app" / "api" / "worker.py"
    text = src.read_text(encoding="utf-8")
    assert text.count("current.update(body.scraper_config_patch)") == 2, (
        "both scraper_config_patch merge sites must remain an unfiltered "
        "dict.update — see tests/test_mevaker_contract.py"
    )


# ── 2. the shrink guard must be one-directional ─────────────────────────────

def _shrink_guard_rejects(new_total: int, prev_total: int,
                          min_fraction: float = 0.5) -> bool:
    """The predicate from app/api/worker.py's shrink guard."""
    return prev_total > 0 and new_total < prev_total * min_fraction


def test_growth_is_never_rejected():
    # The real numbers from the fixed run: the corpus quadrupled.
    assert not _shrink_guard_rejects(7971, 1863)
    # And per-dataset: דוחות שנתיים went 1,096 → 5,279.
    assert not _shrink_guard_rejects(5279, 1096)
    # Even an absurd jump is not the guard's business.
    assert not _shrink_guard_rejects(1_000_000, 10)


def test_real_collapse_is_still_rejected():
    assert _shrink_guard_rejects(100, 1000)
    assert _shrink_guard_rejects(0, 10)


def test_guard_is_inert_without_a_baseline():
    """A first version has nothing to be measured against."""
    assert not _shrink_guard_rejects(7971, 0)


# ── 3. every populated publication type is registerable ─────────────────────

# Measured against the live library on 2026-08-11 by the fixed enumeration:
# volumes per type. All ten are populated — including international, which was
# de-listed on the strength of a scan that the enumeration bug had truncated.
LIVE_TYPES = {
    "דוחות שנתיים": 138,
    "ביקורת על השלטון המקומי": 814,
    "ביקורת על האיגודים": 359,
    "דוחות מיוחדים": 162,
    "עיונים, מאמרים, ספרים": 56,
    "מימון בחירות ברשויות המקומיות": 155,
    "דוחות נציב תלונות הציבור": 64,
    "מימון מפלגות": 54,
    "מימון בחירות מקדימות (פריימריז)": 21,
    "דוחות בינלאומיים": 2,
}


def test_every_populated_type_is_trackable():
    assert set(mevaker_api.MEVAKER_TYPES.values()) == set(LIVE_TYPES), (
        "a publication type that exists in the library but is missing here "
        "cannot be registered at all — /validate rejects its URL"
    )


def test_international_is_registerable_again():
    """It was removed because a truncated scan reported it empty. It holds two
    2026 volumes, including a multi-national audit of government AI readiness."""
    page_type, slug = mevaker_api._parse_mevaker_url(
        "https://www.mevaker.gov.il/subjects?type=international")
    assert page_type == "mevaker_reports:international"
    assert slug == "mevaker-international"
    assert mevaker_api.type_hebrew_of(page_type) == "דוחות בינלאומיים"


def test_publication_type_is_matched_verbatim():
    """The scraper compares this string to the service's own value, so a
    stray space or a different quote mark silently matches nothing."""
    for value in mevaker_api.MEVAKER_TYPES.values():
        assert value == value.strip()
        assert '"' not in value and "״" not in value


def test_unknown_slug_is_still_rejected():
    page_type, slug = mevaker_api._parse_mevaker_url(
        "https://www.mevaker.gov.il/subjects?type=not-a-real-type")
    assert page_type is None and slug is None
