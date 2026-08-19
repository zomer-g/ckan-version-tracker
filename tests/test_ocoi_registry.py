"""Registry mirror + matching (Wave 4).

The behaviour worth pinning is mostly about the things that were WRONG in the
original and are easy to reintroduce: the numberless-row duplication, the
disabled source, the two different scorers, and the clock.
"""
import inspect

import pytest

from app.services import ocoi_db, ocoi_match, ocoi_registry


# ── name normalisation ────────────────────────────────────────────────────────

def test_normalize_strips_legal_boilerplate():
    n = ocoi_registry.normalize_company_name
    assert n('בנק לאומי לישראל בע"מ') == "בנק לאומי"
    assert n("חברת החשמל") == "החשמל"
    assert n("  קבוצת   דלק  ") == "דלק"


def test_normalize_is_not_lowercased():
    """Hebrew is unicase and the original did not lowercase — a Latin name must
    come back with its case intact, or it stops matching the mirrored rows."""
    assert ocoi_registry.normalize_company_name("Intel Corp") == "Intel Corp"


def test_normalize_handles_empty():
    assert ocoi_registry.normalize_company_name("") == ""
    assert ocoi_registry.normalize_company_name(None) == ""


# ── scoring ───────────────────────────────────────────────────────────────────

def test_identical_after_normalisation_scores_one():
    assert ocoi_registry.match_score('בנק לאומי לישראל בע"מ',
                                     "בנק לאומי לישראל") == 1.0


def test_unrelated_names_score_below_threshold():
    s = ocoi_registry.match_score('אורבונד בע"מ', "מפעלי ים המלח")
    assert s < ocoi_registry.MATCH_THRESHOLD


def test_empty_name_scores_zero():
    assert ocoi_registry.match_score("", "בנק לאומי") == 0.0
    assert ocoi_registry.match_score('בע"מ', "בנק לאומי") == 0.0


def test_registry_scorer_is_not_the_duplicate_scorer():
    """Two scorers exist on purpose and were tuned separately. If someone
    collapses them into one, this fails."""
    assert ocoi_registry.match_score is not ocoi_match.similarity
    # ocoi_match.similarity returns (score, reasons); this one returns a float.
    assert isinstance(ocoi_registry.match_score("א ב", "א ב"), float)


# ── source configuration ──────────────────────────────────────────────────────

def test_five_sources_are_declared():
    assert set(ocoi_registry.REGISTRY_SOURCES) == {
        "companies", "associations", "public_benefit",
        "local_authorities", "municipal_corporations"}


def test_municipal_corporations_is_disabled_and_excluded():
    """It carries no registration number, so it can never produce a match, and
    the number field OCOI configured does not exist in the resource (CKAN
    answers 409). It stays listed so the admin can see WHY it is empty."""
    cfg = ocoi_registry.REGISTRY_SOURCES["municipal_corporations"]
    assert cfg["enabled"] is False
    assert cfg["note"]
    assert "municipal_corporations" not in ocoi_registry._ENABLED
    assert "municipal_corporations" not in ocoi_registry.COMPANY_SOURCES


def test_every_enabled_source_declares_a_number_field():
    for key in ocoi_registry._ENABLED:
        cfg = ocoi_registry.REGISTRY_SOURCES[key]
        assert cfg["number"], f"{key} has no number field to match on"
        assert cfg["entity_type"] in ("company", "association")


def test_company_and_association_sources_partition_the_enabled_set():
    assert set(ocoi_registry.COMPANY_SOURCES) | set(
        ocoi_registry.ASSOCIATION_SOURCES) == set(ocoi_registry._ENABLED)


def test_sync_refuses_a_disabled_source():
    import asyncio
    with pytest.raises(RuntimeError):
        asyncio.run(ocoi_registry.sync_registry("municipal_corporations"))


# ── the clock (the pooler drops the session timezone) ─────────────────────────

def test_now_local_is_jerusalem_not_the_container_clock():
    """`datetime.now().astimezone()` reads the container clock — UTC on Render,
    Jerusalem on a dev box — so the same code wrote two different times."""
    src = inspect.getsource(ocoi_db.now_local)
    assert "_JERUSALEM" in src
    assert ".astimezone()" not in src


def test_now_local_and_now_utc_differ_by_the_offset():
    delta = (ocoi_db.now_utc().replace(tzinfo=None)
             - ocoi_db.now_local()).total_seconds()
    assert -4 * 3600 < delta < -1 * 3600  # IST/IDT is UTC+2 or +3


def test_naive_timestamp_writes_never_use_a_bare_sql_now():
    """Only the five timestamptz columns may use bare now(). Everything else
    must say `now() AT TIME ZONE 'Asia/Jerusalem'` or bind now_local()."""
    import pathlib
    tz_tables = ("registry_sync_status", "ocoi_jobs")
    for name in ("ocoi_registry.py", "ocoi_match.py"):
        text = pathlib.Path("app/services", name).read_text(encoding="utf-8")
        for line in text.splitlines():
            if "now()" in line and "AT TIME ZONE" not in line and "def " not in line:
                # allowed only inside a statement touching a timestamptz table
                idx = text.index(line)
                stmt = text[max(0, idx - 400):idx + 200]
                assert any(t in stmt for t in tz_tables), (
                    f"{name}: bare now() outside a timestamptz table:\n{line}")
