"""נדל"ן לעם — publishing the address spine, and the gate that decides when.

The gate is the whole design, so it is where the tests are. The states it
exists to catch both report `ok` from every stage and differ from a finished
corpus only in the row count:

  * a rebuild that has not had the geocoded points merged back — 357,679
    points instead of 451,667, i.e. 20.8% short and perfectly plausible;
  * a corpus whose `pip` has not re-run, so the points are there and the
    parcel links are not.

A gate reading stage names sees neither. This one reads counts.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import asyncio  # noqa: E402
import pytest  # noqa: E402

from app.services import address_dataset as ad  # noqa: E402

# The real production figures, so the test fails if the gate stops recognising
# the actual shape of the problem rather than a toy version of it.
HEALTHY = {"rows": 617876, "with_point": 451667, "with_parcel": 430057,
           "register": 357679, "govmap": 93988}
REBUILT_NOT_MERGED = {"rows": 617876, "with_point": 357679, "with_parcel": 357679,
                      "register": 357679, "govmap": 0}


# ── the gate ──────────────────────────────────────────────────────────────────
def test_a_healthy_corpus_publishes():
    assert ad.gate(HEALTHY, {}) is None
    assert ad.gate(HEALTHY, HEALTHY) is None


def test_the_first_version_has_nothing_to_compare_against():
    """No previous version must not mean "refuse" — it means publish."""
    assert ad.gate(HEALTHY, {}) is None


def test_a_rebuild_without_its_geocoded_points_is_refused():
    """The case this module exists for. 357,679 of 451,667 is a successful
    build by every stage's own account."""
    why = ad.gate(REBUILT_NOT_MERGED, HEALTHY)
    assert why is not None
    assert "357,679" in why and "451,667" in why


def test_the_refusal_says_what_to_do_about_it():
    """A gate that blocks without naming the cause gets forced by the next
    person who meets it."""
    why = ad.gate(REBUILT_NOT_MERGED, HEALTHY)
    assert "merged back" in why


def test_losing_the_parcel_links_is_refused_too():
    """`pip` not having re-run is its own failure: points present, links gone."""
    counts = dict(HEALTHY, with_parcel=0)
    why = ad.gate(counts, HEALTHY)
    assert why is not None and "pip" in why


def test_an_empty_table_never_publishes():
    assert ad.gate({"rows": 0, "with_point": 0}, HEALTHY) is not None
    assert ad.gate({"rows": 617876, "with_point": 0}, {}) is not None


def test_small_honest_movement_is_not_a_shrink():
    """A rebuild re-reads the register; a few doorways merge or split. The gate
    must not fire on that, or every build becomes a manual override."""
    counts = dict(HEALTHY, with_point=HEALTHY["with_point"] - 500,
                  with_parcel=HEALTHY["with_parcel"] - 500)
    assert ad.gate(counts, HEALTHY) is None


def test_the_floor_sits_above_the_register_only_state():
    """The ratio has to separate 'a few rows moved' from 'the geocoding is
    gone'. 357,679/451,667 = 0.792, so the floor must be above it and below 1."""
    assert 0.8 < ad.MIN_POINT_RATIO < 1.0
    floor = HEALTHY["with_point"] * ad.MIN_POINT_RATIO
    assert REBUILT_NOT_MERGED["with_point"] < floor


# ── what gets published ───────────────────────────────────────────────────────
def test_both_grids_are_published():
    for col in ("lat", "lon", "itm_x", "itm_y"):
        assert col in ad.COLUMNS


def test_provenance_is_published():
    """Without it a 5.5 km GovMap outlier is indistinguishable from a 10 cm
    register point, and the file gives the reader no way to tell."""
    assert "point_source" in ad.COLUMNS


def test_the_postgis_blob_is_not_published():
    """`point` carries nothing the two grids do not, and no spreadsheet reads
    it."""
    assert "point" not in ad.COLUMNS


# ── attribution ───────────────────────────────────────────────────────────────
def test_the_dataset_is_not_filed_under_govmap():
    """Measured: 79.2% of the points are the register's own ITM and 20.8% are
    GovMap's. Filing it under GovMap would misattribute four fifths of the
    corpus — and the GovMap fifth is the less accurate one."""
    src = __import__("inspect").getsource(ad.ensure_dataset)
    assert 'source_type="scraper"' in src
    assert '"govmap"' not in src


def test_the_dataset_is_published_not_hidden():
    """Unlike govmap-geocode, which is a work log kept at status='hidden'."""
    src = __import__("inspect").getsource(ad.ensure_dataset)
    assert 'status="active"' in src


@pytest.mark.parametrize("bad", ["govmap-geocode", "over_re_geocode"])
def test_it_is_not_the_geocoding_ledger(bad):
    assert ad.DATASET_SLUG != bad


# ── a refusal must never be silent ────────────────────────────────────────────
class _FakeDS:
    def __init__(self):
        self.scraper_config = {}
        self.last_error = None
        self.import_warning = None
        self.import_warning_at = None


class _FakeDB:
    async def flush(self):
        return None


def test_each_refusal_is_written_down():
    ds, db = _FakeDS(), _FakeDB()
    out = asyncio.run(ad._record_refusal(db, ds, "the points are gone", HEALTHY))
    assert out["published"] is False
    assert out["refusal_streak"] == 1
    assert "the points are gone" in (ds.last_error or "")


def test_a_run_of_refusals_becomes_a_reader_visible_warning():
    """The whole point. A strict floor that refuses quietly freezes the dataset
    the first time the corpus legitimately shrinks, and the freeze is
    indistinguishable from 'nothing changed' — which is how a register can
    publish healthy-looking versions for years while standing still."""
    ds, db = _FakeDS(), _FakeDB()
    for i in range(1, ad.REFUSALS_BEFORE_WARNING + 1):
        out = asyncio.run(ad._record_refusal(db, ds, "a locality was dropped", HEALTHY))
        assert out["refusal_streak"] == i
    assert ds.import_warning, "a run of refusals must raise import_warning"
    assert ds.import_warning_at is not None
    assert "קפוא" in ds.import_warning, "the warning must say the archive is frozen"


def test_the_warning_does_not_fire_on_a_single_refusal():
    """One refusal is an ordinary mid-build state — the merge tick has not run
    yet. Warning on it would cry wolf after every rebuild."""
    ds, db = _FakeDS(), _FakeDB()
    asyncio.run(ad._record_refusal(db, ds, "mid-build", HEALTHY))
    assert ds.import_warning is None


def test_the_streak_is_what_escalates_not_the_total():
    """A refusal followed by a publish followed by a refusal is not a freeze."""
    ds, db = _FakeDS(), _FakeDB()
    asyncio.run(ad._record_refusal(db, ds, "x", HEALTHY))
    ds.scraper_config = {}          # what a successful publish clears
    asyncio.run(ad._record_refusal(db, ds, "x", HEALTHY))
    assert ds.import_warning is None


def test_force_exists_for_a_human_and_is_not_the_default():
    import inspect
    sig = inspect.signature(ad.snapshot)
    assert sig.parameters["force"].default is False
    assert sig.parameters["force"].kind is inspect.Parameter.KEYWORD_ONLY


def test_the_version_note_states_the_parcel_absence_rate():
    """30.4% of rows have a NULL parcel_key. A consumer reading that column as
    'the parcel this address is in' meets the absence one row at a time unless
    the version says so up front — and the two reasons differ: no point at all
    versus a point outside every polygon."""
    import inspect
    src = inspect.getsource(ad.snapshot)
    assert "no_parcel_no_point" in src and "no_parcel_has_point" in src
    assert "with_parcel" in src


def test_both_reasons_for_a_missing_parcel_are_counted():
    import inspect
    src = inspect.getsource(ad.current_counts)
    assert "parcel_key IS NULL AND point IS NULL" in src
    assert "parcel_key IS NULL AND point IS NOT NULL" in src


# ── never handed to the worker fleet ─────────────────────────────────────────
def test_the_dataset_declares_itself_externally_pushed():
    """No worker can produce this: it is built here from our own tables. Without
    push_mode=external every poll queued a scrape task that a worker claimed and
    failed with "no engine for kind='over_internal'" — 20 failures in 40
    seconds on 2026-09-22, filling the admin's recent-failures panel."""
    src = __import__("inspect").getsource(ad.ensure_dataset)
    assert '"push_mode": "external"' in src


def test_the_row_that_already_exists_is_backfilled(monkeypatch):
    """ensure_dataset returned early for an existing row, so declaring the key
    only at creation would never have reached the one row in production."""
    import asyncio
    from types import SimpleNamespace

    row = SimpleNamespace(scraper_config={"kind": "over_internal",
                                          "storage_backend": "r2"})

    class _Result:
        def scalar_one_or_none(self):
            return row

    class _DB:
        flushed = 0

        async def execute(self, *_a, **_k):
            return _Result()

        async def flush(self):
            _DB.flushed += 1

    got = asyncio.run(ad.ensure_dataset(_DB()))
    assert got is row
    assert row.scraper_config["push_mode"] == "external"
    assert row.scraper_config["kind"] == "over_internal", "nothing else changes"
    assert _DB.flushed == 1

    # ...and a second call is a no-op, not a second write.
    asyncio.run(ad.ensure_dataset(_DB()))
    assert _DB.flushed == 1


def test_an_externally_pushed_dataset_is_recognised_by_the_model():
    """The declaration only helps if it is the key the poll loop reads."""
    from app.models.tracked_dataset import TrackedDataset
    ds = TrackedDataset(scraper_config={"kind": "over_internal",
                                        "push_mode": "external"})
    assert ds.is_externally_pushed is True
