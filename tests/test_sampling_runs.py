"""A register can be sampled four ways, and each sample keeps its own history.

The Jerusalem building-licensing register (ykpubdata) is the source that forced
this: ~90k files behind an edge that tolerates ~2 requests a second, so a full
pass is a ten-hour job and almost never the question anyone is asking. The four
questions are "everything", "only what's new", "only the files at status X" and
"this one file" — the same scrape with a different target list.

What is locked here:

  1. a run's parameters live on the TASK and are merged over the dataset's
     config for that run only — a dataset without params polls exactly as it
     did before this existed;
  2. every mode but the full pass marks itself partial, and the shrink guard
     measures against the newest FULL version, so a one-file sample neither
     gets rejected as a 99.99% collapse nor becomes the baseline that lets a
     real collapse through;
  3. ``latest=true`` reads the archive as ITEMS — one row per item, its newest
     sample — and everything else reads it as the sampling history it is;
  4. a status too large to be a "targeted" run is REFUSED with its count rather
     than silently truncated into something mislabelled.

No DB, no network: assertions are over pure functions and the SQL they build.
"""
import asyncio
import os
import sys
import types

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402

import app.api.worker as worker  # noqa: E402
from app.api.append import _RESERVED, item_spec  # noqa: E402
from app.services import append_store, sampling_runs  # noqa: E402

run = asyncio.run

ITEM_KEY = "מספר תיק"
SAMPLE_COL = "תאריך דגימה"
STATUS_COL = "סטטוס"

SAMPLING = {
    "modes": ["all", "new", "status", "item"],
    "item_key": ITEM_KEY,
    "status_column": STATUS_COL,
    "sample_column": SAMPLE_COL,
    "key_separator": "/",
}

COLS = [ITEM_KEY, STATUS_COL, SAMPLE_COL, "first_seen"]


def _ds(source_url=None, source_type="scraper", **cfg):
    """A dataset stub with just the attributes these paths touch."""
    return types.SimpleNamespace(
        id="00000000-0000-0000-0000-000000000001",
        title="עיריית ירושלים — תיקי רישוי ובנייה",
        source_url=source_url,
        source_type=source_type,
        scraper_config=cfg,
    )


def _async(fn):
    async def wrapper(*a, **k):
        return fn(*a, **k)
    return wrapper


# ── 1. run params ride on the task, merged over the dataset config ───────────

def test_task_params_override_dataset_config_for_one_run():
    ds = _ds(corpus="all", max_docs="50", requests_per_second=2.0)
    task = types.SimpleNamespace(params={"run_mode": "new", "max_docs": "500"})

    cfg = worker._poll_scraper_config(ds, task)

    assert cfg["run_mode"] == "new"
    assert cfg["max_docs"] == "500"            # the run wins over stored config
    assert cfg["requests_per_second"] == 2.0   # untouched keys survive
    # And the dataset itself is not mutated by having been polled.
    assert ds.scraper_config["max_docs"] == "50"


def test_a_task_without_params_polls_exactly_as_before():
    ds = _ds(corpus="all")
    assert worker._poll_scraper_config(ds, None) == worker._poll_scraper_config(ds)
    assert worker._poll_scraper_config(ds, types.SimpleNamespace(params=None)) == {
        "corpus": "all", "download_files": False, "max_missing_fraction": 0.25,
    }


# ── 2. what each mode targets ────────────────────────────────────────────────

def test_full_pass_is_the_only_mode_that_is_not_partial():
    ds = _ds(sampling=SAMPLING)
    params, summary = run(sampling_runs.build_params(ds, None, mode="all"))
    assert params == {"run_mode": "all", "run_partial": False}
    assert summary == sampling_runs.MODE_LABELS_HE["all"]


def test_single_item_run_carries_the_identifier_and_is_partial():
    ds = _ds(sampling=SAMPLING)
    params, summary = run(sampling_runs.build_params(
        ds, None, mode="item", item=" 2024/0123.00 "))
    assert params["run_mode"] == "item"
    assert params["run_item"] == "2024/0123.00"
    assert params["run_partial"] is True
    assert "2024/0123.00" in summary


def test_single_item_run_without_an_identifier_is_refused():
    ds = _ds(sampling=SAMPLING)
    with pytest.raises(sampling_runs.SamplingError):
        run(sampling_runs.build_params(ds, None, mode="item", item="  "))


def test_a_dataset_that_declares_no_sampling_cannot_be_targeted():
    ds = _ds(corpus="all")
    assert sampling_runs.sampling_spec(ds) is None
    with pytest.raises(sampling_runs.SamplingError):
        run(sampling_runs.build_params(ds, None, mode="all"))


def test_a_mode_the_source_does_not_declare_is_refused():
    # "new" only means something for a source that can enumerate what it lacks;
    # a source that can't must not be offered the button.
    ds = _ds(sampling={**SAMPLING, "modes": ["all", "item"]})
    assert sampling_runs.available_modes(ds) == ["all", "item"]
    with pytest.raises(sampling_runs.SamplingError):
        run(sampling_runs.build_params(ds, None, mode="new"))


def test_new_mode_carries_the_frontier_of_each_key_series(monkeypatch):
    """"Only what's new" is "start past the highest key you already hold" — a
    tiny instruction, so it rides in the task instead of being fetched."""
    ds = _ds(sampling=SAMPLING)
    monkeypatch.setattr(sampling_runs, "resolve_table",
                        _async(lambda *a, **k: "append_ykpubdata_x"))
    monkeypatch.setattr(append_store, "key_frontier",
                        _async(lambda *a, **k: {"2025": "2025/0512.00",
                                                "2026": "2026/0200.00"}))
    params, _ = run(sampling_runs.build_params(ds, None, mode="new"))
    assert params["run_frontier"] == {"2025": "2025/0512.00", "2026": "2026/0200.00"}
    assert params["run_partial"] is True


def test_status_run_counts_its_targets_and_refuses_an_oversized_one(monkeypatch):
    ds = _ds(sampling=SAMPLING)
    monkeypatch.setattr(sampling_runs, "resolve_table",
                        _async(lambda *a, **k: "append_ykpubdata_x"))

    monkeypatch.setattr(append_store, "latest_item_keys",
                        _async(lambda *a, **k: ([], 320)))
    params, summary = run(sampling_runs.build_params(
        ds, None, mode="status", status="נדונה בוועדת המשנה"))
    assert params["run_status"] == "נדונה בוועדת המשנה"
    assert params["run_target_count"] == 320
    # The target LIST is deliberately absent — the worker pulls it paged.
    assert "run_targets" not in params
    assert "320" in summary

    # Nothing at that status: refused, rather than queueing a run with no work.
    monkeypatch.setattr(append_store, "latest_item_keys",
                        _async(lambda *a, **k: ([], 0)))
    with pytest.raises(sampling_runs.SamplingError):
        run(sampling_runs.build_params(ds, None, mode="status", status="אין כזה"))

    # Over the cap: refused WITH the count. Truncating the target list would
    # produce a run labelled "the files at status X" that covered some of them.
    monkeypatch.setattr(
        append_store, "latest_item_keys",
        _async(lambda *a, **k: ([], sampling_runs.MAX_TARGETS + 1)))
    with pytest.raises(sampling_runs.SamplingError) as e:
        run(sampling_runs.build_params(
            ds, None, mode="status", status="נפתח תיק רישוי"))
    assert str(sampling_runs.MAX_TARGETS) in str(e.value).replace(",", "")


# ── 3. reading the archive as items vs as samples ────────────────────────────

def test_latest_collapses_to_one_row_per_item_newest_sample_first():
    sql = append_store.latest_source(
        "append_t", COLS, key_col=ITEM_KEY, order_col=SAMPLE_COL)
    assert "DISTINCT ON" in sql
    assert f'"{ITEM_KEY}"' in sql and f'"{SAMPLE_COL}" DESC' in sql
    # Aliased back to the table name so every caller's WHERE/ORDER BY/column
    # list keeps working against it unchanged.
    assert sql.endswith('AS "append_t"')


def test_latest_falls_back_to_first_seen_and_to_the_plain_table():
    # No sample column declared → OVER's own arrival time is the ordering.
    sql = append_store.latest_source("append_t", COLS, key_col=ITEM_KEY, order_col=None)
    assert '"first_seen" DESC' in sql
    # A table without the declared key (renamed resource, older table) is served
    # whole rather than erroring: a visible answer beats a broken page.
    assert append_store.latest_source(
        "append_t", ["a", "b"], key_col=ITEM_KEY, order_col=SAMPLE_COL) == '"append_t"'
    assert append_store.latest_source(
        "append_t", COLS, key_col=None, order_col=None) == '"append_t"'


def test_an_existing_dataset_gets_the_spec_from_its_sources_manifest(monkeypatch):
    """A stored scraper_config is a snapshot from creation time. Without this
    fallback, a manifest that learns to declare sampling would apply only to
    datasets created afterwards — and the ones with history worth reading are
    exactly the old ones."""
    from app.services import source_registry

    manifest = source_registry.validate_manifest({
        "manifest_version": 1,
        "id": "toysrc",
        "label_he": "מקור", "label_en": "Source",
        "site_url": "https://toy.example.gov.il/",
        "badge": {"bg": "#fff", "fg": "#000", "accent": "#123456"},
        "default_config": {"sampling": SAMPLING},
        "url_patterns": [{"regex": r"^https?://toy\.example\.gov\.il/.*$"}],
    })
    monkeypatch.setattr(source_registry, "cached_manifests", lambda: [manifest])

    # Stored config predates the sampling block entirely.
    ds = _ds(source_url="https://toy.example.gov.il/x", corpus="all")
    assert sampling_runs.sampling_spec(ds) == SAMPLING

    # A URL no manifest claims stays unsamplable, and so does a CKAN dataset.
    assert sampling_runs.sampling_spec(
        _ds(source_url="https://elsewhere.example.com/x")) is None
    assert sampling_runs.sampling_spec(
        _ds(source_url="https://toy.example.gov.il/x", source_type="ckan")) is None


def test_a_column_name_with_a_space_can_actually_be_sorted_on():
    """These tables are full of Hebrew names that contain spaces, and the sample
    column is one of them. Splitting the sort on whitespace made the first token
    a word rather than a column, so the ORDER BY was dropped and the caller got
    the default order while believing it had asked for another — which is
    exactly what the item history asks for."""
    cols = {ITEM_KEY, SAMPLE_COL, "first_seen"}
    assert append_store._parse_sort(f"{SAMPLE_COL} desc", cols) == \
        f' ORDER BY "{SAMPLE_COL}" DESC'
    assert append_store._parse_sort(f"{SAMPLE_COL}", cols) == \
        f' ORDER BY "{SAMPLE_COL}" ASC'
    assert append_store._parse_sort(f"{ITEM_KEY} asc, {SAMPLE_COL} desc", cols) == \
        f' ORDER BY "{ITEM_KEY}" ASC, "{SAMPLE_COL}" DESC'
    # An unknown column is still skipped — that part was right.
    assert append_store._parse_sort("nope desc", cols) == ""
    assert append_store._parse_sort(None, cols) == ""


def test_item_key_is_discoverable_and_latest_is_a_reserved_param():
    assert item_spec(_ds(sampling=SAMPLING)) == (ITEM_KEY, SAMPLE_COL)
    # Falls back to keys a dataset may already carry for other reasons.
    assert item_spec(_ds(dedup_key="מספר הסכם")) == ("מספר הסכם", None)
    assert item_spec(_ds()) == (None, None)
    # `latest` must never be mistaken for a per-column filter, or a table with a
    # column called "latest" would break the toggle.
    assert "latest" in _RESERVED


# ── 4. the shrink guard and partial versions ─────────────────────────────────

class _V:
    def __init__(self, n, rows, partial=False):
        self.version_number = n
        self.change_summary = {"total_rows": rows}
        if partial:
            self.change_summary["partial_run"] = True


class _FakeDb:
    """Just enough of AsyncSession for _shrink_baseline_version."""

    def __init__(self, versions):
        self._versions = versions

    async def execute(self, _query):
        versions = self._versions

        class R:
            def scalars(self):
                class S:
                    def all(self_inner):
                        return versions
                return S()
        return R()


def test_shrink_baseline_skips_partial_versions():
    """A single-file sample is real history but not a measurement of the corpus.
    Using it as the baseline would let a genuine collapse through unchallenged."""
    versions = [_V(4, 1, partial=True), _V(3, 12, partial=True), _V(2, 50), _V(1, 48)]
    baseline = run(worker._shrink_baseline_version(_FakeDb(versions), "ds"))
    assert baseline.version_number == 2

    # A dataset that has only ever been sampled partially has nothing to
    # measure against, and the guard stays out of the way.
    assert run(worker._shrink_baseline_version(
        _FakeDb([_V(1, 1, partial=True)]), "ds")) is None


# ── 5. taking the target list from another dataset ───────────────────────────

def test_a_run_can_take_its_items_from_a_sibling_dataset(monkeypatch):
    """Two corpora of one source share their items. Jerusalem's documents index
    is one document per row, but the items it must READ are building files —
    the register's grain, already stored there. Without this the documents run
    re-discovers ~100k file numbers from scratch: ~15 hours to rebuild a list
    that exists."""
    from app.services import append_store as store

    register = _ds(source_url="https://toy.example.gov.il/x", sampling=SAMPLING)
    register.id = "11111111-1111-1111-1111-111111111111"
    register.title = "המרשם"
    docs = _ds(source_url="https://toy.example.gov.il/x",
               sampling={**SAMPLING, "modes": ["all", "status"],
                         "item_key": "קישור למסמך"})

    class _DB:
        async def execute(self, _q):
            class R:
                def scalar_one_or_none(self_inner): return register
            return R()

    monkeypatch.setattr(sampling_runs, "resolve_table",
                        _async(lambda *a, **k: "append_register"))
    monkeypatch.setattr(store, "latest_item_keys", _async(lambda *a, **k: ([], 100072)))

    params, summary = run(sampling_runs.build_params(
        docs, _DB(), mode="status", targets_from=register.id))

    # The run is a targeted re-read whose LIST comes from elsewhere.
    assert params["run_targets_dataset"] == register.id
    assert params["run_target_count"] == 100072
    assert params["run_partial"] is True
    assert "100,072" in summary and "המרשם" in summary
    # Still not embedded — the worker pulls it paged, from that dataset.
    assert "run_targets" not in params


def test_the_sibling_cap_is_far_above_the_status_cap():
    """The status cap stops a 'targeted' run that is secretly a full sweep.
    Naming a source dataset IS asking for its whole corpus, so the size is
    stated rather than refused — but still bounded."""
    assert sampling_runs.MAX_TARGETS_FROM_DATASET > sampling_runs.MAX_TARGETS
    assert sampling_runs.MAX_TARGETS_FROM_DATASET >= 100072


# ── 6. the walk is bounded to the LIVE series ────────────────────────────────

def test_the_new_walk_only_looks_at_the_series_a_new_item_can_open_in():
    """"What's new" must mean "since we last looked", not "re-prove that 1974 is
    over". A frontier holds one entry per series ever seen — 90 of them on the
    Jerusalem register — but a key's prefix is the year the file was OPENED and
    the walk probes the ``.00`` a new file starts at, so a new file can only
    appear at the end of the current series. The older ones cost ~25 misses
    each and can return nothing."""
    frontier = {str(y): f"{y}/0100.00" for y in range(1936, 2027)}
    assert len(frontier) == 91

    walked = sampling_runs.recent_series(frontier)
    assert sorted(walked) == ["2025", "2026"]
    assert walked["2026"] == "2026/0100.00"

    # The window is the source's to widen, and 0 means "every series" for a
    # numbering that genuinely never retires.
    assert len(sampling_runs.recent_series(frontier, 5)) == 5
    assert sampling_runs.recent_series(frontier, 0) == frontier
    # A frontier smaller than the window is not padded, and a junk window falls
    # back to the default rather than trimming to nothing.
    assert sampling_runs.recent_series({"2026": "2026/1.00"}, 4) == {"2026": "2026/1.00"}
    assert len(sampling_runs.recent_series(frontier, "לא מספר")) == 2


def test_a_new_run_sends_only_the_live_frontier_and_says_which(monkeypatch):
    """The narrowing has to reach the params — that dict is the whole
    instruction the worker gets — and the summary has to name the series, or a
    run that silently stopped covering a year would read exactly like one that
    covered it."""
    ds = _ds(sampling={**SAMPLING, "new_series_window": 2})
    monkeypatch.setattr(sampling_runs, "resolve_table",
                        _async(lambda *a, **k: "append_ykpubdata_x"))
    monkeypatch.setattr(append_store, "key_frontier", _async(lambda *a, **k: {
        "2024": "2024/0622.00", "2025": "2025/0578.00", "2026": "2026/0372.00",
        "1936": "1936/0004.00",
    }))
    params, summary = run(sampling_runs.build_params(ds, None, mode="new"))
    assert params["run_frontier"] == {"2026": "2026/0372.00", "2025": "2025/0578.00"}
    assert "2026" in summary and "2026/0372.00" in summary
    assert "1936" not in summary


MAVAT_SAMPLING = {
    "modes": ["all", "open", "new", "status", "item"],
    "item_key": "מספר תכנית",
    "status_column": "סטטוס",
    "sample_column": "תאריך דגימה",
    "key_separator": "-",
    "new_series_window": 0,
}


def test_a_mode_missing_from_MODES_is_dropped_from_what_a_source_declared():
    """The failure this guards is silent in both directions. A source declaring
    a mode OVER does not list gets it filtered away by available_modes — no
    button, no label, and build_params refusing it — while the mode itself works
    perfectly if something puts it in a config by hand. That is exactly how the
    מבא"ת weekly run spent its first week: running, and invisible."""
    assert "open" in sampling_runs.MODES
    assert "open" in sampling_runs.MODE_LABELS_HE
    assert "open" in sampling_runs.available_modes(_ds(sampling=MAVAT_SAMPLING))
    # And a mode nobody defined is still refused, however a manifest spells it.
    assert "המצאה" not in sampling_runs.available_modes(
        _ds(sampling={**MAVAT_SAMPLING, "modes": ["all", "המצאה"]}))


def test_an_open_run_carries_a_freshly_computed_frontier(monkeypatch):
    """The status filter runs at the source and can only return what the source
    calls open NOW, so a plan that first appears already approved is invisible
    to it — permanently, since no later run calls it open either. The frontier
    is the other half, and it must be rebuilt per run: one frozen into a
    dataset's config keeps working while only ever finding what was new on the
    day it was written."""
    ds = _ds(sampling=MAVAT_SAMPLING)
    frontier = {"501": "501-1566249", "101": "101-1600436"}
    monkeypatch.setattr(sampling_runs, "resolve_table",
                        _async(lambda *a, **k: "append_mavat_all_x"))
    monkeypatch.setattr(append_store, "key_frontier", _async(lambda *a, **k: frontier))

    params, summary = run(sampling_runs.build_params(ds, None, mode="open"))
    assert params["run_mode"] == "open"
    assert params["run_partial"] is True
    assert params["run_frontier"] == frontier
    assert "2" in summary  # the series COUNT, not 844 series spelled out
    assert "501-1566249" not in summary


def test_a_register_whose_prefix_is_a_place_keeps_every_series():
    """DEFAULT_NEW_SERIES_WINDOW assumes a prefix is a PERIOD, so the newest few
    sort last and the rest have retired. מבא"ת prefixes are planning spaces —
    844 of them, none retiring — and the two that sort highest are legacy
    one-offs. Trimmed to the default this frontier would forget 842 spaces, and
    because is_new calls an unknown prefix new, the walk meant to find ~50 plans
    would hand back the register. The source says 0 to prevent that."""
    frontier = {**{str(500 + i): f"{500 + i}-15000{i:02d}" for i in range(20)},
                "תתל/ 99": "תתל/ 99", "תתל/ 98": "תתל/ 98"}
    assert sampling_runs.recent_series(frontier, 0) == frontier
    # Left at the default, the two survivors are the legacy names — every real
    # planning space gone.
    trimmed = sampling_runs.recent_series(frontier)
    assert sorted(trimmed) == ["תתל/ 98", "תתל/ 99"]


# ── 7. a cadence the SOURCE declares ─────────────────────────────────────────

def test_a_source_declares_its_own_sampling_cadence():
    """The schedule belongs to the source, not to OVER: a manifest asks for it
    and a source that says nothing keeps polling exactly as before."""
    weekly = _ds(sampling={**SAMPLING, "schedule": {"new": 604800}})
    assert sampling_runs.schedule_for(weekly, "new") == 604800

    # Silence is not a default cadence.
    assert sampling_runs.schedule_for(_ds(sampling=SAMPLING), "new") is None
    # A mode this module has no basis to aim on its own is never scheduled,
    # however the manifest spells it.
    assert sampling_runs.schedule_for(
        _ds(sampling={**SAMPLING, "schedule": {"status": 604800}}), "status") is None
    # "open" is schedulable for the same reason "new" is — the run is fully
    # specified without anyone choosing a selector.
    assert "open" in sampling_runs.SCHEDULABLE_MODES
    assert sampling_runs.schedule_for(
        _ds(sampling={**MAVAT_SAMPLING, "schedule": {"open": 604800}}),
        "open") == 604800
    # Junk and zero mean "no", not "every tick".
    for bad in ({"new": 0}, {"new": -1}, {"new": "שבוע"}, [604800]):
        assert sampling_runs.schedule_for(
            _ds(sampling={**SAMPLING, "schedule": bad}), "new") is None


def _toy_manifest(monkeypatch, sampling):
    from app.services import source_registry

    manifest = source_registry.validate_manifest({
        "manifest_version": 1,
        "id": "toysrc",
        "label_he": "מקור", "label_en": "Source",
        "site_url": "https://toy.example.gov.il/",
        "badge": {"bg": "#fff", "fg": "#000", "accent": "#123456"},
        "default_config": {"sampling": sampling},
        "url_patterns": [{"regex": r"^https?://toy\.example\.gov\.il/.*$"}],
    })
    monkeypatch.setattr(source_registry, "cached_manifests", lambda: [manifest])
    return manifest


def test_the_manifest_beats_a_datasets_stored_snapshot(monkeypatch):
    """A stored ``scraper_config`` is a photograph of the manifest taken the day
    the dataset was created, and nothing writes into it afterwards. So the
    manifest is the authority and the stored copy is only a fallback.

    Layering the stored copy on top instead looks conservative and is the
    opposite. It works for a key the snapshot never had — which is how
    ``schedule`` first reached a register older than the feature — and fails for
    a key the snapshot HAS and the manifest has since changed."""
    _toy_manifest(monkeypatch, {**SAMPLING, "schedule": {"new": 604800},
                                "new_series_window": 2})

    # Stored block predates the schedule entirely: the manifest supplies it.
    ds = _ds(source_url="https://toy.example.gov.il/x", sampling=SAMPLING)
    spec = sampling_runs.sampling_spec(ds)
    assert spec["schedule"] == {"new": 604800}
    assert spec["item_key"] == ITEM_KEY

    # A URL no manifest claims falls back to the stored copy, which is the only
    # thing a source without a manifest ever has.
    orphan = _ds(source_url="https://elsewhere.example.com/x", sampling=SAMPLING)
    assert sampling_runs.sampling_spec(orphan)["modes"] == SAMPLING["modes"]


def test_a_stale_stored_mode_list_cannot_hide_a_new_mode(monkeypatch):
    """The regression this exists for, and it failed in production silently.

    ykpubdata's register stored its sampling block on 2026-08-01, when the mode
    list was [all, new, status, item]. The manifest later gained "group" and two
    group cadences. With the stored copy layered on top, its OLD ``modes`` list
    shadowed the new one, ``available_modes`` never saw "group", and
    ``scheduled_runs`` silently discarded BOTH group cadences — while the
    weekly "new" run, which the stale list did contain, kept working. Nothing
    errored. Nothing logged. The two runs simply never started."""
    _toy_manifest(monkeypatch, {
        **SAMPLING,
        "modes": ["all", "new", "group", "status", "item"],
        "groups": {"live": {"label_he": "פעילים", "activity_within_days": 365}},
        "activity_column": "תאריך סטטוס",
        "schedule": {"new": 604800, "group:live": 259200},
    })

    stale = _ds(source_url="https://toy.example.gov.il/x",
                sampling={**SAMPLING, "modes": ["all", "new", "status", "item"]})
    assert "group" in sampling_runs.available_modes(stale)
    assert [r["key"] for r in sampling_runs.scheduled_runs(stale)] == [
        "new", "group:live"]


def test_a_corpus_the_manifest_narrows_stays_narrow(monkeypatch):
    """The manifest winning must not widen a corpus that is deliberately
    narrow. It does not, because the narrowing comes from the manifest's own
    url_pattern — which is the matcher this resolves through — and not from the
    dataset's stored copy."""
    from app.services import source_registry

    manifest = source_registry.validate_manifest({
        "manifest_version": 1,
        "id": "toysrc",
        "label_he": "מקור", "label_en": "Source",
        "site_url": "https://toy.example.gov.il/",
        "badge": {"bg": "#fff", "fg": "#000", "accent": "#123456"},
        "default_config": {"sampling": {**SAMPLING, "modes": ["all", "new", "group"],
                                        "schedule": {"new": 604800}}},
        "url_patterns": [
            {"regex": r"^https?://toy\.example\.gov\.il/docs.*$",
             "page_type": "toy_docs",
             "config": {"sampling": {"modes": ["all"], "item_key": ITEM_KEY}}},
            {"regex": r"^https?://toy\.example\.gov\.il/.*$"},
        ],
    })
    monkeypatch.setattr(source_registry, "cached_manifests", lambda: [manifest])

    wide = _ds(source_url="https://toy.example.gov.il/x", sampling=SAMPLING)
    narrow = _ds(source_url="https://toy.example.gov.il/docs", sampling=SAMPLING)
    assert "group" in sampling_runs.available_modes(wide)
    assert sampling_runs.available_modes(narrow) == ["all"]
    assert sampling_runs.scheduled_runs(narrow) == []


# ── 8. queueing: who may re-aim a task that is already there ─────────────────

class _QueueDb:
    """Enough AsyncSession for queue_run: one optional existing task."""

    def __init__(self, existing=None):
        self.existing = existing
        self.added = []
        self.committed = False

    async def execute(self, _q):
        existing = self.existing

        class R:
            def scalar_one_or_none(self_inner): return existing
        return R()

    def add(self, obj): self.added.append(obj)
    async def commit(self): self.committed = True
    async def rollback(self): pass


def _queue_env(monkeypatch):
    """build_params stubbed to a fixed instruction; the activity log silenced."""
    from app.services import activity_log

    monkeypatch.setattr(sampling_runs, "build_params",
                        _async(lambda *a, **k: ({"run_mode": k.get("mode", "new"),
                                                 "run_partial": True}, "סיכום")))
    monkeypatch.setattr(activity_log, "log_event", _async(lambda **k: None))


def test_a_scheduled_run_never_re_aims_a_task_that_is_already_queued(monkeypatch):
    """At most one active task exists per dataset, so a new one has to either
    re-aim the pending task or yield to it. A scheduled walk must yield: the
    task it would overwrite can be the monthly full pass, and converting a
    48-hour sweep into a 40-minute walk every week would quietly cancel the one
    run that finds what a forward walk cannot reach."""
    _queue_env(monkeypatch)
    ds = _ds(sampling=SAMPLING)
    ds.is_active = True
    pending = types.SimpleNamespace(status="pending", priority=100,
                                    params={}, message="", id="t1")

    db = _QueueDb(pending)
    with pytest.raises(sampling_runs.SamplingBusy):
        run(sampling_runs.queue_run(ds, db, mode="new", reaim=False))
    assert pending.params == {}          # untouched
    assert db.committed is False

    # The admin's click DOES supersede it — that is what clicking meant.
    db = _QueueDb(pending)
    _t, _s, params = run(sampling_runs.queue_run(ds, db, mode="new", reaim=True))
    assert pending.params == params and db.committed is True


def test_a_run_in_flight_is_refused_to_everyone(monkeypatch):
    """Re-aiming a scrape already running would publish its version under a
    label describing rows it never read."""
    _queue_env(monkeypatch)
    ds = _ds(sampling=SAMPLING)
    ds.is_active = True
    running = types.SimpleNamespace(status="running", priority=200,
                                    params={"run_mode": "all"}, message="", id="t2")
    for reaim in (True, False):
        with pytest.raises(sampling_runs.SamplingBusy):
            run(sampling_runs.queue_run(ds, _QueueDb(running), mode="new",
                                        reaim=reaim))


def test_an_empty_queue_gets_a_fresh_task_at_the_callers_band(monkeypatch):
    """A scheduled run is routine work: it must not jump ahead of the manual
    band, or clicking 'דגום' would stop meaning 'now'."""
    from app.models.scrape_task import PRIORITY_MANUAL, PRIORITY_ROUTINE
    _queue_env(monkeypatch)
    ds = _ds(sampling=SAMPLING)
    ds.is_active = True

    db = _QueueDb(None)
    run(sampling_runs.queue_run(ds, db, mode="new", priority=PRIORITY_ROUTINE,
                                actor="scheduler", note="דגימה מתוזמנת"))
    assert len(db.added) == 1
    assert db.added[0].priority == PRIORITY_ROUTINE < PRIORITY_MANUAL
    assert db.added[0].status == "pending"
    assert "דגימה מתוזמנת" in db.added[0].message

    # A paused dataset is not sampled at all, scheduled or otherwise.
    ds.is_active = False
    with pytest.raises(sampling_runs.SamplingError):
        run(sampling_runs.queue_run(ds, _QueueDb(None), mode="new"))


# ── 9. named tracking groups ─────────────────────────────────────────────────
#
# "לפי סטטוס" turned out to be the wrong axis on the register that forced all of
# this. 79,943 of its files are at a non-terminal status, but 61,334 of them last
# moved over a decade ago — re-reading 200 of those against the live site found
# ZERO changes in five days, while 200 recently-moved ones yielded twelve. A
# group is what lets a source say "the ones that actually move" instead.

GROUPS = {
    "publication": {"label_he": "שעוני פרסום", "statuses": ["תום תקופת פרסום", "נוסח פרסום אושר"]},
    "active": {"label_he": "תיקים שזזו בשנה האחרונה", "activity_within_days": 365,
               "exclude_statuses": ["הבקשה נסגרה", "הבקשה נגנזה"]},
}
SAMPLING_G = {**SAMPLING, "modes": ["all", "new", "group", "status", "item"],
              "activity_column": "תאריך סטטוס", "groups": GROUPS,
              "schedule": {"new": 604800, "group:publication": 259200,
                           "group:active": 604800}}


def test_a_group_selects_on_a_set_of_statuses():
    f = sampling_runs.group_filters(_ds(sampling=SAMPLING_G), "publication")
    assert f["value_col"] == STATUS_COL
    assert f["include_values"] == ["תום תקופת פרסום", "נוסח פרסום אושר"]
    assert "activity_since" not in f


def test_a_group_selects_on_WHEN_an_item_last_moved():
    """The axis that separates the live register from the dead one. Resolved to
    an absolute date on every call — a window written once into a dataset's
    config is a fixed date that silently stops moving, and the run keeps
    succeeding while covering an ever-staler slice."""
    from datetime import datetime, timedelta, timezone

    f = sampling_runs.group_filters(_ds(sampling=SAMPLING_G), "active")
    assert f["activity_col"] == "תאריך סטטוס"
    assert f["exclude_values"] == ["הבקשה נסגרה", "הבקשה נגנזה"]
    expected = (datetime.now(timezone.utc) - timedelta(days=365)).date().isoformat()
    assert f["activity_since"] == expected


def test_an_unknown_or_empty_group_is_refused_by_name():
    ds = _ds(sampling=SAMPLING_G)
    with pytest.raises(sampling_runs.SamplingError) as e:
        sampling_runs.group_filters(ds, "לא קיים")
    assert "publication" in str(e.value)      # says what IS declared
    # A group that selects on nothing would silently mean "the whole register".
    empty = _ds(sampling={**SAMPLING_G, "groups": {"g": {"label_he": "ריק"}}})
    with pytest.raises(sampling_runs.SamplingError):
        sampling_runs.group_filters(empty, "g")


def test_an_activity_group_without_a_date_column_is_refused():
    """Silently dropping the window would turn 'the year's movers' into the
    whole corpus — a two-hour run into a two-day one, under the same label."""
    ds = _ds(sampling={**SAMPLING_G, "activity_column": None})
    with pytest.raises(sampling_runs.SamplingError):
        sampling_runs.group_filters(ds, "active")


def test_a_group_run_is_a_named_list_the_ENGINE_calls_status(monkeypatch):
    """The engine's fallback branch is a full-corpus discovery sweep, so a mode
    it does not recognise turns a two-hour run into a two-day one. A group is
    OVER's idea about how the list was CHOSEN; what the scraper gets told is the
    thing it already knows how to do — read a named list."""
    ds = _ds(sampling=SAMPLING_G)
    monkeypatch.setattr(sampling_runs, "resolve_table",
                        _async(lambda *a, **k: "append_ykpubdata_x"))
    monkeypatch.setattr(append_store, "latest_item_keys", _async(lambda *a, **k: ([], 4699)))

    params, summary = run(sampling_runs.build_params(ds, None, mode="group", group="active"))
    assert params["run_mode"] == "status"          # what the engine understands
    assert params["run_group"] == "active"         # what OVER meant
    assert params["run_partial"] is True
    assert params["run_target_count"] == 4699
    assert "4,699" in summary and "שזזו בשנה האחרונה" in summary
    # Still not embedded — the worker pulls it from /keys?group=active.
    assert "run_targets" not in params


def test_a_group_holding_nothing_is_refused_rather_than_run_empty(monkeypatch):
    ds = _ds(sampling=SAMPLING_G)
    monkeypatch.setattr(sampling_runs, "resolve_table",
                        _async(lambda *a, **k: "append_ykpubdata_x"))
    monkeypatch.setattr(append_store, "latest_item_keys", _async(lambda *a, **k: ([], 0)))
    with pytest.raises(sampling_runs.SamplingError):
        run(sampling_runs.build_params(ds, None, mode="group", group="publication"))
    with pytest.raises(sampling_runs.SamplingError):
        run(sampling_runs.build_params(ds, None, mode="group", group=""))


def test_several_cadences_coexist_on_one_dataset():
    """A source can want more than one, and they are not one per mode: this
    register wants its numbering walked weekly, its publication clocks read
    every three days and its year's movers read weekly."""
    runs = sampling_runs.scheduled_runs(_ds(sampling=SAMPLING_G))
    assert {r["key"] for r in runs} == {"new", "group:publication", "group:active"}
    by_key = {r["key"]: r for r in runs}
    assert by_key["group:publication"]["interval"] == 259200
    assert by_key["group:publication"]["mode"] == "group"
    assert by_key["group:publication"]["group"] == "publication"
    assert by_key["new"]["group"] is None
    # A schedule entry for a group that isn't declared, or a mode the source
    # doesn't offer, is ignored rather than queued as something undefined.
    bogus = _ds(sampling={**SAMPLING_G,
                          "schedule": {"group:nope": 3600, "status": 3600, "all": 3600}})
    assert sampling_runs.scheduled_runs(bogus) == []
    # And the plain-mode helper still answers for plain modes only.
    assert sampling_runs.schedule_for(_ds(sampling=SAMPLING_G), "new") == 604800
    assert sampling_runs.schedule_for(_ds(sampling=SAMPLING_G), "group") is None


# ── 10. whitespace drift in a status column ──────────────────────────────────

class _CapturePool:
    """Captures the SQL latest_item_keys builds, without a database."""

    def __init__(self):
        self.queries: list[str] = []
        self.args: list[tuple] = []

    def acquire(self):
        pool = self

        class _Conn:
            async def fetchval(self, q, *a):
                pool.queries.append(q); pool.args.append(a); return 0

            async def fetch(self, q, *a):
                pool.queries.append(q); pool.args.append(a); return []

        class _Ctx:
            async def __aenter__(self_inner): return _Conn()
            async def __aexit__(self_inner, *exc): return False

        return _Ctx()


def test_a_status_filter_survives_the_source_changing_its_whitespace(monkeypatch):
    """The Jerusalem register publishes some statuses with a LEADING SPACE, and
    the source stopped doing so partway through: the first weekly re-read of
    4,690 files rewrote 115 of them without it.

    Matched exactly, the publication group — defined on the spaced spelling —
    dropped from 569 files to 448 overnight, and would have lost more every
    week, because each run trims whatever it touches. It would have decayed to
    matching only the files nobody had re-read yet, while still reporting
    success. Both sides are trimmed so neither spelling can hide a file."""
    pool = _CapturePool()
    monkeypatch.setattr(append_store, "get_pool", _async(lambda: pool))
    monkeypatch.setattr(append_store, "user_columns",
                        _async(lambda t: [ITEM_KEY, STATUS_COL, SAMPLE_COL]))

    run(append_store.latest_item_keys(
        "append_x", key_col=ITEM_KEY, order_col=SAMPLE_COL, value_col=STATUS_COL,
        include_values=[" תחילת הודעות לפני תקנה 36"],
        exclude_values=["הבקשה נסגרה"], limit=1,
    ))
    sql = " ".join(pool.queries)
    # The column is trimmed…
    assert f'btrim("{STATUS_COL}"::text)' in sql
    # …and so is every value it is compared against, so a group declared with
    # the old spelling still matches a row rewritten without it.
    assert sql.count("SELECT btrim(v) FROM unnest(") >= 2
    # The exclusion still keeps a row whose status the source never set.
    assert "COALESCE" in sql

    # A single exact status is trimmed on both sides too — the same drift breaks
    # a "by status" run just as quietly.
    pool.queries.clear()
    run(append_store.latest_item_keys(
        "append_x", key_col=ITEM_KEY, value_col=STATUS_COL,
        value=" תחילת הודעות לפני תקנה 36", limit=1))
    assert "btrim(" in " ".join(pool.queries)
