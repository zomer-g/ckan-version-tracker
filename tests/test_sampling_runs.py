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


def _ds(**cfg):
    """A dataset stub with just the attributes these paths touch."""
    return types.SimpleNamespace(
        id="00000000-0000-0000-0000-000000000001",
        title="עיריית ירושלים — תיקי רישוי ובנייה",
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
