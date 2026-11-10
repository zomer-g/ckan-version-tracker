from datetime import datetime, timezone

from app.services.removals_log import diff_removals, family

T = datetime(2026, 11, 1, tzinfo=timezone.utc)


def v(n, kind, ids, names=None, partial=False):
    cs = {"type": kind} if kind else {}
    if partial:
        cs["partial_run"] = True
    rm = {"_resource_ids": ids}
    if names:
        rm["_names"] = names
    return {"version_number": n, "detected_at": T, "change_summary": cs, "resource_mappings": rm}


def test_family():
    assert family({}) == "ckan"
    assert family({"type": "append_db"}) == "datastore"
    assert family({"type": "append_db_multi"}) == "datastore"
    assert family({"type": "scraper"}) is None
    assert family({"type": "append_db", "partial_run": True}) is None


def test_removed_file_named_from_previous_version():
    out = diff_removals([
        v(1, "append_db_multi", ["a", "b"], {"a": "A — 1.1.2026", "b": "B"}),
        v(2, "append_db_multi", ["a"], {"a": "A — 1.1.2026"}),
    ])
    assert len(out) == 1
    assert out[0]["resource_id"] == "b" and out[0]["name"] == "B"
    assert out[0]["version_number"] == 2 and out[0]["previous_version"] == 1
    assert out[0]["replaced"] is False


def test_kind_change_is_not_a_removal():
    # The legacy snapshot lists the PDF; the datastore poll never does.
    out = diff_removals([
        v(1, None, ["csv", "pdf"]),
        v(2, "append_db_multi", ["csv"]),
    ])
    assert out == []


def test_compares_within_family_across_other_kinds():
    out = diff_removals([
        v(1, "append_db", ["a", "b"]),
        v(2, "scraper", ["r2:x"]),
        v(3, "append_db_multi", ["a"]),
    ])
    assert [e["resource_id"] for e in out] == ["b"]
    assert out[0]["previous_version"] == 1


def test_partial_run_neither_reports_nor_becomes_the_baseline():
    out = diff_removals([
        v(1, "append_db", ["a", "b"]),
        v(2, "append_db", ["a"], partial=True),
        v(3, "append_db", ["a", "b"]),
    ])
    assert out == []


def test_same_name_new_id_is_a_replacement():
    out = diff_removals([
        v(1, "append_db_multi", ["old"], {"old": "סניפים — 13.07.2026"}),
        v(2, "append_db_multi", ["new"], {"new": "סניפים — 20.07.2026"}),
    ])
    assert len(out) == 1 and out[0]["replaced"] is True


def test_empty_id_list_is_skipped():
    out = diff_removals([
        v(1, "append_db", ["a"]),
        v(2, "append_db", []),
        v(3, "append_db", ["a"]),
    ])
    assert out == []
