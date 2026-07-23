"""Stage-1 sync engine for the index → NEON mirror
(docs/neon-index-pilot/README.md §10.3).
"""
import asyncio

from app.services import append_store, index_mirror


class _DS:
    def __init__(self, source_type="govmap", status="active", ckan_name="x",
                 id="304e43d5-c419-43bd-8b46-f31a4da0c075", title="T"):
        self.source_type, self.status = source_type, status
        self.ckan_name, self.id, self.title = ckan_name, id, title


# ── eligibility ──────────────────────────────────────────────────────────────

def test_scraper_and_govmap_are_eligible():
    assert index_mirror.dataset_is_index_mirror_eligible(_DS(source_type="govmap"))
    assert index_mirror.dataset_is_index_mirror_eligible(_DS(source_type="scraper"))


def test_ckan_is_not_eligible():
    """CKAN datasets already stream their rows into public.append_* via
    archive_neon — mirroring them again would duplicate the data."""
    assert not index_mirror.dataset_is_index_mirror_eligible(_DS(source_type="ckan"))
    assert not index_mirror.dataset_is_index_mirror_eligible(_DS(source_type="cbs"))


def test_inactive_datasets_are_not_eligible():
    assert not index_mirror.dataset_is_index_mirror_eligible(_DS(status="paused"))
    assert not index_mirror.dataset_is_index_mirror_eligible(_DS(status="deleted"))


# ── the index CSV is found only when it is a real stored object ──────────────

def test_index_csv_value_requires_a_storage_marked_value():
    assert index_mirror.index_csv_value(
        {"נתוני הסורק": "r2:datasets/a/v1/x_csv"}) == "r2:datasets/a/v1/x_csv"
    # An ODATA resource id (not an r2: value) is not a mirrorable object.
    assert index_mirror.index_csv_value({"נתוני הסורק": "abc-123-not-r2"}) is None
    assert index_mirror.index_csv_value({"_zip": "r2:x"}) is None
    assert index_mirror.index_csv_value({}) is None
    assert index_mirror.index_csv_value(None) is None


# ── the read-only role wiring (without it /data cannot see idx at all) ───────

def test_readonly_role_is_parsed_from_the_url(monkeypatch):
    monkeypatch.setattr(append_store.settings, "append_readonly_database_url",
                        "postgresql://over_readonly:pw@ep-x.aws.neon.tech/neondb")
    assert index_mirror._readonly_role() == "over_readonly"


def test_readonly_role_is_none_when_unset(monkeypatch):
    monkeypatch.setattr(append_store.settings, "append_readonly_database_url", "")
    assert index_mirror._readonly_role() is None


def test_ensure_schema_grants_to_the_console_role(monkeypatch):
    """The schema is created at runtime, so create_append_readonly_role.sql
    cannot have covered it — the GRANTs have to happen here or the console sees
    no idx tables."""
    executed = []

    class _Conn:
        async def execute(self, sql, *a):
            executed.append(sql)

    monkeypatch.setattr(append_store.settings, "append_readonly_database_url",
                        "postgresql://over_readonly:pw@h/db")
    asyncio.run(index_mirror.ensure_schema(_Conn()))
    joined = " | ".join(executed)
    assert 'CREATE SCHEMA IF NOT EXISTS "idx"' in joined
    assert 'GRANT USAGE ON SCHEMA "idx" TO "over_readonly"' in joined
    assert 'GRANT SELECT ON ALL TABLES IN SCHEMA "idx"' in joined
    assert "ALTER DEFAULT PRIVILEGES" in joined


def test_ensure_schema_survives_a_failing_grant(monkeypatch):
    """A missing/renamed role must not break the sync itself."""
    class _Conn:
        def __init__(self):
            self.n = 0

        async def execute(self, sql, *a):
            self.n += 1
            if "GRANT" in sql:
                raise RuntimeError("role does not exist")

    monkeypatch.setattr(append_store.settings, "append_readonly_database_url",
                        "postgresql://ghost:pw@h/db")
    asyncio.run(index_mirror.ensure_schema(_Conn()))  # must not raise


# ── PostGIS geometry column (docs/neon-postgis/README.md) ────────────────────

class _GeomConn:
    """Records SQL and fakes the one value _add_geometry reads (the WKT sample).

    ``fail_on`` makes a statement raise, so the savepoint path can be exercised.
    """

    def __init__(self, sample="POLYGON((34.78 32.08, 34.79 32.09))", fail_on=None):
        self.executed: list[str] = []
        self.sample, self.fail_on = sample, fail_on

    async def fetchval(self, sql, *a):
        self.executed.append(sql)
        return self.sample

    async def execute(self, sql, *a):
        self.executed.append(sql)
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("boom")
        return "UPDATE 1234"

    def transaction(self):
        conn = self

        class _Tx:
            async def __aenter__(self_inner):
                conn.executed.append("-- SAVEPOINT")
                return self_inner

            async def __aexit__(self_inner, *exc):
                return False
        return _Tx()


def _enable_postgis(monkeypatch, on=True):
    monkeypatch.setattr(index_mirror.settings, "index_mirror_postgis_enabled", on)


def test_classify_wkt_crs_separates_degrees_from_itm():
    """The two ranges are three orders of magnitude apart, which is the whole
    reason a first-coordinate sniff is safe enough to gate on."""
    assert index_mirror.classify_wkt_crs("POINT(34.78 32.08)") == "degrees"
    assert index_mirror.classify_wkt_crs(
        "MULTIPOLYGON(((245134.2 698829.0, 245135.0 698830.0)))") == "itm"
    assert index_mirror.classify_wkt_crs("POINT(-118.24 34.05)") == "unknown"
    assert index_mirror.classify_wkt_crs("") == "unknown"
    assert index_mirror.classify_wkt_crs(None) == "unknown"
    assert index_mirror.classify_wkt_crs("GEOMETRYCOLLECTION EMPTY") == "unknown"


def test_geometry_step_is_a_no_op_while_the_flag_is_off(monkeypatch):
    """A deploy with the flag still false must behave exactly as before —
    no sample read, no DDL."""
    _enable_postgis(monkeypatch, False)
    conn = _GeomConn()
    got = asyncio.run(index_mirror._add_geometry(conn, "t__stg", ["geometry_wkt"]))
    assert got == {"skipped": "postgis disabled"}
    assert conn.executed == []


def test_geometry_step_skips_tables_without_a_wkt_column(monkeypatch):
    _enable_postgis(monkeypatch)
    conn = _GeomConn()
    got = asyncio.run(index_mirror._add_geometry(conn, "t__stg", ["a", "b"]))
    assert got == {"skipped": "no geometry column"}
    assert conn.executed == []


def test_geometry_step_builds_the_column_and_a_gist_index(monkeypatch):
    _enable_postgis(monkeypatch)
    conn = _GeomConn()
    got = asyncio.run(index_mirror._add_geometry(conn, "t__stg", ["geometry_wkt"]))
    assert got == {"rows": 1234}          # parsed from the "UPDATE 1234" tag
    joined = " | ".join(conn.executed)
    assert 'ADD COLUMN "geom" "extensions".geometry(Geometry, 4326)' in joined
    assert '"extensions".ST_GeomFromText' in joined
    assert "USING GIST" in joined


def test_geometry_step_qualifies_every_postgis_reference(monkeypatch):
    """The worker's connection carries NO search_path, so a bare `geometry` or
    `ST_GeomFromText` raises 42704. This is the regression guard for that —
    it was caught in the pilot, not in production."""
    _enable_postgis(monkeypatch)
    conn = _GeomConn()
    asyncio.run(index_mirror._add_geometry(conn, "t__stg", ["geometry_wkt"]))
    for sql in conn.executed:
        if "geometry(" in sql or "ST_GeomFromText" in sql:
            assert '"extensions".' in sql, f"unqualified PostGIS use: {sql}"


def test_geometry_step_refuses_itm_wkt_instead_of_converting_it(monkeypatch):
    """Converting ITM metres as 4326 yields geometry that is wrong but looks
    valid — the worst possible failure. Skip and say why."""
    _enable_postgis(monkeypatch)
    conn = _GeomConn(sample="MULTIPOLYGON(((245134.2 698829.0, 1 2)))")
    got = asyncio.run(index_mirror._add_geometry(conn, "t__stg", ["geometry_wkt"]))
    assert "itm" in got["skipped"]
    assert not any("ADD COLUMN" in s for s in conn.executed)


def test_geometry_step_skips_a_table_with_no_geometry_rows(monkeypatch):
    _enable_postgis(monkeypatch)
    conn = _GeomConn(sample=None)
    got = asyncio.run(index_mirror._add_geometry(conn, "t__stg", ["geometry_wkt"]))
    assert got == {"skipped": "no geometry rows"}
    assert not any("ADD COLUMN" in s for s in conn.executed)


def test_geometry_failure_is_reported_not_raised(monkeypatch):
    """Geometry is an enhancement: a layer whose WKT will not convert must still
    get its content refreshed, so the caller can swap in a geom-less table."""
    _enable_postgis(monkeypatch)
    conn = _GeomConn(fail_on="ST_GeomFromText")
    got = asyncio.run(index_mirror._add_geometry(conn, "t__stg", ["geometry_wkt"]))
    assert "RuntimeError" in got["error"]
    assert "rows" not in got


def test_backfill_is_a_no_op_while_the_flag_is_off(monkeypatch):
    _enable_postgis(monkeypatch, False)
    got = asyncio.run(index_mirror.backfill_geometry(limit=5))
    assert got == {"skipped": "postgis disabled"}


def test_backfill_uses_the_final_index_name_not_a_staging_one(monkeypatch):
    """The backfill converts a LIVE table, so its index must be born with the
    name the table will keep — there is no swap afterwards to rename it."""
    _enable_postgis(monkeypatch)
    conn = _GeomConn()
    asyncio.run(index_mirror._add_geometry(conn, "govmap_9_abc", ["geometry_wkt"]))
    created = [s for s in conn.executed if "CREATE INDEX" in s]
    assert created and "govmap_9_abc_geom_gix" in created[0]
    assert "__stg" not in created[0]


def test_geom_index_name_stays_inside_the_identifier_budget():
    long_table = "govmap_" + "א" * 40          # Hebrew: 2 bytes per char
    name = index_mirror._geom_index_name(long_table)
    assert len(name.encode("utf-8")) <= 63
    assert name.endswith("_geom_gix")


# ── the version-landed trigger ───────────────────────────────────────────────

def _fake_pending_env(monkeypatch, *, mirrored: dict):
    monkeypatch.setattr(index_mirror, "loaded_versions",
                        lambda: _coro(mirrored))


def _coro(v):
    async def _f(*a, **k):
        return v
    return _f()


def test_sync_one_records_failure_and_does_not_raise(monkeypatch):
    """A dataset whose CSV is unreachable must be recorded (so it is retried)
    and must not abort the rest of the chunk."""
    recorded = {}

    async def fake_load(value, table):
        raise RuntimeError("object missing")

    async def fake_record(dsid, table, vnum, rows, error, **kw):
        recorded.update(dataset_id=dsid, table=table, version=vnum,
                        rows=rows, error=error)

    monkeypatch.setattr(index_mirror, "load_index_csv", fake_load)
    monkeypatch.setattr(index_mirror, "_record", fake_record)

    item = {"dataset_id": "d1", "title": "T", "table": "t",
            "version_number": 4, "r2_value": "r2:k"}
    out = asyncio.run(index_mirror.sync_one(item))
    assert out["ok"] is False and "object missing" in out["error"]
    assert recorded["error"] and recorded["rows"] is None


def test_sync_one_records_success(monkeypatch):
    recorded = {}

    async def fake_load(value, table):
        return {"table": table, "rows": 42, "columns": 3}

    async def fake_record(dsid, table, vnum, rows, error, **kw):
        recorded.update(rows=rows, error=error, version=vnum)

    monkeypatch.setattr(index_mirror, "load_index_csv", fake_load)
    monkeypatch.setattr(index_mirror, "_record", fake_record)

    out = asyncio.run(index_mirror.sync_one(
        {"dataset_id": "d1", "title": "T", "table": "t",
         "version_number": 7, "r2_value": "r2:k"}))
    assert out["ok"] and out["rows"] == 42
    assert recorded == {"rows": 42, "error": None, "version": 7}


def test_sync_due_is_a_noop_without_the_append_db(monkeypatch):
    monkeypatch.setattr(append_store.settings, "append_database_url", "")
    out = asyncio.run(index_mirror.sync_due(db=None))
    assert out == {"skipped": "append DB not configured"}


def test_sync_due_invalidates_the_catalog_cache(monkeypatch):
    """A swapped-in table changes the queryable table list, so a stale /data
    catalog would hide the freshly mirrored dataset for up to the TTL."""
    from app.services import data_catalog

    monkeypatch.setattr(append_store.settings, "append_database_url", "postgresql://x/y")

    async def fake_pending(db, limit=None, dataset_id=None):
        return [{"dataset_id": "d1", "title": "T", "table": "t",
                 "version_number": 1, "r2_value": "r2:k"}]

    async def fake_sync_one(item, **kw):
        return {**item, "ok": True, "rows": 5, "columns": 2}

    monkeypatch.setattr(index_mirror, "pending", fake_pending)
    monkeypatch.setattr(index_mirror, "sync_one", fake_sync_one)

    calls = {"n": 0}
    monkeypatch.setattr(data_catalog, "invalidate_catalog_cache",
                        lambda: calls.__setitem__("n", calls["n"] + 1))

    out = asyncio.run(index_mirror.sync_due(db=None, limit=5))
    assert out["synced"] == 1 and out["failed"] == 0 and out["rows"] == 5
    assert calls["n"] == 1


def test_sync_due_reports_failures_without_invalidating(monkeypatch):
    from app.services import data_catalog

    monkeypatch.setattr(append_store.settings, "append_database_url", "postgresql://x/y")

    async def fake_pending(db, limit=None, dataset_id=None):
        return [{"dataset_id": "d1", "title": "T", "table": "t",
                 "version_number": 1, "r2_value": "r2:k"}]

    async def fake_sync_one(item, **kw):
        return {**item, "ok": False, "error": "boom"}

    monkeypatch.setattr(index_mirror, "pending", fake_pending)
    monkeypatch.setattr(index_mirror, "sync_one", fake_sync_one)
    calls = {"n": 0}
    monkeypatch.setattr(data_catalog, "invalidate_catalog_cache",
                        lambda: calls.__setitem__("n", calls["n"] + 1))

    out = asyncio.run(index_mirror.sync_due(db=None, limit=5))
    assert out["failed"] == 1 and out["synced"] == 0
    assert calls["n"] == 0


def test_results_never_leak_the_storage_key(monkeypatch):
    """The summary is returned to an admin endpoint; the r2 key is internal."""
    monkeypatch.setattr(append_store.settings, "append_database_url", "postgresql://x/y")

    async def fake_pending(db, limit=None, dataset_id=None):
        return [{"dataset_id": "d1", "title": "T", "table": "t",
                 "version_number": 1, "r2_value": "r2:secret/key"}]

    async def fake_sync_one(item, **kw):
        return {**item, "ok": True, "rows": 1, "columns": 1}

    monkeypatch.setattr(index_mirror, "pending", fake_pending)
    monkeypatch.setattr(index_mirror, "sync_one", fake_sync_one)
    from app.services import data_catalog
    monkeypatch.setattr(data_catalog, "invalidate_catalog_cache", lambda: None)

    out = asyncio.run(index_mirror.sync_due(db=None, limit=5))
    assert all("r2_value" not in r for r in out["results"])


# ── the /data console must reach the new schema ──────────────────────────────

def test_console_search_path_includes_idx():
    from app.services.data_catalog import CONSOLE_SEARCH_PATH
    assert "idx" in [s.strip() for s in CONSOLE_SEARCH_PATH.split(",")]
    # and the guard still accepts it
    assert append_store._safe_search_path(CONSOLE_SEARCH_PATH)


# ── geometry is listed but never previewed (the 46-second finding) ───────────

def test_bulk_geometry_columns_are_recognised():
    assert "geometry_wkt" in append_store._BULK_COLS
    for c in ("geometry", "geom", "wkt"):
        assert c in append_store._BULK_COLS


def test_geometry_is_typed_from_udt_name_not_from_user_defined():
    """information_schema reports USER-DEFINED for EVERY custom type, so
    data_type alone cannot tell geometry from an enum. The /data list puts a map
    marker on any column typed "geometry", so guessing here would mark tables
    that hold no geometry at all."""
    t = append_store._ckan_type
    assert t("USER-DEFINED", "geometry") == "geometry"
    assert t("USER-DEFINED", "geography") == "geometry"
    assert t("geometry") == "geometry"          # asyncpg reports the name directly
    # An enum must NOT be mistaken for geometry, and an unknown user type with no
    # udt_name falls back to text rather than claiming to be spatial.
    assert t("USER-DEFINED", "my_enum") == "text"
    assert t("USER-DEFINED") == "text"


def test_sample_rows_reports_skipped_geometry_instead_of_hiding_it(monkeypatch):
    """Not previewing geometry is correct (46 seconds); dropping it from the
    response entirely is not. Until 2026-07-23 the /data cube did exactly that,
    so a GovMap layer looked like it had no spatial column at all."""
    class _Attr:
        def __init__(self, name): self.name = name

    class _Prepared:
        def get_attributes(self):
            return [_Attr("objectId"), _Attr("שם האתר"),
                    _Attr("geometry_wkt"), _Attr("geom")]

    class _Conn:
        async def execute(self, *a, **k): return "SET"

        async def prepare(self, sql): return _Prepared()

        async def fetch(self, sql, *a):
            assert "geom" not in sql, "geometry must not be SELECTed for a preview"
            return []

        def transaction(self, **k):
            class _Tx:
                async def __aenter__(s): return s
                async def __aexit__(s, *e): return False
            return _Tx()

    class _Pool:
        def acquire(self):
            class _Acq:
                async def __aenter__(s): return _Conn()
                async def __aexit__(s, *e): return False
            return _Acq()

    async def fake_ro_pool(): return _Pool()

    monkeypatch.setattr(append_store, "get_readonly_pool", fake_ro_pool)
    out = asyncio.run(append_store.sample_rows("govmap_1_a_b", schema="idx"))

    assert out["columns"] == ["objectId", "שם האתר"]
    assert set(out["omitted_columns"]) == {"geometry_wkt", "geom"}


# ── the size gate and the crash-loop guard (§10.9) ───────────────────────────

def test_oversized_csv_is_deferred_before_any_download(monkeypatch):
    """The gate must fire on a HEAD — downloading first is exactly what took the
    dyno down."""
    from app.services import storage_client as sc
    downloaded = {"n": 0}
    recorded = {}

    async def fake_size(v):
        return 400 * 2**20

    async def fake_load(v, t):
        downloaded["n"] += 1
        return {"rows": 1, "columns": 1}

    async def fake_record(dsid, table, vnum, rows, error, **kw):
        recorded.update(error=error, **kw)

    monkeypatch.setattr(sc.storage_client, "object_size", fake_size)
    monkeypatch.setattr(index_mirror, "load_index_csv", fake_load)
    monkeypatch.setattr(index_mirror, "_record", fake_record)

    out = asyncio.run(index_mirror.sync_one(
        {"dataset_id": "d", "title": "big", "table": "t",
         "version_number": 1, "r2_value": "r2:k"},
        max_bytes=25 * 2**20))
    assert out["ok"] is False and "cap" in out["deferred"]
    assert downloaded["n"] == 0, "must not download an oversized CSV"
    assert recorded["deferred"] and recorded["csv_bytes"] == 400 * 2**20


def test_unknown_size_is_treated_as_too_big(monkeypatch):
    from app.services import storage_client as sc

    async def fake_size(v):
        raise RuntimeError("HEAD failed")

    async def fake_record(dsid, table, vnum, rows, error, **kw):
        pass

    monkeypatch.setattr(sc.storage_client, "object_size", fake_size)
    monkeypatch.setattr(index_mirror, "_record", fake_record)
    out = asyncio.run(index_mirror.sync_one(
        {"dataset_id": "d", "title": "x", "table": "t",
         "version_number": 1, "r2_value": "r2:k"}, max_bytes=25 * 2**20))
    assert out["ok"] is False and "unknown" in out["deferred"]


def test_within_the_cap_loads_normally(monkeypatch):
    from app.services import storage_client as sc

    async def fake_size(v):
        return 5 * 2**20

    async def fake_load(v, t):
        return {"rows": 7, "columns": 2}

    async def fake_record(dsid, table, vnum, rows, error, **kw):
        pass

    monkeypatch.setattr(sc.storage_client, "object_size", fake_size)
    monkeypatch.setattr(index_mirror, "load_index_csv", fake_load)
    monkeypatch.setattr(index_mirror, "_record", fake_record)
    out = asyncio.run(index_mirror.sync_one(
        {"dataset_id": "d", "title": "x", "table": "t",
         "version_number": 1, "r2_value": "r2:k"}, max_bytes=25 * 2**20))
    assert out["ok"] and out["rows"] == 7


def test_attempt_is_claimed_before_the_load(monkeypatch):
    """An OOM kills the process before any result can be written, so the attempt
    counter has to be persisted BEFORE the load — otherwise the same dataset is
    picked again every tick and the crash loop never ends."""
    order = []

    async def fake_record(dsid, table, vnum, rows, error, **kw):
        order.append(("record", error, kw.get("bump_attempt")))

    async def fake_load(v, t):
        order.append(("load", None, None))
        return {"rows": 1, "columns": 1}

    monkeypatch.setattr(index_mirror, "_record", fake_record)
    monkeypatch.setattr(index_mirror, "load_index_csv", fake_load)
    asyncio.run(index_mirror.sync_one(
        {"dataset_id": "d", "title": "x", "table": "t",
         "version_number": 1, "r2_value": "r2:k"}))
    assert order[0][0] == "record" and order[0][2] is True, "attempt not claimed first"
    assert order[1][0] == "load"


def test_max_attempts_is_small_enough_to_bound_a_crash_loop():
    assert 1 <= index_mirror.MAX_ATTEMPTS <= 5


# ── kinds whose index duplicates a better copy elsewhere ─────────────────────

class _KDS(_DS):
    def __init__(self, kind=None, **kw):
        super().__init__(**kw)
        self.kind = kind


def test_knesset_committee_protocols_are_excluded():
    """Their index is protocol metadata, which the `knesset` schema already
    holds in 48 ODATA tables synced from the Knesset's own API — a richer and
    fresher copy. Mirroring it again would put two versions of the same facts in
    /data."""
    assert not index_mirror.dataset_is_index_mirror_eligible(
        _KDS(kind="knesset", source_type="scraper"))


def test_knesset_mmm_stays_eligible():
    """MMM is a separate source (research papers), not part of the ODATA feed."""
    assert index_mirror.dataset_is_index_mirror_eligible(
        _KDS(kind="knesset_mmm", source_type="scraper"))


def test_other_kinds_and_missing_kind_stay_eligible():
    assert index_mirror.dataset_is_index_mirror_eligible(
        _KDS(kind="govmap", source_type="govmap"))
    assert index_mirror.dataset_is_index_mirror_eligible(
        _KDS(kind=None, source_type="scraper"))
    assert index_mirror.dataset_is_index_mirror_eligible(
        _KDS(kind="mevaker", source_type="scraper"))
