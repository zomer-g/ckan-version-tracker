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
    ``sample`` doubles as the CRS sniff's input, and ``columns`` as the live
    column list _ensure_degrees introspects — which is how the ITM→WGS84
    conversion is exercised without a database.
    """

    def __init__(self, sample="POLYGON((34.78 32.08, 34.79 32.09))", fail_on=None,
                 bad_rows=0, columns=("geometry_wkt",)):
        self.executed: list[str] = []
        self.sample, self.fail_on = sample, fail_on
        self.bad_rows = bad_rows
        self.columns = list(columns)

    async def fetch(self, sql, *a):
        self.executed.append(sql)
        if "information_schema.columns" in sql:
            return [{"column_name": c} for c in self.columns]
        return []

    async def fetchval(self, sql, *a):
        self.executed.append(sql)
        # The second fetchval in _add_geometry counts unparseable rows.
        if "IS NULL" in sql and "count(*)" in sql:
            return self.bad_rows
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


def _enable_postgis(monkeypatch, on=True, installed=True, srid=True):
    """Set the WANT (the setting) and the CAN (what the database has).

    The two are deliberately separate in the module — the setting says we want
    geometry, the probes say the database can give it — and the probe results
    are process-cached, so every test states both explicitly rather than
    inheriting whatever the previous test happened to leave behind."""
    monkeypatch.setattr(index_mirror.settings, "index_mirror_postgis_enabled", on)
    monkeypatch.setattr(index_mirror, "_postgis_present", installed)
    monkeypatch.setattr(index_mirror, "_srid_present",
                        {index_mirror.ITM_SRID: srid})


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
    assert '"idx"."try_geom"' in joined    # row-tolerant, not bare ST_GeomFromText
    assert "USING GIST" in joined


def test_one_unparseable_row_does_not_cost_the_layer_its_geometry(monkeypatch):
    """Measured in production: GovMap emitted rings of three points whose first
    and last are identical — a line wearing a polygon's name. ST_GeomFromText
    aborts the whole statement on the first such value, which cost a
    12,309-feature layer ALL of its geometry. Bad rows become NULL and are
    counted; the rest convert."""
    _enable_postgis(monkeypatch)
    conn = _GeomConn(bad_rows=3)
    got = asyncio.run(index_mirror._add_geometry(conn, "t__stg", ["geometry_wkt"]))
    assert got["rows"] == 1231                    # 1234 attempted − 3 refused
    assert "3 of 1234" in got["skipped"]          # the gap is reported, not silent
    assert any("try_geom" in s for s in conn.executed), \
        "must use the row-tolerant parser, not bare ST_GeomFromText"


def test_a_clean_layer_reports_no_skipped_note(monkeypatch):
    _enable_postgis(monkeypatch)
    got = asyncio.run(index_mirror._add_geometry(_GeomConn(), "t__stg", ["geometry_wkt"]))
    assert got == {"rows": 1234}


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


_ITM_SAMPLE = "MULTIPOLYGON(((245134.2 698829.0, 1 2)))"


def test_itm_wkt_is_reprojected_so_the_layer_still_gets_geometry(monkeypatch):
    """A layer last scraped before the 2026-07-08 WGS84 switch holds ITM metres
    in the same column a newer layer holds degrees in. Refusing it (the old
    behaviour) left the layer without geometry until a re-scrape — up to 90 days
    for GovMap, and meanwhile the console's map drew it in the Gulf of Guinea.
    Convert it instead, via a real ST_Transform from 6991."""
    _enable_postgis(monkeypatch)
    conn = _GeomConn(sample=_ITM_SAMPLE)
    got = asyncio.run(index_mirror._add_geometry(conn, "t__stg", ["geometry_wkt"]))
    assert got["rows"] == 1234
    joined = " | ".join(conn.executed)
    assert "ST_Transform" in joined and "6991" in joined
    assert any("ADD COLUMN" in s for s in conn.executed)


def test_reprojection_never_relabels_itm_as_degrees(monkeypatch):
    """The failure this whole path exists to avoid: declaring ITM metres to be
    EPSG:4326 produces geometry that is wrong but perfectly valid-looking. The
    conversion must PARSE as 6991 and transform — never parse as 4326."""
    _enable_postgis(monkeypatch)
    conn = _GeomConn(sample=_ITM_SAMPLE)
    asyncio.run(index_mirror._add_geometry(conn, "t__stg", ["geometry_wkt"]))
    convert = [s for s in conn.executed if "ST_Transform" in s]
    assert convert, "no conversion statement was issued"
    for sql in convert:
        # The WKT is parsed AS 6991 and transformed TO 4326, in that order.
        assert f"try_geom" in sql
        assert f", {index_mirror.ITM_SRID})" in sql
        assert f", {index_mirror.GEOM_SRID})" in sql
        assert sql.index(f", {index_mirror.ITM_SRID})") < sql.index(
            f", {index_mirror.GEOM_SRID})")


def test_reprojection_rehashes_so_the_next_sync_does_not_double_the_table(monkeypatch):
    """geometry_wkt is one of the columns _row_hash is taken over. Rewriting it
    without recomputing the hash would make every row of the next sync read as
    new — a silently doubled table, half in each frame."""
    _enable_postgis(monkeypatch)
    conn = _GeomConn(sample=_ITM_SAMPLE,
                     columns=("geometry_wkt", "_row_hash"))
    asyncio.run(index_mirror._ensure_degrees(conn, "t"))
    assert any('SET "_row_hash"' in s for s in conn.executed), \
        "converted the geometry but left the row hashes describing the old text"


def test_reprojection_is_skipped_when_the_database_lacks_epsg_6991(monkeypatch):
    """ST_Transform against an SRID that is not in spatial_ref_sys aborts the
    statement. Better to stay text-only and say so."""
    _enable_postgis(monkeypatch, srid=False)
    conn = _GeomConn(sample=_ITM_SAMPLE)
    got = asyncio.run(index_mirror._add_geometry(conn, "t__stg", ["geometry_wkt"]))
    assert "6991" in got["skipped"]
    assert not any("ADD COLUMN" in s for s in conn.executed)


def test_geometry_step_is_a_recorded_no_op_without_the_extension(monkeypatch):
    """The setting says we WANT geometry; the database says whether it CAN. With
    the setting on and PostGIS absent, the content still mirrors and the reason
    is recorded — no error, no per-tick exception."""
    _enable_postgis(monkeypatch, installed=False)
    conn = _GeomConn()
    got = asyncio.run(index_mirror._add_geometry(conn, "t__stg", ["geometry_wkt"]))
    assert got == {"skipped": "postgis not installed on the append DB"}
    assert not any("ADD COLUMN" in s for s in conn.executed)


def test_geometry_step_skips_a_table_with_no_geometry_rows(monkeypatch):
    _enable_postgis(monkeypatch)
    conn = _GeomConn(sample=None)
    got = asyncio.run(index_mirror._add_geometry(conn, "t__stg", ["geometry_wkt"]))
    assert got == {"skipped": "no geometry rows"}
    assert not any("ADD COLUMN" in s for s in conn.executed)


def test_geometry_failure_is_reported_not_raised(monkeypatch):
    """Geometry is an enhancement: a layer whose geometry step blows up must
    still get its content refreshed, so the caller can swap in a geom-less
    table. (Individual unparseable ROWS no longer reach here — try_geom NULLs
    them; this is the path for a failure of the step itself, e.g. the index.)"""
    _enable_postgis(monkeypatch)
    conn = _GeomConn(fail_on="CREATE INDEX")
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


# ── incremental append: a refresh costs the change, not the table ────────────
#
# The replace-every-sync loader rewrote the whole relation to record a handful of
# new rows — הסדרים מותנים משטרה is a 244MB CSV that gains ~45 rows a week. These
# pin the rule that decides rebuild-vs-append and the shape of each path.

def _live(*cols):
    return list(cols)


def test_a_table_that_does_not_exist_yet_is_rebuilt():
    assert not index_mirror._can_append(None, ["a", "b"])


def test_a_table_without_the_hash_column_is_rebuilt():
    """Mirrored before this mode existed: there is no identity to diff against.
    The rebuild is free — it replaces a load that would have happened anyway."""
    assert not index_mirror._can_append(_live("a", "b"), ["a", "b"])


def test_a_matching_table_is_appended_to():
    assert index_mirror._can_append(
        _live("a", "b", index_mirror.HASH_COLUMN,
              index_mirror.FIRST_SEEN_COLUMN), ["a", "b"])


def test_a_changed_column_set_forces_a_rebuild():
    """The hash is computed over the source columns, so a different SET is a
    different identity and EVERY row would read as new — the table would double
    instead of growing by the delta."""
    live = _live("a", "b", index_mirror.HASH_COLUMN, index_mirror.FIRST_SEEN_COLUMN)
    assert not index_mirror._can_append(live, ["a", "b", "c"])
    assert not index_mirror._can_append(live, ["a"])


def test_a_reordered_column_set_still_appends():
    """Order changing is not a schema change: the loader hashes in the LIVE
    table's order on both sides, so a source that shuffles its columns must not
    trigger a full rebuild."""
    live = _live("a", "b", index_mirror.HASH_COLUMN, index_mirror.FIRST_SEEN_COLUMN)
    assert index_mirror._can_append(live, ["b", "a"])


def test_the_geometry_column_is_not_mistaken_for_a_source_column():
    """`geom` is derived, not source — counting it would make every GovMap layer
    look like a schema change and rebuild forever."""
    live = _live("a", "geometry_wkt", "geom", index_mirror.HASH_COLUMN,
                 index_mirror.FIRST_SEEN_COLUMN)
    assert index_mirror._source_columns(live) == ["a", "geometry_wkt"]
    assert index_mirror._can_append(live, ["a", "geometry_wkt"])


def test_incremental_can_be_switched_off(monkeypatch):
    monkeypatch.setattr(index_mirror.settings, "index_mirror_incremental", False)
    assert not index_mirror._can_append(
        _live("a", index_mirror.HASH_COLUMN), ["a"])


def test_the_hash_matches_the_public_append_tables():
    """One definition of "the same row" for idx and public.append_*, not two
    that drift apart."""
    assert (index_mirror._hash_expr(["a", "b"])
            == append_store._content_hash_expr(["a", "b"]))


def test_the_hash_depends_on_column_order():
    """Which is why every caller passes the LIVE order — hashing in CSV order
    would make every existing row look new the first time a source reorders."""
    assert index_mirror._hash_expr(["a", "b"]) != index_mirror._hash_expr(["b", "a"])


def test_loader_columns_cannot_be_shadowed_by_the_source():
    for c in ("_id", index_mirror.HASH_COLUMN, index_mirror.FIRST_SEEN_COLUMN):
        assert c in index_mirror.SYSTEM_COLUMNS


class _LoadConn:
    """Enough of an asyncpg connection for the two load paths."""

    def __init__(self, inserted=3, total=10):
        self.executed: list[str] = []
        self.copied: list[tuple] = []
        self.inserted, self.total = inserted, total

    async def execute(self, sql, *a):
        self.executed.append(sql)
        if "INSERT INTO" in sql:
            return f"INSERT 0 {self.inserted}"
        return "OK"

    async def fetchval(self, sql, *a):
        if "count(*)" in sql:
            return self.total
        return None

    async def fetch(self, *a):
        return []

    async def copy_records_to_table(self, table, *, schema_name=None,
                                    columns=None, records=None):
        self.copied.append((table, schema_name, columns, list(records)))

    def transaction(self):
        class _Tx:
            async def __aenter__(s): return s
            async def __aexit__(s, *e): return False
        return _Tx()


def _csv(tmp_path, text):
    p = tmp_path / "idx.csv"
    p.write_text(text, encoding="utf-8")
    return str(p)


def test_append_writes_only_the_rows_the_table_lacks(tmp_path, monkeypatch):
    monkeypatch.setattr(index_mirror.settings, "index_mirror_postgis_enabled", False)
    conn = _LoadConn(inserted=45, total=32752)
    path = _csv(tmp_path, "a,b\n1,2\n3,4\n")

    out = asyncio.run(index_mirror._append(
        conn, path, "t", ["a", "b"], ["a", "b"], [0, 1]))

    assert out["mode"] == "append"
    assert out["new_rows"] == 45          # what was WRITTEN
    assert out["rows"] == 32752           # what the table now holds
    assert out["seen_rows"] == 2          # what the CSV carried
    joined = " | ".join(conn.executed)
    assert "NOT EXISTS" in joined, "the diff is what makes this incremental"
    assert '"idx"."t"' in joined


class _CrsConn(_LoadConn):
    """A _LoadConn whose WKT sniff answers differently for the live table and
    the staging one, so the two frames can be made to disagree."""

    def __init__(self, live_wkt, incoming_wkt, **kw):
        super().__init__(**kw)
        self.live_wkt, self.incoming_wkt = live_wkt, incoming_wkt

    async def fetchval(self, sql, *a):
        if "substring" in sql:
            return self.incoming_wkt if "__stg" in sql else self.live_wkt
        return await super().fetchval(sql, *a)


_ITM_WKT = "MULTIPOLYGON(((245134.2 698829.0, 1 2)))"
_WGS_WKT = "POLYGON((34.78 32.08, 34.79 32.09))"


def test_a_changed_geometry_frame_forces_a_rebuild_instead_of_doubling(
        tmp_path, monkeypatch):
    """The 2026-07-08 ITM→WGS84 switch rewrites geometry_wkt for EVERY row, so
    every hash differs and a plain append would insert the whole layer a second
    time — half in metres, half in degrees, with nothing afterwards to tell the
    halves apart. Hand the table back for a rebuild instead."""
    _enable_postgis(monkeypatch, on=False)   # the guard is not a geometry feature
    conn = _CrsConn(live_wkt=_ITM_WKT, incoming_wkt=_WGS_WKT)
    path = _csv(tmp_path, "geometry_wkt\nPOINT(34.78 32.08)\n")

    out = asyncio.run(index_mirror._append(
        conn, path, "t", ["geometry_wkt"], ["geometry_wkt"], [0]))

    assert out is None, "an append across a frame change doubles the layer"
    assert not any("INSERT INTO" in s for s in conn.executed)


def test_an_unchanged_geometry_frame_still_appends(tmp_path, monkeypatch):
    """The guard must not turn every ordinary refresh into a full rebuild."""
    _enable_postgis(monkeypatch, on=False)
    conn = _CrsConn(live_wkt=_WGS_WKT, incoming_wkt=_WGS_WKT)
    path = _csv(tmp_path, "geometry_wkt\nPOINT(34.78 32.08)\n")

    out = asyncio.run(index_mirror._append(
        conn, path, "t", ["geometry_wkt"], ["geometry_wkt"], [0]))

    assert out is not None and out["mode"] == "append"
    assert any("INSERT INTO" in s for s in conn.executed)


def test_append_never_drops_or_renames_the_live_table(tmp_path, monkeypatch):
    """The regression that would silently undo the whole change: an append path
    that still swaps a table in writes the full relation every time."""
    monkeypatch.setattr(index_mirror.settings, "index_mirror_postgis_enabled", False)
    conn = _LoadConn()
    path = _csv(tmp_path, "a,b\n1,2\n")
    asyncio.run(index_mirror._append(conn, path, "t", ["a", "b"], ["a", "b"], [0, 1]))

    for sql in conn.executed:
        assert not ("DROP TABLE" in sql and '"idx"."t"' in sql), sql
        assert "RENAME TO" not in sql, sql


def test_append_stages_in_an_unlogged_table_not_a_temp_one(tmp_path, monkeypatch):
    """The CSV still has to be streamed in full (R2 holds a whole snapshot per
    version). UNLOGGED keeps that read out of the WAL so only the delta is
    written durably — and it must NOT be TEMP.

    Measured in production on the 244MB הסדרים מותנים משטרה CSV: a temp table
    lives in the session's LOCAL buffer pool, capped by temp_buffers (8MB by
    default), and Postgres aborted the whole append with "no empty local buffer
    available" (localbuf.c). An unlogged table uses shared_buffers and has no
    such ceiling."""
    monkeypatch.setattr(index_mirror.settings, "index_mirror_postgis_enabled", False)
    conn = _LoadConn()
    path = _csv(tmp_path, "a,b\n1,2\n")
    asyncio.run(index_mirror._append(conn, path, "t", ["a", "b"], ["a", "b"], [0, 1]))

    assert any("CREATE UNLOGGED TABLE" in s for s in conn.executed)
    assert not any("TEMP" in s for s in conn.executed), \
        "temp_buffers caps a temp table at a size these CSVs exceed"
    assert conn.copied and conn.copied[0][1] == "idx"


def test_append_always_drops_its_staging_table(tmp_path, monkeypatch):
    """Staging is a permanent relation now, so nothing cleans it up implicitly —
    a leftover would be re-COPYed into on the next run and double the delta."""
    monkeypatch.setattr(index_mirror.settings, "index_mirror_postgis_enabled", False)

    class _Boom(_LoadConn):
        async def execute(self, sql, *a):
            self.executed.append(sql)
            if "INSERT INTO" in sql:
                raise RuntimeError("diff blew up")
            return "OK"

    conn = _Boom()
    path = _csv(tmp_path, "a,b\n1,2\n")
    try:
        asyncio.run(index_mirror._append(conn, path, "t", ["a", "b"],
                                         ["a", "b"], [0, 1]))
    except RuntimeError:
        pass
    drops = [s for s in conn.executed if "DROP TABLE" in s and "__stg" in s]
    assert len(drops) == 2, "staging must be dropped before AND after the load"


def test_append_hashes_in_the_live_order_not_the_csv_order(tmp_path, monkeypatch):
    """A source that swaps two columns must not orphan every existing row."""
    monkeypatch.setattr(index_mirror.settings, "index_mirror_postgis_enabled", False)
    conn = _LoadConn()
    path = _csv(tmp_path, "b,a\n2,1\n")
    asyncio.run(index_mirror._append(conn, path, "t", ["a", "b"], ["b", "a"], [0, 1]))

    insert = [s for s in conn.executed if "INSERT INTO" in s][0]
    assert index_mirror._hash_expr(["a", "b"], alias="s") in insert


def test_append_skips_the_geometry_pass_when_nothing_was_inserted(tmp_path, monkeypatch):
    """A poll that finds no new rows must cost nothing beyond the diff."""
    monkeypatch.setattr(index_mirror.settings, "index_mirror_postgis_enabled", True)
    conn = _LoadConn(inserted=0)
    path = _csv(tmp_path, "a,geometry_wkt\n1,POINT(34.7 32.0)\n")
    out = asyncio.run(index_mirror._append(
        conn, path, "t", ["a", "geometry_wkt"], ["a", "geometry_wkt"], [0, 1]))

    assert out["new_rows"] == 0
    assert not any("ANALYZE" in s for s in conn.executed)
    assert not any("try_geom" in s for s in conn.executed)


def test_rebuild_creates_the_identity_the_next_sync_diffs_against(tmp_path, monkeypatch):
    """Without the hash column and its index, every later sync falls back to a
    rebuild — the mode would never actually engage."""
    monkeypatch.setattr(index_mirror.settings, "index_mirror_postgis_enabled", False)
    conn = _LoadConn()
    path = _csv(tmp_path, "a,b\n1,2\n3,4\n")

    out = asyncio.run(index_mirror._rebuild(conn, path, "t", ["a", "b"], [0, 1]))

    assert out["mode"] == "rebuild" and out["rows"] == 2 and out["new_rows"] == 2
    joined = " | ".join(conn.executed)
    assert index_mirror.HASH_COLUMN in joined
    assert index_mirror.FIRST_SEEN_COLUMN in joined
    assert "CREATE INDEX" in joined and "RENAME TO" in joined


def test_rebuild_index_is_not_unique(tmp_path, monkeypatch):
    """A source CSV that legitimately repeats a row would fail a UNIQUE build,
    and losing the whole table to that is worse than holding the duplicate."""
    monkeypatch.setattr(index_mirror.settings, "index_mirror_postgis_enabled", False)
    conn = _LoadConn()
    path = _csv(tmp_path, "a\n1\n1\n")
    asyncio.run(index_mirror._rebuild(conn, path, "t", ["a"], [0]))

    created = [s for s in conn.executed if "CREATE INDEX" in s or "CREATE UNIQUE" in s]
    assert created and not any("UNIQUE" in s for s in created)


def test_hash_index_name_stays_inside_the_identifier_budget():
    long_table = "govmap_" + "א" * 40          # Hebrew: 2 bytes per char
    name = index_mirror._hash_index_name(long_table)
    assert len(name.encode("utf-8")) <= 63
    assert name.endswith("_hash_ix")


def test_a_staging_index_never_collides_with_the_live_table_s(monkeypatch):
    """Index names share the relation namespace with TABLES. table_name() can
    emit a full 63 bytes, and clipping alone made a long layer's staging index
    resolve to the SAME name its live index already held — so every sync after
    the first hit "relation already exists" and the layer silently lost its
    geometry. Both indexes are built on staging now, so both need this."""
    table = "govmap_" + "x" * 47 + "_ab12cd34"          # exactly 63 bytes
    staging = index_mirror._staging_name(table)
    assert len(table.encode()) == 63

    for name_of in (index_mirror._geom_index_name, index_mirror._hash_index_name):
        assert name_of(staging) != name_of(table), name_of.__name__
        assert len(name_of(staging).encode("utf-8")) <= 63
        assert len(name_of(table).encode("utf-8")) <= 63


def test_short_table_names_keep_their_existing_index_names():
    """The common case must not be renamed — these indexes already exist in
    production under exactly these names."""
    assert index_mirror._geom_index_name("govmap_9_abc") == "govmap_9_abc_geom_gix"
    assert index_mirror._hash_index_name("govmap_9_abc") == "govmap_9_abc_hash_ix"


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


def test_every_udt_name_read_has_a_query_that_selects_it():
    """Source-level guard for a class of bug the mocked tests cannot see.

    Reading r["udt_name"] from an information_schema query that does not SELECT
    it raises KeyError only against a REAL database — every unit test here fakes
    the rows, so it passes locally and takes out /data in production. That is
    exactly what happened on 2026-07-23: two of the three column queries were
    updated together with their reads, the third was not, and the catalog
    endpoint started 500ing.

    So: count the reads and count the SELECTs. They must match."""
    import re as _re
    src = open(append_store.__file__, encoding="utf-8").read()
    reads = len(_re.findall(r'\["udt_name"\]', src))
    selects = len(_re.findall(r"SELECT[^\"]*udt_name", src))
    assert reads == selects, (
        f"{reads} reads of udt_name but {selects} queries select it — a read "
        f"without a matching SELECT is a KeyError that only fires in production")


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


class _NeonDS(_DS):
    def __init__(self, kind=None, archive_neon=None, **kw):
        super().__init__(**kw)
        self.kind = kind
        self.archive_neon = archive_neon


def test_archive_neon_scrapers_are_excluded():
    """registries / munidata / servicescompass / emun already dual-write every
    row into public.append_* — an idx mirror would be a second, diverging copy
    (it holds only the latest version while the append table accumulates all of
    them). This was a real double-count: emun 989 in idx vs 1006 in append."""
    for kind in ("registries", "munidata", "servicescompass", "emun"):
        assert not index_mirror.dataset_is_index_mirror_eligible(
            _NeonDS(kind=kind, source_type="scraper", archive_neon="true")), kind


def test_archive_neon_flag_is_read_as_text_or_bool():
    """The flag reaches the eligibility check as JSONB-astext ('true') from the
    sync/purge queries, but a real bool must also count."""
    assert not index_mirror.dataset_is_index_mirror_eligible(
        _NeonDS(kind="emun", source_type="scraper", archive_neon=True))
    assert not index_mirror.dataset_is_index_mirror_eligible(
        _NeonDS(kind="emun", source_type="scraper", archive_neon="true"))


def test_non_archive_neon_scrapers_stay_eligible():
    """A plain document scraper (jda) has no public.append_ twin, so its index
    CSV is still worth mirroring. archive_neon absent or explicitly false."""
    assert index_mirror.dataset_is_index_mirror_eligible(
        _NeonDS(kind="jda", source_type="scraper", archive_neon=None))
    assert index_mirror.dataset_is_index_mirror_eligible(
        _NeonDS(kind="jda", source_type="scraper", archive_neon="false"))


def test_govmap_stays_eligible_despite_being_tabular():
    """GovMap layers are NOT archive_neon — they need the idx mirror for the
    geometry column and PostGIS. The flag, not 'has rows', is the gate."""
    assert index_mirror.dataset_is_index_mirror_eligible(
        _NeonDS(source_type="govmap", archive_neon=None))


def test_other_kinds_and_missing_kind_stay_eligible():
    assert index_mirror.dataset_is_index_mirror_eligible(
        _KDS(kind="govmap", source_type="govmap"))
    assert index_mirror.dataset_is_index_mirror_eligible(
        _KDS(kind=None, source_type="scraper"))
    assert index_mirror.dataset_is_index_mirror_eligible(
        _KDS(kind="mevaker", source_type="scraper"))


# ── loaded_versions cache (Neon scale-to-zero) ───────────────────────────────
#
# loaded_versions() is the ONLY thing the 10-minute scheduler tick does when
# nothing changed, and it used to cost ~12 append-DB statements (ensure_schema's
# CREATE/GRANT/ALTER DEFAULT PRIVILEGES + the state-table DDL + the SELECT) —
# several of them catalog writes. On Neon that kept the compute from ever
# scaling to zero. These pin the cache and, more importantly, every path that
# MUST invalidate it.

class _FakeConn:
    def __init__(self, rows, log):
        self._rows, self._log = rows, log

    async def fetch(self, *a):
        self._log.append("fetch")
        return self._rows

    async def execute(self, *a):
        self._log.append("execute")
        return "DELETE 3"


class _FakePool:
    def __init__(self, rows, log):
        self._rows, self._log = rows, log

    def acquire(self):
        rows, log = self._rows, self._log

        class _Acq:
            async def __aenter__(self): return _FakeConn(rows, log)
            async def __aexit__(self, *a): return False
        return _Acq()


def _install_fake_pool(monkeypatch, rows, log):
    monkeypatch.setattr(index_mirror, "_loaded_versions_cache", None)
    monkeypatch.setattr(append_store, "is_configured", lambda: True)

    async def _pool():
        return _FakePool(rows, log)
    monkeypatch.setattr(append_store, "get_pool", _pool)
    # ensure_schema/_ensure_state_table are the expensive part being skipped;
    # count them too.
    async def _noop(conn):
        log.append("ddl")
    monkeypatch.setattr(index_mirror, "ensure_schema", _noop)
    monkeypatch.setattr(index_mirror, "_ensure_state_table", _noop)


_ROW = {"dataset_id": "304e43d5-c419-43bd-8b46-f31a4da0c075",
        "version_number": 5, "error": None, "deferred": None, "attempts": 0}


def test_loaded_versions_reads_once_then_serves_from_memory(monkeypatch):
    log = []
    _install_fake_pool(monkeypatch, [_ROW], log)

    first = asyncio.run(index_mirror.loaded_versions())
    second = asyncio.run(index_mirror.loaded_versions())

    assert first == second == {_ROW["dataset_id"]: 5}
    assert log.count("fetch") == 1      # one SELECT total
    assert log.count("ddl") == 2        # …and the DDL ran only with it


def test_loaded_versions_returns_a_copy_callers_cannot_poison(monkeypatch):
    log = []
    _install_fake_pool(monkeypatch, [_ROW], log)

    got = asyncio.run(index_mirror.loaded_versions())
    got["injected"] = 999
    assert "injected" not in asyncio.run(index_mirror.loaded_versions())


def test_record_invalidates_the_cache(monkeypatch):
    """Otherwise a just-synced dataset is re-offered for the life of the
    process — an infinite re-sync loop, the worst possible failure here."""
    log = []
    _install_fake_pool(monkeypatch, [_ROW], log)
    asyncio.run(index_mirror.loaded_versions())
    assert index_mirror._loaded_versions_cache is not None

    asyncio.run(index_mirror._record(
        _ROW["dataset_id"], "idx_t", 6, 100, None))
    assert index_mirror._loaded_versions_cache is None


def test_retry_deferred_invalidates_the_cache(monkeypatch):
    """Admin "retry deferred" must take effect now, not after the next deploy."""
    log = []
    _install_fake_pool(monkeypatch, [_ROW], log)
    asyncio.run(index_mirror.loaded_versions())
    assert index_mirror._loaded_versions_cache is not None

    assert asyncio.run(index_mirror.retry_deferred()) == 3
    assert index_mirror._loaded_versions_cache is None


# ── staged retry: the backlog is worked off in tiers, not in one go ──────────
#
# 54 datasets are deferred, 9GB of CSV, the largest 3.5GB. Clearing all of them
# on a 512MB dyno that also serves the site is the exact failure the size gate
# was added to stop, so the filters below are what make a rollout possible.

class _SqlConn:
    def __init__(self, log):
        self._log = log

    async def fetch(self, *a):
        return []

    async def execute(self, sql, *args):
        self._log.append((sql, args))
        return "DELETE 2"


class _SqlPool:
    def __init__(self, log):
        self._log = log

    def acquire(self):
        log = self._log

        class _Acq:
            async def __aenter__(self): return _SqlConn(log)
            async def __aexit__(self, *a): return False
        return _Acq()


def _install_sql_pool(monkeypatch):
    log: list = []
    monkeypatch.setattr(append_store, "is_configured", lambda: True)
    monkeypatch.setattr(index_mirror, "_loaded_versions_cache", None)

    async def _pool():
        return _SqlPool(log)

    async def _noop(conn):
        pass

    monkeypatch.setattr(append_store, "get_pool", _pool)
    monkeypatch.setattr(index_mirror, "_ensure_state_table", _noop)
    return log


def test_retry_deferred_can_be_limited_to_a_size_tier(monkeypatch):
    log = _install_sql_pool(monkeypatch)
    asyncio.run(index_mirror.retry_deferred(max_csv_mb=250))
    sql, args = log[-1]
    assert "csv_bytes <= $1" in sql
    assert args == (250 * 1024 * 1024,)
    # Unknown size is what the gate already treats as too big — a tiered rollout
    # must not smuggle one in under the ceiling.
    assert "csv_bytes IS NOT NULL" in sql


def test_retry_deferred_can_be_limited_to_one_dataset(monkeypatch):
    log = _install_sql_pool(monkeypatch)
    asyncio.run(index_mirror.retry_deferred(dataset_id="d49264eb"))
    sql, args = log[-1]
    assert "dataset_id = $1" in sql and args == ("d49264eb",)


def test_force_clears_a_clean_checkpoint_for_one_dataset(monkeypatch):
    """"Mirror this one again" — the only way to re-offer a dataset at the
    version it already holds, and the only way to exercise the append path on
    demand instead of waiting for the source to move."""
    log = _install_sql_pool(monkeypatch)
    asyncio.run(index_mirror.retry_deferred(dataset_id="d1", force=True))
    sql, args = log[-1]
    assert "deferred IS NOT NULL" not in sql, "force must ignore the deferred state"
    assert "dataset_id = $1" in sql and args == ("d1",)


def test_force_without_a_dataset_id_is_refused(monkeypatch):
    """It would re-offer all ~900 mirrored datasets and re-download the whole
    corpus — never what anyone means to ask for."""
    _install_sql_pool(monkeypatch)
    try:
        asyncio.run(index_mirror.retry_deferred(force=True))
    except ValueError as e:
        assert "dataset_id" in str(e)
    else:
        raise AssertionError("force with no dataset_id must raise")


def test_retry_deferred_without_filters_clears_everything(monkeypatch):
    """The original behaviour has to survive: no filters, no extra predicates."""
    log = _install_sql_pool(monkeypatch)
    asyncio.run(index_mirror.retry_deferred())
    sql, args = log[-1]
    assert args == ()
    assert "csv_bytes" not in sql and "dataset_id =" not in sql


# ── pending(): refreshes outrank first-time loads ────────────────────────────
#
# The driver takes only a few datasets per tick, so the ORDER pending() returns
# is what decides whether "a new version reaches SQL automatically" means
# minutes or a day and a half.

class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _FakeDB:
    """Answers pending()'s queries in order: datasets, then version numbers,
    then (only if something moved) resource_mappings."""

    def __init__(self, datasets, versions):
        self.datasets, self.versions = datasets, versions
        self.queries = 0
        self.jsonb_fetches = 0

    def _asked_for(self, q) -> set:
        """The ids this query's ``WHERE tracked_dataset_id IN (...)`` names.

        Honouring the filter is the whole point: the bug being guarded against
        was a dataset never appearing in the IN-list, so a fake that returns
        every row regardless would report the mirror as healthy while
        production could not see the version at all."""
        from sqlalchemy.dialects import postgresql
        asked = set()
        for v in q.compile(dialect=postgresql.dialect()).params.values():
            asked.update(str(x) for x in v) if isinstance(v, (list, tuple)) \
                else asked.add(str(v))
        return {d.id for d, _, _ in self.versions if str(d.id) in asked}

    async def execute(self, q):
        self.queries += 1
        if self.queries == 1:
            return _FakeResult(self.datasets)
        asked = self._asked_for(q)
        if self.queries == 2:                     # (dataset_id, version_number)
            return _FakeResult([(d.id, v) for d, v, _ in self.versions
                                if d.id in asked])
        self.jsonb_fetches += 1                   # + resource_mappings
        return _FakeResult([(d.id, v, m) for d, v, m in self.versions
                            if d.id in asked])


def _mapping(key="k"):
    return {index_mirror.CSV_RESOURCE_KEY: f"r2:{key}"}


def test_pending_offers_a_refresh_before_a_first_time_load(monkeypatch):
    """A live table whose version moved must not queue behind the backfill.

    With ordering by title alone, "אבן" (never mirrored) preceded "תל" (already
    mirrored, new version) and the stale visible table waited for the entire
    backlog — 611 datasets / ~34 hours as measured on 2026-07-23."""
    fresh = _DS(id="11111111-1111-1111-1111-111111111111", title="אבן",
                ckan_name="even")
    stale = _DS(id="22222222-2222-2222-2222-222222222222", title="תל",
                ckan_name="tel")
    db = _FakeDB([fresh, stale],
                 [(fresh, 1, _mapping()), (stale, 7, _mapping())])

    async def fake_loaded():
        return {stale.id: 6}          # mirrored at v6, source is now at v7

    monkeypatch.setattr(index_mirror, "loaded_versions", fake_loaded)
    out = asyncio.run(index_mirror.pending(db))

    assert [r["title"] for r in out] == ["תל", "אבן"]
    assert out[0]["refresh"] is True and out[1]["refresh"] is False


def test_pending_sees_a_new_version_while_a_backlog_exists(monkeypatch):
    """The regression this replaces: the version query used to skip every
    already-settled dataset whenever an unmirrored one existed, so a refresh was
    invisible — not merely last in line."""
    stale = _DS(id="22222222-2222-2222-2222-222222222222", title="תל",
                ckan_name="tel")
    backlog = [_DS(id=f"3333333{i}-3333-3333-3333-333333333333", title=f"ב{i}",
                   ckan_name=f"b{i}") for i in range(5)]
    db = _FakeDB([stale, *backlog],
                 [(stale, 7, _mapping()), *[(d, 1, _mapping()) for d in backlog]])

    async def fake_loaded():
        return {stale.id: 6}

    monkeypatch.setattr(index_mirror, "loaded_versions", fake_loaded)
    out = asyncio.run(index_mirror.pending(db, limit=1))

    assert len(out) == 1 and out[0]["dataset_id"] == stale.id


def test_pending_is_one_cheap_query_when_nothing_moved(monkeypatch):
    """The idle tick is the common case (GovMap polls every 90 days). It must
    not pull a single resource_mappings JSONB."""
    a = _DS(id="11111111-1111-1111-1111-111111111111", title="א", ckan_name="a")
    b = _DS(id="22222222-2222-2222-2222-222222222222", title="ב", ckan_name="b")
    db = _FakeDB([a, b], [(a, 3, _mapping()), (b, 4, _mapping())])

    async def fake_loaded():
        return {a.id: 3, b.id: 4}

    monkeypatch.setattr(index_mirror, "loaded_versions", fake_loaded)
    assert asyncio.run(index_mirror.pending(db)) == []
    assert db.jsonb_fetches == 0
    assert db.queries == 2            # datasets + version numbers, nothing else


def test_pending_skips_a_version_without_an_index_csv(monkeypatch):
    a = _DS(id="11111111-1111-1111-1111-111111111111", title="א", ckan_name="a")
    db = _FakeDB([a], [(a, 2, {"אחר": "r2:other"})])

    async def fake_loaded():
        return {}

    monkeypatch.setattr(index_mirror, "loaded_versions", fake_loaded)
    assert asyncio.run(index_mirror.pending(db)) == []


def test_pending_treats_a_deferred_dataset_as_a_refresh(monkeypatch):
    """It jumps the queue when its version moves, then costs one HEAD and is
    deferred again without a download — cheaper than a stale live table."""
    d = _DS(id="11111111-1111-1111-1111-111111111111", title="ת", ckan_name="d")
    n = _DS(id="22222222-2222-2222-2222-222222222222", title="א", ckan_name="n")
    db = _FakeDB([d, n], [(d, 4, _mapping()), (n, 1, _mapping())])

    async def fake_loaded():
        return {d.id: 3}              # settled-as-deferred at v3

    monkeypatch.setattr(index_mirror, "loaded_versions", fake_loaded)
    out = asyncio.run(index_mirror.pending(db))
    assert [r["title"] for r in out] == ["ת", "א"]
