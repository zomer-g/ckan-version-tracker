"""Phase 4 of docs/history-storage-strategy.md: wrong closures, fully cold datasets.

Written BEFORE the code (docs/history-phase4-plan.md section 4.2). No Postgres
and no R2 here, so as in phases 2 and 3:

* SQL is checked through its pure builders, and where the semantics matter
  (the duplicate cleanup of the reopen tool) the same statements run on a real
  DuckDB table, with ``ctid`` read as DuckDB's ``rowid``;
* the table side of export_full / rehydrate is a fake with the interface of the
  locked Postgres transaction (``history_export.PgTable``);
* Parquet is real (pyarrow), R2 is a dict, and the engine is real DuckDB.
"""
import asyncio
import types
import uuid
from datetime import date, datetime, timedelta, timezone

import pytest

duckdb = pytest.importorskip("duckdb")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from fastapi import HTTPException  # noqa: E402

from app.config import settings  # noqa: E402
from app.services import append_store  # noqa: E402
from app.services import history_export as hx  # noqa: E402
from app.services import history_query as hq  # noqa: E402

UTC = timezone.utc
NOW = datetime(2026, 10, 17, 9, 0, tzinfo=UTC)
DSID = "d3d20a89-5c41-4b49-a7a8-cc1c73e070f3"
TABLE = "append_rain_elon_d3d20a89_11112222"


def _ts(y, m, d, h=0):
    return datetime(y, m, d, h, tzinfo=UTC)


def _ds(cfg=None, **kw):
    base = dict(id=uuid.UUID(DSID), title="תחזית גשם", status="active", poll_interval=604800,
                source_type="scraper", source_url="https://www.gov.il/he/pages/cpfta_prices_regulations",
                scraper_config=cfg, storage_mode="append_only", ckan_name="rain", is_active=True)
    base.update(kw)
    return types.SimpleNamespace(**base)


# ── 1. should_close reads the manifest when the dataset is silent ─────────────

@pytest.fixture
def closing_on(monkeypatch):
    monkeypatch.setattr(settings, "append_close_rows_enabled", True)


def _manifest(monkeypatch, cfg):
    from app.services import source_registry

    monkeypatch.setattr(source_registry, "cached_manifests", lambda: ["m"])
    monkeypatch.setattr(
        source_registry, "match_manifests",
        lambda url, mans: None if cfg is None else types.SimpleNamespace(scraper_config=cfg))


def test_manifest_opt_out_reaches_an_existing_dataset(closing_on, monkeypatch):
    _manifest(monkeypatch, {"kind": "prices", "append_closes_rows": False})
    assert append_store.closes_rows_setting(_ds({"kind": "prices"})) is False
    assert append_store.should_close(_ds({"kind": "prices"})) is False


def test_the_dataset_config_wins_when_it_has_the_key(closing_on, monkeypatch):
    _manifest(monkeypatch, {"append_closes_rows": True})
    assert append_store.should_close(_ds({"append_closes_rows": False})) is False
    _manifest(monkeypatch, {"append_closes_rows": False})
    assert append_store.should_close(_ds({"append_closes_rows": True})) is True


def test_without_a_manifest_closing_is_the_default(closing_on, monkeypatch):
    _manifest(monkeypatch, None)
    assert append_store.closes_rows_setting(_ds({})) is None
    assert append_store.should_close(_ds({})) is True
    # a CKAN dataset never asks the registry
    from app.services import source_registry

    monkeypatch.setattr(source_registry, "match_manifests",
                        lambda *a: (_ for _ in ()).throw(AssertionError("asked")))
    assert append_store.should_close(_ds({}, source_type="ckan")) is True


def test_a_broken_registry_never_breaks_a_load(closing_on, monkeypatch):
    from app.services import source_registry

    def boom(*a):
        raise RuntimeError("registry down")

    monkeypatch.setattr(source_registry, "match_manifests", boom)
    assert append_store.closes_rows_setting(_ds({})) is None


# ── 2. close_guard: the minimum pass fraction ────────────────────────────────

def test_a_one_percent_pass_is_refused_even_with_allow_shrink():
    g = append_store.close_guard
    why = g(open_rows=100_000, closing=99_000, max_fraction=0.5, allow_shrink=True,
            min_pass_fraction=0.2, declared=False)
    assert why and "20%" in why


def test_an_explicit_declaration_lets_flydata_through():
    g = append_store.close_guard
    assert g(open_rows=338_587, closing=336_357, max_fraction=0.5, allow_shrink=True,
             min_pass_fraction=0.2, declared=True) is None
    # without allow_shrink the 50% rule still applies to a declared source
    assert g(open_rows=338_587, closing=336_357, max_fraction=0.5,
             min_pass_fraction=0.2, declared=True) is not None


def test_a_large_pass_is_not_touched_by_the_new_rule():
    g = append_store.close_guard
    assert g(open_rows=1000, closing=300, max_fraction=0.5, allow_shrink=False,
             min_pass_fraction=0.2, declared=False) is None


def test_the_min_pass_fraction_setting():
    assert settings.history_close_min_pass_fraction == 0.2


def test_close_missing_passes_the_declaration(monkeypatch):
    import inspect

    sig = inspect.signature(append_store.PassTracker.close_missing)
    assert "declared" in sig.parameters
    import pathlib

    root = pathlib.Path(__file__).parent.parent / "app"
    for f in ("api/worker.py", "services/delta_archiver.py"):
        src = (root / f).read_text(encoding="utf-8")
        for chunk in src.split(".close_missing(")[1:]:
            assert "declared=" in chunk[:300], f


# ── 3. reopen_closures ────────────────────────────────────────────────────────

def _duck_sql(sql: str) -> str:
    return sql.replace("ctid", "rowid").replace('"public".', "")


def _reopen_table():
    conn = duckdb.connect(":memory:")
    conn.execute('CREATE TABLE "t" ("row_hash" VARCHAR, "a" VARCHAR, "last_seen_date" VARCHAR, '
                 '"first_seen" TIMESTAMPTZ, "last_seen" TIMESTAMPTZ)')
    rows = [
        # h1: closed wrongly, then the same state came back as a new open row
        ("h1", "x", "", _ts(2026, 10, 1), _ts(2026, 10, 15)),
        ("h1", "x", "2026-10-16", _ts(2026, 10, 16), None),
        # h2: closed only
        ("h2", "y", "", _ts(2026, 9, 1), _ts(2026, 10, 15)),
        # h3: open, untouched
        ("h3", "z", "", _ts(2026, 9, 1), None),
        # h4: closed twice (closed, back, closed again)
        ("h4", "w", "", _ts(2026, 9, 1), _ts(2026, 9, 10)),
        ("h4", "w", "", _ts(2026, 9, 20), _ts(2026, 10, 15)),
    ]
    for r in rows:
        conn.execute('INSERT INTO "t" VALUES (?, ?, ?, ?, ?)', list(r))
    return conn


def test_reopen_sql_keeps_the_earliest_row_and_reopens_the_rest():
    conn = _reopen_table()
    counts = conn.execute(_duck_sql(append_store.reopen_counts_sql("t", "row_hash"))).fetchone()
    assert counts == (4, 2)  # 4 closed rows, 2 rows that are extra copies of an ident
    conn.execute(_duck_sql(append_store.reopen_stamp_sql("t", "row_hash", "last_seen_date")))
    conn.execute(_duck_sql(append_store.reopen_dedup_sql("t", "row_hash", batch=1000)))
    while True:
        before = conn.execute('SELECT count(*) FROM "t" WHERE last_seen IS NOT NULL').fetchone()[0]
        conn.execute(_duck_sql(append_store.reopen_open_sql("t", "row_hash", batch=1)))
        after = conn.execute('SELECT count(*) FROM "t" WHERE last_seen IS NOT NULL').fetchone()[0]
        if after == before:
            break
    out = {r[0]: r[1:] for r in conn.execute(
        'SELECT row_hash, first_seen, last_seen, last_seen_date FROM "t" ORDER BY 1').fetchall()}
    assert set(out) == {"h1", "h2", "h3", "h4"}
    assert conn.execute('SELECT count(*) FROM "t"').fetchone()[0] == 4
    assert all(v[1] is None for v in out.values())
    assert out["h1"][0] == _ts(2026, 10, 1)        # the original first_seen survives
    assert out["h1"][2] == "2026-10-16"            # the newer copy's stamp is carried
    assert out["h4"][0] == _ts(2026, 9, 1)


def test_reopen_open_sql_works_in_ctid_batches():
    sql = append_store.reopen_open_sql("t", "row_hash", batch=50_000)
    assert 'SET "last_seen" = NULL' in sql and "ctid IN" in sql and "LIMIT 50000" in sql
    # never against an ident that is open already (a poll that landed meanwhile)
    assert "NOT EXISTS" in sql


class _RecConn:
    def __init__(self, closed=10, dups=2):
        self.sql: list[str] = []
        self.closed, self.dups = closed, dups

    async def fetchrow(self, sql, *a, **k):
        self.sql.append(sql)
        return {"closed": self.closed, "duplicates": self.dups}

    async def fetchval(self, sql, *a, **k):
        self.sql.append(sql)
        return 0

    async def execute(self, sql, *a, **k):
        self.sql.append(sql)
        if sql.startswith("DELETE"):
            n, self.dups = self.dups, 0
            return f"DELETE {n}"
        if sql.startswith("UPDATE") and 'SET "last_seen" = NULL' in sql:
            n, self.closed = self.closed, 0
            return f"UPDATE {n}"
        return "OK"


def _pool_of(conn):
    class _Acq:
        async def __aenter__(self):
            return conn

        async def __aexit__(self, *a):
            return False

    async def get_pool():
        return types.SimpleNamespace(acquire=lambda: _Acq())
    return get_pool


def test_dry_run_writes_nothing(monkeypatch):
    conn = _RecConn()
    monkeypatch.setattr(append_store, "get_pool", _pool_of(conn))
    out = asyncio.run(append_store.reopen_closures("t", dry_run=True))
    assert out["closed"] == 10 and out["duplicates"] == 2 and out["dry_run"] is True
    assert not any(s.split()[0] in ("UPDATE", "DELETE", "VACUUM") for s in conn.sql)


def test_a_real_run_dedups_reopens_and_vacuums(monkeypatch):
    conn = _RecConn()
    monkeypatch.setattr(append_store, "get_pool", _pool_of(conn))
    out = asyncio.run(append_store.reopen_closures("t", dry_run=False, stamp_col="last_seen_date"))
    assert out["deleted"] == 2 and out["reopened"] == 10
    kinds = [s.split()[0] for s in conn.sql]
    assert kinds.index("DELETE") < max(i for i, k in enumerate(kinds) if k == "UPDATE")
    assert conn.sql[-1].startswith("VACUUM (ANALYZE)")


def test_reopen_is_refused_where_closing_is_in_force(closing_on, monkeypatch):
    _manifest(monkeypatch, None)
    assert append_store.reopen_refusal(_ds({})) is not None
    assert append_store.reopen_refusal(_ds({"append_closes_rows": True})) is not None
    assert append_store.reopen_refusal(_ds({"append_closes_rows": False})) is None
    _manifest(monkeypatch, {"append_closes_rows": False})
    assert append_store.reopen_refusal(_ds({"kind": "prices"})) is None


def test_admin_has_the_reopen_route():
    from app.api import admin_storage

    paths = {(tuple(sorted(r.methods)), r.path) for r in admin_storage.router.routes}
    assert (("POST",), "/api/admin/storage/closures/reopen") in paths
    assert (("GET",), "/api/admin/storage/closures/reopen") in paths


def test_reopen_targets_by_source_or_dataset(monkeypatch):
    from app.api import admin_storage

    a = _ds({"kind": "prices", "append_closes_rows": False}, id=uuid.uuid4())
    b = _ds({"kind": "prices", "append_closes_rows": True}, id=uuid.uuid4())
    c = _ds({"kind": "other"}, id=uuid.uuid4())
    ok, refused = admin_storage.reopen_targets([a, b, c], source="prices")
    assert ok == [a] and [r["dataset_id"] for r in refused] == [str(b.id)]
    ok, refused = admin_storage.reopen_targets([a, b, c], dataset_id=str(b.id))
    assert ok == [] and len(refused) == 1


# ── 4. export_full ────────────────────────────────────────────────────────────

class _FakeStore:
    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.deleted: list[str] = []
        self.corrupt = None

    def is_configured(self):
        return True

    def public_url(self, key):
        return f"https://files.example/{key}"

    async def upload_object(self, key, *, file_content=None, file_path=None, content_type=None):
        with open(file_path, "rb") as f:
            self.objects[key] = f.read()
        return key

    async def download_to_file(self, key, dest):
        data = self.objects.get(key)
        if data is None:
            return False
        if self.corrupt:
            data = self.corrupt(key, data)
        with open(dest, "wb") as f:
            f.write(data)
        return True

    async def delete_object(self, key):
        self.deleted.append(key)
        self.objects.pop(key, None)


@pytest.fixture
def store(monkeypatch):
    s = _FakeStore()
    monkeypatch.setattr(hx, "storage_client", s)
    return s


def _rows():
    out = []
    for i in range(7):
        out.append({"station": "ELON", "pr": str(i), "first_seen": _ts(2026, 3, 1),
                    "last_seen": None if i < 5 else _ts(2026, 8, i), "row_hash": str(uuid.UUID(int=i + 1))})
    return out


class _FakeTable:
    """The locked Postgres transaction on one table, in a list."""

    def __init__(self, rows, cols=("station", "pr"), *, sql_rows=None, geom=False):
        self.rows = [dict(r) for r in rows]
        self.cols = list(cols)
        self.sql_rows = sql_rows
        self.geom = geom
        self.deleted_pairs = []
        self.copied = []
        self.rolled_back = False
        self.committed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, *a):
        if exc_type:
            self.rolled_back = True
            self.copied = []
        else:
            self.committed = True
        return False

    async def describe(self):
        if self.geom:
            raise hx.ColdRefused("geometry")
        return self.cols, "row_hash", "uuid"

    async def count(self):
        return len(self.rows)

    async def stream(self, cols, ident_col):
        for r in sorted(self.rows, key=lambda r: r[ident_col]):
            yield r

    async def table_hash(self, ident_col, ident_type):
        src = self.sql_rows if self.sql_rows is not None else self.rows + self.copied
        return {"rows": len(src), "content_hash": hx.content_hash(str(r[ident_col]) for r in src)}

    async def delete_pairs(self, ident_col, ident_type, pairs):
        self.deleted_pairs.extend(pairs)
        keep = [r for r in self.rows if (r[ident_col], r["last_seen"]) not in set(
            (str(i), ls) for i, ls in pairs)]
        n = len(self.rows) - len(keep)
        self.rows = keep
        return n

    async def copy(self, cols, records):
        self.copied.extend(dict(zip(cols, rec)) for rec in records)


@pytest.fixture
def catalog(monkeypatch):
    cat: list[dict] = []

    async def rec(parts):
        cat.extend(dict(p) for p in parts)

    async def parts_of(table):
        return [dict(p) for p in cat if p["table_name"] == table]

    async def set_deleted(ids, value):
        for p in cat:
            if p["id"] in ids:
                p["deleted_from_sql"] = value
                p["deleted_at"] = NOW if value else None

    monkeypatch.setattr(hx, "_record_parts", rec)
    monkeypatch.setattr(hx, "_table_parts", parts_of)
    monkeypatch.setattr(hx, "_set_deleted", set_deleted)
    return cat


def test_export_full_writes_open_rows_too(store, catalog):
    t = _FakeTable(_rows())
    out = asyncio.run(hx.export_full(DSID, TABLE, table_side=t, now=NOW))
    assert out["ok"] is True and out["rows"] == 7 and out["deleted"] == 7
    assert [p["kind"] for p in catalog] == ["full"]
    p = catalog[0]
    assert p["deleted_from_sql"] is True and p["closed_month"] == date(2026, 10, 1)
    assert "/full_month=2026-10/" in p["r2_key"]
    import io

    back = pq.read_table(io.BytesIO(store.objects[p["r2_key"]]))
    assert back.column("last_seen").null_count == 5
    assert t.rows == [] and t.committed


def test_export_full_verification_failure_touches_nothing(store, catalog):
    t = _FakeTable(_rows(), sql_rows=_rows()[:6])
    out = asyncio.run(hx.export_full(DSID, TABLE, table_side=t, now=NOW))
    assert out["ok"] is False and "differ" in out["error"]
    assert catalog == [] and t.deleted_pairs == [] and len(t.rows) == 7
    assert store.objects == {} and store.deleted


def test_export_full_refuses_a_geometry_table(store, catalog):
    out = asyncio.run(hx.export_full(DSID, TABLE, table_side=_FakeTable(_rows(), geom=True), now=NOW))
    assert out["ok"] is False and catalog == []


def test_full_delete_matches_null_safely():
    sql = hx.full_delete_sql(TABLE, ident_col="row_hash", ident_type="uuid")
    assert "IS NOT DISTINCT FROM" in sql
    assert '"last_seen" =' not in sql and '"last_seen" IN' not in sql
    assert "unnest($1::uuid[], $2::timestamptz[])" in sql


def test_full_part_keys_never_collide_with_closed_ones():
    k = hx.full_part_key(DSID, TABLE, date(2026, 10, 1), 1)
    assert k == f"history/d3d20a89/{TABLE}/full_month=2026-10/part-0001.parquet"
    assert k != hx.part_key(DSID, TABLE, date(2026, 10, 1), 1)


def test_a_rerun_after_a_crash_marks_the_recorded_parts(store, catalog):
    """Parts recorded and rows deleted, but the process died before the mark."""
    t = _FakeTable(_rows())
    asyncio.run(hx.export_full(DSID, TABLE, table_side=t, now=NOW))
    catalog[0]["deleted_from_sql"] = False
    out = asyncio.run(hx.export_full(DSID, TABLE, table_side=_FakeTable([]), now=NOW))
    assert out["ok"] is True and catalog[0]["deleted_from_sql"] is True and len(catalog) == 1


# ── 5. rehydrate ──────────────────────────────────────────────────────────────

def test_rehydrate_loads_verifies_and_only_then_marks(store, catalog, monkeypatch):
    asyncio.run(hx.export_full(DSID, TABLE, table_side=_FakeTable(_rows()), now=NOW))
    empty = _FakeTable([])
    out = asyncio.run(hx.rehydrate(DSID, TABLE, table_side=empty))
    assert out["ok"] is True and out["rows"] == 7 and empty.committed
    assert len(empty.copied) == 7
    assert {r["row_hash"] for r in empty.copied} == {r["row_hash"] for r in _rows()}
    assert sum(r["last_seen"] is None for r in empty.copied) == 5
    assert all(p["deleted_from_sql"] is False for p in catalog)


def test_rehydrate_failure_leaves_the_catalog_and_rolls_back(store, catalog):
    asyncio.run(hx.export_full(DSID, TABLE, table_side=_FakeTable(_rows()), now=NOW))
    bad = _FakeTable([], sql_rows=_rows()[:3])
    out = asyncio.run(hx.rehydrate(DSID, TABLE, table_side=bad))
    assert out["ok"] is False and bad.rolled_back and bad.copied == []
    assert all(p["deleted_from_sql"] is True for p in catalog)


def test_rehydrate_refuses_a_table_that_is_not_empty(store, catalog):
    asyncio.run(hx.export_full(DSID, TABLE, table_side=_FakeTable(_rows()), now=NOW))
    out = asyncio.run(hx.rehydrate(DSID, TABLE, table_side=_FakeTable(_rows()[:1])))
    assert out["ok"] is False and all(p["deleted_from_sql"] for p in catalog)


def test_rehydrate_reads_deleted_closed_parts_too(store, catalog):
    asyncio.run(hx.export_full(DSID, TABLE, table_side=_FakeTable(_rows()), now=NOW))
    extra = {**catalog[0], "id": str(uuid.uuid4()), "kind": "closed", "part": 1,
             "closed_month": date(2026, 7, 1), "r2_key": "history/x/closed.parquet"}
    still_in_sql = {**extra, "id": str(uuid.uuid4()), "r2_key": "history/x/in_sql.parquet",
                    "deleted_from_sql": False}
    catalog.extend([extra, still_in_sql])
    picked = hx.rehydrate_parts(catalog)
    assert [p["r2_key"] for p in picked] == ["history/x/closed.parquet", catalog[0]["r2_key"]]


def test_rehydrate_clears_the_tier(monkeypatch):
    cfg = {"history_tier": "cold", "history_rehydrate_on_poll": True, "kind": "x"}
    assert hx.config_after_rehydrate(cfg) == {"kind": "x"}


# ── 6. when a dataset may go cold ─────────────────────────────────────────────

def test_an_active_weekly_dataset_needs_the_explicit_promise():
    ds = _ds({}, status="active", poll_interval=7 * 86400)
    assert hx.cold_move_refusal(ds, confirm_rehydrate=False)
    assert hx.cold_move_refusal(ds, confirm_rehydrate=True) is None


def test_a_paused_or_rare_dataset_may_go_cold():
    assert hx.cold_move_refusal(_ds({}, status="paused"), confirm_rehydrate=False) is None
    assert hx.cold_move_refusal(_ds({}, poll_interval=31 * 86400),
                                confirm_rehydrate=False) is None


def test_a_cold_dataset_cannot_go_cold_again():
    assert hx.cold_move_refusal(_ds({"history_tier": "cold"}, status="paused"),
                                confirm_rehydrate=False)


def test_the_tier_reader():
    assert hx.history_tier_of(_ds({"history_tier": "cold"})) == "cold"
    assert hx.history_tier_of(_ds({})) == "hot"
    assert hx.history_tier_of(_ds(None)) == "hot"


# ── 7. phase 2's plan skips a cold dataset ────────────────────────────────────

def test_plan_skips_a_cold_dataset(monkeypatch):
    cold = _ds({"history_tier": "cold"})

    class _Res:
        def scalars(self):
            return types.SimpleNamespace(all=lambda: [cold])

    class _Db:
        async def execute(self, q):
            return _Res()

    async def latest(db, ids):
        raise AssertionError("a cold dataset reached the mapping lookup")

    async def all_parts(db):
        return []

    import app.services.parquet_export as pe

    monkeypatch.setattr(pe, "_latest_mappings", lambda db, ids: _empty())
    monkeypatch.setattr(hx, "_all_parts", all_parts)
    monkeypatch.setattr(append_store, "tables_from_mappings",
                        lambda ds, m: (_ for _ in ()).throw(AssertionError("planned")))
    monkeypatch.setattr(append_store, "get_pool", _pool_of(_RecConn()))
    assert asyncio.run(hx.plan(_Db(), now=NOW)) == []


async def _empty():
    return {}


# ── 8. the engine ─────────────────────────────────────────────────────────────

def _write_parquet(path, rows, cols):
    data = {c: pa.array([r.get(c) for r in rows], type=pa.string()) for c in cols}
    for c in ("first_seen", "last_seen"):
        data[c] = pa.array([r.get(c) for r in rows], type=pa.timestamp("us", tz="UTC"))
    data["row_hash"] = pa.array([r["row_hash"] for r in rows], type=pa.string())
    pq.write_table(pa.table(data), str(path))


def _cpart(table, key, *, kind, deleted, deleted_at=None, rows=1, month=date(2026, 10, 1),
           window_days=None):
    return {"id": str(uuid.uuid4()), "dataset_id": DSID, "table_name": table, "kind": kind,
            "window_days": window_days,
            "closed_month": month, "part": 1, "r2_key": key, "rows": rows, "bytes": 1,
            "content_hash": "x", "first_seen_min": None, "first_seen_max": None,
            "last_seen_min": _ts(2026, 7, 1), "last_seen_max": _ts(2026, 7, 9),
            "deleted_from_sql": deleted, "deleted_at": deleted_at}


STATIONS = [f"S{i:02d}" for i in range(60)]
RAIN_TABLES = [f"append_rain_{s.lower()}_d3d20a89" for s in STATIONS]
PARTIAL = "append_flights_6fb0e203"


@pytest.fixture
def engine(monkeypatch, tmp_path):
    paths, cat = {}, {}
    for s, t in zip(STATIONS, RAIN_TABLES):
        rows = [{"pr": str(i), "first_seen": _ts(2026, 1, 1), "last_seen": None,
                 "row_hash": f"{s}-{i}"} for i in range(2)]
        p = tmp_path / f"{t}.parquet"
        _write_parquet(p, rows, ["pr"])
        key = hx.full_part_key(DSID, t, date(2026, 10, 1), 1)
        paths[key] = p.as_posix()
        cat[t] = [_cpart(t, key, kind="full", deleted=True, rows=2)]
    # a closed part of 5 rows, 2 of which the window already deleted from SQL
    five = [{"pr": str(i), "first_seen": _ts(2026, 6, 1), "last_seen": _ts(2026, 7, i + 1),
             "row_hash": str(uuid.UUID(int=100 + i))} for i in range(5)]
    p = tmp_path / "partial.parquet"
    _write_parquet(p, five, ["pr"])
    key = "history/6fb0e203/partial/part-0001.parquet"
    paths[key] = p.as_posix()
    # the dataset has a 30-day window and the part is older: deletion has begun
    cat[PARTIAL] = [_cpart(PARTIAL, key, kind="closed", deleted=False, rows=5, window_days=30)]

    def hot(conn):
        conn.execute("ATTACH ':memory:' AS hot")
        conn.execute('CREATE SCHEMA hot."public"')
        conn.execute(f'CREATE TABLE hot."public"."{PARTIAL}" ("pr" VARCHAR, first_seen TIMESTAMPTZ, '
                     'last_seen TIMESTAMPTZ, row_hash UUID)')
        for r in five[2:]:  # three of the five are still in SQL
            conn.execute(f'INSERT INTO hot."public"."{PARTIAL}" VALUES (?, ?, ?, ?)',
                         [r["pr"], r["first_seen"], r["last_seen"], r["row_hash"]])
        conn.execute(f"INSERT INTO hot.\"public\".\"{PARTIAL}\" VALUES ('open', now(), NULL, uuid())")
        # deliberately NO hot table for the cold rain tables

    async def fake_catalog(table):
        return [dict(p) for p in cat.get(table, [])]

    async def branches(view):
        if view == "rain_forecast_stations":
            return list(zip(RAIN_TABLES, STATIONS))
        return None

    async def ident(table):
        return "row_hash"

    monkeypatch.setattr(hq, "_catalog_parts", fake_catalog)
    monkeypatch.setattr(hq, "union_branches", branches)
    monkeypatch.setattr(hq, "_ident_for", ident)
    monkeypatch.setattr(hq, "_part_url", lambda key: paths[key])
    monkeypatch.setattr(hq, "_hot_backend", lambda: hq.MemoryHot(hot))
    monkeypatch.setattr(hq, "is_available", lambda: (True, ""))
    monkeypatch.setattr(settings, "history_query_enabled", True)
    monkeypatch.setattr(settings, "history_query_memory_mb", 256)
    hq.reset_state()
    return cat


def _run(sql, tables):
    return asyncio.run(hq.run(sql, tables=tables))


def test_a_cold_table_is_read_without_a_hot_side(engine):
    t = RAIN_TABLES[0]
    out = _run(f"SELECT count(*) AS n, count(last_seen) AS closed FROM {t}", [t])
    assert out["rows"] == [{"n": 2, "closed": 0}] and out["engine"] == "duckdb"


def test_as_of_and_current_on_a_cold_table(engine):
    t = RAIN_TABLES[0]
    out = asyncio.run(hq.run(f"SELECT count(*) AS n FROM {t}", tables=[t], current=True))
    assert out["rows"] == [{"n": 2}]
    out = asyncio.run(hq.run(f"SELECT count(*) AS n FROM {t}", tables=[t],
                             as_of=_ts(2025, 1, 1)))
    assert out["rows"] == [{"n": 0}]


def test_rain_forecast_stations_is_one_view_over_sixty_parts(engine):
    out = _run("SELECT count(*) AS n, count(DISTINCT station) AS s FROM rain_forecast_stations",
               ["rain_forecast_stations"])
    assert out["rows"] == [{"n": 120, "s": 60}] and out["cold_parts"] == 60
    one = _run("SELECT count(*) AS n FROM rain_forecast_stations WHERE station = 'S07'",
               [("public", "rain_forecast_stations")])
    assert one["rows"] == [{"n": 2}]


def test_the_union_view_warms_every_part_once_before_any_bind(engine, monkeypatch):
    """Plan 6.9: on prod the 180 footers of the rain forecast were read over
    httpfs about five times per query, one table at a time, serially (every
    DESCRIBE binds read_parquet again): past 30 s even for count(*). Now one
    parallel pass over every part of every spec, cached for the connection."""
    calls = []
    real = hq._warm_metadata

    def spy(conn, urls):
        calls.append(list(urls))
        return real(conn, urls)

    monkeypatch.setattr(hq, "_warm_metadata", spy)
    out = _run("SELECT station, count(*) AS n FROM rain_forecast_stations GROUP BY 1 ORDER BY 1",
               ["rain_forecast_stations"])
    assert out["row_count"] == 60 and out["rows"][0] == {"station": "S00", "n": 2}
    assert len(calls) == 1
    assert sorted(calls[0]) == sorted(hq._part_url(p["r2_key"]) for t in RAIN_TABLES
                                      for p in engine[t])
    calls.clear()
    assert _run("SELECT 1 AS one", [])["rows"] == [{"one": 1}]
    assert all(not urls for urls in calls)  # nothing cold, nothing to warm


def test_union_view_sql_is_pure():
    sql = hq.build_union_view("rain_forecast_stations", "station",
                              [("t1", "ELON"), ("t2", "O'K")],
                              {"t1": [("pr", "VARCHAR"), ("first_seen", "TIMESTAMP WITH TIME ZONE"),
                                      ("row_hash", "VARCHAR")],
                               "t2": [("pr", "VARCHAR"), ("extra", "VARCHAR")]})
    assert sql.count("UNION ALL") == 1 and "'O''K'" in sql
    assert '"station"' in sql and "row_hash" not in sql
    assert 'NULL::TIMESTAMPTZ AS "first_seen"' in sql and 'NULL::VARCHAR AS "extra"' in sql


def test_a_partially_deleted_part_shows_each_row_once(engine):
    out = _run(f"SELECT count(*) AS n, count(DISTINCT row_hash) AS d FROM {PARTIAL}", [PARTIAL])
    # 3 in SQL + 1 open + the 2 that are only in the part
    assert out["rows"] == [{"n": 6, "d": 6}]


def test_cold_parts_for_includes_full_and_partial_parts(engine):
    parts = asyncio.run(hq.cold_parts_for(PARTIAL))
    assert len(parts) == 1 and parts[0]["partial"] is True
    parts = asyncio.run(hq.cold_parts_for(RAIN_TABLES[0]))
    assert parts[0]["kind"] == "full" and not parts[0].get("partial")


def test_a_never_deleted_part_stays_out(monkeypatch):
    async def fake(table):
        return [_cpart(table, "k", kind="closed", deleted=False),
                _cpart(table, "k2", kind="closed", deleted=False, window_days=400)]

    monkeypatch.setattr(hq, "_catalog_parts", fake)
    assert asyncio.run(hq.cold_parts_for("t")) == []


def test_the_view_builder_for_cold_and_partial():
    full = _cpart("t", "k1", kind="full", deleted=True)
    sql = hq.build_history_view("t", [full], [], ["pr", "first_seen", "last_seen", "row_hash"],
                                url_of=lambda k: "s3://b/" + k, cold=True)
    assert "hot." not in sql and "UNION ALL" not in sql
    partial = {**_cpart("t", "k2", kind="closed", deleted=False, deleted_at=NOW), "partial": True}
    sql = hq.build_history_view("t", [partial], ["pr", "first_seen", "last_seen", "row_hash"],
                                ["pr", "first_seen", "last_seen", "row_hash"],
                                url_of=lambda k: "s3://b/" + k, ident_col="row_hash")
    assert "NOT EXISTS" in sql and 'h."last_seen" = c."last_seen"' in sql


# ── 9. routing a cold dataset ─────────────────────────────────────────────────

from app.api import append as api  # noqa: E402


@pytest.fixture
def cold_api(monkeypatch):
    calls = {"pg": [], "hq": [], "rows": []}
    ds = _ds({"history_tier": "cold"})

    async def fake_resolve(dataset_id, db, selector=None):
        return ds, TABLE, [{"table": TABLE}]

    async def fake_pg(sql, **kw):
        calls["pg"].append(sql)
        return {"columns": ["n"], "fields": [], "rows": [{"n": 0}], "truncated": False,
                "row_count": 1}

    async def fake_hq(sql, **kw):
        calls["hq"].append(kw)
        return {"columns": ["n"], "fields": [], "rows": [{"n": 62}], "truncated": False,
                "row_count": 1, "engine": "duckdb", "cold_parts": 1, "elapsed_ms": 1}

    async def fake_query(table, **kw):
        calls["pg"].append(kw)
        return {"columns": [], "rows": [], "total": 0, "limit": 50, "offset": 0}

    async def fake_rows(table, **kw):
        calls["rows"].append(kw)
        return {"columns": ["pr"], "rows": [], "total": 62, "engine": "duckdb"}

    async def cold(table):
        return [_cpart(table, "k", kind="full", deleted=True)]

    monkeypatch.setattr(api, "_resolve", fake_resolve)
    monkeypatch.setattr(append_store, "run_readonly_sql", fake_pg)
    monkeypatch.setattr(append_store, "query", fake_query)
    monkeypatch.setattr(hq, "run", fake_hq)
    monkeypatch.setattr(hq, "rows_page", fake_rows)
    monkeypatch.setattr(hq, "cold_parts_for", cold)
    monkeypatch.setattr(hq, "is_available", lambda: (True, ""))
    monkeypatch.setattr(settings, "history_query_enabled", True)
    monkeypatch.setattr(api, "admit", lambda *a, **k: None)
    return calls


def test_sql_on_a_cold_dataset_goes_to_the_engine_without_the_switch(cold_api):
    fn = getattr(api.archive_sql, "__wrapped__", api.archive_sql)
    out = asyncio.run(fn(DSID, request=types.SimpleNamespace(query_params={}),
                         body=api.SqlBody(sql=f"SELECT count(*) AS n FROM {TABLE}"),
                         table=None, db=None, caller=None))
    assert out["engine"] == "duckdb" and cold_api["pg"] == []


def test_rows_on_a_cold_dataset_go_to_the_engine(cold_api):
    fn = getattr(api.archive_rows, "__wrapped__", api.archive_rows)
    out = asyncio.run(fn(DSID, request=types.SimpleNamespace(query_params={}), limit=50,
                         offset=0, sort=None, order="desc", q=None, table=None, latest=False,
                         as_of=None, current=False, db=None))
    assert out["total"] == 62 and cold_api["pg"] == [] and len(cold_api["rows"]) == 1


def test_rows_on_a_cold_dataset_with_the_engine_off_say_so(cold_api, monkeypatch):
    monkeypatch.setattr(settings, "history_query_enabled", False)
    fn = getattr(api.archive_rows, "__wrapped__", api.archive_rows)
    out = asyncio.run(fn(DSID, request=types.SimpleNamespace(query_params={}), limit=50,
                         offset=0, sort=None, order="desc", q=None, table=None, latest=False,
                         as_of=None, current=False, db=None))
    assert out["total"] == 0 and out["history_note"] and cold_api["rows"] == []


def test_schema_publishes_the_tier(cold_api, monkeypatch):
    async def fake_cols(t):
        return ["pr"]

    async def fake_count(t):
        return 0

    async def fake_summary(db, dataset_id, table):
        return hx.summarize([])

    monkeypatch.setattr(append_store, "user_columns", fake_cols)
    monkeypatch.setattr(append_store, "table_count", fake_count)
    monkeypatch.setattr(hx, "summary_for", fake_summary)
    fn = getattr(api.archive_schema, "__wrapped__", api.archive_schema)
    out = asyncio.run(fn(DSID, request=types.SimpleNamespace(query_params={}), table=None, db=None))
    assert out["history_tier"] == "cold"


def test_a_hot_dataset_schema_says_hot(monkeypatch):
    async def fake_resolve(dataset_id, db, selector=None):
        return _ds({}), TABLE, [{"table": TABLE}]

    async def fake_cols(t):
        return ["pr"]

    async def fake_count(t):
        return 3

    async def fake_summary(db, dataset_id, table):
        return hx.summarize([])

    monkeypatch.setattr(api, "_resolve", fake_resolve)
    monkeypatch.setattr(append_store, "user_columns", fake_cols)
    monkeypatch.setattr(append_store, "table_count", fake_count)
    monkeypatch.setattr(hx, "summary_for", fake_summary)
    fn = getattr(api.archive_schema, "__wrapped__", api.archive_schema)
    out = asyncio.run(fn(DSID, request=types.SimpleNamespace(query_params={}), table=None, db=None))
    assert out["history_tier"] == "hot" and out["total"] == 3


def test_console_note_for_a_cold_table_without_the_switch(monkeypatch):
    from app.api import tables as tapi

    async def fake_pg(sql, **kw):
        return {"columns": ["n"], "fields": [], "rows": [{"n": 0}], "truncated": False,
                "row_count": 1}

    monkeypatch.setattr(append_store, "run_readonly_sql", fake_pg)
    monkeypatch.setattr(append_store, "is_configured", lambda: True)
    monkeypatch.setattr(hq, "_cold_tables", {"rain_forecast_stations", TABLE})
    fn = getattr(tapi.run_sql, "__wrapped__", tapi.run_sql)
    out = asyncio.run(fn(request=types.SimpleNamespace(),
                         body=tapi.SqlBody(sql="SELECT count(*) FROM rain_forecast_stations"),
                         user=None))
    assert out["rows"] == [{"n": 0}] and "כולל היסטוריה" in out["history_note"]
    out = asyncio.run(fn(request=types.SimpleNamespace(),
                         body=tapi.SqlBody(sql="SELECT 1 FROM append_other_x"), user=None))
    assert "history_note" not in out


def test_route_sql_treats_a_union_of_cold_tables_as_cold(monkeypatch):
    async def no_parts(table):
        return []

    async def branches(view):
        return [(RAIN_TABLES[0], "S00")] if view == "rain_forecast_stations" else None

    async def cold(table):
        return [_cpart(table, "k", kind="full", deleted=True)] if table == RAIN_TABLES[0] else []

    called = []

    async def fake_run(sql, **kw):
        called.append(kw)
        return {"rows": [], "engine": "duckdb"}

    monkeypatch.setattr(hq, "cold_parts_for", cold)
    monkeypatch.setattr(hq, "union_branches", branches)
    monkeypatch.setattr(hq, "run", fake_run)
    monkeypatch.setattr(hq, "is_available", lambda: (True, ""))
    monkeypatch.setattr(settings, "history_query_enabled", True)
    out = asyncio.run(hq.route_sql("SELECT 1", tables=[("public", "rain_forecast_stations")],
                                   run_pg=no_parts))
    assert out["engine"] == "duckdb" and called


# ── 10. a poll on a cold dataset ──────────────────────────────────────────────

def test_a_load_into_a_cold_dataset_without_the_promise_is_a_409(monkeypatch):
    from app.api import worker

    with pytest.raises(HTTPException) as e:
        asyncio.run(worker._cold_guard(_ds({"history_tier": "cold"})))
    assert e.value.status_code == 409 and "קר" in str(e.value.detail)


def test_a_load_with_the_promise_rehydrates_first(monkeypatch):
    from app.api import worker

    called = []

    async def fake_rehydrate(dataset_id):
        called.append(str(dataset_id))
        return {"ok": True}

    monkeypatch.setattr(hx, "rehydrate_dataset", fake_rehydrate)
    asyncio.run(worker._cold_guard(_ds({"history_tier": "cold", "history_rehydrate_on_poll": True})))
    assert called == [DSID]


def test_a_failed_rehydrate_blocks_the_load(monkeypatch):
    from app.api import worker

    async def bad(dataset_id):
        return {"ok": False, "error": "hash differs"}

    monkeypatch.setattr(hx, "rehydrate_dataset", bad)
    with pytest.raises(HTTPException) as e:
        asyncio.run(worker._cold_guard(_ds({"history_tier": "cold",
                                            "history_rehydrate_on_poll": True})))
    assert e.value.status_code == 409


def test_a_hot_dataset_is_untouched(monkeypatch):
    from app.api import worker

    async def boom(dataset_id):
        raise AssertionError("rehydrated a hot dataset")

    monkeypatch.setattr(hx, "rehydrate_dataset", boom)
    asyncio.run(worker._cold_guard(_ds({})))


def test_the_guard_runs_before_any_load():
    import pathlib

    root = pathlib.Path(__file__).parent.parent / "app"
    src = (root / "api" / "worker.py").read_text(encoding="utf-8")
    push = src.split("async def push_version(")[1]
    assert "await _cold_guard(ds)" in push
    assert push.index("await _cold_guard(ds)") < push.index("_neon_")
    poll = (root / "worker" / "poll_job.py").read_text(encoding="utf-8")
    body = poll.split("async def _poll_dataset(")[1]
    assert "cold_poll_gate(" in body
    assert body.index("cold_poll_gate(") < body.index("_create_scrape_task(")


# ── 11. candidates and window recommendations ────────────────────────────────

def _cand(**kw):
    base = dict(dataset_id=str(uuid.uuid4()), title="t", status="active", tier="hot",
                last_version_at=NOW - timedelta(days=200), last_access_at=None,
                rows=62_000_000, bytes=9_000_000_000)
    base.update(kw)
    return base


def test_a_stale_and_unread_dataset_is_a_candidate():
    rows = [_cand(title="rain"),
            _cand(title="read yesterday", last_access_at=NOW - timedelta(days=1)),
            _cand(title="updated", last_version_at=NOW - timedelta(days=10)),
            _cand(title="already", tier="cold")]
    got = hx.cold_candidates(rows, now=NOW)
    assert [c["title"] for c in got] == ["rain"]
    assert got[0]["days_since_version"] == 200


def test_access_older_than_the_quiet_window_does_not_save_it():
    got = hx.cold_candidates([_cand(last_access_at=NOW - timedelta(days=91))], now=NOW)
    assert len(got) == 1


def test_window_recommendation_estimates_bytes():
    rec = hx.window_recommendation(table_rows=1_000_000, table_bytes=500_000_000,
                                   closed_old=336_000)
    assert rec == {"closed_over_90_days": 336_000, "est_bytes": 168_000_000}
    assert hx.window_recommendation(table_rows=0, table_bytes=0, closed_old=0)["est_bytes"] == 0


def test_admin_has_the_tier_routes():
    from app.api import admin_storage

    paths = {(tuple(sorted(r.methods)), r.path) for r in admin_storage.router.routes}
    for want in [(("GET",), "/api/admin/storage/history/candidates"),
                 (("PATCH",), "/api/admin/storage/history/windows"),
                 (("POST",), "/api/admin/storage/history/cold/{dataset_id}"),
                 (("POST",), "/api/admin/storage/history/rehydrate/{dataset_id}")]:
        assert want in paths


# ── 12. PATCH /history/windows ────────────────────────────────────────────────

def test_window_items_are_validated_all_or_nothing():
    a, b = str(uuid.uuid4()), str(uuid.uuid4())
    assert hx.validate_window_items([{"dataset_id": a, "days": 30},
                                     {"dataset_id": b, "days": None}]) == [
        (uuid.UUID(a), 30), (uuid.UUID(b), None)]
    with pytest.raises(ValueError) as e:
        hx.validate_window_items([{"dataset_id": a, "days": 30}, {"dataset_id": b, "days": 29}])
    assert b in str(e.value)
    with pytest.raises(ValueError):
        hx.validate_window_items([{"dataset_id": "nope", "days": 30}])
    with pytest.raises(ValueError):
        hx.validate_window_items([{"dataset_id": a, "days": 30}, {"dataset_id": a, "days": 40}])


def test_apply_windows_is_atomic():
    a = _ds({"history_window_days": 60}, id=uuid.uuid4())
    b_id = uuid.uuid4()

    class _Db:
        committed = False

        async def execute(self, q):
            return types.SimpleNamespace(
                scalars=lambda: types.SimpleNamespace(all=lambda: [a]))

        async def commit(self):
            self.committed = True

    db = _Db()
    with pytest.raises(ValueError):
        asyncio.run(hx.apply_windows(db, [(a.id, 30), (b_id, 30)]))
    assert a.scraper_config == {"history_window_days": 60} and not db.committed
    out = asyncio.run(hx.apply_windows(db, [(a.id, None)]))
    assert a.scraper_config == {} and db.committed and out == [
        {"dataset_id": str(a.id), "history_window_days": None}]


# ── 13. archive_state ─────────────────────────────────────────────────────────

def test_fidelity_cold():
    from app.services import archive_state

    st = archive_state.describe({"append_table": "t"}, has_versions=True, plan="neon",
                                archives_neon=True, stores_files=False, cold=True)
    assert st["fidelity"] == "cold"
    st = archive_state.describe({"append_table": "t"}, has_versions=True, plan="neon",
                                archives_neon=True, stores_files=False, has_deleted_parts=True)
    assert st["fidelity"] == "tiered"


# ── migration + model ─────────────────────────────────────────────────────────

def test_migration_073_adds_kind():
    import pathlib

    src = (pathlib.Path(__file__).parent.parent / "alembic" / "versions"
           / "073_history_parts_kind.py").read_text(encoding="utf-8")
    assert 'down_revision = "072"' in src and "DEFAULT 'closed'" in src
    from app.models.history_part import HistoryPart

    assert "kind" in HistoryPart.__table__.columns
