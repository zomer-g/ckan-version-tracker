"""Stored function bodies are recreated when the PostGIS schema they name moves."""
import asyncio
from contextlib import asynccontextmanager

from app.services import postgis_functions as pf
from app.services import postgis_schema


class _Conn:
    def __init__(self, stale):
        self.stale, self.calls = stale, []

    async def fetch(self, sql, schemas, funcs, q):
        self.calls.append((schemas, funcs, q))
        return [{"nspname": s, "proname": f} for s, f in self.stale]


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


def _run(monkeypatch, stale, writers=True):
    from app.config import settings
    from app.services import append_store, index_mirror, nadlan_index
    conn, done = _Conn(stale), []
    monkeypatch.setattr(type(settings), "writers_enabled", property(lambda s: writers))
    monkeypatch.setattr(settings, "append_database_url", "postgresql://x/y")

    async def get_pool():
        return _Pool(conn)

    async def ensure_schema(c):
        done.append("mirror")

    async def ensure_functions():
        done.append("nadlan")

    monkeypatch.setattr(append_store, "get_pool", get_pool)
    monkeypatch.setattr(index_mirror, "ensure_schema", ensure_schema)
    monkeypatch.setattr(nadlan_index, "ensure_functions", ensure_functions)
    monkeypatch.setattr(postgis_schema, "_schema", "postgis")
    return asyncio.run(pf.refresh_if_stale()), conn, done


def test_nothing_stale_recreates_nothing(monkeypatch):
    out, conn, done = _run(monkeypatch, [])
    assert done == [] and out["recreated"] == []
    assert conn.calls[0][2] == '"postgis"'


def test_stale_bodies_are_recreated_by_their_owner(monkeypatch):
    out, _conn, done = _run(monkeypatch, [("public", "over_parcel_at"), ("idx", "try_geom")])
    assert done == ["mirror", "nadlan"]
    assert out["recreated"] == ["idx.try_geom", "public.over_parcel_at"]


def test_a_reader_instance_never_writes(monkeypatch):
    out, conn, done = _run(monkeypatch, [("public", "over_parcel_at")], writers=False)
    assert out == {"skipped": "not a writer"} and conn.calls == [] and done == []


def test_a_schema_change_schedules_a_refresh(monkeypatch):
    calls = []
    monkeypatch.setattr(pf, "schedule_refresh", lambda: calls.append(1))
    monkeypatch.setattr(postgis_schema, "_schema", "extensions")
    postgis_schema.set_schema("extensions")
    assert calls == []
    postgis_schema.set_schema("postgis")
    assert calls == [1]
    monkeypatch.setattr(postgis_schema, "_schema", "extensions")
