"""API access log: classification, secret scrubbing and the middleware's row."""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from app.api_access_log_middleware import ApiAccessLogMiddleware
from app.config import settings
from app.services import api_access_log as log


def test_should_log_scope():
    assert log.should_log("GET", "/api/v1/datasets")
    assert log.should_log("POST", "/deals/mcp")
    assert log.should_log("POST", "/mcp")
    assert not log.should_log("OPTIONS", "/api/v1/datasets")
    assert not log.should_log("GET", "/datasets/abc")
    assert not log.should_log("GET", "/assets/index.js")
    assert not log.should_log("GET", "/whatever/mcp")  # only the real MCP servers
    # Worker/admin: only failures (probes) are kept.
    assert log.skip_success("/api/worker/claim", 200)
    assert log.skip_success("/api/admin/pending", 200)
    assert not log.skip_success("/api/admin/pending", 403)
    assert not log.skip_success("/api/administrator", 200)


def test_area():
    assert log.area_of("/api/v1/datasets/x") == "v1"
    assert log.area_of("/api/append/ds/sql") == "append"
    assert log.area_of("/mcp") == "mcp:over"
    assert log.area_of("/deals/mcp") == "mcp:deals"
    assert log.area_of("/data/mcp/") == "mcp:data"


def test_client_family():
    assert log.client_family("curl/8.4.0") == "curl"
    assert log.client_family("python-requests/2.31") == "python-requests"
    assert log.client_family("Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 Chrome/128.0 Safari/537.36") == "Chrome"
    assert log.client_family("Mozilla/5.0 ... Chrome/128 Safari/537.36 Edg/128.0") == "Edge"
    assert log.client_family("Mozilla/5.0 (compatible; Google-Apps-Script; beanserver)") == "Google Apps Script"
    assert log.client_family("Claude-User/1.0") == "Claude"
    assert log.client_family("") is None


def test_channel():
    browser = "Mozilla/5.0 Chrome/128 Safari/537.36"
    assert log.channel_of("v1", {"sec-fetch-site": "same-origin", "user-agent": browser}, "Chrome") == "site"
    assert log.channel_of("v1", {"sec-fetch-site": "cross-site", "user-agent": browser}, "Chrome") == "browser"
    assert log.channel_of("v1", {"user-agent": "curl/8"}, "curl") == "script"
    assert log.channel_of("v1", {}, None) == "script"
    assert log.channel_of("v1", {"user-agent": "Googlebot/2.1"}, "Googlebot") == "bot"
    assert log.channel_of("mcp:over", {"user-agent": "curl/8"}, "curl") == "mcp"


def test_scrub_query_keeps_search_terms_and_hides_secrets():
    q = log.scrub_query("q=abc&keyword=x&settlement_code=70&api_key=S&sso_token=T&code=C&limit=5&sid=Z&X-Amz-Credential=W")
    assert "sid=%2A%2A%2A" in q and "X-Amz-Credential=%2A%2A%2A" in q
    assert "S" not in q.split("api_key=")[1][:1]
    assert "api_key=%2A%2A%2A" in q and "sso_token=%2A%2A%2A" in q and "code=%2A%2A%2A" in q
    assert "keyword=x" in q and "settlement_code=70" in q and "limit=5" in q
    assert log.scrub_query("") is None


def test_target_of():
    assert log.target_of({"dataset_id": "abc", "x": "y"}) == "abc"
    assert log.target_of({"foo": "bar"}) == "foo=bar"
    assert log.target_of({}) is None


def test_actor_from_bearer_is_never_the_token(monkeypatch):
    monkeypatch.setattr(settings, "sql_service_token", "svc-secret")
    kind, aid, label = log.resolve_actor({}, {"authorization": "Bearer svc-secret"}, "/api/append/x/sql", "1.2.3.4")
    assert (kind, aid) == ("sql_service", None) and "svc-secret" not in (label or "")
    kind, aid, _ = log.resolve_actor({}, {"authorization": "Bearer garbage"}, "/api/v1/x", "1.2.3.4")
    assert (kind, aid) == ("bearer_invalid", None)
    assert log.resolve_actor({}, {}, "/api/v1/x", "1.2.3.4")[0] == "anonymous"
    stamped = {"api_actor": ("mcp_user", "u-1", "a@b.c")}
    assert log.resolve_actor(stamped, {}, "/mcp", "1.2.3.4") == ("mcp_user", "u-1", "a@b.c")


def _app():
    app = FastAPI()

    @app.get("/api/v1/datasets/{dataset_id}")
    async def one(dataset_id: str, request: Request):
        log.stamp_actor(request, "user", "u-42", None)
        return {"id": dataset_id, "pad": "x" * 100}

    @app.get("/api/worker/claim")
    async def worker():
        return {}

    app.add_middleware(ApiAccessLogMiddleware)
    return app


def test_middleware_records_one_row(monkeypatch):
    rows = []
    monkeypatch.setattr(log, "enqueue", rows.append)
    monkeypatch.setattr(settings, "api_access_log_enabled", True)
    monkeypatch.setattr(settings, "maintenance_mode", False)
    c = TestClient(_app())
    r = c.get("/api/v1/datasets/abc?limit=5&token=zzz",
              headers={"user-agent": "python-requests/2.31", "cf-ipcountry": "IL",
                       "cf-connecting-ip": "8.8.4.4", "x-forwarded-for": "8.8.4.4, 172.64.0.1"})
    assert r.status_code == 200
    c.get("/api/worker/claim")
    assert len(rows) == 1
    row = dict(rows[0])
    assert row["route"] == "/api/v1/datasets/{dataset_id}"
    assert row["target"] == "abc"
    assert row["area"] == "v1"
    assert row["status"] == 200
    assert row["bytes_out"] == len(r.content)
    assert row["client"] == "python-requests" and row["channel"] == "script"
    assert (row["actor_kind"], row["actor_id"]) == ("user", "u-42")
    assert row["country"] == "IL" and row["ip"] == "8.8.4.4"
    assert "zzz" not in (row["query"] or "")
    # Not through Cloudflare: CF-IPCountry is ignored, it could be forged.
    c.get("/api/v1/datasets/abc", headers={"cf-ipcountry": "XX", "x-forwarded-for": "9.9.9.9"})
    assert rows[-1]["country"] is None
    # A 404 scan keeps a short path and no query.
    c.get("/api/" + "a" * 300 + "?q=secretish")
    assert rows[-1]["route"] is None and len(rows[-1]["path"]) == 120 and rows[-1]["query"] is None
    # A non-ASCII bearer must not drop the row.
    c.get("/api/v1/datasets/abc", headers={"authorization": "Bearer \xe9".encode("latin-1")})
    assert rows[-1]["path"] == "/api/v1/datasets/abc"


def test_middleware_off_when_disabled(monkeypatch):
    rows = []
    monkeypatch.setattr(log, "enqueue", rows.append)
    monkeypatch.setattr(settings, "api_access_log_enabled", False)
    TestClient(_app()).get("/api/v1/datasets/abc")
    assert rows == []


def test_per_ip_cap_collapses_a_flood(monkeypatch):
    monkeypatch.setattr(settings, "api_access_log_enabled", True)
    monkeypatch.setattr(settings, "maintenance_mode", False)
    monkeypatch.setattr(log, "_buffer", __import__("collections").deque())
    monkeypatch.setattr(log, "_ip_minute", {})
    monkeypatch.setattr(log.asyncio, "get_running_loop", lambda: (_ for _ in ()).throw(RuntimeError()))
    now = [1_000_000.0]
    monkeypatch.setattr(log.time, "time", lambda: now[0])
    base = {"ip": "9.9.9.9", "area": "v1", "channel": "script", "actor_kind": "anonymous"}
    for _ in range(log.PER_IP_PER_MINUTE + 50):
        log.enqueue(dict(base))
    assert len(log._buffer) == log.PER_IP_PER_MINUTE
    now[0] += 60
    log.enqueue(dict(base))
    rows = list(log._buffer)
    summary = [r for r in rows if r.get("path") == "(suppressed)"]
    assert len(summary) == 1 and summary[0]["actor_label"].startswith("50 ")
    assert len(rows) == log.PER_IP_PER_MINUTE + 2


def test_flush_failure_does_not_log_row_contents(monkeypatch, caplog):
    import asyncio, collections
    monkeypatch.setattr(log, "_buffer", collections.deque([{"ip": "203.0.113.77"}]))
    monkeypatch.setattr(log, "_last_warn", 0.0)

    class Boom:
        async def __aenter__(self):
            raise RuntimeError("[parameters: 203.0.113.77 secret@example.com]")
        async def __aexit__(self, *a):
            return False
    import app.database as D
    monkeypatch.setattr(D, "async_session", lambda: Boom())
    caplog.set_level("WARNING")
    asyncio.run(log.flush())
    assert "RuntimeError" in caplog.text
    assert "203.0.113.77" not in caplog.text and "secret@" not in caplog.text


def test_admin_queries_name_every_table_in_full():
    """users lives in `auth`, outside the app search_path: a bare `users` 500s."""
    import asyncio
    import app.api.admin_api_access as A
    seen = []

    async def fake(db, sql, params):
        seen.append(sql)
        return [{"first_ts": None, "last_ts": None}] if "percentile_cont" in sql else []

    orig = A._rows
    A._rows = fake
    try:
        f = A._filters(days=7, exclude_site=True, area=None, channel=None, actor_kind=None, ip=None,
                       actor_id=None, client=None, status=None, route=None, target=None)
        stats = getattr(A.api_access_stats, "__wrapped__", A.api_access_stats)
        recent = getattr(A.api_access_recent, "__wrapped__", A.api_access_recent)
        asyncio.run(stats(None, f, None, None))
        asyncio.run(recent(None, f, 10, 0, None, None))
    finally:
        A._rows = orig
    joins = [s for s in seen if " JOIN " in s]
    assert len(joins) == 2
    assert all("JOIN auth.users u" in s for s in joins)
