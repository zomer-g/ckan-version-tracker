"""CSP (Report-Only by default), HSTS and Permissions-Policy, and the report sink."""
import base64
import hashlib

from fastapi.testclient import TestClient

import app.main as M
from app import security_headers as S
from app.config import settings

SHELL = """<html><head>
<script>(function(){var t=1;})();</script>
<script async src="https://www.googletagmanager.com/gtag/js?id=X"></script>
<script type="application/ld+json">{"@context":"https://schema.org"}</script>
<script>
  gtag('js', new Date());
</script></head></html>"""


def _h(body):
    return "'sha256-" + base64.b64encode(hashlib.sha256(body.encode()).digest()).decode() + "'"


def test_shell_inline_scripts_are_hashed_and_json_ld_skipped():
    S.register_shell(SHELL)
    v = S.csp_value()
    assert _h("(function(){var t=1;})();") in v
    assert _h("\n  gtag('js', new Date());\n") in v
    assert _h('{"@context":"https://schema.org"}') not in v
    assert "'unsafe-inline'" not in v.split("script-src", 1)[1].split(";", 1)[0]
    assert "frame-ancestors 'self'" in v and "object-src 'none'" in v


class _Resp:
    def __init__(self, ctype):
        self.headers = {"content-type": ctype}


class _Req:
    def __init__(self, path, scheme="https"):
        self.url = type("U", (), {"path": path, "scheme": scheme})()


def test_report_only_by_default_and_enforced_when_asked(monkeypatch):
    monkeypatch.setattr(settings, "csp_enforce", False)
    r = _Resp("text/html; charset=utf-8")
    S.apply(_Req("/"), r)
    assert "Content-Security-Policy-Report-Only" in r.headers
    assert "Content-Security-Policy" not in r.headers
    assert r.headers["Strict-Transport-Security"].startswith("max-age=")
    assert "camera=()" in r.headers["Permissions-Policy"]
    monkeypatch.setattr(settings, "csp_enforce", True)
    r = _Resp("text/html")
    S.apply(_Req("/"), r)
    assert "Content-Security-Policy" in r.headers


def test_no_csp_on_json_or_story_pages_and_no_hsts_on_http():
    r = _Resp("application/json")
    S.apply(_Req("/api/x"), r)
    assert not any(k.startswith("Content-Security") for k in r.headers)
    r = _Resp("text/html")
    S.apply(_Req("/story/deck"), r)
    assert not any(k.startswith("Content-Security") for k in r.headers)
    r = _Resp("text/html")
    S.apply(_Req("/", scheme="http"), r)
    assert "Strict-Transport-Security" not in r.headers


def test_report_endpoint_accepts_both_formats():
    c = TestClient(M.app)
    legacy = {"csp-report": {"violated-directive": "script-src",
                             "blocked-uri": "https://evil.example/x.js?q=1",
                             "document-uri": "https://over.org.il/data?sql=x"}}
    assert c.post("/api/csp-report", json=legacy).status_code == 204
    modern = [{"type": "csp-violation", "body": {"effectiveDirective": "img-src",
                                                 "blockedURL": "data", "documentURL": "/"}}]
    assert c.post("/api/csp-report", json=modern).status_code == 204
    assert c.post("/api/csp-report", content=b"not json").status_code == 204
