"""The Referer must never carry the /data console's SQL.

The console keeps the running query in the page URL so it can be shared and
reloaded. Under the browser default (strict-origin-when-cross-origin, which
sends the FULL url on same-origin requests) that put raw SQL into the Referer of
every API call, and a WAF inspecting Referer for SQL keywords answered 403 —
verified against production, where the same request passed with a plain Referer
and was blocked with the query in it. The symptom was brutal to diagnose:
opening a shared query link left the console unable to run anything in that tab,
permanently, with an error that pointed at rate limiting.

`strict-origin` sends only the origin. This test exists because the protection
is invisible in normal use — nothing fails when it is dropped, until a WAF rule
somewhere decides the Referer looks like an attack.
"""
import os

from fastapi.testclient import TestClient

os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.main import app  # noqa: E402

client = TestClient(app)


def test_responses_carry_a_referrer_policy():
    r = client.get("/api/tables")
    assert r.headers.get("referrer-policy") == "strict-origin"


def test_policy_is_on_error_responses_too():
    # A 404 or a 403 is a response the browser learns the policy from just the
    # same — and an error page is exactly where a user retries, which is when
    # the poisoned Referer would strike again.
    r = client.get("/api/definitely-not-a-route")
    assert r.status_code == 404
    assert r.headers.get("referrer-policy") == "strict-origin"


def test_the_static_page_declares_it_as_well():
    # The response header covers what this app serves; the meta tag travels with
    # the built HTML wherever it is cached or previewed.
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    html = open(os.path.join(here, "frontend", "index.html"), encoding="utf-8").read()
    assert '<meta name="referrer" content="strict-origin"' in html
