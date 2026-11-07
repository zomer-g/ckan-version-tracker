"""Sign-in is bound to the browser that started it, and only a Google-verified
email signs in."""
import asyncio
import types
from urllib.parse import parse_qs, urlsplit

import httpx

from app.api import oauth
from app.config import settings
from tests import test_oauth_flow as F


def _run(verified=True):
    class _Google(F._FakeGoogle):
        async def get(self, *a, **k):
            return F._FakeResp({"email": "Admin@Example.com", "name": "A",
                                "verified_email": verified})

    async def go():
        settings.google_client_id, settings.google_client_secret = "cid", "csec"
        oauth.httpx = types.SimpleNamespace(AsyncClient=_Google)
        app = F._make_app(await F._setup())
        attacker, victim = F._client(app), F._client(app)
        async with attacker, victim:
            start = await attacker.get("/api/auth/sso/google")
            state = parse_qs(urlsplit(start.headers["location"]).query)["state"][0]
            # The victim follows the attacker's callback link: no state cookie.
            v = await victim.get("/api/auth/sso/google/callback", params={"code": "g", "state": state})
            # The attacker completes their own callback and gets a code...
            a = await attacker.get("/api/auth/sso/google/callback", params={"code": "g", "state": state})
            code = a.headers["location"].split("code=", 1)[1] if "code=" in a.headers["location"] else ""
            # ...which the victim's browser cannot redeem.
            vx = await victim.post("/api/auth/sso/exchange", json={"code": code}) if code else None
            ax = await attacker.post("/api/auth/sso/exchange", json={"code": code}) if code else None
            return v.headers["location"], a.headers["location"], vx, ax

    return asyncio.run(go())


def test_callback_and_code_only_work_in_the_starting_browser():
    v_loc, a_loc, vx, ax = _run()
    assert v_loc == "/admin/login?error=google_failed"
    assert a_loc.startswith("/admin/login?code=")
    assert vx.status_code == 400
    assert ax.status_code == 200


def test_unverified_google_email_is_refused():
    _, a_loc, _, _ = _run(verified=False)
    assert a_loc == "/admin/login?error=no_email"
