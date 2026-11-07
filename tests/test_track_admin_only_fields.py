"""POST /api/datasets: a signed-in non-admin cannot set the worker config.

Any Google account can sign in, and its request is created pending with the
config it sent; approval then activated that config unchanged. The anonymous
/requests endpoint already refused a free-form scraper_config; now the
signed-in path does too, for everyone but an admin.
"""
from types import SimpleNamespace

from app.api.datasets import TrackRequest, _without_admin_only_fields
import app.api.datasets as datasets_api


def _body():
    return TrackRequest(
        source_type="scraper", source_url="https://www.gov.il/he/x", title="t",
        scraper_config={"kind": "datacollector_api", "max_docs": 10 ** 9},
        storage_mode="append_only", append_key="id", seen_window_versions=5,
    )


def test_non_admin_fields_reset(monkeypatch):
    monkeypatch.setattr(datasets_api, "is_admin", lambda u: False)
    b = _without_admin_only_fields(_body(), SimpleNamespace(email="x@gmail.com"))
    assert b.scraper_config is None
    assert b.storage_mode == "full_snapshot"
    assert b.append_key is None and b.seen_window_versions is None
    assert b.source_url == "https://www.gov.il/he/x" and b.title == "t"


def test_admin_keeps_everything(monkeypatch):
    monkeypatch.setattr(datasets_api, "is_admin", lambda u: True)
    body = _body()
    assert _without_admin_only_fields(body, SimpleNamespace(email="a@x")) is body
