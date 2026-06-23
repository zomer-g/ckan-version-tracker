"""The worker-version dispatch gate must not fail OPEN to a stale worker
when GitHub has a transient blip.

Regression: a flaky GitHub fetch overwrote the cached known-good SHA with
None; the gate then read `required_version is None` and failed open, which
let a worker running pre-fix code grab a govmap task and crash it with the
old WFS ParseError. `_store_sticky` keeps the last known-good value across
failures so the gate keeps a real value to compare against.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import app.services.worker_version as wv  # noqa: E402


def _reset(key):
    wv._cache.pop(key, None)
    wv._last_fetch.pop(key, None)


def test_sticky_keeps_known_good_on_transient_none():
    key = "owner/repo@master"
    _reset(key)
    # a good value was fetched earlier
    assert wv._store_sticky(key, "abc123") == "abc123"
    # GitHub blips → fetch returns None → MUST keep the good value
    assert wv._store_sticky(key, None) == "abc123"
    assert wv._cache[key][1] == "abc123"
    # …repeatedly
    assert wv._store_sticky(key, None) == "abc123"


def test_sticky_updates_on_new_value():
    key = "owner/repo@master"
    _reset(key)
    wv._store_sticky(key, "old")
    assert wv._store_sticky(key, "new") == "new"
    assert wv._cache[key][1] == "new"


def test_sticky_cold_start_none_stays_none():
    key = "owner/repo@cold"
    _reset(key)
    assert wv._store_sticky(key, None) is None


def test_fail_open_default_so_cold_start_doesnt_block_good_workers():
    # Default fail-OPEN: this server reaches GitHub unreliably, so a cold
    # required=None must not block the correct worker. Hard protection is
    # opt-in via worker_required_version + worker_version_fail_closed=True.
    assert wv.settings.worker_version_fail_closed is False


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main([__file__, "-v"]))
