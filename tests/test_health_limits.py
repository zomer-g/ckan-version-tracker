"""The cap that decides how much of a public register gets published.

``HEALTH_DEFAULT_LIMITS`` is written into a dataset's scraper_config once, at
creation, and the worker is handed that stored value on every poll. It said
50,000 until 2026-09-25, and רפואה holds 57,530 practitioners: version 6 of
רופאים בעלי רשיון ותחומי מומחיותם stopped at exactly 50,000 and published 87%
of the licensed doctors in the country. It passed shrink_guard because the
registry's move to a new host had doubled its row count, so even truncated it
had more rows than the version before it.

What is pinned here is the property that failure had: the cap must clear the
registries the portal actually holds, because a cap that does not is not a
safety limit — it is an editorial decision nobody made.
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.api.health import (  # noqa: E402
    HEALTH_DEFAULT_LIMITS,
    HEALTH_REGISTRY_LIMITS,
    get_health_limits,
)

#: Measured live on 2026-09-25 against registries.health.gov.il, the portal
#: the practitioners registry moved to: getoskim reports totalRows per
#: profession key. These are the two tracked registries.
MEASURED = {
    "1": 57_530,   # רפואה
    "3": 12_311,   # רוקחים
}


def test_the_default_cap_clears_every_registry_we_track():
    _, max_docs = HEALTH_DEFAULT_LIMITS
    for registry_id, holdings in MEASURED.items():
        assert max_docs > holdings, (
            f"registry {registry_id} holds {holdings} practitioners and the "
            f"default cap is {max_docs} — it would publish part of a register"
        )


def test_the_default_leaves_room_for_registries_we_do_not_track_yet():
    """Nurses are the big one and are not tracked here. A cap sized to the
    largest registry we happen to have seen would be the same mistake one
    dataset later."""
    _, max_docs = HEALTH_DEFAULT_LIMITS
    assert max_docs >= 3 * max(MEASURED.values())


def test_a_registry_override_still_wins():
    """The per-registry table is the place to tune down deliberately; this
    keeps it wired while the default moves."""
    probe = dict(HEALTH_REGISTRY_LIMITS)
    probe["9999"] = (3, 7)
    try:
        HEALTH_REGISTRY_LIMITS.update(probe)
        assert get_health_limits("health_practitioners:9999") == (3, 7)
    finally:
        HEALTH_REGISTRY_LIMITS.pop("9999", None)


def test_an_unknown_registry_gets_the_default():
    assert get_health_limits("health_practitioners:1") == HEALTH_DEFAULT_LIMITS
    assert get_health_limits("health_practitioners") == HEALTH_DEFAULT_LIMITS
    assert get_health_limits("") == HEALTH_DEFAULT_LIMITS
