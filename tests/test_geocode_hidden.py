"""נדל"ן לעם — the geocoding ledger is tracked but not published.

Two independent surfaces have to be closed, and closing either one alone leaves
the table readable:

  * the /data CATALOG, which is what a person browsing sees;
  * the read-only ROLE, because the console runs free-form SQL and will happily
    answer `SELECT * FROM over_re_geocode` for a table no catalog mentions.
    Measured before the fix: 111,908 rows to an anonymous caller.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import inspect  # noqa: E402

from app.services import data_catalog, geocode_queue as gq  # noqa: E402


def test_the_ledger_is_not_in_the_data_catalog():
    assert gq.GEOCODE_TABLE in data_catalog._OVER_HIDDEN
    src = inspect.getsource(data_catalog._over_index_records)
    assert "_OVER_HIDDEN" in src, "the denylist must actually be applied"


def test_the_published_crosswalk_tables_are_untouched():
    """Hiding the work log must not hide the product."""
    for t in ("over_re_addresses", "over_re_parcels", "over_re_streets",
              "over_re_zip5", "over_re_parcel_gazetteer"):
        assert t not in data_catalog._OVER_HIDDEN
        assert t in data_catalog._OVER_TITLES


def test_the_console_role_loses_the_grant_on_every_ensure():
    """The role never gets an explicit grant here — it inherits one from
    ALTER DEFAULT PRIVILEGES, which re-applies whenever the table is recreated.
    So the revoke has to be re-asserted, not run once by hand."""
    assert "_revoke_from_public_console" in inspect.getsource(gq.ensure_tables)
    src = inspect.getsource(gq._revoke_from_public_console)
    assert "REVOKE" in src and "GEOCODE_TABLE" in src


def test_no_read_of_the_ledger_uses_the_console_role():
    """The revoke would break these. Every one is an admin or worker path."""
    src = inspect.getsource(gq)
    assert "get_readonly_pool" not in src, \
        "a read on the console role would fail once the grant is revoked"


def test_the_dataset_is_created_unpublished():
    assert 'status="hidden"' in inspect.getsource(gq.ensure_dataset)


# ── the by-id paths ───────────────────────────────────────────────────────────
def test_by_id_endpoints_are_gated():
    """The lists filter `status IN (active, pending)`, so a hidden dataset drops
    out of them for free. The BY-ID paths do not: they load by primary key and
    404 only if the row is missing. Measured in production before this fix —
    `/api/v1/datasets/{id}` returned 200 for the hidden dataset while
    `/api/datasets/public/{id}` correctly returned 404. Its versions and its
    files were reachable to anyone holding the UUID."""
    import app.api.v1 as v1
    import app.api.versions as versions_api

    v1_src = inspect.getsource(v1)
    assert "require_visible(ds)" in v1_src
    # the shared helper covers /status and all three version routes
    assert "return require_visible(ds)" in inspect.getsource(v1._get_dataset_or_404)

    src = inspect.getsource(versions_api)
    assert src.count("require_visible") >= 4, \
        "list, detail, download and symbology all resolve by id"


def test_only_hidden_is_excluded_not_every_non_public_status():
    """Restricting the by-id paths to the public statuses would 404 duplicates
    and failed datasets too — behaviour well beyond the request, and it would
    break existing links."""
    from app.services import dataset_visibility as dv

    class _DS:
        def __init__(self, status): self.status = status

    assert dv.is_hidden(_DS("hidden"))
    for still_served in ("active", "pending", "duplicate", "failed", "rejected"):
        assert not dv.is_hidden(_DS(still_served))
        dv.require_visible(_DS(still_served))   # must not raise


def test_a_hidden_dataset_404s_rather_than_403s():
    """A 403 would confirm the id is real."""
    import pytest
    from fastapi import HTTPException
    from app.services import dataset_visibility as dv

    class _DS: status = "hidden"
    with pytest.raises(HTTPException) as e:
        dv.require_visible(_DS())
    assert e.value.status_code == 404
