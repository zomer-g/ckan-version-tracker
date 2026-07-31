"""The public API's `source_url` must be where the dataset actually comes from.

Only CKAN datasets are on data.gov.il and only they lack a stored source_url.
Gating the passthrough on `source_type == "scraper"` sent every other kind
through the data.gov.il builder, which fabricates a link from the internal slug:
897 govmap datasets and one cbs dataset advertised a data.gov.il dataset that
does not exist instead of the govmap.gov.il layer they track.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import app.api.v1 as v1  # noqa: E402


class _DS:
    def __init__(self, **kw):
        self.source_type = "ckan"
        self.source_url = None
        self.organization = None
        self.ckan_name = None
        self.resource_id = None
        self.__dict__.update(kw)


def test_govmap_reports_its_layer_url():
    ds = _DS(source_type="govmap",
             source_url="https://www.govmap.gov.il/?lay=228014",
             organization="govmap.gov.il",
             ckan_name="govmap-228014-4c1ba63b")
    assert v1._source_url(ds) == "https://www.govmap.gov.il/?lay=228014"


def test_cbs_reports_its_own_url():
    ds = _DS(source_type="cbs", source_url="https://www.cbs.gov.il/he/Pages/default.aspx",
             organization="cbs", ckan_name="cbs-index")
    assert v1._source_url(ds) == "https://www.cbs.gov.il/he/Pages/default.aspx"


def test_scraper_is_unchanged():
    ds = _DS(source_type="scraper", source_url="https://www.gov.il/he/departments/x",
             organization="gov", ckan_name="x")
    assert v1._source_url(ds) == "https://www.gov.il/he/departments/x"


def test_ckan_still_builds_the_data_gov_il_permalink():
    ds = _DS(source_type="ckan", organization="ministry-of-transport", ckan_name="bus-lines")
    url = v1._source_url(ds)
    assert url.endswith("/he/datasets/ministry-of-transport/bus-lines")


def test_ckan_permalink_keeps_the_resource_segment():
    ds = _DS(source_type="ckan", organization="org", ckan_name="pkg", resource_id="res-1")
    assert v1._source_url(ds).endswith("/he/datasets/org/pkg/res-1")


def test_a_source_type_without_a_stored_url_falls_back():
    # Defensive: a govmap row whose source_url was never written still gets a
    # link rather than an empty string.
    ds = _DS(source_type="govmap", source_url=None,
             organization="govmap.gov.il", ckan_name="govmap-11-abc")
    assert v1._source_url(ds).endswith("/he/datasets/govmap.gov.il/govmap-11-abc")
