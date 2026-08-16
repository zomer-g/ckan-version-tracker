"""A registered source may claim a gov.il page; nothing else may be shadowed.

The manifest registry is consulted LAST, so a broadly-written manifest regex
can never steal a URL from one of the hardcoded parsers. gov.il's three page
types are the exception that has to exist: they match EVERY page on
www.gov.il by construction, so without a carve-out a source written for one
specific gov.il page (localgrants → /he/pages/grants-grants) could never be
reached at all — the dataset would be created as generic gov.il no matter what
the manifest said.

What must stay true is that the carve-out is exactly three page types wide.
"""
from __future__ import annotations

import pytest

from app.api.datasets import GENERIC_GOVIL_PAGE_TYPES
from app.api.govil import _parse_govil_url


def test_the_carve_out_is_only_the_catch_alls():
    assert GENERIC_GOVIL_PAGE_TYPES == {
        "content_page", "dynamic_collector", "traditional_collector",
    }


@pytest.mark.parametrize("url,expected", [
    ("https://www.gov.il/he/pages/grants-grants?chapterIndex=5", "content_page"),
    ("https://www.gov.il/he/pages/guidelines_state", "content_page"),
    ("https://www.gov.il/he/departments/dynamiccollectors/x", "dynamic_collector"),
    ("https://www.gov.il/he/collectors/policies", "traditional_collector"),
])
def test_gov_il_catch_alls_are_overridable(url, expected):
    """Each of these claims the URL first today, and each must be beatable."""
    page_type, collector_name = _parse_govil_url(url)
    assert page_type == expected
    assert collector_name
    assert page_type in GENERIC_GOVIL_PAGE_TYPES


def test_a_specific_hardcoded_source_stays_unshadowable():
    """The named sources are keyed to hosts of their own; a manifest must not
    be able to take one of those URLs, which is what the ordering protects."""
    from app.api.health import _parse_health_url
    from app.api.knesset import _parse_knesset_url

    for parse, url in [
        (_parse_health_url, "https://practitioners.health.gov.il/"),
        (_parse_knesset_url,
         "https://main.knesset.gov.il/apps/legislation/main/bills/2"),
    ]:
        page_type, collector_name = parse(url)
        if collector_name:
            assert page_type not in GENERIC_GOVIL_PAGE_TYPES, (page_type, url)


def test_the_raw_collector_api_type_is_not_overridable():
    """data_collector_api is a shape, not a page — it names a specific backend
    and is collected locally, so a manifest must not be able to divert it."""
    page_type, collector_name = _parse_govil_url(
        "https://openapi-gc.digital.gov.il/collectors/v1/api/DataCollector/GetResults")
    assert collector_name
    assert page_type == "data_collector_api"
    assert page_type not in GENERIC_GOVIL_PAGE_TYPES


def test_both_creation_paths_apply_the_same_rule():
    """The tracking path and the request path each have their own copy of the
    branch; a rule applied in one and not the other means the card and the
    dataset disagree."""
    import inspect

    from app.api import datasets

    source = inspect.getsource(datasets)
    guard = "if not collector_name or page_type in GENERIC_GOVIL_PAGE_TYPES:"
    assert source.count(guard) == 2, (
        "expected the registry-precedence guard in both the tracking and the "
        "request creation paths")
