"""Re-pointing a dataset at a different source page.

GovMap renumbers layer ids — measured 1.8.2026: of 22 layers its catalog no
longer lists, 3 exist today under a new id with the same fields. The scraper's
own failure message says to "update the dataset's lay= id", and until now the
admin API had no field that could. These tests cover the validation, since a
bad re-point silently turns every future scrape into "No 'lay' param in URL".
"""
import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fastapi import HTTPException  # noqa: E402

import app.api.datasets as datasets  # noqa: E402


class _DS:
    def __init__(self, source_type="govmap", ds_id="11111111-1111-1111-1111-111111111111"):
        self.id = ds_id
        self.source_type = source_type
        self.source_url = "https://www.govmap.gov.il/?lay=407"
        self.scraper_config = {"kind": "govmap", "layer_id": "407"}


def _apply(ds, url, existing=None):
    """Call the validator with find_datasets_for_url stubbed.

    The repo has no pytest-asyncio; async helpers are driven with asyncio.run,
    same as tests/test_append_api_multi.py.
    """
    calls = list(existing or [])

    async def fake_lookup(db, u, strict=False):
        return calls

    orig = datasets.find_datasets_for_url
    datasets.find_datasets_for_url = fake_lookup
    try:
        return asyncio.run(datasets._apply_source_url(ds, url, db=None))
    finally:
        datasets.find_datasets_for_url = orig


def test_a_valid_govmap_repoint_is_accepted():
    url = "https://www.govmap.gov.il/?lay=238837"
    assert _apply(_DS(), url) == url


def test_whitespace_is_trimmed():
    assert _apply(_DS(), "  https://www.govmap.gov.il/?lay=238837  ") == \
        "https://www.govmap.gov.il/?lay=238837"


def test_a_govmap_url_without_a_layer_is_refused():
    # Storing this would make every future scrape fail with "No 'lay' param".
    with pytest.raises(HTTPException) as e:
        _apply(_DS(), "https://www.govmap.gov.il/")
    assert e.value.status_code == 400


def test_an_empty_url_is_refused():
    with pytest.raises(HTTPException):
        _apply(_DS(), "   ")


def test_ckan_cannot_be_repointed():
    # A CKAN dataset is addressed by package name; a stored URL would do nothing.
    with pytest.raises(HTTPException) as e:
        _apply(_DS(source_type="ckan"), "https://data.gov.il/dataset/x")
    assert "package name" in e.value.detail


def test_repointing_onto_an_already_tracked_layer_is_refused():
    # This is how the duplicates migration 046 had to merge got created.
    other = [{"id": "22222222-2222-2222-2222-222222222222", "title": "תחנות רכבת"}]
    with pytest.raises(HTTPException) as e:
        _apply(_DS(), "https://www.govmap.gov.il/?lay=238837", existing=other)
    assert "already tracked" in e.value.detail


def test_matching_only_itself_is_not_a_clash():
    # The resolver legitimately returns THIS dataset when the URL is unchanged
    # in identity (e.g. the same layer with different viewport params).
    me = [{"id": "11111111-1111-1111-1111-111111111111", "title": "עצמי"}]
    url = "https://www.govmap.gov.il/?c=1,2&lay=407&z=6"
    assert _apply(_DS(), url, existing=me) == url
