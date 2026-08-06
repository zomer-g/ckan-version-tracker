"""Every list payload that renders a source chip must carry ``ckan_id``.

The bug this locks: on a tag page (``/tags/<id>``) datasets from מרשם ההסכמים
הקיבוציים, מצב השלטון המקומי, חוזרי מנכ"ל and friends all wore the generic
GOV.IL chip, while the /sources page listed them as the separate sources they
are. Same data, two answers.

The cause was not in the badge logic but in the payload feeding it.
``frontend/src/utils/sourceBadge.ts`` decides a scraper dataset's source from
the ``ckan_id`` PREFIX ("munidata-scraper-", "mankal-scraper-", …) — stamped at
create time and never changed. ``organization`` is only a fallback hint, and
deliberately a weak one: ministry slugs are shared between sources, and admins
reassign them. Three endpoints serialized their datasets without ``ckan_id`` at
all, so every scraper dataset fell off the end of the ladder to GOV.IL.

Also, for the runtime-registered (manifest-declared) sources, the ckan_id
prefix is the ONLY signal that exists — there is no org hint to fall back on,
which is why the tag page collapsed so many distinct sources into one chip.

So the invariant is a payload contract, not a rendering detail: if a response
model describes a dataset that gets a source chip, it exposes ``ckan_id``.
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402

from app.api.organizations import DatasetMini  # noqa: E402
from app.api.resolve import ResolveMatch  # noqa: E402
from app.api.tags import TagDatasetMini  # noqa: E402
from app.services.dataset_lookup import find_datasets_for_url  # noqa: E402

from test_url_resolve import _seed  # noqa: E402


@pytest.mark.parametrize(
    "model",
    [TagDatasetMini, DatasetMini, ResolveMatch],
    ids=["tag detail", "organization detail", "url resolve"],
)
def test_dataset_payloads_expose_the_source_chip_signal(model):
    assert "ckan_id" in model.model_fields, (
        f"{model.__name__} feeds a SourceChip; without ckan_id every scraper "
        "dataset in it renders as GOV.IL"
    )


def test_the_resolver_returns_the_ckan_id_it_matched_on():
    """The payload contract above is only worth anything if the value is real."""
    async def go():
        Session = await _seed([
            {
                "ckan_name": "munidata-hr-seniors",
                "ckan_id": "munidata-scraper-hr-seniors",
                "title": "מצב השלטון המקומי — ותק מנהלים",
                "source_type": "scraper",
                "source_url": "https://municipal-data.org/dashboard/hr",
            },
        ])
        async with Session() as db:
            return await find_datasets_for_url(
                db, "https://municipal-data.org/dashboard/hr"
            )

    matches = asyncio.run(go())
    assert len(matches) == 1
    # Not "some non-empty string": the PREFIX is what the client matches on.
    assert matches[0]["ckan_id"].startswith("munidata-scraper-")
