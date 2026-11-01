"""Atlas layers are their own source.

Revision ID: 075
Revises: 074
Create Date: 2026-11-01

The 290 layers requested from אטלס ישראל (https://www.atlas.gov.il/) on
2026-11-01 were created as plain GovMap datasets (ckan_id ``govmap-…``,
organization ``govmap.gov.il``). They stay GovMap layers (source_type
``govmap``, the GovMap pipeline), but they are a source of their own: the
``atlas-scraper-`` ckan_id prefix is what source_load.source_key reads, so
Atlas gets its own chip, its own /sources row and its own worker cap. New Atlas
datasets are created that way (app/api/govmap.govmap_dataset_origin); this
moves the ones created before it.

Only rows whose source_url is an Atlas link are touched. A layer that is also
in GovMap's public catalog and was already tracked from govmap.gov.il keeps its
GovMap identity.

The downgrade puts them back under GovMap.
"""
from alembic import op

revision = "075"
down_revision = "074"
branch_labels = None
depends_on = None

_ATLAS_ROWS = (
    "source_type = 'govmap' "
    "AND (source_url LIKE 'https://www.atlas.gov.il/%' "
    "OR source_url LIKE 'https://atlas.gov.il/%')"
)


def upgrade() -> None:
    op.execute(
        "UPDATE app.tracked_datasets "
        "SET ckan_id = 'atlas-scraper-' || substr(ckan_id, length('govmap-') + 1), "
        "organization = 'atlas.gov.il' "
        f"WHERE {_ATLAS_ROWS} AND ckan_id LIKE 'govmap-%'"
    )


def downgrade() -> None:
    op.execute(
        "UPDATE app.tracked_datasets "
        "SET ckan_id = 'govmap-' || substr(ckan_id, length('atlas-scraper-') + 1), "
        "organization = 'govmap.gov.il' "
        f"WHERE {_ATLAS_ROWS} AND ckan_id LIKE 'atlas-scraper-%'"
    )
