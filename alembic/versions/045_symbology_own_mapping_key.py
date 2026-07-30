"""File the GovMap documentation bundle under its own mapping key

A govmap version carries, besides its data, a documentation bundle: the layer's
OGC SLD symbology plus the field dictionary that maps the CSV's machine column
names to their Hebrew aliases. The worker has no dedicated channel for it, so it
rides in on the generic attachment upload and lands in `_zip_parts` — where the
site labels it "קבצים מצורפים (ZIP)" (it is not attachments; the layer publishes
none) and the public API drops it entirely, because list-valued mapping keys
other than `_geojson` were never emitted as resources.

The code fix ships in the same deploy (worker.py routes it to `_symbology`,
v1.py emits every list-valued key). This moves the versions already committed:

  * every `_zip_parts` element whose object key ends `_symbology.zip` /
    `_fields.zip` moves to a `_symbology` list, appended to whatever is already
    there; `_zip_parts` keeps the rest and is dropped when nothing remains,
  * the same for a scalar `_zip` that is a documentation bundle.

The filename is the only signal a mapping value carries, and it exists solely on
R2 keys — an ODATA resource_id is an opaque UUID — so the match is anchored to
`r2:`. Every govmap version is R2-backed, so nothing real is left behind.

No bytes move: the R2 objects keep their keys, and both old and new mappings
point at the same values, so existing download links stay valid throughout.

Revision ID: 045
Revises: 044
Create Date: 2026-07-30
"""
from typing import Sequence, Union

from alembic import op


revision: str = "045"
down_revision: Union[str, None] = "044"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Anchored to r2: (see the module docstring) and to the key tail written by
# storage_client.build_key — `<rand8>_<safe filename>`, where the Hebrew layer
# name collapses away and leaves `…_symbology.zip` / `…_fields.zip`.
_DOC = r"^r2:.*_(symbology|fields)\.zip$"


def upgrade() -> None:
    # 1) Multipart channel: split each version's `_zip_parts` in place.
    op.execute(
        f"""
        WITH split AS (
            SELECT v.id,
                   jsonb_agg(e.val) FILTER (WHERE e.val #>> '{{}}' ~ '{_DOC}')
                       AS docs,
                   jsonb_agg(e.val) FILTER (WHERE NOT (e.val #>> '{{}}' ~ '{_DOC}'))
                       AS rest
            FROM version_index v,
                 LATERAL jsonb_array_elements(v.resource_mappings->'_zip_parts')
                      AS e(val)
            WHERE jsonb_typeof(v.resource_mappings->'_zip_parts') = 'array'
            GROUP BY v.id
        )
        UPDATE version_index v
        SET resource_mappings =
            (CASE WHEN s.rest IS NULL
                  THEN v.resource_mappings - '_zip_parts'
                  ELSE jsonb_set(v.resource_mappings, '{{_zip_parts}}', s.rest)
             END)
            || jsonb_build_object(
                   '_symbology',
                   COALESCE(v.resource_mappings->'_symbology', '[]'::jsonb)
                       || s.docs)
        FROM split s
        WHERE v.id = s.id AND s.docs IS NOT NULL
        """
    )

    # 2) Single-ZIP channel: a scalar `_zip` that is really a documentation
    #    bundle. Rare (the worker uses the list form), but the same mislabel.
    op.execute(
        f"""
        UPDATE version_index v
        SET resource_mappings = (v.resource_mappings - '_zip')
            || jsonb_build_object(
                   '_symbology',
                   COALESCE(v.resource_mappings->'_symbology', '[]'::jsonb)
                       || jsonb_build_array(v.resource_mappings->'_zip'))
        WHERE jsonb_typeof(v.resource_mappings->'_zip') = 'string'
          AND v.resource_mappings->>'_zip' ~ '{_DOC}'
        """
    )


def downgrade() -> None:
    # Fold `_symbology` back into `_zip_parts` — the pre-migration shape, where
    # the bundle is indistinguishable from attachments.
    op.execute(
        """
        UPDATE version_index v
        SET resource_mappings = (v.resource_mappings - '_symbology')
            || jsonb_build_object(
                   '_zip_parts',
                   COALESCE(v.resource_mappings->'_zip_parts', '[]'::jsonb)
                       || v.resource_mappings->'_symbology')
        WHERE jsonb_typeof(v.resource_mappings->'_symbology') = 'array'
        """
    )
