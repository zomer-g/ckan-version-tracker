"""Narrow the truncation warning to gaps that are actually truncation

057 flagged every layer whose published count fell below what filter/count
declares — 59 of them. That conflated two findings that have nothing to do with
each other, and 44 of the 59 were the wrong one.

The distribution is bimodal and says so plainly: 21 layers are short by exactly
ONE record, 7 by 2-5, 10 by 6-50 — and then a gap, and 10 short by more than a
thousand. The small side is not truncation at all. It is the finding already
recorded in docs/govmap-repair-plan.md: `layer-data` is a spatial query and
returns only records that carry geometry, while `filter/count` counts rows in
the table, so a layer holding N non-spatial records is short by exactly N
forever, and no scrape will ever close it. Telling a reader that "1% of אתרי
המורשת is missing" because 197 of 198 records have coordinates is noise — and I
argued in 053 that a warning which fires on healthy data is worse than none,
then shipped exactly that.

Kept: a gap of at least 1,000 records, or at least 50 records amounting to 5% or
more of the layer. That is every real truncation (נחלים 35.5%, נקודות בקרה
44.9%, מעג"ל כבישים 48.5%, יעודי קרקע 30.2%, the five stopped dead at the
1,000,000 cap) and none of the no-geometry cases. The largest thing it now lets
through is דרכים באר שבע at 374 of 128,502 — 0.3%, comfortably inside the
no-geometry pattern.

Revision ID: 058
Revises: 057
Create Date: 2026-08-04
"""
from typing import Sequence, Union

from alembic import op


revision: str = "058"
down_revision: Union[str, None] = "057"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Clear 057 wholesale, then re-apply to the material gaps only.
    op.execute(
        "UPDATE tracked_datasets SET import_warning = NULL, import_warning_at = NULL "
        "WHERE import_warning LIKE '%4.8.2026%'"
    )
    op.execute(
        """
        UPDATE tracked_datasets t
        SET import_warning =
                'נמדד ב-4.8.2026: הגרסה האחרונה מכילה '
                || to_char(v.pub_n, 'FM999,999,999')
                || ' רשומות, בעוד המקור מצהיר על '
                || to_char(v.dec_n, 'FM999,999,999')
                || ' — כ-' || v.pct_n
                || '% מהשכבה אינם כלולים בגרסה זו. '
                || 'הרשומות שנשמרו תקינות, אך השכבה חלקית '
                || 'ואינה מייצגת את המקור במלואו.',
            import_warning_at = now()
        FROM (VALUES
    ('cf4cecc5-07a9-4be9-83e5-22a5d59e610f'::uuid, 2224, 3143, 29),
    ('b5756e0a-21fb-49ce-9215-adbe7211005b'::uuid, 2520, 3209, 21),
    ('126a3c3d-8ff0-469a-aea2-2b128675d7bf'::uuid, 1000008, 1054486, 5),
    ('950f1975-007d-469e-82e4-30290fd87cac'::uuid, 543086, 986377, 45),
    ('d3e557c0-d4f6-4a37-8245-9ee3a8528c05'::uuid, 37228, 42381, 12),
    ('eecbe0cc-1a69-4600-9b42-bfd7c57730f0'::uuid, 402, 462, 13),
    ('232ec1ff-a061-4f24-9419-332d6d1053d5'::uuid, 228118, 242681, 6),
    ('21eb1598-60fa-41b6-b124-b179accc85c9'::uuid, 0, 82, 100),
    ('1812c679-942c-4428-98b3-56e983eab5bc'::uuid, 385742, 748570, 48),
    ('560a9c78-a3f6-4eef-93fb-db1bec71596b'::uuid, 1000009, 1549529, 35),
    ('e6a8ce08-c77f-44e7-9037-7f5022b74fb2'::uuid, 6646, 7173, 7),
    ('68b8e2fe-d757-4478-aa46-ee756001f6bc'::uuid, 1000000, 1094327, 9),
    ('c077a2d5-8347-4509-96b4-b9c746459969'::uuid, 550299, 788513, 30),
    ('7b613f82-0f57-442b-abaf-29c5a8c504fa'::uuid, 1000000, 1092903, 9),
    ('9b125307-2a3a-47a7-9ea8-85d5f2439810'::uuid, 1000000, 1097715, 9)
        ) AS v(id, pub_n, dec_n, pct_n)
        WHERE t.id = v.id
        """
    )


def downgrade() -> None:
    op.execute(
        "UPDATE tracked_datasets SET import_warning = NULL, import_warning_at = NULL "
        "WHERE import_warning_at IS NOT NULL"
    )
