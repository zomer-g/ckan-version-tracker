"""Raise the stored max_docs on health practitioner registries.

Revision ID: 070
Revises: 069
Create Date: 2026-09-25

``HEALTH_DEFAULT_LIMITS`` moved from 50,000 to 200,000, but that constant is
read once, at dataset creation, through ``sc.setdefault("max_docs", docs)`` —
every registry tracked before today still carries 50,000 in its own
``scraper_config``, and ``_poll_scraper_config`` sends the dataset's stored
config to the worker. Without this backfill the new default reaches new
datasets only.

Why it matters: רפואה holds 57,530 practitioners. Version 6 of
רופאים בעלי רשיון ותחומי מומחיותם stopped at exactly 50,000 and published
87% of the licensed doctors in the country — past shrink_guard, because the
registry's move to a new host had doubled its row count, so the truncated
scrape still had more rows (100,007) than the version before it (82,408).

Only ``health_practitioners`` datasets are touched, and only where the stored
cap is below the new default: a cap someone raised deliberately is left alone,
and a registry whose config never carried max_docs is not given one here (the
API fills it on the next write, and the worker's own default is the same
200,000).
"""
from alembic import op

revision = "070"
down_revision = "069"
branch_labels = None
depends_on = None

_NEW_CAP = 200000
_OLD_DEFAULT = 50000


def upgrade() -> None:
    op.execute(
        f"""
        UPDATE tracked_datasets
           SET scraper_config =
                 jsonb_set(scraper_config, '{{max_docs}}', '{_NEW_CAP}'::jsonb, true)
         WHERE scraper_config->>'kind' = 'health_practitioners'
           AND scraper_config->'max_docs' IS NOT NULL
           AND (scraper_config->>'max_docs') ~ '^[0-9]+$'
           AND (scraper_config->>'max_docs')::bigint < {_NEW_CAP}
        """
    )


def downgrade() -> None:
    # Back to the old default only where this migration could have put the new
    # one — a cap someone chose by hand after the fact is not ours to undo.
    op.execute(
        f"""
        UPDATE tracked_datasets
           SET scraper_config =
                 jsonb_set(scraper_config, '{{max_docs}}', '{_OLD_DEFAULT}'::jsonb, true)
         WHERE scraper_config->>'kind' = 'health_practitioners'
           AND (scraper_config->>'max_docs') ~ '^[0-9]+$'
           AND (scraper_config->>'max_docs')::bigint = {_NEW_CAP}
        """
    )
