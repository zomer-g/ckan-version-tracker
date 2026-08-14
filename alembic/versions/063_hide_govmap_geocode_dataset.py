"""Unpublish the GovMap geocoding ledger: status 'active' -> 'hidden'.

`over_re_geocode` is a per-address work log — asked / found / missed / wrong
locality — that is only ~46% answered and exists to fill edge cases in
`over_re_addresses`. Published on its own it reads as "the addresses GovMap
knows", which it is not. The finished product, the point itself, is already in
`over_re_addresses` and stays public.

'hidden' is a new lifecycle value meaning **tracked and polled, but not
published**. It is deliberately implemented as a `status` rather than a new
`hidden` boolean: every public surface in this codebase already filters
`status IN ('active','pending')` — the datasets list, API v1, tags,
organizations, the MCP server, deep search, `over_datasets`, the /data catalog —
so a new value is excluded from all of them by default. A boolean would have
required finding and patching each one, and any surface missed would leak. This
way the failure mode is "an admin surface forgot to include it", which is
visible, rather than "a public surface forgot to exclude it", which is not.

Three places therefore had to be told about it explicitly:
  * `admin._admin_dataset_conds` and AdminPage.tsx — hidden is still administered
  * `dataset_lookup` and `sources.preview` — they filter `!=` rather than `IN`

Polling is unaffected: geocoding batches come from `geocode_enqueue_job` ->
`enqueue_next_batch` -> `ensure_dataset`, which looks the row up by `ckan_name`
with no status filter. `init_scheduler` will stop giving it a per-dataset poll
job, which is correct — a routine poll of this dataset would compete with the
batch enqueuer.

Revision ID: 063
Revises: 062
"""
from alembic import op

revision = "063"
down_revision = "062"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        UPDATE tracked_datasets SET status = 'hidden'
        WHERE ckan_name = 'govmap-geocode' AND status = 'active'
    """)


def downgrade() -> None:
    op.execute("""
        UPDATE tracked_datasets SET status = 'active'
        WHERE ckan_name = 'govmap-geocode' AND status = 'hidden'
    """)
