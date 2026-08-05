"""Migration 059 must convert exactly the data.gov.il datasets that have no
NEON half — no more, no fewer.

The plan is not a column. It is derived from three keys inside scraper_config
(``upload_mode`` / ``storage_backend`` / ``archive_neon``) by
``storage_target_of``, and the migration re-states that derivation in SQL. Two
spellings of one rule is the same shape of mistake that produced the bug this
migration cleans up, so the SQL is pinned against the Python here rather than
trusted.

Sweeping too wide is the expensive direction: a dataset flipped to the dual
write streams its whole datastore on the next poll. A 'local_only' dataset
derives to "local", not "r2" — 048's predicate omitted that guard and would
have caught one.
"""
import re
from pathlib import Path

import pytest

from app.api.datasets import storage_target_of

_MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "alembic" / "versions" / "059_ckan_dual_write_backfill_all.py"
)


def _predicate() -> str:
    text = _MIGRATION.read_text(encoding="utf-8")
    m = re.search(r'_DERIVES_TO_R2 = """(.*?)"""', text, re.S)
    assert m, "migration 059 no longer declares _DERIVES_TO_R2"
    return m.group(1)


# Every config shape the derivation distinguishes, with the plan it produces.
# Only the "r2" rows are the migration's business.
_CASES = [
    ({}, "r2"),                                              # nothing pinned → global default
    (None, "r2"),                                            # no config at all
    ({"storage_backend": "r2"}, "r2"),                       # pinned explicitly
    ({"append_key": "id"}, "r2"),                            # unrelated keys don't matter
    ({"storage_backend": "r2", "archive_neon": True}, "r2+neon"),
    ({"archive_neon": True}, "r2+neon"),
    ({"storage_backend": "neon"}, "neon"),
    ({"storage_backend": "odata"}, "odata"),
    ({"storage_backend": "odata", "archive_neon": True}, "odata+neon"),
    ({"upload_mode": "local_only"}, "local"),
    # The one 048 would have swept in: local_only with no backend pinned, where
    # COALESCE(storage_backend,'r2') reads 'r2' and only the upload_mode guard
    # keeps it out.
    ({"upload_mode": "local_only", "archive_neon": False}, "local"),
]


@pytest.mark.parametrize("config,expected", _CASES)
def test_the_derivation_these_conditions_mirror(config, expected):
    """Pins what each shape means, so a change to the Python rule fails here
    before it can silently diverge from the SQL below."""
    assert storage_target_of(config) == expected


def test_the_sql_guards_every_key_the_derivation_reads():
    """`storage_target_of` consults three keys. A predicate that checks only two
    converts datasets whose plan was never "r2" — which is how a deliberate
    local-only or ODATA choice would get overwritten by a migration."""
    sql = _predicate()
    for key in ("upload_mode", "archive_neon", "storage_backend"):
        assert key in sql, f"the predicate ignores scraper_config.{key}"
    assert "local_only" in sql, "nothing keeps a local-only dataset out"


def test_the_predicate_is_not_scoped_to_one_package():
    """048 was deliberately scoped to the elections package and said the rest was
    the user's call. 059 IS that call — if this ever regains a ckan_id filter,
    the 42 it exists to convert are silently skipped."""
    text = _MIGRATION.read_text(encoding="utf-8")
    upgrade = text.split("def upgrade()", 1)[1].split("def downgrade()", 1)[0]
    assert "ckan_id" not in upgrade


def test_downgrade_does_not_undo_048():
    """048's rows satisfy this migration's predicate too. Reverting 059 must not
    take the elections package's NEON half with it."""
    text = _MIGRATION.read_text(encoding="utf-8")
    downgrade = text.split("def downgrade()", 1)[1]
    assert "_MIGRATION_048_CKAN_ID" in downgrade


def test_the_migration_chain_has_exactly_one_head():
    """A second file claiming a revision number already in use forks the chain,
    and alembic then refuses to run ANY of it — including migrations unrelated
    to the fork. Cheap to assert, and it catches the mistake at the moment the
    file is written rather than on deploy."""
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    root = Path(__file__).resolve().parents[1]
    script = ScriptDirectory.from_config(Config(str(root / "alembic.ini")))
    assert len(script.get_heads()) == 1, script.get_heads()
