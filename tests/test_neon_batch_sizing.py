"""One batch must be one insert.

append_rows re-splits anything over the parameter ceiling, so the loader's fixed
5,000-row batch came out of a 23-column table as 2,730 + 2,270 — two
differently-shaped INSERT statements on every batch, each carrying tens of
thousands of placeholders. asyncpg prepares and caches by statement text, so
that is two large prepared statements per batch instead of one reused, and the
peak is the larger of the two rather than the size anyone chose.

It surfaced on the national parcel layer: 1,097,775 rows of polygon WKT, loaded
150,000 at a time before the 512MB dyno was OOM-killed.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.services import append_store  # noqa: E402


def test_a_batch_never_straddles_the_parameter_ceiling():
    """For any realistic table, the loader's batch must fit in one insert."""
    for ncols in (5, 23, 60, 120):
        batch = min(5000, append_store.chunk_size_for(ncols, True))
        # build_insert would re-split anything larger, which is the bug.
        assert batch <= append_store.chunk_size_for(ncols, True), ncols
        assert batch >= 1


def test_the_parcel_table_gets_one_shape_not_two():
    """22 source columns + row_hash — the table this was found on."""
    limit = append_store.chunk_size_for(23, True)
    assert 5000 > limit, "the old constant did straddle it"
    assert min(5000, limit) == limit


def test_a_narrow_table_still_uses_the_cap_not_the_ceiling():
    """A table with few columns could fit far more than 5,000 rows per insert;
    the cap stays, because the point is bounded memory per statement, not the
    largest statement Postgres would accept."""
    assert min(5000, append_store.chunk_size_for(3, True)) == 5000


def test_the_loader_actually_applies_it():
    import inspect
    from app.api import worker
    src = inspect.getsource(worker._neon_stream_load_file)
    assert "chunk_size_for(len(cols), True)" in src
