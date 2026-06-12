"""Regression test for append-mode schema drift.

When an append-only dataset's source grows new columns between versions,
the shared datastore resource was created with the old field set, so
``datastore_upsert`` (insert) rejects every record with a 409
``row "1" has extra keys "…"``. ``_push_batch_with_retry`` must recover by
calling ``datastore_create`` on the existing resource with the superset of
fields (which ALTERs the table to add the missing columns without dropping
data), then retrying the upsert.

This reproduced the production failure on the "החלטות ממשלה" dataset:
  push-version failed: 502 … datastore_upsert 409:
  row "1" has extra keys "משרד, מספר החלטה, תאריך פרסום, …"
"""
import asyncio
import os
import sys

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.services.odata_client import ODataClient  # noqa: E402


class _Recorder:
    """Stands in for ODataClient._post. Fails the first datastore_upsert
    with an extra-keys 409, lets datastore_create succeed, then lets the
    retried upsert succeed."""

    def __init__(self):
        self.calls: list[str] = []
        self.upserts = 0

    async def __call__(self, action, data=None, timeout=None):
        self.calls.append(action)
        if action == "datastore_upsert":
            self.upserts += 1
            if self.upserts == 1:
                raise RuntimeError(
                    'datastore_upsert 409: {"records": ["row \\"1\\" has '
                    'extra keys \\"משרד, מספר החלטה\\""], "__type": '
                    '"Validation Error"}'
                )
            return {"success": True}
        if action == "datastore_create":
            return {"success": True, "result": {}}
        return {"success": True}


def test_upsert_extends_schema_on_extra_keys():
    async def _run():
        client = ODataClient(base_url="https://example.test", api_key="x")
        rec = _Recorder()
        client._post = rec  # type: ignore[assignment]

        fields = [
            {"id": "משרד", "type": "text"},
            {"id": "מספר החלטה", "type": "text"},
        ]
        records = [{"משרד": "ראש הממשלה", "מספר החלטה": "123"}]

        # Must NOT raise — the extra-keys 409 triggers a schema extend + retry.
        await client._push_batch_with_retry(
            resource_id="res-1",
            fields=fields,
            records_batch=records,
            create=False,
            batch_num=1,
            is_last=True,
        )
        return rec

    rec = asyncio.run(_run())
    # The recovery path ran: upsert → datastore_create (extend) → upsert.
    assert rec.calls == ["datastore_upsert", "datastore_create", "datastore_upsert"]
    assert rec.upserts == 2


def test_schema_extend_attempted_only_once():
    """If the extend doesn't help and the upsert keeps failing with extra
    keys, we don't loop forever extending — it falls through to the normal
    retry/backoff and eventually raises."""
    calls: list[str] = []

    async def _run():
        client = ODataClient(base_url="https://example.test", api_key="x")

        async def _always_extra_keys(action, data=None, timeout=None):
            calls.append(action)
            if action == "datastore_upsert":
                raise RuntimeError('datastore_upsert 409: row "1" has extra keys "x"')
            return {"success": True}

        client._post = _always_extra_keys  # type: ignore[assignment]

        with pytest.raises(RuntimeError, match="failed after"):
            await client._push_batch_with_retry(
                resource_id="res-2",
                fields=[{"id": "x", "type": "text"}],
                records_batch=[{"x": "1"}],
                create=False,
                batch_num=1,
                is_last=True,
                max_attempts=2,
            )

    asyncio.run(_run())
    # Exactly one schema-extend attempt, not one per retry.
    assert calls.count("datastore_create") == 1
