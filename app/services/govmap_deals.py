"""GovMap's real-estate deals (layer 16) as one queryable table.

Layer 16 is the deal register GovMap draws on its map: 2,501,231 deals in the
version of 3.10.2026, one point per building, every deal of a building on that
point. Its versions are too big for a "נתוני הסורק" CSV, so they land as
GPKG + Parquet only, and the ``idx`` mirror (which reads the CSV) never sees
them. This module loads the Parquet of the latest version into
``public.govmap_deals`` — typed columns, a point, indexes — so the /deals page
can search it and draw it, and the /data console can query it.

What the Parquet looks like, and why the loader is not a plain COPY:

* **Every column is a string, and an absent value is the string "None".**
  The writer str()-ed Python's None. Kept as text it would sort, count and
  filter as a real value; it is turned back into NULL here.
* **Dates run from 1900 to 2048.** That is the source, not a parse error; the
  loader keeps them, and the page's date filters simply exclude what nobody
  asks for. ``deal_date_raw`` keeps the published string.
* **Geometry is a one-point MultiPoint in WKB, OGC:CRS84** (lon/lat). Every deal
  of a building shares the building's point, so it is stored as a Point.
* **objectid is unique; deal_id is not** (8,113 deals appear on more than one
  record — a deal spanning several parcels).

Loaded whole into a staging table and swapped in with one rename, so a reader
sees the previous version or the new one, never a half load. The version that
was loaded is recorded in ``govmap_deals_state``; ``ensure_current`` compares it
with the dataset's latest version and loads only when they differ, which is what
the post-push hook and the boot check both call.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import struct
import tempfile
from datetime import date
from decimal import Decimal, InvalidOperation

from sqlalchemy import select

from app.services import append_store
from app.services.storage_client import storage_client

logger = logging.getLogger(__name__)

# The GovMap layer-16 dataset. One dataset, tracked by id, as deals_query does
# for the tax register.
DATASET_ID = "4174aac2-c8b2-443e-9425-3773ce47c8da"
LAYER_ID = "16"

TABLE = "govmap_deals"
STAGING = "govmap_deals_stg"
RAW = "govmap_deals_raw"
STATE_TABLE = "govmap_deals_state"
BATCH = 20_000

# (column, postgres type, parquet field, converter name)
COLUMNS = [
    ("objectid", "bigint", "objectId", "int"),
    ("deal_id", "bigint", "dealId", "int"),
    ("deal_date", "date", "dealDate", "date"),
    ("deal_date_raw", "text", "dealDate", "text"),
    ("deal_amount", "bigint", "dealAmount", "int"),
    ("settlement_id", "integer", "settlementId", "int"),
    ("settlement", "text", "settlementNameHeb", "text"),
    ("settlement_en", "text", "settlementNameEng", "text"),
    ("street_code", "bigint", "streetCode", "int"),
    ("street", "text", "streetNameHeb", "text"),
    ("street_en", "text", "streetNameEng", "text"),
    ("house_num", "text", "houseNum", "text"),
    ("floor", "text", "floorNo", "text"),
    ("asset_area", "numeric", "assetArea", "num"),
    ("rooms", "numeric", "assetRoomNum", "num"),
    ("property_type", "text", "propertyTypeDescription", "text"),
    ("deal_nature", "text", "dealNatureDescription", "text"),
    ("neighborhood", "text", "neighborhood", "text"),
    ("gush", "integer", "gushNum", "int"),
    ("parcel", "integer", "parcelNum", "int"),
    ("sub_parcel", "integer", "subParcelNum", "int"),
    ("polygon_id", "text", "polygonId", "text"),
]

_ABSENT = {"", "None", "none", "null", "NULL", "nan"}
_DATE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")


def _text(v):
    if v is None:
        return None
    s = str(v).strip()
    return None if s in _ABSENT else s


def _int(v):
    s = _text(v)
    if s is None:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _num(v):
    # Decimal, not float: asyncpg's binary COPY encodes ``numeric`` from Decimal.
    s = _text(v)
    if s is None:
        return None
    try:
        d = Decimal(s)
    except InvalidOperation:
        return None
    return d if d.is_finite() else None


def _date(v):
    s = _text(v)
    m = _DATE.match(s or "")
    if not m:
        return None
    try:
        return date(int(m[1]), int(m[2]), int(m[3]))
    except ValueError:
        return None


_CONVERT = {"int": _int, "num": _num, "text": _text, "date": _date}


def point_of(wkb) -> tuple[float, float] | None:
    """(lon, lat) of a WKB Point or one-point MultiPoint, or None.

    Read in the byte order the geometry declares. A MultiPoint of more than
    one point is not this layer's shape and is refused, so a change at GovMap
    shows up as missing points rather than wrong ones."""
    if not wkb or len(wkb) < 21:
        return None
    b = bytes(wkb)
    order = "<" if b[0] == 1 else ">"
    gtype = struct.unpack(order + "I", b[1:5])[0] % 1000
    if gtype == 1 and len(b) >= 21:
        x, y = struct.unpack(order + "dd", b[5:21])
    elif gtype == 4 and len(b) >= 30:
        n = struct.unpack(order + "I", b[5:9])[0]
        if n != 1:
            return None
        inner = "<" if b[9] == 1 else ">"
        x, y = struct.unpack(inner + "dd", b[14:30])
    else:
        return None
    if not (-180 <= x <= 180 and -90 <= y <= 90):
        return None
    return x, y


def records_from_batch(batch) -> list[tuple]:
    """One Arrow record batch → COPY records in COLUMNS order + lon, lat."""
    cols = {name: batch.column(name).to_pylist() for name in batch.schema.names}
    geoms = cols.get("geometry") or [None] * batch.num_rows
    out = []
    for i in range(batch.num_rows):
        row = [_CONVERT[conv](cols[src][i]) if src in cols else None
               for _, _, src, conv in COLUMNS]
        pt = point_of(geoms[i])
        row.extend(pt if pt else (None, None))
        out.append(tuple(row))
    return out


async def _postgis(conn) -> bool:
    try:
        return bool(await conn.fetchval(
            "SELECT true FROM pg_extension WHERE extname = 'postgis'"))
    except Exception:  # noqa: BLE001
        return False


async def _ensure_state(conn) -> None:
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS public.{STATE_TABLE} (
            id              boolean PRIMARY KEY DEFAULT true CHECK (id),
            version_number  integer NOT NULL,
            rows            bigint NOT NULL,
            with_point      bigint NOT NULL,
            source          text NOT NULL,
            loaded_at       timestamptz NOT NULL DEFAULT now()
        )""")


async def loaded_version() -> int | None:
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await _ensure_state(conn)
        return await conn.fetchval(f"SELECT version_number FROM public.{STATE_TABLE}")


async def load(parquet_value: str, version_number: int) -> dict:
    """Load one Parquet (an ``r2:`` value) into ``public.govmap_deals``."""
    import pyarrow.parquet as pq

    if not append_store.is_configured():
        raise RuntimeError("append DB is not configured")
    fd, tmp = tempfile.mkstemp(suffix=".parquet", prefix="govmap-deals-")
    os.close(fd)
    try:
        if not await storage_client.download_to_file(parquet_value, tmp):
            raise RuntimeError(f"could not download {parquet_value}")
        pf = pq.ParquetFile(tmp)
        names = [c for c, _, _, _ in COLUMNS] + ["lon", "lat"]
        ddl = ", ".join(f"{c} {t}" for c, t, _, _ in COLUMNS)
        pool = await append_store.get_pool()
        rows = with_point = 0
        async with pool.acquire() as conn:
            for t in (RAW, STAGING):
                await conn.execute(f"DROP TABLE IF EXISTS public.{t}")
            # One write: rows COPY straight into the staging table, and the
            # point is a generated column the database fills during the COPY.
            # (xhostd refuses UNLOGGED tables — it backs up only logged data —
            # so a raw table and a second pass would double every write.)
            gis = await _postgis(conn)
            geom = (", geom geometry(Point, 4326) GENERATED ALWAYS AS ("
                    "CASE WHEN lon IS NOT NULL THEN ST_SetSRID(ST_MakePoint(lon, lat), 4326) END"
                    ") STORED" if gis else "")
            await conn.execute(f"CREATE TABLE public.{STAGING} "
                               f"({ddl}, lon double precision, lat double precision{geom})")
            buf: list[tuple] = []
            for batch in pf.iter_batches(batch_size=BATCH):
                recs = await asyncio.to_thread(records_from_batch, batch)
                buf.extend(recs)
                if len(buf) >= BATCH:
                    await conn.copy_records_to_table(STAGING, records=buf, columns=names,
                                                     schema_name="public")
                    rows += len(buf)
                    with_point += sum(1 for r in buf if r[-1] is not None)
                    buf = []
            if buf:
                await conn.copy_records_to_table(STAGING, records=buf, columns=names,
                                                 schema_name="public")
                rows += len(buf)
                with_point += sum(1 for r in buf if r[-1] is not None)
            if rows != pf.metadata.num_rows:
                raise RuntimeError(f"loaded {rows} of {pf.metadata.num_rows} rows")

            for name, expr in (
                ("objectid", "objectid"), ("settlement", "settlement"),
                ("gush_parcel", "gush, parcel"), ("deal_date", "deal_date"),
                ("polygon", "polygon_id"), ("street", "settlement, street"),
            ):
                await conn.execute(f"CREATE INDEX {STAGING}_{name} ON public.{STAGING} ({expr})")
            if gis:
                await conn.execute(f"CREATE INDEX {STAGING}_geom ON public.{STAGING} USING gist (geom)")
            else:
                await conn.execute(f"CREATE INDEX {STAGING}_lonlat ON public.{STAGING} (lon, lat)")
            await conn.execute(f"ANALYZE public.{STAGING}")

            async with conn.transaction():
                await conn.execute(f"DROP TABLE IF EXISTS public.{TABLE}")
                await conn.execute(f"ALTER TABLE public.{STAGING} RENAME TO {TABLE}")
                for name in ("objectid", "settlement", "gush_parcel", "deal_date",
                             "polygon", "street", "geom", "lonlat"):
                    await conn.execute(f"ALTER INDEX IF EXISTS public.{STAGING}_{name} "
                                       f"RENAME TO {TABLE}_{name}")
                await _ensure_state(conn)
                await conn.execute(
                    f"INSERT INTO public.{STATE_TABLE} (id, version_number, rows, with_point, source) "
                    f"VALUES (true, $1, $2, $3, $4) ON CONFLICT (id) DO UPDATE SET "
                    f"version_number = $1, rows = $2, with_point = $3, source = $4, loaded_at = now()",
                    version_number, rows, with_point, parquet_value)
        logger.info("govmap deals: loaded v%s — %d rows, %d with a point%s",
                    version_number, rows, with_point, "" if gis else " (no PostGIS)")
        return {"version": version_number, "rows": rows, "with_point": with_point,
                "postgis": gis}
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass


def _parquet_of(mappings: dict | None) -> str | None:
    vals = (mappings or {}).get("_parquet") or []
    return vals[0] if vals else None


async def ensure_current(force: bool = False) -> dict:
    """Load the dataset's latest version if it is not the one already loaded."""
    from app.database import async_session
    from app.models.version_index import VersionIndex

    async with async_session() as db:
        latest = (await db.execute(
            select(VersionIndex.version_number, VersionIndex.resource_mappings)
            .where(VersionIndex.tracked_dataset_id == DATASET_ID)
            .order_by(VersionIndex.version_number.desc())
        )).all()
    for number, mappings in latest:
        value = _parquet_of(mappings)
        if not value:
            continue
        if not force and await loaded_version() == number:
            return {"version": number, "skipped": "already loaded"}
        return await load(value, number)
    return {"skipped": "no version with a Parquet file"}


_task: asyncio.Task | None = None


def schedule(force: bool = False) -> bool:
    """Run ensure_current in the background, once at a time."""
    global _task
    if _task is not None and not _task.done():
        return False

    async def _run():
        try:
            await ensure_current(force=force)
        except Exception:  # noqa: BLE001 — a failed load leaves the old table
            logger.exception("govmap deals: load failed")

    _task = asyncio.create_task(_run())
    return True


async def boot_check() -> None:
    """The scheduler's one-shot after boot (a coroutine, so it runs on the
    loop rather than in the scheduler's thread pool)."""
    schedule()
