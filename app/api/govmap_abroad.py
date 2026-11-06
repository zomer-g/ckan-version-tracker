"""Public read API for the "חו\"ל" project: GovMap features outside Israel's
outline (app/services/govmap_abroad.py).

    GET /api/govmap-abroad/summary        totals, publishers, layers with features abroad
    GET /api/govmap-abroad/features?…     the features themselves, paged
    GET /api/govmap-abroad/features.csv?… the same, as CSV, for one publisher or layer

The CSV is what a letter to a ministry is drafted from: one row per feature,
with its layer, a point on it (WGS84 and a GovMap link), its distance from the
outline and every attribute the layer publishes for it.
"""
import csv
import io
import json
import re
import urllib.parse
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response

from app.rate_limit import limiter
from app.services import append_store, govmap_abroad

router = APIRouter(prefix="/api/govmap-abroad", tags=["govmap-abroad"])

#: Attribute names that usually carry a feature's human name, in preference order.
_NAME_KEYS = ("name", "שם", "NAME", "Name", "new_name", "label", "LABEL", "shem", "name_heb",
              "fname", "prjname", "title", "caption")


def feature_name(attrs: dict) -> str:
    """Best-effort human name of a feature from its attributes, or ''."""
    for k in _NAME_KEYS:
        v = attrs.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    for k, v in attrs.items():
        if isinstance(v, str) and re.search(r"[א-ת]", v) and len(v) <= 120:
            return v.strip()
    return ""


def _require_configured() -> None:
    if not append_store.is_configured():
        raise HTTPException(status_code=503, detail="Append DB is not configured")


@router.get("/summary")
@limiter.limit("30/minute")
async def summary(request: Request):
    _require_configured()
    return await govmap_abroad.summary()


@router.get("/features")
@limiter.limit("60/minute")
async def features(
    request: Request,
    dataset_id: str | None = Query(None, max_length=64),
    publisher: str | None = Query(None, max_length=200),
    limit: int = Query(500, ge=1, le=govmap_abroad.MAX_FEATURES_PAGE),
    offset: int = Query(0, ge=0),
):
    _require_configured()
    out = await govmap_abroad.features(dataset_id=dataset_id, publisher=publisher,
                                       limit=limit, offset=offset)
    for it in out["items"]:
        it["name"] = feature_name(it["attrs"])
    return out


@router.get("/features.csv")
@limiter.limit("10/minute")
async def features_csv(
    request: Request,
    dataset_id: str | None = Query(None, max_length=64),
    publisher: str | None = Query(None, max_length=200),
):
    """Every abroad feature of one publisher or one layer, as CSV (UTF-8 with
    BOM, so Excel reads the Hebrew)."""
    _require_configured()
    if not dataset_id and not publisher:
        raise HTTPException(status_code=422, detail="pass dataset_id or publisher")
    rows: list[dict] = []
    offset = 0
    while True:
        page = await govmap_abroad.features(dataset_id=dataset_id, publisher=publisher,
                                            limit=govmap_abroad.MAX_FEATURES_PAGE,
                                            offset=offset)
        rows.extend(page["items"])
        offset += len(page["items"])
        if not page["items"] or offset >= page["total"]:
            break
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["גוף מפרסם", "שכבה", "מספר שכבה", "שם הפריט", "מזהה הפריט (objectId)",
                "סוג גאומטריה", "קו אורך", "קו רוחב", "מרחק מהגבול (ק\"מ)",
                "קישור ל-GovMap", "כל השדות"])
    for it in rows:
        a = it["attrs"] or {}
        w.writerow([it["publisher"], it["title"], it["layer_id"] or "", feature_name(a),
                    a.get("objectId", ""), it["geom_type"] or "",
                    f"{it['lon']:.6f}" if it["lon"] is not None else "",
                    f"{it['lat']:.6f}" if it["lat"] is not None else "",
                    f"{it['distance_km']:.3f}" if it["distance_km"] is not None else "",
                    it["govmap_url"] or "", json.dumps(a, ensure_ascii=False)])
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    key = re.sub(r"[^\w-]+", "_", publisher or dataset_id or "")[:60].strip("_") or "export"
    filename = f"govmap_abroad_{key}_{stamp}.csv"
    return Response(
        content="﻿" + buf.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition":
                 f"attachment; filename=\"govmap_abroad_{stamp}.csv\"; "
                 f"filename*=UTF-8''{urllib.parse.quote(filename)}"},
    )
