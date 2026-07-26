"""Diary auto-import for יומן לעם (Ocal), migrated into OVER.

Discovers new diary resources on odata.org.il (CKAN keyword "יומן"), fetches +
parses them, heuristically maps their columns to the ``diary_events`` schema,
and upserts into the dedicated Ocal Neon DB (app/services/ocal_db.py) with a
linked ``diary_sources`` row. Driven by an APScheduler job (see
app/worker/scheduler.py) so the corpus keeps growing once the legacy Ocal Node
service is retired.

REUSE: the file fetch + CSV/XLS/XLSX/ICAL parsing is the layer the parallel
session already ported from OCAL into app/services/odata_import.py — we call its
``_fetch_json`` / ``_download_file`` / ``_download_dump`` / ``_parse_file`` and
route the parsed ``(columns, rows)`` through the Ocal diary pipeline instead of
dumping to a generic table. The field-mapping heuristics, date parsing, row→event
transform and owner-matching are ports of Ocal's fieldMapper/pipeline/dateParser/
autoImport services.

NOTE on parsing: ``odata_import`` stringifies every cell, so Excel serial dates
(``45270``) and time fractions (``0.4375``) arrive as strings — the date/time
parsers below handle those numeric-string forms as well as ISO / DD-MM-YYYY /
Hebrew free-text.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import tempfile
import uuid
from datetime import date, datetime, timedelta, timezone

import httpx

from app.config import settings
from app.services import ocal_db
from app.services import odata_import as odi

logger = logging.getLogger(__name__)

CKAN_BASE = odi.ODATA_BASE
DIARY_QUERY = "יומן"
PAGE_SIZE = 200
MAX_DATASETS = 500
_UA = odi._UA
_UTC = timezone.utc

# 10-colour palette (matches Ocal's SOURCE_COLORS), assigned round-robin.
SOURCE_COLORS = [
    "#3B82F6", "#EF4444", "#10B981", "#F59E0B", "#8B5CF6",
    "#EC4899", "#14B8A6", "#F97316", "#6366F1", "#84CC16",
]


class SkipImport(Exception):
    """Raised when a discovered resource does not pass the auto-import gate."""


# ── field mapping (ported from OCAL fieldMapper.ts HEURISTIC_PATTERNS) ───────
# Ordered specific → general; the earliest matching pattern for a column wins,
# and across columns the one with the lowest pattern index claims the target.
def _pats(*raw: str) -> list[re.Pattern]:
    return [re.compile(p, re.IGNORECASE) for p in raw]


HEURISTIC_PATTERNS: dict[str, list[re.Pattern]] = {
    "title": _pats(
        r"^נושא$", r"נושא", r"^title$", r"^subject$", r"כותרת", r"תיאור",
        r"^description$", r"שם.?אירוע", r"שם.?פגישה", r"^שם$", r"אירוע.?פגישה",
        r"פעילות", r"פירוט", r"סוג.?אירוע", r"תוכן", r"^אירוע$", r"^פגישה$",
        r"^event$", r"^summary$",
    ),
    "start_date": _pats(
        r"^תאריך\s*התחלה$", r"תאריך.?התחלה", r"^start.*date$", r"^start$",
        r"תאריך.?פגישה", r"^תאריך$", r"^date$", r"יום.?בשבוע", r"^יום$",
        r"מתאריך", r"תאריך.?אירוע", r"מועד.?התחלה", r"^מועד$",
    ),
    "start_time": _pats(
        r"^שעת\s*התחלה$", r"שעת.?התחלה", r"^start.*time$", r"^שעה$",
        r"^time$", r"משעה", r"שעה.?התחלה",
    ),
    "end_date": _pats(
        r"^תאריך\s*סיום$", r"תאריך.?סיום", r"^end.*date$", r"^end$",
        r"עד.?תאריך", r"מועד.?סיום",
    ),
    "end_time": _pats(
        r"^שעת\s*סיום$", r"שעת.?סיום", r"^end.*time$", r"עד.?שעה", r"שעה.?סיום",
    ),
    "location": _pats(
        r"^מיקום$", r"מיקום", r"^where$", r"^location$", r"מקום", r"חדר",
        r"^room$", r"משאבי.?פגישה", r"משאבי.?אירוע", r"אולם", r"כתובת", r"^venue$",
    ),
    "participants": _pats(
        r"^מוזמנים$", r"מוזמנים", r"משתתפ", r"^attendee", r"^participant",
        r"נוכחים", r"^all\s*attendee", r"נפגש.?עם", r"עם.?מי",
    ),
    "organizer": _pats(
        r"מארגן", r"^organiz", r"יוזם", r"^organized\s*by$", r"אחראי", r"בעל.?האירוע",
    ),
    "notes": _pats(
        r"הערות", r"^notes$", r"סיווג", r"^comments$", r"הערה", r"^remarks$",
        r"עילת", r"חוק.?חופש.?המידע", r"השחרה", r"סיבה", r"מדיניות", r"ביאור", r"פירוט.?נוסף",
    ),
}
_N_TARGETS = len(HEURISTIC_PATTERNS)
_REQUIRED = ("title", "start_date")


def map_fields(columns: list[str]) -> tuple[dict, float]:
    """Map source column names → diary_events targets, with a confidence 0..1.

    Returns ``{target: original_column_name}`` and a confidence. For each target,
    among unclaimed columns, pick the one whose earliest matching pattern index
    is lowest; a column is claimed only once."""
    norm = [(odi._norm_header(c), c) for c in columns]
    claimed: set[str] = set()
    mapping: dict[str, str] = {}
    for target, patterns in HEURISTIC_PATTERNS.items():
        best_orig = None
        best_idx = 10**6
        for ncol, orig in norm:
            if ncol in claimed:
                continue
            for pi, pat in enumerate(patterns):
                if pat.search(ncol):
                    if pi < best_idx:
                        best_idx, best_orig, best_claim = pi, orig, ncol
                    break
        if best_orig is not None:
            claimed.add(best_claim)
            mapping[target] = best_orig
    n = len(mapping)
    if all(t in mapping for t in _REQUIRED):
        conf = min(1.0, (n / _N_TARGETS) * 1.2)
    else:
        conf = (n / _N_TARGETS) * 0.5
    return mapping, conf


# ── LLM-assisted field mapping (ported from OCAL fieldMapper.mapFields) ────────
# Table files (CSV/XLS/ICAL) with creative Hebrew headers or serial/fraction
# date-time columns often defeat the regex heuristic. When heuristic confidence
# is below _LLM_MAPPING_THRESHOLD and an LLM key is set, ask the model to map
# columns → schema (given the first 3 sample rows), and use it only if it beats
# the heuristic — exactly what the legacy Node service did on every import.
_LLM_MAPPING_THRESHOLD = 0.8
_VALID_TARGETS = frozenset(HEURISTIC_PATTERNS.keys())

_LLM_MAPPING_PROMPT = (
    "You are mapping columns from an Israeli public official's calendar/diary "
    "(יומן) dataset to a standardized schema.\n\n"
    "IMPORTANT data patterns to recognize:\n"
    "- Excel serial date numbers (e.g. 45270, 45882) represent dates — the column "
    "containing them is a date column\n"
    "- Decimal fractions (e.g. 0.4375, 0.625, 0.708) represent times as fractions "
    "of a day — the column containing them is a time column\n"
    "- Hebrew column names may use creative or non-standard naming (e.g. \"פעילות\" "
    "= title, \"שם אירוע/פגישה\" = title, \"יום בשבוע\" with serial numbers = "
    "start_date, \"משאבי פגישה\" with venues = location)\n\n"
    "Given these column names: {{FIELDS}}\n\n"
    "And these sample records (first 3): {{SAMPLES}}\n\n"
    "Map each column to ONE of these target fields:\n"
    "- title (required): The event subject/title/name\n"
    "- start_date (required): The event date (look for date strings, serial "
    "numbers, or day columns)\n"
    "- start_time: The event start time (time strings or decimal fractions)\n"
    "- end_date: The event end date (if separate from start_date)\n"
    "- end_time: The event end time\n"
    "- location: The event location/venue/room/resources\n"
    "- participants: The event participants/attendees/invitees\n"
    "- organizer: The event organizer\n"
    "- notes: Notes, comments, or classification\n\n"
    "Return ONLY a valid JSON object mapping target field names to source column "
    "names. Only include mappings you are confident about. Do NOT include unmapped "
    'fields.\nExample: {"title":"נושא","start_date":"תאריך","start_time":"שעת התחלה",'
    '"location":"מיקום"}'
)


def _mapping_llm_provider() -> str | None:
    """DeepSeek preferred (deepseek-chat, the model Ocal used), then Anthropic."""
    if not settings.ocal_llm_mapping_enabled:
        return None
    if settings.deepseek_api_key:
        return "deepseek"
    if settings.anthropic_api_key:
        return "anthropic"
    return None


async def _llm_map_call(prompt: str) -> dict:
    """One LLM round-trip returning the raw {target: source_col} object."""
    provider = _mapping_llm_provider()
    if provider == "deepseek":
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=settings.deepseek_api_key,
                             base_url="https://api.deepseek.com")
        resp = await client.chat.completions.create(
            model="deepseek-chat", temperature=0, max_tokens=500,
            response_format={"type": "json_object"},
            messages=[{"role": "user", "content": prompt}],
        )
        content = resp.choices[0].message.content or "{}"
    elif provider == "anthropic":
        from anthropic import AsyncAnthropic
        client = AsyncAnthropic(api_key=settings.anthropic_api_key)
        resp = await client.messages.create(
            model="claude-opus-4-8", max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )
        content = "".join(b.text for b in resp.content
                          if getattr(b, "type", None) == "text") or "{}"
    else:
        return {}
    content = content.strip()
    if content.startswith("```"):
        content = re.sub(r"^```[a-zA-Z]*\n?", "", content)
        content = re.sub(r"\n?```$", "", content).strip()
    try:
        obj = json.loads(content)
    except (ValueError, TypeError):
        i, j = content.find("{"), content.rfind("}")
        obj = json.loads(content[i:j + 1]) if i >= 0 and j > i else {}
    return obj if isinstance(obj, dict) else {}


async def map_fields_llm(columns: list[str], rows: list) -> tuple[dict, float]:
    """LLM field mapping with the same validation + confidence as Ocal.

    ``rows`` are value-lists aligned with ``columns``; the first 3 become the
    sample records shown to the model. Returns ({target: original_column}, conf).
    Raises on network/parse failure so the caller can fall back to heuristic."""
    samples = [dict(zip(columns, r)) for r in rows[:3]]
    prompt = (_LLM_MAPPING_PROMPT
              .replace("{{FIELDS}}", json.dumps(columns, ensure_ascii=False))
              .replace("{{SAMPLES}}", json.dumps(samples, ensure_ascii=False, default=str)))
    raw = await _llm_map_call(prompt)

    norm_cols = [odi._norm_header(c) for c in columns]
    mapping: dict[str, str] = {}
    claimed: set[str] = set()
    for target, value in raw.items():
        if target not in _VALID_TARGETS or not isinstance(value, str):
            continue
        nv = odi._norm_header(value)
        if nv and nv not in claimed and nv in norm_cols:
            mapping[target] = columns[norm_cols.index(nv)]
            claimed.add(nv)

    has_required = all(t in mapping for t in _REQUIRED)
    conf = min(0.95, 0.7 + len(mapping) * 0.05) if has_required else 0.3
    return mapping, conf


async def map_fields_best(columns: list[str], rows: list) -> tuple[dict, float, str]:
    """Heuristic first; if weak (<0.8) and an LLM key is set, try LLM and keep it
    only if it beats the heuristic. Mirrors Ocal fieldMapper.mapFields."""
    mapping, conf = map_fields(columns)
    if conf >= _LLM_MAPPING_THRESHOLD or _mapping_llm_provider() is None:
        return mapping, conf, "heuristic"
    try:
        llm_map, llm_conf = await map_fields_llm(columns, rows)
        if llm_conf > conf:
            logger.info("ocal_import: LLM field-mapping used (conf %.2f > heuristic %.2f)",
                        llm_conf, conf)
            return llm_map, llm_conf, "llm"
    except Exception as e:  # noqa: BLE001 — LLM mapping is best-effort
        logger.warning("ocal_import: LLM field-mapping failed, using heuristic: %s", e)
    return mapping, conf, "heuristic"


# ── date/time parsing (ported from OCAL dateParser.ts, string-hardened) ──────

def _mk(y: int, mo: int, d: int) -> datetime | None:
    try:
        return datetime(y, mo, d, tzinfo=_UTC)
    except ValueError:
        return None


def _excel_serial_dt(v: float) -> datetime | None:
    # 25569 = days from the Excel epoch to 1970-01-01 (absorbs the 1900 leap bug).
    try:
        return datetime(1970, 1, 1, tzinfo=_UTC) + timedelta(days=float(v) - 25569)
    except (ValueError, OverflowError):
        return None


def _to_float(s: str) -> float | None:
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def parse_date(value) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=_UTC)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=_UTC)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return _excel_serial_dt(value) if 1 < value < 73050 else None
    s = str(value or "").strip()
    if not s:
        return None
    # Numeric string → Excel serial (openpyxl/xlrd values stringified upstream).
    if re.match(r"^\d+(\.\d+)?$", s):
        f = _to_float(s)
        if f is not None and 1 < f < 73050:
            return _excel_serial_dt(f)
    # ISO date / datetime prefix (e.g. "2024-01-15 11:30:00").
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        return _mk(int(m[1]), int(m[2]), int(m[3]))
    # DD.MM.YYYY / DD/MM/YYYY (Israeli day-first), anchored.
    m = re.match(r"^(\d{1,2})[./](\d{1,2})[./](\d{4})$", s)
    if m:
        return _mk(int(m[3]), int(m[2]), int(m[1]))
    # DD.MM.YY.
    m = re.match(r"^(\d{1,2})[./](\d{1,2})[./](\d{2})$", s)
    if m:
        return _mk(2000 + int(m[3]), int(m[2]), int(m[1]))
    # Embedded DD/MM/YYYY inside Hebrew free-text ("יום ב 01/04/2024 11:30").
    m = re.search(r"(\d{1,2})[./](\d{1,2})[./](\d{4})", s)
    if m:
        return _mk(int(m[3]), int(m[2]), int(m[1]))
    return None


def parse_time(value) -> tuple[int, int] | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if 0 <= value < 1:
            m = round(value * 24 * 60)
            return (m // 60 % 24, m % 60)
        if 0 <= value <= 23:
            return (int(value), 0)
        return None
    s = str(value or "").strip()
    if not s:
        return None
    if re.match(r"^0?\.\d+$", s):                     # Excel time fraction
        f = _to_float(s)
        if f is not None and 0 <= f < 1:
            m = round(f * 24 * 60)
            return (m // 60 % 24, m % 60)
    m = re.search(r"(\d{1,2}):(\d{2})(?::\d{2})?", s)  # HH:MM (standalone/trailing)
    if m:
        h, mi = int(m[1]), int(m[2])
        if 0 <= h <= 23 and 0 <= mi <= 59:
            return (h, mi)
    if re.match(r"^\d{1,2}$", s):                     # bare hour
        h = int(s)
        if 0 <= h <= 23:
            return (h, 0)
    return None


def parse_datetime(date_value, time_value=None) -> datetime | None:
    dt = parse_date(date_value)
    if dt is None:
        return None
    applied = False
    if time_value not in (None, ""):
        t = parse_time(time_value)
        if t:
            dt = dt.replace(hour=t[0], minute=t[1], second=0, microsecond=0)
            applied = True
    # Combined date+time cell (only when there's an explicit HH:MM in the string).
    if not applied and isinstance(date_value, str) and re.search(r"\d{1,2}:\d{2}", date_value):
        t = parse_time(date_value)
        if t:
            dt = dt.replace(hour=t[0], minute=t[1], second=0, microsecond=0)
    return dt


# ── row → diary_events transform (ported from OCAL pipeline.ts) ───────────────

def _safe(v) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def transform_record(record: dict, mapping: dict, source_id, dataset_name: str,
                     dataset_link: str | None, ckan_row_id: int) -> dict | None:
    raw_title = record.get(mapping.get("title"))
    title = _safe(raw_title)
    if not title:
        return None
    start = parse_datetime(
        record.get(mapping.get("start_date")),
        record.get(mapping["start_time"]) if mapping.get("start_time") else None,
    )
    if start is None:
        return None
    end = None
    if mapping.get("end_date"):
        end = parse_datetime(
            record.get(mapping["end_date"]),
            record.get(mapping["end_time"]) if mapping.get("end_time") else None,
        )
    location = _safe(record.get(mapping["location"])) if mapping.get("location") else None
    participants = _safe(record.get(mapping["participants"])) if mapping.get("participants") else None
    mapped_names = {v for v in mapping.values() if v}
    other = {k: v for k, v in record.items()
             if k not in mapped_names and k != "_id" and v not in (None, "")}
    return {
        "source_id": source_id, "title": title, "start_time": start, "end_time": end,
        "location": location, "participants": participants,
        "dataset_name": dataset_name, "dataset_link": dataset_link,
        "other_fields": other, "ckan_row_id": ckan_row_id,
    }


# ── fetch + parse (reuses odata_import) ──────────────────────────────────────

async def _fetch_and_parse(res: dict) -> tuple[list[str], list[list]]:
    """Download a resource and parse it into (columns, rows). Prefers the RAW
    file (preserves Hebrew column names, which field-mapping needs) over the
    CKAN datastore dump (which ASCII-mangles them)."""
    rid = res["id"]
    fmt = (res.get("format") or "").upper()
    url = (res.get("url") or "").strip()
    ds_active = bool(res.get("datastore_active"))
    suffix = ".ics" if fmt in odi.ICAL_FORMATS else (".xlsx" if fmt in odi.SPREADSHEET_FORMATS else ".csv")
    fd, tmp = tempfile.mkstemp(suffix=suffix, prefix="ocal-diary-")
    os.close(fd)
    try:
        if url and fmt in odi.SUPPORTED_FILE_FORMATS:
            await odi._download_file(url, tmp)
            return await asyncio.to_thread(odi._parse_file, tmp, fmt)
        if ds_active:
            await odi._download_dump(rid, tmp)
            return await asyncio.to_thread(odi._parse_file, tmp, "CSV")
        raise SkipImport(f"resource has no downloadable file (format={fmt or '—'})")
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass


# ── owner matching (ported from OCAL autoImport.identifyOwner) ───────────────

def _jaccard(a: str, b: str) -> float:
    A, B = set(a.split()), set(b.split())
    if not A or not B:
        return 0.0
    return len(A & B) / len(A | B)


async def _identify_owner(dataset_title: str | None, resource_name: str | None,
                          odata_org: str | None) -> tuple[uuid.UUID | None, uuid.UUID | None, float]:
    people = await ocal_db.fetch(
        "SELECT p.id, p.name, o.name AS org_name FROM people p "
        "LEFT JOIN organizations o ON o.id = p.organization_id")
    orgs = await ocal_db.fetch("SELECT id, name FROM organizations")
    title = (dataset_title or "").lower()
    resn = (resource_name or "").lower()
    combined = f"{title} {resn}"
    best_p, best_ps = None, 0.0
    for p in people:
        nm = (p["name"] or "").lower()
        if len(nm) < 2:
            continue
        if nm in title:
            s = 0.95
        elif nm in resn:
            s = 0.90
        else:
            s = max(_jaccard(nm, title), _jaccard(nm, resn)) + (0.3 if nm in combined else 0.0)
        if s > best_ps:
            best_ps, best_p = s, p
    best_o, best_os = None, 0.0
    if odata_org:
        og = odata_org.lower()
        for o in orgs:
            s = _jaccard((o["name"] or "").lower(), og)
            if s > best_os:
                best_os, best_o = s, o
    if best_p and odata_org and best_p["org_name"] and _jaccard(best_p["org_name"].lower(), odata_org.lower()) > 0.5:
        best_ps = min(1.0, best_ps + 0.05)
    person_id = best_p["id"] if best_p and best_ps >= 0.5 else None
    org_id = best_o["id"] if best_o and best_os >= 0.5 else None
    return person_id, org_id, best_ps


# ── diary_sources upsert ──────────────────────────────────────────────────────

async def _ensure_source(res: dict, pkg: dict, mapping: dict,
                         person_id, org_id) -> uuid.UUID:
    rid = res["id"]
    ds_id = res.get("package_id")
    title = ((pkg.get("title") or "").strip() or (res.get("name") or "").strip()
             or pkg.get("name") or rid)
    ds_url = f"{CKAN_BASE}/dataset/{ds_id}" if ds_id else None
    res_url = (f"{CKAN_BASE}/dataset/{ds_id}/resource/{rid}" if ds_id
               else (res.get("url") or None))
    ckan_meta = {
        "datasetTitle": pkg.get("title"), "resourceName": res.get("name"),
        "resourceUrl": res_url, "datasetUrl": ds_url,
        "organization": ((pkg.get("organization") or {}) or {}).get("title"),
        "lastModified": res.get("last_modified"),
    }
    pool = await ocal_db.get_pool()
    async with pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT id FROM diary_sources WHERE resource_id = $1", rid)
        if existing:
            await conn.execute(
                "UPDATE diary_sources SET name=$2, field_mapping=$3, ckan_metadata=$4, "
                "updated_at=now() WHERE id=$1",
                existing["id"], title, mapping, ckan_meta)
            return existing["id"]
        cnt = await conn.fetchval("SELECT count(*) FROM diary_sources") or 0
        color = SOURCE_COLORS[cnt % len(SOURCE_COLORS)]
        row = await conn.fetchrow(
            "INSERT INTO diary_sources "
            "(name, dataset_id, resource_id, dataset_url, resource_url, color, "
            " is_enabled, sync_status, field_mapping, ckan_metadata, person_id, organization_id) "
            "VALUES ($1,$2,$3,$4,$5,$6,true,'pending',$7,$8,$9,$10) RETURNING id",
            title, ds_id, rid, ds_url, res_url, color, mapping, ckan_meta, person_id, org_id)
        return row["id"]


_UPSERT_SQL = """
INSERT INTO diary_events
    (source_id, title, start_time, end_time, location, participants,
     dataset_name, dataset_link, is_active, other_fields, ckan_row_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,true,$9,$10)
ON CONFLICT (source_id, ckan_row_id) WHERE ckan_row_id IS NOT NULL
DO UPDATE SET title=EXCLUDED.title, start_time=EXCLUDED.start_time,
    end_time=EXCLUDED.end_time, location=EXCLUDED.location,
    participants=EXCLUDED.participants, other_fields=EXCLUDED.other_fields,
    updated_at=now()
"""


async def _upsert_events(records: list[dict]) -> int:
    if not records:
        return 0
    args = [(
        r["source_id"], r["title"], r["start_time"], r["end_time"], r["location"],
        r["participants"], r["dataset_name"], r["dataset_link"], r["other_fields"],
        r["ckan_row_id"],
    ) for r in records]
    pool = await ocal_db.get_pool()
    async with pool.acquire() as conn:
        for i in range(0, len(args), 500):
            await conn.executemany(_UPSERT_SQL, args[i:i + 500])
    return len(records)


async def _update_source_stats(source_id) -> None:
    await ocal_db.execute(
        "UPDATE diary_sources SET "
        "  total_events = (SELECT count(*) FROM diary_events WHERE source_id=$1 AND is_active), "
        "  first_event_date = (SELECT min(event_date) FROM diary_events WHERE source_id=$1 AND is_active), "
        "  last_event_date = (SELECT max(event_date) FROM diary_events WHERE source_id=$1 AND is_active), "
        "  sync_status='completed', last_sync_at=now(), updated_at=now() "
        "WHERE id=$1",
        source_id)


async def _record_exception(res: dict, pkg: dict, reason: str) -> None:
    ds_title = (pkg.get("title") or res.get("name") or res.get("id") or "")[:500]
    await ocal_db.execute(
        "INSERT INTO diary_exceptions "
        "  (dataset_title, resource_id, dataset_id, resource_format, resource_name, exception_reason) "
        "SELECT $1,$2,$3,$4,$5,$6 "
        "WHERE NOT EXISTS (SELECT 1 FROM diary_exceptions WHERE resource_id=$2)",
        ds_title, res["id"], res.get("package_id"),
        (res.get("format") or None), res.get("name"), reason)


# ── the import ────────────────────────────────────────────────────────────────

async def import_resource(resource_id: str, *, force: bool = False,
                          enrich: bool = True) -> dict:
    """Import one odata diary resource into the ocal DB.

    ``force`` (manual admin action) bypasses the auto-import gate. Without it, a
    resource that fails the gate raises SkipImport and is recorded in
    diary_exceptions so discovery won't re-evaluate it. ``enrich`` runs the free
    entity-extraction / cross-ref / matching chain afterwards (non-fatal)."""
    if not ocal_db.is_configured():
        raise RuntimeError("OCAL_DATABASE_URL not configured")

    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0), follow_redirects=True) as client:
        res = await odi._fetch_json(client, "resource_show", id=resource_id)
        pkg: dict = {}
        if res.get("package_id"):
            try:
                pkg = await odi._fetch_json(client, "package_show", id=res["package_id"])
            except Exception:  # noqa: BLE001 — package metadata is nice-to-have
                logger.debug("ocal_import: package_show failed for %s", res.get("package_id"))

    columns, rows = await _fetch_and_parse(res)
    mapping, conf, map_method = await map_fields_best(columns, rows)

    if not force:
        if not all(t in mapping for t in _REQUIRED):
            await _record_exception(res, pkg, "auto_rejected")
            raise SkipImport("no title / start-date column mapped")
        if conf < settings.ocal_import_confidence:
            await _record_exception(res, pkg, "auto_rejected")
            raise SkipImport(f"low mapping confidence {conf:.2f}")
        if len(rows) < settings.ocal_import_min_rows:
            await _record_exception(res, pkg, "auto_rejected")
            raise SkipImport(f"too few rows ({len(rows)})")

    person_id, org_id, _owner_score = await _identify_owner(
        pkg.get("title"), res.get("name"), ((pkg.get("organization") or {}) or {}).get("title"))
    source_id = await _ensure_source(res, pkg, mapping, person_id, org_id)

    ds_name = ((pkg.get("title") or "").strip() or (res.get("name") or "").strip()
               or pkg.get("name") or "יומן")
    ds_id = res.get("package_id")
    ds_link = f"{CKAN_BASE}/dataset/{ds_id}" if ds_id else (res.get("url") or None)

    records = []
    for i, row in enumerate(rows, start=1):
        rec = dict(zip(columns, row))
        ev = transform_record(rec, mapping, source_id, ds_name, ds_link, i)
        if ev:
            records.append(ev)

    inserted = await _upsert_events(records)
    await _update_source_stats(source_id)
    logger.info("ocal_import: %s -> source %s (%d/%d rows, conf=%.2f, map=%s)",
                resource_id, source_id, inserted, len(rows), conf, map_method)

    enriched = None
    if enrich:
        try:
            from app.services import ocal_enrich
            # AI-NER stays off the auto path unless ocal_ai_ner_auto is set —
            # the legacy Ocal pipeline imported with skipAI=true (free stages
            # only); the paid LLM stage is an admin-triggered action by default.
            enriched = await ocal_enrich.enrich_source(
                source_id, is_resync=False, run_ai=settings.ocal_ai_ner_auto)
        except Exception:  # noqa: BLE001 — enrichment is best-effort
            logger.exception("ocal_import: enrichment failed for %s", source_id)

    return {
        "resource_id": resource_id, "source_id": str(source_id), "title": ds_name,
        "rows_parsed": len(rows), "events_upserted": inserted,
        "mapping": mapping, "confidence": round(conf, 3), "map_method": map_method,
        "person_matched": person_id is not None, "enriched": enriched,
    }


# ── discovery + scan ──────────────────────────────────────────────────────────

def _importable_resources(pkg: dict) -> list[dict]:
    out = []
    names_lower = []
    for r in pkg.get("resources") or []:
        url = (r.get("url") or "").strip()
        if not url and not r.get("datastore_active"):
            continue
        fmt = (r.get("format") or "").upper()
        if not (r.get("datastore_active") or fmt in odi.SUPPORTED_FILE_FORMATS):
            continue
        out.append(r)
        names_lower.append((r.get("name") or "").lower())
    # Drop CKAN auto-generated "Converted CSV" dupes when a real file also exists.
    if len(out) > 1:
        out = [r for r in out if "converted csv" not in (r.get("name") or "").lower()] or out
    return out


async def _known_resource_ids() -> set[str]:
    rows = await ocal_db.fetch(
        "SELECT resource_id FROM diary_sources WHERE resource_id IS NOT NULL "
        "UNION SELECT resource_id FROM diary_exceptions")
    return {r["resource_id"] for r in rows}


async def discover_candidates(limit: int | None = None) -> list[dict]:
    """New importable diary resources (not already a source or an exception),
    newest datasets first."""
    packages: list[dict] = []
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0), follow_redirects=True) as client:
        start = 0
        while len(packages) < MAX_DATASETS:
            r = await odi._fetch_json(
                client, "package_search", q=DIARY_QUERY, rows=PAGE_SIZE,
                start=start, sort="metadata_modified desc")
            batch = r.get("results") or []
            total = r.get("count") or 0
            packages.extend(batch)
            if len(batch) < PAGE_SIZE or len(packages) >= total:
                break
            start += PAGE_SIZE

    known = await _known_resource_ids()
    cands: list[dict] = []
    for pkg in packages[:MAX_DATASETS]:
        org = ((pkg.get("organization") or {}) or {}).get("title")
        for r in _importable_resources(pkg):
            if r.get("id") in known:
                continue
            cands.append({
                "resource_id": r.get("id"), "resource_name": r.get("name"),
                "format": r.get("format"), "dataset_title": pkg.get("title"),
                "dataset_id": pkg.get("id"), "organization": org,
                "last_modified": r.get("last_modified"),
            })
            if limit and len(cands) >= limit:
                return cands
    return cands


async def scan_once(max_import: int | None = None) -> dict:
    """Discover new diary resources and import up to ``max_import`` that pass the
    gate; the rest of those evaluated are recorded as exceptions."""
    if not ocal_db.is_configured():
        return {"enabled": False}
    cap = max_import if max_import is not None else settings.ocal_import_per_tick
    cands = await discover_candidates()
    imported, skipped, errors = [], 0, 0
    for c in cands:
        if len(imported) >= cap:
            break
        rid = c["resource_id"]
        try:
            imported.append(await import_resource(rid, force=False))
        except SkipImport as e:
            skipped += 1
            logger.info("ocal_import: skip %s — %s", rid, e)
        except Exception:  # noqa: BLE001 — one bad resource must not stop the scan
            errors += 1
            logger.exception("ocal_import: failed to import %s", rid)
    return {"candidates": len(cands), "imported": len(imported),
            "skipped": skipped, "errors": errors, "results": imported}
