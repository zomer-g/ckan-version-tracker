"""Data-driven alias harvest for the settlement/authority index.

The rule-based inflections (settlement_index.aliases_for) only ever saw the CBS
OFFICIAL names. This pass instead looks at what people ACTUALLY wrote: it scans
every locality/authority column the profiler flagged, across all datasets, and
for each distinct real value:

  1. tries the fast rule index (over_settlement_code / over_authority_code);
  2. the MISSES are the genuine gaps — a real-world variant, typo, abbreviation,
     English spelling. It asks an LLM for each miss's official name and validates
     that guess back through the index; a validated guess is written as a new
     ``llm`` alias (so it resolves instantly next time — the self-improving loop);
  3. whatever the LLM still can't map is recorded in ``over_settlement_unresolved``
     with its source table(s)/column(s) and occurrence count, for human review.

Everything stays separate from the source data — this only READS the datasets and
WRITES to the OVER index tables.
"""
from __future__ import annotations

import json
import logging

from app.config import settings
from app.services import append_store, settlement_index
from app.services.append_store import _qi
from app.services.settlement_index import norm
from app.services import table_profiler  # provider selection + JSON parse reuse

logger = logging.getLogger(__name__)

UNRESOLVED_TABLE = "over_settlement_unresolved"
_LOCALITY_ENTITIES = ("locality", "municipality")
LLM_BATCH = 40


async def ensure_unresolved_table() -> None:
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS public.{_qi(UNRESOLVED_TABLE)} (
                norm_key      text PRIMARY KEY,
                value_raw     text,
                occurrences   bigint,
                sources       jsonb,
                llm_guess     text,
                resolved_code integer,
                resolved_name text,
                entity        text,        -- settlement | authority | null
                status        text,        -- llm_mapped | unresolved
                updated_at    timestamptz DEFAULT now()
            )
            """
        )


async def locality_columns() -> list[dict]:
    """Every (schema, table, column) the profiler classified as a locality or
    municipality — from over_table_profiles (heuristic guess OR LLM semantic_type)."""
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT p.schema_name, p.table_name, c.col AS column_name
            FROM public.{_qi(table_profiler.PROFILE_TABLE)} p,
                 jsonb_each(p.sql_profile->'columns') AS c(col, data)
            WHERE (c.data->'entity_guess'->>'guess') = ANY($1)
               OR (p.llm_enrichment->'columns'->c.col->>'semantic_type') = ANY($1)
            """,
            list(_LOCALITY_ENTITIES),
        )
    return [{"schema": r["schema_name"], "table": r["table_name"], "column": r["column_name"]}
            for r in rows]


# A column is only treated as a genuine locality column if at least this fraction
# of its DISTINCT values already resolve to a real settlement/authority. This
# filters out free-text/name columns the profiler heuristic over-flagged (which
# would otherwise dump thousands of non-place values into the unresolved pile).
MIN_RESOLVE_RATE = 0.4
MIN_DISTINCT = 3


async def _unresolved_values(schema: str, table: str, col: str, cap: int) -> list[tuple[str, int]]:
    """Distinct UNRESOLVED values of one locality column — but ONLY if the column
    is genuinely locality-like (≥ MIN_RESOLVE_RATE of its distinct values resolve).
    A column that fails the gate returns [] (it isn't really a place column)."""
    ro = await append_store.get_readonly_pool()
    ref = f"{_qi(schema)}.{_qi(table)}"
    q = _qi(col)
    sql = (
        f"WITH d AS (SELECT {q}::text AS val, count(*) c FROM {ref} "
        f"           WHERE {q} IS NOT NULL AND {q}::text <> '' GROUP BY 1), "
        f"     r AS (SELECT val, c, (public.over_settlement_code(val) IS NOT NULL "
        f"                       OR public.over_authority_code(val) IS NOT NULL) resolved FROM d), "
        f"     s AS (SELECT count(*) tot, count(*) FILTER (WHERE resolved) res FROM r) "
        f"SELECT r.val, r.c FROM r, s "
        f"WHERE NOT r.resolved AND s.tot >= {MIN_DISTINCT} "
        f"  AND s.res::float / NULLIF(s.tot,0) >= {MIN_RESOLVE_RATE} "
        f"ORDER BY r.c DESC LIMIT {int(cap)}"
    )
    async with ro.acquire() as conn:
        async with conn.transaction(readonly=True):
            await conn.execute("SET LOCAL statement_timeout = 60000")
            recs = await conn.fetch(sql)
    return [(r["val"], int(r["c"])) for r in recs]


_LLM_PROMPT = """אתה ממפה שמות של מקומות בישראל (יישובים ורשויות מקומיות). לכל קלט — \
שעשוי להיות משובש, מקוצר, בכתיב חריג, באנגלית, או עם תוספות — החזר את השם הרשמי \
הנוכחי בעברית של היישוב או הרשות המקומית, או null אם אין זה שם של מקום בישראל.
החזר JSON array בלבד: [{"input":"<כפי שהתקבל>","official":"<שם רשמי או null>"}]. \
אל תמציא מקומות שאינם קיימים; בספק — null."""


def _loads(raw: str):
    """Tolerant JSON load that PRESERVES a top-level list (strips ``` fences)."""
    import re
    s = (raw or "").strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z]*\n?", "", s)
        s = re.sub(r"\n?```$", "", s).strip()
    try:
        return json.loads(s)
    except (ValueError, TypeError):
        for a, b in (("[", "]"), ("{", "}")):
            i, j = s.find(a), s.rfind(b)
            if i >= 0 and j > i:
                try:
                    return json.loads(s[i:j + 1])
                except (ValueError, TypeError):
                    pass
    return None


async def _llm_map(values: list[str]) -> dict[str, str]:
    """{input_value: official_name_guess} for a batch, via DeepSeek→Anthropic."""
    provider = table_profiler.llm_provider()
    if not provider:
        return {}
    user = json.dumps([{"input": v} for v in values], ensure_ascii=False)
    try:
        if provider == "deepseek":
            from openai import AsyncOpenAI
            client = AsyncOpenAI(api_key=settings.deepseek_api_key,
                                 base_url=table_profiler._DEEPSEEK_BASE_URL, timeout=90)
            resp = await client.chat.completions.create(
                model=table_profiler._DEEPSEEK_MODEL, temperature=0, max_tokens=4000,
                response_format={"type": "json_object"},
                messages=[{"role": "system", "content": _LLM_PROMPT},
                          {"role": "user", "content": user}])
            raw = resp.choices[0].message.content or ""
        else:
            from anthropic import AsyncAnthropic
            client = AsyncAnthropic(api_key=settings.anthropic_api_key, timeout=90)
            resp = await client.messages.create(
                model=table_profiler._ANTHROPIC_MODEL, max_tokens=4000,
                system=_LLM_PROMPT, messages=[{"role": "user", "content": user}])
            raw = "".join(b.text for b in resp.content if getattr(b, "type", None) == "text")
    except Exception:  # noqa: BLE001
        logger.exception("harvest LLM batch failed")
        return {}
    parsed = _loads(raw)  # list OR dict (table_profiler._parse_json drops lists)
    out: dict[str, str] = {}

    def _clean(v):
        s = "" if v is None else str(v).strip()
        return s if s and s.lower() not in ("null", "none", "-") else None

    # Shape 1: an array of {input, official}. Shape 2: {results|data|...: [...]}.
    # Shape 3 (what DeepSeek json_object mode tends to return): a FLAT object
    # {input: official}. Handle all three so the mapping is never silently lost.
    lst = None
    if isinstance(parsed, list):
        lst = parsed
    elif isinstance(parsed, dict):
        for key in ("results", "data", "mappings", "items", "output"):
            if isinstance(parsed.get(key), list):
                lst = parsed[key]; break
        if lst is None:
            lst = next((v for v in parsed.values() if isinstance(v, list)), None)
    if lst is not None:
        for it in lst:
            if isinstance(it, dict):
                inp, off = it.get("input"), _clean(it.get("official"))
                if inp and off:
                    out[str(inp)] = off
    elif isinstance(parsed, dict):
        for k, v in parsed.items():                 # flat {input: official}
            off = _clean(v)
            if off:
                out[str(k)] = off
    return out


async def _add_alias(entity: str, value: str, code: int) -> None:
    """Persist a validated value→code as an 'llm' alias so it resolves instantly next time."""
    table = (settlement_index.ALIASES_TABLE if entity == "settlement"
             else settlement_index.AUTH_ALIASES_TABLE)
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            f"""INSERT INTO public.{_qi(table)} (variant,code,surface,kind,weight)
                VALUES ($1,$2,$3,'llm',60)
                ON CONFLICT (variant,code) DO UPDATE SET surface=EXCLUDED.surface""",
            norm(value), code, value)


async def harvest_scan(*, rebuild: bool = True, per_col_cap: int = 3000) -> dict:
    """Phase A — SQL only, no LLM. Scan every locality column and UPSERT the
    unresolved distinct values INCREMENTALLY (per column), so progress is visible
    in over_settlement_unresolved as it runs and one slow column can't lose the
    rest. Accumulates occurrences + source list across columns via ON CONFLICT."""
    await ensure_unresolved_table()
    pool = await append_store.get_pool()
    if rebuild:
        async with pool.acquire() as conn:
            await conn.execute(f"TRUNCATE public.{_qi(UNRESOLVED_TABLE)}")
    cols = await locality_columns()
    scanned = 0
    for c in cols:
        try:
            vals = await _unresolved_values(c["schema"], c["table"], c["column"], per_col_cap)
        except Exception as exc:  # noqa: BLE001 — skip a bad/huge column, keep going
            logger.info("harvest: skipped %s.%s.%s: %s", c["schema"], c["table"], c["column"], exc)
            continue
        scanned += 1
        if not vals:
            continue
        src = {"schema": c["schema"], "table": c["table"], "column": c["column"]}
        rows = [(norm(v), v, cnt, json.dumps([{**src, "count": cnt}], ensure_ascii=False))
                for v, cnt in vals if norm(v)]
        async with pool.acquire() as conn:
            await conn.executemany(
                f"""INSERT INTO public.{_qi(UNRESOLVED_TABLE)}
                    (norm_key,value_raw,occurrences,sources,status,updated_at)
                    VALUES ($1,$2,$3,$4::jsonb,'pending', now())
                    ON CONFLICT (norm_key) DO UPDATE SET
                      occurrences = public.{_qi(UNRESOLVED_TABLE)}.occurrences + EXCLUDED.occurrences,
                      sources = public.{_qi(UNRESOLVED_TABLE)}.sources || EXCLUDED.sources,
                      updated_at = now()""",
                rows)
    async with pool.acquire() as conn:
        total = await conn.fetchval(f"SELECT count(*) FROM public.{_qi(UNRESOLVED_TABLE)}")
    return {"locality_columns": len(cols), "columns_scanned": scanned,
            "distinct_unresolved_values": int(total or 0)}


async def harvest_llm_pass(*, limit: int = 4000) -> dict:
    """Phase B — take the still-pending unresolved values, ask the LLM for each
    one's official name, validate that guess through the index, and on success
    write an 'llm' alias + mark the row llm_mapped. Batched, timeout-guarded."""
    if not table_profiler.llm_available():
        return {"llm_available": False}
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        # 'pending' = not yet tried; also RETRY 'unresolved' rows that never got a
        # guess (e.g. an earlier parse bug), but leave ones already llm_mapped.
        pend = await conn.fetch(
            f"SELECT norm_key, value_raw FROM public.{_qi(UNRESOLVED_TABLE)} "
            f"WHERE resolved_code IS NULL AND (status = 'pending' OR llm_guess IS NULL) "
            f"ORDER BY occurrences DESC LIMIT {int(limit)}")
    mapped = still = failed = 0
    for i in range(0, len(pend), LLM_BATCH):
        batch = pend[i:i + LLM_BATCH]
        try:
            await _process_batch(pool, batch)
        except Exception:  # noqa: BLE001 — one bad batch must not kill the whole pass
            logger.exception("harvest_llm_pass: batch %d failed", i)
            failed += len(batch)
    # Recount from the DB (authoritative) rather than trusting the loop counters.
    async with pool.acquire() as conn:
        mapped = await conn.fetchval(
            f"SELECT count(*) FROM public.{_qi(UNRESOLVED_TABLE)} WHERE resolved_code IS NOT NULL")
        still = await conn.fetchval(
            f"SELECT count(*) FROM public.{_qi(UNRESOLVED_TABLE)} "
            f"WHERE llm_guess IS NOT NULL AND resolved_code IS NULL")
    return {"processed": len(pend), "llm_mapped": int(mapped or 0),
            "unresolved": int(still or 0), "failed_rows": failed}


async def _process_batch(pool, batch) -> None:
    """Map + persist one LLM batch; each row guarded so a single bad row (odd DB
    error, unexpected value) can't abort the batch."""
    guesses = await _llm_map([r["value_raw"] for r in batch])
    for r in batch:
        try:
            official = guesses.get(r["value_raw"])
            code = name = entity = None
            if official:
                s = await settlement_index.resolve(official)
                if s:
                    code, name, entity = s["code"], s["name"], "settlement"
                else:
                    a = await _resolve_authority(official)
                    if a:
                        code, name, entity = a["code"], a["name"], "authority"
            if code is not None:
                await _add_alias(entity, r["value_raw"], code)
            async with pool.acquire() as conn:
                if code is not None:
                    await conn.execute(
                        f"""UPDATE public.{_qi(UNRESOLVED_TABLE)} SET llm_guess=$2,
                            resolved_code=$3, resolved_name=$4, entity=$5,
                            status='llm_mapped', updated_at=now() WHERE norm_key=$1""",
                        r["norm_key"], official, code, name, entity)
                else:
                    await conn.execute(
                        f"""UPDATE public.{_qi(UNRESOLVED_TABLE)} SET llm_guess=$2,
                            status='unresolved', updated_at=now() WHERE norm_key=$1""",
                        r["norm_key"], official)
        except Exception:  # noqa: BLE001 — skip a single bad row, keep the batch going
            logger.exception("harvest row failed: %r", r["value_raw"])


async def harvest(*, use_llm: bool = True, per_col_cap: int = 3000) -> dict:
    """Full harvest: incremental scan, then (optionally) the LLM pass."""
    scan = await harvest_scan(rebuild=True, per_col_cap=per_col_cap)
    llm = await harvest_llm_pass() if use_llm else {"skipped": True}
    return {**scan, "llm": llm}


async def _resolve_authority(q: str) -> dict | None:
    ro = await append_store.get_readonly_pool()
    async with ro.acquire() as conn:
        row = await conn.fetchrow(
            f"""SELECT s.code, s.name
                FROM public.{_qi(settlement_index.AUTH_ALIASES_TABLE)} a
                JOIN public.{_qi(settlement_index.AUTHORITIES_TABLE)} s ON s.code=a.code
                WHERE a.variant = $1 ORDER BY a.weight DESC LIMIT 1""",
            norm(q))
    return dict(row) if row else None
