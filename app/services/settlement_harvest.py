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


async def _unresolved_values(schema: str, table: str, col: str, cap: int) -> list[tuple[str, int]]:
    """Distinct values of one locality column that the rule index does NOT resolve
    (neither as a settlement nor an authority), with their occurrence counts."""
    ro = await append_store.get_readonly_pool()
    ref = f"{_qi(schema)}.{_qi(table)}"
    q = _qi(col)
    sql = (
        f"WITH d AS (SELECT {q}::text AS val, count(*) c FROM {ref} "
        f"           WHERE {q} IS NOT NULL AND {q}::text <> '' GROUP BY 1) "
        f"SELECT val, c FROM d "
        f"WHERE public.over_settlement_code(val) IS NULL "
        f"  AND public.over_authority_code(val) IS NULL "
        f"ORDER BY c DESC LIMIT {int(cap)}"
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
                                 base_url=table_profiler._DEEPSEEK_BASE_URL)
            resp = await client.chat.completions.create(
                model=table_profiler._DEEPSEEK_MODEL, temperature=0, max_tokens=4000,
                response_format={"type": "json_object"},
                messages=[{"role": "system", "content": _LLM_PROMPT},
                          {"role": "user", "content": user}])
            raw = resp.choices[0].message.content or ""
        else:
            from anthropic import AsyncAnthropic
            client = AsyncAnthropic(api_key=settings.anthropic_api_key)
            resp = await client.messages.create(
                model=table_profiler._ANTHROPIC_MODEL, max_tokens=4000,
                system=_LLM_PROMPT, messages=[{"role": "user", "content": user}])
            raw = "".join(b.text for b in resp.content if getattr(b, "type", None) == "text")
    except Exception:  # noqa: BLE001
        logger.exception("harvest LLM batch failed")
        return {}
    parsed = table_profiler._parse_json(raw)
    items = parsed if isinstance(parsed, list) else (
        parsed.get("results") or parsed.get("data") or [])
    out: dict[str, str] = {}
    for it in items:
        if isinstance(it, dict) and it.get("input") and it.get("official"):
            out[str(it["input"])] = str(it["official"])
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


async def harvest(*, use_llm: bool = True, per_col_cap: int = 3000) -> dict:
    """Run the full harvest. Returns a summary of what was scanned/mapped."""
    await ensure_unresolved_table()
    cols = await locality_columns()

    # Aggregate unresolved distinct values across all locality columns by norm key.
    agg: dict[str, dict] = {}
    scanned_cols = 0
    for c in cols:
        try:
            vals = await _unresolved_values(c["schema"], c["table"], c["column"], per_col_cap)
        except Exception as exc:  # noqa: BLE001 — skip a bad/huge column, keep going
            logger.info("harvest: skipped %s.%s.%s: %s", c["schema"], c["table"], c["column"], exc)
            continue
        scanned_cols += 1
        for val, cnt in vals:
            k = norm(val)
            if not k:
                continue
            e = agg.setdefault(k, {"value": val, "occ": 0, "sources": []})
            e["occ"] += cnt
            e["sources"].append({"schema": c["schema"], "table": c["table"],
                                  "column": c["column"], "count": cnt})

    unresolved_keys = list(agg.keys())
    llm_mapped = 0
    still_unresolved = 0

    # LLM pass over the unique surface values, then validate the guess via the index.
    guesses: dict[str, str] = {}
    if use_llm and unresolved_keys and table_profiler.llm_available():
        uniq_values = [agg[k]["value"] for k in unresolved_keys]
        for i in range(0, len(uniq_values), LLM_BATCH):
            guesses.update(await _llm_map(uniq_values[i:i + LLM_BATCH]))

    pool = await append_store.get_pool()
    for k, e in agg.items():
        val = e["value"]
        official = guesses.get(val)
        resolved_code = resolved_name = entity = None
        status = "unresolved"
        if official:
            s = await settlement_index.resolve(official)  # settlement first
            if s:
                resolved_code, resolved_name, entity = s["code"], s["name"], "settlement"
            else:
                a = await _resolve_authority(official)
                if a:
                    resolved_code, resolved_name, entity = a["code"], a["name"], "authority"
        if resolved_code is not None:
            await _add_alias(entity, val, resolved_code)
            status = "llm_mapped"; llm_mapped += 1
        else:
            still_unresolved += 1
        async with pool.acquire() as conn:
            await conn.execute(
                f"""INSERT INTO public.{_qi(UNRESOLVED_TABLE)}
                    (norm_key,value_raw,occurrences,sources,llm_guess,resolved_code,
                     resolved_name,entity,status,updated_at)
                    VALUES ($1,$2,$3,$4::jsonb,$5,$6,$7,$8,$9, now())
                    ON CONFLICT (norm_key) DO UPDATE SET
                      value_raw=EXCLUDED.value_raw, occurrences=EXCLUDED.occurrences,
                      sources=EXCLUDED.sources, llm_guess=EXCLUDED.llm_guess,
                      resolved_code=EXCLUDED.resolved_code, resolved_name=EXCLUDED.resolved_name,
                      entity=EXCLUDED.entity, status=EXCLUDED.status, updated_at=now()""",
                k, val, e["occ"], json.dumps(e["sources"], ensure_ascii=False),
                official, resolved_code, resolved_name, entity, status)

    return {
        "locality_columns": len(cols), "columns_scanned": scanned_cols,
        "distinct_unresolved_values": len(agg),
        "llm_mapped": llm_mapped, "still_unresolved": still_unresolved,
    }


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
