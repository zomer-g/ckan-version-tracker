"""Settlement (יישוב) reference index — official CBS names + their inflections.

A PROCESSED reference dataset, like מידע לעם / יומן לעם: it takes the CBS
"קובץ היישובים" (bycode) as the authoritative source of official locality names
and codes (סמל יישוב), and derives, for every locality, the many surface forms
people actually write — Hebrew prepositional prefixes (ב/ל/מ/ה/ו…), hyphen/space
and geresh variants, parenthetical strippings (שבט/קיבוץ), and the English /
transliteration spellings. Those go into an ALIAS index so any free-text
locality value in the site's datasets can be resolved to one canonical name.

Two tables in the append DB's ``public`` schema (so they are queryable from the
same /data SQL console and get the read-only role's SELECT automatically):

* ``over_settlements``        — one row per official locality (code = PK).
* ``over_settlement_aliases`` — one row per (normalized variant → code), with a
  ``weight`` so the resolver prefers an official-name hit over a prefixed guess.

The ``over_`` prefix marks these as OVER-generated artifacts (same convention as
over_table_profiles and the coming per-dataset ``Over_Settlement`` columns).

Seed: ``data/settlements_2024.json`` (generated from bycode2024.xlsx, committed).
Load via ``POST /api/admin/settlements/load`` (worker/admin).
"""
from __future__ import annotations

import json
import logging
import os
import re

from app.services import append_store
from app.services.append_store import _qi

logger = logging.getLogger(__name__)

SETTLEMENTS_TABLE = "over_settlements"
ALIASES_TABLE = "over_settlement_aliases"
SEED_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                         "data", "settlements_2024.json")

# Hebrew one-letter prepositions/conjunctions that attach to a place name, plus
# the common two-letter combinations. Applied to the normalized base form.
_PREFIXES = ["ה", "ו", "ב", "ל", "מ", "כ", "ש",
             "וה", "ול", "וב", "ומ", "וכ", "כש", "מה", "שב", "של", "שה", "לכ"]

_PAREN_RE = re.compile(r"\s*\([^)]*\)\s*")          # "(שבט)", "(קיבוץ)"
# Keep ONLY Hebrew consonant letters (alef..tav incl. finals, U+05D0–U+05EA),
# Latin letters, and digits. This deliberately drops niqqud, the Hebrew geresh
# ׳ / gershayim ״ / maqaf ־ (all inside the Hebrew block) as well as ASCII
# punctuation and whitespace, so every geresh/hyphen/space variant collapses.
_KEEP = re.compile(r"[^0-9A-Za-zא-ת]+")


def norm(s: str | None) -> str:
    """Aggressive normalization for the alias KEY: drop everything but Hebrew
    consonant letters, Latin letters, and digits, and lowercase Latin.
    'תל אביב-יפו' → 'תלאביביפו'; \"אבו ג'ווייעד\" and 'אבו ג׳ווייעד' → same key."""
    if not s:
        return ""
    return _KEEP.sub("", str(s)).lower()


def strip_paren(s: str) -> str:
    return _PAREN_RE.sub(" ", s or "").strip()


# ── alias generation (pure) ───────────────────────────────────────────────────
def aliases_for(rec: dict) -> list[tuple[str, str, str, int]]:
    """All (variant_key, surface, kind, weight) aliases for one settlement.

    weight orders resolution when a key is ambiguous: official > no-paren >
    english/translit > prefixed."""
    out: list[tuple[str, str, str, int]] = []
    seen: set[str] = set()

    def add(key: str, surface: str, kind: str, weight: int):
        if not key or key in seen:
            return
        seen.add(key)
        out.append((key, surface, kind, weight))

    name = (rec.get("name") or "").strip()
    if not name:
        return out
    no_paren = strip_paren(name)

    # Hebrew surface forms → normalized base + prefixed variants.
    heb_forms = [(name, "official", 100)]
    if no_paren and no_paren != name:
        heb_forms.append((no_paren, "no_paren", 90))
    for surface, kind, w in heb_forms:
        base = norm(surface)
        if not base:
            continue
        add(base, surface, kind, w)
        # Only prefix genuinely Hebrew forms.
        if re.search(r"[֐-׿]", base):
            for p in _PREFIXES:
                add(p + base, p + surface, "prefix", 50)

    # Latin spellings — English + transliteration (no Hebrew prefixing).
    for field, kind in (("name_en", "english"), ("translit", "translit")):
        v = (rec.get(field) or "").strip()
        k = norm(v)
        if k and re.search(r"[A-Za-z]", k):
            add(k, v, kind, 70)

    return out


# ── DDL + load ────────────────────────────────────────────────────────────────
async def ensure_tables() -> None:
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS public.{_qi(SETTLEMENTS_TABLE)} (
                code                 integer PRIMARY KEY,
                name                 text NOT NULL,
                name_en              text,
                translit             text,
                district             text,
                subdistrict          text,
                municipal_status     text,
                local_authority      text,
                local_authority_code integer,
                population           integer,
                year                 integer,
                updated_at           timestamptz DEFAULT now()
            )
            """
        )
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS public.{_qi(ALIASES_TABLE)} (
                variant  text    NOT NULL,
                code     integer NOT NULL,
                surface  text,
                kind     text,
                weight   integer DEFAULT 50,
                PRIMARY KEY (variant, code)
            )
            """
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS {_qi(ALIASES_TABLE + '_variant_idx')} "
            f"ON public.{_qi(ALIASES_TABLE)} (variant)")


def load_seed() -> list[dict]:
    with open(SEED_PATH, encoding="utf-8") as fh:
        return json.load(fh)


async def load(*, rebuild: bool = True) -> dict:
    """(Re)load the settlements + regenerate the alias index from the seed."""
    recs = load_seed()
    await ensure_tables()
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            if rebuild:
                await conn.execute(f"TRUNCATE public.{_qi(SETTLEMENTS_TABLE)}")
                await conn.execute(f"TRUNCATE public.{_qi(ALIASES_TABLE)}")
            await conn.executemany(
                f"""INSERT INTO public.{_qi(SETTLEMENTS_TABLE)}
                    (code,name,name_en,translit,district,subdistrict,municipal_status,
                     local_authority,local_authority_code,population,year)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                    ON CONFLICT (code) DO UPDATE SET
                      name=EXCLUDED.name, name_en=EXCLUDED.name_en, translit=EXCLUDED.translit,
                      district=EXCLUDED.district, subdistrict=EXCLUDED.subdistrict,
                      municipal_status=EXCLUDED.municipal_status,
                      local_authority=EXCLUDED.local_authority,
                      local_authority_code=EXCLUDED.local_authority_code,
                      population=EXCLUDED.population, year=EXCLUDED.year, updated_at=now()""",
                [(r["code"], r["name"], r.get("name_en"), r.get("translit"), r.get("district"),
                  r.get("subdistrict"), r.get("municipal_status"), r.get("local_authority"),
                  r.get("local_authority_code"), r.get("population"), r.get("year")) for r in recs],
            )
            alias_rows: list[tuple] = []
            for r in recs:
                for key, surface, kind, weight in aliases_for(r):
                    alias_rows.append((key, r["code"], surface, kind, weight))
            await conn.executemany(
                f"""INSERT INTO public.{_qi(ALIASES_TABLE)} (variant,code,surface,kind,weight)
                    VALUES ($1,$2,$3,$4,$5)
                    ON CONFLICT (variant,code) DO UPDATE SET
                      surface=EXCLUDED.surface, kind=EXCLUDED.kind, weight=EXCLUDED.weight""",
                alias_rows,
            )
    logger.info("settlement index loaded: %d settlements, %d aliases", len(recs), len(alias_rows))
    return {"settlements": len(recs), "aliases": len(alias_rows)}


# ── query ─────────────────────────────────────────────────────────────────────
async def resolve(q: str) -> dict | None:
    """Resolve a free-text locality value to its official settlement.

    Normalizes the input and looks it up in the alias index; on an ambiguous key
    (several codes) picks the highest-weight, then most-populous. Returns the
    canonical settlement row plus how it matched, or None."""
    key = norm(q)
    if not key:
        return None
    pool = await append_store.get_readonly_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""SELECT s.*, a.kind AS match_kind, a.weight AS match_weight
                FROM public.{_qi(ALIASES_TABLE)} a
                JOIN public.{_qi(SETTLEMENTS_TABLE)} s ON s.code = a.code
                WHERE a.variant = $1
                ORDER BY a.weight DESC, s.population DESC NULLS LAST
                LIMIT 1""",
            key,
        )
    return dict(row) if row else None


async def search(q: str | None, limit: int = 25) -> list[dict]:
    pool = await append_store.get_readonly_pool()
    async with pool.acquire() as conn:
        if q:
            rows = await conn.fetch(
                f"""SELECT * FROM public.{_qi(SETTLEMENTS_TABLE)}
                    WHERE name ILIKE $1 OR name_en ILIKE $1 OR translit ILIKE $1
                    ORDER BY population DESC NULLS LAST LIMIT $2""",
                f"%{q}%", limit)
        else:
            rows = await conn.fetch(
                f"SELECT * FROM public.{_qi(SETTLEMENTS_TABLE)} "
                f"ORDER BY population DESC NULLS LAST LIMIT $1", limit)
    return [dict(r) for r in rows]


async def get(code: int) -> dict | None:
    pool = await append_store.get_readonly_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT * FROM public.{_qi(SETTLEMENTS_TABLE)} WHERE code=$1", code)
        aliases = await conn.fetch(
            f"SELECT variant,surface,kind,weight FROM public.{_qi(ALIASES_TABLE)} "
            f"WHERE code=$1 ORDER BY weight DESC, kind", code)
    if not row:
        return None
    d = dict(row)
    d["aliases"] = [dict(a) for a in aliases]
    return d


async def stats() -> dict:
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        try:
            ns = await conn.fetchval(f"SELECT count(*) FROM public.{_qi(SETTLEMENTS_TABLE)}")
            na = await conn.fetchval(f"SELECT count(*) FROM public.{_qi(ALIASES_TABLE)}")
        except Exception:  # noqa: BLE001 — not loaded yet
            return {"loaded": False, "settlements": 0, "aliases": 0}
    return {"loaded": bool(ns), "settlements": int(ns or 0), "aliases": int(na or 0)}
