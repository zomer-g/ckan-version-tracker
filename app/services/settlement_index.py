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
over_table_profiles). Cross-referencing is done WITHOUT touching the original
state data: SQL functions ``over_settlement(text)`` / ``over_settlement_code(text)``
(see ensure_functions) let a /data console user JOIN a locality value to its
official settlement at query time, so the source table stays untouched.

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
AUTHORITIES_TABLE = "over_authorities"
AUTH_ALIASES_TABLE = "over_authority_aliases"
_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data")
SEED_PATH = os.path.join(_DATA_DIR, "settlements_2024.json")
AUTH_SEED_PATH = os.path.join(_DATA_DIR, "authorities_2024.json")
# Curated supplement for what CBS official names miss: short forms (תל אביב →
# תל אביב -יפו), renames (נצרת עילית → נוף הגליל), and ktiv-haser spellings.
MANUAL_PATH = os.path.join(_DATA_DIR, "settlement_aliases_manual.json")

# Hebrew one-letter prepositions/conjunctions that attach to a place name, plus
# the common two-letter combinations. Applied to the normalized base form.
_PREFIXES = ["ה", "ו", "ב", "ל", "מ", "כ", "ש",
             "וה", "ול", "וב", "ומ", "וכ", "כש", "מה", "שב", "של", "שה", "לכ"]

# Administrative descriptors people prepend to an authority/place name in the
# data — "עיריית תל אביב", "מ.א. מטה בנימין", "מועצה מקומית X". Stored as their
# NORMALIZED forms (norm() drops the dots/spaces): "מ.א." → "מא". Prepended to the
# normalized base so "מ.א. מטה בנימין" (norm "מאמטהבנימין") resolves to the
# authority מטה בנימין. Especially recovers the regional councils.
_ADMIN_PREFIXES = ["עיריית", "עירית", "מועצהמקומית", "מועצהאזורית", "מא", "ממ", "מועצה"]

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
            for ap in _ADMIN_PREFIXES:            # "עיריית X", "מ.א. X", …
                add(ap + base, surface, "admin_prefix", 55)

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
        # Local authorities (רשות מקומית) — incl. regional councils, which are
        # NOT settlements. Same shape as the settlement index.
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS public.{_qi(AUTHORITIES_TABLE)} (
                code             integer PRIMARY KEY,
                name             text NOT NULL,
                district         text,
                municipal_status text,
                year             integer,
                updated_at       timestamptz DEFAULT now()
            )
            """
        )
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS public.{_qi(AUTH_ALIASES_TABLE)} (
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
            f"CREATE INDEX IF NOT EXISTS {_qi(AUTH_ALIASES_TABLE + '_variant_idx')} "
            f"ON public.{_qi(AUTH_ALIASES_TABLE)} (variant)")


async def ensure_functions() -> None:
    """Create the SQL cross-reference functions so a /data console user can JOIN
    a free-text locality value to the official settlement WITHOUT touching the
    original table — the state's data stays untouched, the mapping stays here:

        SELECT "רשות", over_settlement("רשות")      AS official_name,
                        over_settlement_code("רשות") AS code
        FROM   append_munidata_...;

    ``over_settlement_norm`` mirrors the Python norm() exactly (keep only Hebrew
    consonant letters + Latin + digits, lowercase) so SQL and API resolve
    identically. EXECUTE defaults to PUBLIC, so the read-only console role can
    call them; the functions run as the caller and read only the (SELECT-granted)
    index tables."""
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE OR REPLACE FUNCTION public.over_settlement_norm(q text)
            RETURNS text LANGUAGE sql IMMUTABLE AS $$
              SELECT lower(regexp_replace(coalesce(q, ''), '[^0-9A-Za-zא-ת]+', '', 'g'))
            $$;
            """
        )
        await conn.execute(
            """
            CREATE OR REPLACE FUNCTION public.over_settlement_code(q text)
            RETURNS integer LANGUAGE sql STABLE AS $$
              SELECT a.code
              FROM public.over_settlement_aliases a
              JOIN public.over_settlements s ON s.code = a.code
              WHERE a.variant = public.over_settlement_norm(q)
              ORDER BY a.weight DESC, s.population DESC NULLS LAST
              LIMIT 1
            $$;
            """
        )
        await conn.execute(
            """
            CREATE OR REPLACE FUNCTION public.over_settlement(q text)
            RETURNS text LANGUAGE sql STABLE AS $$
              SELECT s.name
              FROM public.over_settlement_aliases a
              JOIN public.over_settlements s ON s.code = a.code
              WHERE a.variant = public.over_settlement_norm(q)
              ORDER BY a.weight DESC, s.population DESC NULLS LAST
              LIMIT 1
            $$;
            """
        )
        # Authority resolvers — same norm, over the authority alias index.
        await conn.execute(
            """
            CREATE OR REPLACE FUNCTION public.over_authority_code(q text)
            RETURNS integer LANGUAGE sql STABLE AS $$
              SELECT a.code
              FROM public.over_authority_aliases a
              WHERE a.variant = public.over_settlement_norm(q)
              ORDER BY a.weight DESC
              LIMIT 1
            $$;
            """
        )
        await conn.execute(
            """
            CREATE OR REPLACE FUNCTION public.over_authority(q text)
            RETURNS text LANGUAGE sql STABLE AS $$
              SELECT s.name
              FROM public.over_authority_aliases a
              JOIN public.over_authorities s ON s.code = a.code
              WHERE a.variant = public.over_settlement_norm(q)
              ORDER BY a.weight DESC
              LIMIT 1
            $$;
            """
        )


def _load_json(path: str) -> list[dict]:
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        logger.warning("index seed missing: %s", path)
        return []


def load_seed() -> list[dict]:
    with open(SEED_PATH, encoding="utf-8") as fh:
        return json.load(fh)


def manual_alias_rows(recs: list[dict]) -> list[tuple]:
    """Curated (variant→code) rows from MANUAL_PATH, resolved by official name.

    weight 85 sits above prefixed guesses (50) and English/translit (70) but
    below an exact official-name hit (100). Entries whose target name is not in
    the seed are skipped (logged), so a typo can't silently mis-map."""
    try:
        with open(MANUAL_PATH, encoding="utf-8") as fh:
            manual = json.load(fh)
    except FileNotFoundError:
        return []
    name2code = {r["name"]: r["code"] for r in recs}
    rows: list[tuple] = []
    seen: set[tuple[str, int]] = set()
    for m in manual:
        code = name2code.get(m.get("name"))
        if code is None:
            logger.warning("manual alias target not found: %r", m.get("name"))
            continue
        variant = m.get("variant") or ""
        base = norm(variant)
        if not base:
            continue
        if (base, code) not in seen:
            seen.add((base, code)); rows.append((base, code, variant, "manual", 85))
        # Prefix-expand Hebrew manual variants too (so 'בתל אביב' resolves like
        # the auto layer does for official names), one weight tier lower.
        if re.search(r"[֐-׿]", base):
            for p in _PREFIXES:
                k = p + base
                if (k, code) not in seen:
                    seen.add((k, code)); rows.append((k, code, p + variant, "manual_prefix", 55))
    return rows


async def load(*, rebuild: bool = True) -> dict:
    """(Re)load the settlements + regenerate the alias index from the seed."""
    recs = load_seed()
    await ensure_tables()
    await ensure_functions()
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            if rebuild:
                await conn.execute(f"TRUNCATE public.{_qi(SETTLEMENTS_TABLE)}")
                # Keep harvested 'llm' aliases (learned from real data) — only the
                # deterministically-generated ones are rebuilt from the seed.
                await conn.execute(f"DELETE FROM public.{_qi(ALIASES_TABLE)} WHERE kind <> 'llm'")
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
            alias_rows.extend(manual_alias_rows(recs))
            await conn.executemany(
                f"""INSERT INTO public.{_qi(ALIASES_TABLE)} (variant,code,surface,kind,weight)
                    VALUES ($1,$2,$3,$4,$5)
                    ON CONFLICT (variant,code) DO UPDATE SET
                      surface=EXCLUDED.surface, kind=EXCLUDED.kind, weight=EXCLUDED.weight""",
                alias_rows,
            )
            # ── Authorities (רשות מקומית) — same alias engine ──────────────
            auth = _load_json(AUTH_SEED_PATH)
            if rebuild:
                await conn.execute(f"TRUNCATE public.{_qi(AUTHORITIES_TABLE)}")
                await conn.execute(f"DELETE FROM public.{_qi(AUTH_ALIASES_TABLE)} WHERE kind <> 'llm'")
            await conn.executemany(
                f"""INSERT INTO public.{_qi(AUTHORITIES_TABLE)}
                    (code,name,district,municipal_status,year) VALUES ($1,$2,$3,$4,$5)
                    ON CONFLICT (code) DO UPDATE SET
                      name=EXCLUDED.name, district=EXCLUDED.district,
                      municipal_status=EXCLUDED.municipal_status,
                      year=EXCLUDED.year, updated_at=now()""",
                [(r["code"], r["name"], r.get("district"), r.get("municipal_status"),
                  r.get("year")) for r in auth],
            )
            auth_alias_rows: list[tuple] = []
            for r in auth:
                for key, surface, kind, weight in aliases_for(r):
                    auth_alias_rows.append((key, r["code"], surface, kind, weight))
            await conn.executemany(
                f"""INSERT INTO public.{_qi(AUTH_ALIASES_TABLE)} (variant,code,surface,kind,weight)
                    VALUES ($1,$2,$3,$4,$5)
                    ON CONFLICT (variant,code) DO UPDATE SET
                      surface=EXCLUDED.surface, kind=EXCLUDED.kind, weight=EXCLUDED.weight""",
                auth_alias_rows,
            )
    logger.info("index loaded: %d settlements (%d aliases), %d authorities (%d aliases)",
                len(recs), len(alias_rows), len(auth), len(auth_alias_rows))
    return {"settlements": len(recs), "aliases": len(alias_rows),
            "authorities": len(auth), "authority_aliases": len(auth_alias_rows)}


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
