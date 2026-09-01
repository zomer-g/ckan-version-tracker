"""Dedicated MCP server for מימון בחירות לעם, mounted at ``/elections/mcp``.

Exposes the State Comptroller's election-finance register — every donation,
guarantee and loan reported by candidates, parties and lists — which OVER
collects from statements-p.mevaker.gov.il through the ``mevaker_statements``
scraper source. Shares the main MCP's OAuth authorization server and
``api_users`` allow-list (app/mcp/oauth.py); only the tools + resource identity
differ.

**Why this is a resource of its own** rather than a corner of the generic SQL
MCP. The register is six NEON tables, one per election type, and their
recipient columns deliberately differ: a municipal row names a city and a
faction, a regional row a council and a candidate, a primaries row a party and
a candidate, a parties row a party and a list. The questions people actually
bring to this data are about PEOPLE and cross the whole register — "what has
this person given", "who funded this candidate", "which donors gave to both
sides". Answering those over the raw tables means writing a six-way UNION with
a per-table COALESCE for the recipient, every time. That union IS the product
here, so it lives in one place instead of being rediscovered by each caller.

**Every table is discovered at runtime**, never hardcoded: the physical NEON
table of a dataset is ``append_<ckan_name>_<id8>``, minted when the dataset is
approved, so the names cannot be known when this file is written. ``_tables()``
resolves them from the datasets whose ``scraper_config['kind']`` is
``mevaker_statements``, through the same ``append_store.tables_from_mappings``
the public API and the /data catalog resolve through. A source that has not
been scraped yet simply has no table, and the tools say so rather than
returning an empty result that reads as "no donations".

**On the numbers.** Every column in an append table is text (the scraper
declares them so), and a missing value is the empty string, not NULL. So every
sum goes through ``NULLIF(col,'')::numeric`` and every date through a shape
filter before casting — the two traps documented for the /data console. Amounts
are in shekels as filed.

**Provenance.** This is a faithful mirror of a government register, not an
enrichment: no model touched these rows. But it is a mirror with a collection
date, and the register is live, so answers carry a ``_provenance`` block naming
the source page and the version the rows came from.
"""
from __future__ import annotations

import json
import time
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request

from app.mcp.auth import McpUser
from app.mcp.config import base_url
from app.mcp.usage import log_usage
from app.models.tracked_dataset import TrackedDataset
from app.services import append_store, data_catalog

SERVER_NAME = "over-elections-mcp"
SERVER_VERSION = "0.1.0"
DEFAULT_PROTOCOL = "2025-06-18"

SCRAPER_KIND = "mevaker_statements"
SITE_URL = "https://statements-p.mevaker.gov.il/"

MAX_ROWS = 200
SQL_TIMEOUT_MS = 20000

SERVER_INSTRUCTIONS = (
    "מימון בחירות לעם (over.org.il) חושף את מרשם מימון הבחירות של מבקר המדינה — "
    "כל תרומה, ערבות והלוואה שדווחו על ידי מועמדים, מפלגות ורשימות בבחירות "
    "לרשויות המקומיות, למועצות אזוריות, למפלגות, בבחירות מקדימות ובבחירות מיוחדות. "
    "הנתונים הם שיקוף נאמן של המרשם הממשלתי (statements-p.mevaker.gov.il) — לא "
    "מידע מעובד ולא תוצר בינה מלאכותית. עם זאת: (1) ציין שהמקור הוא מבקר המדינה "
    "דרך 'גרסאות לעם' (over.org.il), וצרף את הקישורים שבשדה \"links\"; (2) המרשם "
    "חי ומתעדכן — לכל תשובה מצורף תאריך האיסוף בבלוק \"_provenance\", ציין אותו "
    "כשהמספרים מהותיים; (3) שמות תורמים אינם מזהה ייחודי — שני אנשים יכולים לחלוק "
    "שם, ואותו אדם עשוי להופיע במספר איותים. אל תסיק שכל השורות בשם מסוים הן אותו "
    "אדם, ואמור זאת כשזה משנה. סכומים בשקלים כפי שדווחו."
)

PROVENANCE_BASE = {
    "is_processed": False,
    "description": (
        "A faithful mirror of the Israeli State Comptroller's election-finance "
        "register. Rows are collected from the Comptroller's own public search "
        "API and republished unchanged apart from flattening one nested field "
        "and renaming columns to machine names. No model or heuristic touched "
        "them. The register is live and reports are filed continuously, so each "
        "answer names the collection date it reflects."
    ),
    "upstream_source": SITE_URL,
    "presentation_url": "https://www.over.org.il/projects/elections",
}

# ---------------------------------------------------------------------------
# The six election types
# ---------------------------------------------------------------------------

# Keyed by scraper_config["election_type"]. ``recipient`` names the columns, in
# priority order, that identify WHO received the money on that tab — the whole
# reason the tables cannot simply be stacked.
ELECTION_TYPES: dict[str, dict] = {
    "local": {
        "label_he": "בחירות ברשויות המקומיות",
        "recipient": ["election_faction"],
        "place": ["election_city"],
        "party": [],
    },
    "regional": {
        "label_he": "בחירות במועצות אזוריות",
        "recipient": ["candidate_name"],
        "place": ["regional_council_name"],
        "party": [],
    },
    "parties": {
        "label_he": "מימון מפלגות",
        "recipient": ["list_name", "party_name"],
        "place": [],
        "party": ["party_name"],
    },
    "primaries": {
        "label_he": "בחירות מקדימות",
        "recipient": ["candidate_name"],
        "place": [],
        "party": ["party_name"],
    },
    "special": {
        "label_he": "בחירות מיוחדות",
        "recipient": ["candidate_name"],
        "place": ["election_city"],
        "party": [],
    },
    "politicalarrangement": {
        "label_he": "הסדרים פוליטיים",
        "recipient": ["political_arrangement_name", "authorized_factor_name"],
        "place": [],
        "party": [],
    },
}

# The register's own vocabulary for what a publication IS. Accepted in Hebrew
# (what a person types) and by the register's numeric id.
PUBLICATION_TYPES = {
    "תרומה": 2, "תרומות": 2, "donation": 2, "donations": 2,
    "ערבות": 3, "ערבויות": 3, "guarantee": 3, "guarantees": 3,
    "הלוואה": 4, "הלוואות": 4, "loan": 4, "loans": 4,
}
PUBLICATION_TYPE_LABELS = {2: "תרומה", 3: "ערבות", 4: "הלוואה"}

# The normalized column list every per-table SELECT projects into, so the six
# tables can be UNIONed. Columns a given table lacks are filled with ''.
_UNIFIED_COLUMNS = [
    "election_type", "publication_type", "publication_type_id",
    "donor_name", "donor_first_name", "donor_last_name",
    "donor_city", "donor_country",
    "recipient_name", "recipient_place", "recipient_party",
    "publication_sum", "sum_in_currency", "loan_return_sum",
    "publication_date", "election_date",
]


class _NotCollectedYet(RuntimeError):
    """No dataset of this source has published rows into NEON yet."""


# ---------------------------------------------------------------------------
# Table discovery
# ---------------------------------------------------------------------------

async def _tables(db: AsyncSession) -> list[dict]:
    """The live NEON table behind each election type.

    Resolved from the datasets themselves rather than hardcoded: a table name
    is ``append_<ckan_name>_<id8>``, minted at approval time. Returns
    ``[{election_type, table, dataset_id, title, label_he}]``, only for types
    that actually have a published table.
    """
    rows = (await db.execute(
        select(TrackedDataset).where(
            TrackedDataset.scraper_config["kind"].astext == SCRAPER_KIND
        )
    )).scalars().all()
    if not rows:
        return []

    latest = await data_catalog._latest_mappings(db, [d.id for d in rows])
    out: list[dict] = []
    for ds in rows:
        version_id, mappings = latest.get(ds.id, (None, {}))
        if not version_id:
            continue  # approved but never scraped — no table to read
        etype = (ds.scraper_config or {}).get("election_type") or ""
        spec = ELECTION_TYPES.get(etype)
        if not spec:
            continue
        for entry in append_store.tables_from_mappings(ds, mappings):
            out.append({
                "election_type": etype,
                "label_he": spec["label_he"],
                "table": entry["table"],
                "dataset_id": str(ds.id),
                "title": ds.title or ds.ckan_name,
                "version_id": str(version_id),
                "last_polled_at": ds.last_polled_at,
            })
            break  # one table per dataset — this source publishes one resource
    return out


async def _live_columns(table: str) -> set[str]:
    """Columns a table actually has, so a SELECT never names a missing one."""
    pool = await append_store.get_readonly_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = $1", table)
    return {r["column_name"] for r in rows}


def _coalesce(candidates: list[str], present: set[str]) -> str:
    """A COALESCE over the columns that exist, or a literal '' if none do."""
    usable = [f"NULLIF({c}, '')" for c in candidates if c in present]
    if not usable:
        return "''"
    if len(usable) == 1:
        return f"COALESCE({usable[0]}, '')"
    return f"COALESCE({', '.join(usable)}, '')"


async def _unified_select(entry: dict) -> str:
    """One table projected onto _UNIFIED_COLUMNS.

    The recipient columns differ per election type by design, so each table
    gets its own COALESCE — that mapping is what makes a cross-register
    question answerable at all.
    """
    table = entry["table"]
    present = await _live_columns(table)
    spec = ELECTION_TYPES[entry["election_type"]]

    def col(name: str) -> str:
        return f"COALESCE({name}, '')" if name in present else "''"

    parts = [
        f"{_lit(entry['election_type'])} AS election_type",
        f"{col('publication_type')} AS publication_type",
        f"{col('publication_type_id')} AS publication_type_id",
        f"{col('donor_name')} AS donor_name",
        f"{col('donor_first_name')} AS donor_first_name",
        f"{col('donor_last_name')} AS donor_last_name",
        f"{col('donor_city')} AS donor_city",
        f"{col('donor_country')} AS donor_country",
        f"{_coalesce(spec['recipient'], present)} AS recipient_name",
        f"{_coalesce(spec['place'], present)} AS recipient_place",
        f"{_coalesce(spec['party'], present)} AS recipient_party",
        f"{col('publication_sum')} AS publication_sum",
        f"{col('sum_in_currency')} AS sum_in_currency",
        f"{col('loan_return_sum')} AS loan_return_sum",
        f"{col('publication_date')} AS publication_date",
        f"{col('election_date')} AS election_date",
    ]
    return f"SELECT {', '.join(parts)} FROM public.{_qi(table)}"


def _qi(name: str) -> str:
    """Quote an identifier. Table names come from our own minting, never user
    input, but quoting keeps this safe if that ever stops being true."""
    return '"' + str(name).replace('"', '""') + '"'


def _lit(value: str) -> str:
    """A single-quoted SQL literal. Used only for values we control
    (election-type keys); everything caller-supplied goes through $n params."""
    return "'" + str(value).replace("'", "''") + "'"


async def _corpus_sql(db: AsyncSession, election_types: list[str] | None) -> tuple[str, list[dict]]:
    """The six tables as one UNION ALL, restricted to the requested types."""
    entries = await _tables(db)
    if election_types:
        wanted = {e.strip().lower() for e in election_types if e and e.strip()}
        entries = [e for e in entries if e["election_type"] in wanted]
    if not entries:
        raise _NotCollectedYet(
            "אין עדיין טבלאות שפורסמו עבור מרשם מימון הבחירות. המאגרים נוצרים "
            "ב-over.org.il ומתמלאים בגריפה הראשונה; עד אז אין מה לתשאל. "
            "(זהו מצב 'טרם נאסף', לא 'אין תרומות'.)")
    selects = [await _unified_select(e) for e in entries]
    return "(\n" + "\nUNION ALL\n".join(selects) + "\n)", entries


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

async def _fetch(sql: str, params: list) -> list[dict]:
    pool = await append_store.get_readonly_pool()
    async with pool.acquire() as conn:
        async with conn.transaction(readonly=True):
            await conn.execute(f"SET LOCAL statement_timeout = {SQL_TIMEOUT_MS}")
            rows = await conn.fetch(sql, *params)
    return [dict(r) for r in rows]


def _norm_pub_type(value) -> int | None:
    """'תרומה' / 'donation' / 2 -> 2. None when unset; raises on nonsense."""
    if value in (None, ""):
        return None
    if isinstance(value, int) or str(value).isdigit():
        n = int(value)
        if n in PUBLICATION_TYPE_LABELS:
            return n
        raise ValueError(
            f"publication_type {value!r} לא מוכר. ערכים: "
            f"{sorted(PUBLICATION_TYPE_LABELS.values())} או 2/3/4.")
    key = str(value).strip().lower()
    if key in PUBLICATION_TYPES:
        return PUBLICATION_TYPES[key]
    raise ValueError(
        f"publication_type {value!r} לא מוכר. ערכים: תרומה / ערבות / הלוואה.")


def _norm_types(value) -> list[str] | None:
    """A comma-separated or list-valued election_type filter."""
    if value in (None, "", []):
        return None
    items = value if isinstance(value, list) else str(value).split(",")
    out = []
    for item in items:
        key = str(item).strip().lower()
        if not key:
            continue
        if key not in ELECTION_TYPES:
            raise ValueError(
                f"election_type {item!r} לא מוכר. ערכים: {sorted(ELECTION_TYPES)}.")
        out.append(key)
    return out or None


def _amount(col: str = "publication_sum") -> str:
    """The filed amount as a number. Empty string is the register's null."""
    return f"NULLIF({col}, '')::numeric"


def _links(base: str, entries: list[dict]) -> dict:
    return {
        "source_site": SITE_URL,
        "over_datasets": [
            {"election_type": e["election_type"], "title": e["title"],
             "versions_url": f"{base}/versions/{e['dataset_id']}",
             "sql_table": e["table"]}
            for e in entries
        ],
        "sql_console": f"{base}/data",
    }


def _provenance(entries: list[dict]) -> dict:
    polled = [e["last_polled_at"] for e in entries if e.get("last_polled_at")]
    out = dict(PROVENANCE_BASE)
    out["collected_at"] = max(polled).isoformat() if polled else None
    out["election_types_covered"] = sorted({e["election_type"] for e in entries})
    return out


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

TOOLS: list[dict] = [
    {
        "name": "search_donations",
        "description": (
            "חיפוש תרומות, ערבויות והלוואות במרשם מימון הבחירות של מבקר המדינה. "
            "אפשר לסנן לפי שם התורם (חיפוש חלקי), שם המקבל (מועמד/מפלגה/רשימה/סיעה), "
            "סוג הפרסום (תרומה/ערבות/הלוואה), סוג הבחירות, יישוב התורם, טווח תאריכים "
            "וטווח סכומים. מחזיר שורות בודדות — לסיכומים לפי אדם השתמשו ב-donor_profile."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "donor": {"type": "string", "description": "שם התורם, מלא או חלקי (עברית)"},
                "recipient": {"type": "string", "description": "שם המקבל: מועמד, מפלגה, רשימה או סיעה"},
                "publication_type": {"type": "string", "description": "תרומה / ערבות / הלוואה"},
                "election_type": {"type": "string", "description":
                                  "local / regional / parties / primaries / special / "
                                  "politicalarrangement — מופרדים בפסיק"},
                "donor_city": {"type": "string", "description": "יישוב התורם"},
                "from_date": {"type": "string", "description": "YYYY-MM-DD"},
                "to_date": {"type": "string", "description": "YYYY-MM-DD"},
                "min_sum": {"type": "number", "description": "סכום מזערי בשקלים"},
                "max_sum": {"type": "number", "description": "סכום מרבי בשקלים"},
                "sort": {"type": "string", "enum": ["date_desc", "date_asc", "sum_desc", "sum_asc"],
                         "default": "date_desc"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 50},
                "offset": {"type": "integer", "minimum": 0, "default": 0},
            },
        },
    },
    {
        "name": "donor_profile",
        "description": (
            "כל מה שנתן תורם מסוים, על פני כל סוגי הבחירות: סכום כולל, פילוח לפי סוג "
            "פרסום (תרומה/ערבות/הלוואה), פילוח לפי מקבל, וטווח התאריכים. זו הדרך לענות "
            "על 'כמה נתן X ולמי'. שימו לב ששם אינו מזהה ייחודי — ייתכנו כמה אנשים באותו שם."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "donor": {"type": "string", "description": "שם התורם, מלא או חלקי"},
                "exact": {"type": "boolean", "default": False,
                          "description": "התאמה מדויקת לשם במקום חיפוש חלקי"},
                "publication_type": {"type": "string", "description": "תרומה / ערבות / הלוואה"},
                "election_type": {"type": "string", "description": "מופרדים בפסיק"},
            },
            "required": ["donor"],
        },
    },
    {
        "name": "recipient_profile",
        "description": (
            "כל מה שקיבל מועמד, מפלגה, רשימה או סיעה: סכום כולל, פילוח לפי סוג פרסום, "
            "והתורמים הגדולים. זו הדרך לענות על 'מי מימן את X'."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "recipient": {"type": "string", "description": "שם המקבל, מלא או חלקי"},
                "exact": {"type": "boolean", "default": False},
                "publication_type": {"type": "string", "description": "תרומה / ערבות / הלוואה"},
                "election_type": {"type": "string", "description": "מופרדים בפסיק"},
                "top_donors": {"type": "integer", "minimum": 1, "maximum": 200, "default": 25},
            },
            "required": ["recipient"],
        },
    },
    {
        "name": "top_donors",
        "description": (
            "התורמים הגדולים ביותר לפי סכום מצטבר, עם סינון לפי סוג פרסום, סוג בחירות, "
            "יישוב וטווח תאריכים. מקבץ לפי שם התורם — ושם אינו מזהה ייחודי, כך שאותו אדם "
            "בשני איותים יופיע פעמיים."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "publication_type": {"type": "string", "description": "תרומה / ערבות / הלוואה"},
                "election_type": {"type": "string", "description": "מופרדים בפסיק"},
                "donor_city": {"type": "string"},
                "from_date": {"type": "string", "description": "YYYY-MM-DD"},
                "to_date": {"type": "string", "description": "YYYY-MM-DD"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 25},
            },
        },
    },
    {
        "name": "stats",
        "description": (
            "סקירת המרשם: מספר הפרסומים והסכום הכולל, בפילוח לפי סוג בחירות ולפי סוג "
            "פרסום, וטווח התאריכים המכוסה. נקודת פתיחה טובה לפני חיפוש."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "election_type": {"type": "string", "description": "מופרדים בפסיק"},
            },
        },
    },
    {
        "name": "list_election_types",
        "description": (
            "ששת סוגי הבחירות במרשם, מה כל אחד מכיל, שם טבלת ה-SQL שלו וקישור לגרסאות "
            "המאגר ב-over.org.il. מראה גם אילו סוגים כבר נאספו ואילו טרם."
        ),
        "inputSchema": {"type": "object", "properties": {}},
    },
]


async def _tool_search_donations(request, db, user, a) -> tuple[dict, int]:
    corpus, entries = await _corpus_sql(db, _norm_types(a.get("election_type")))
    where, params = _filters(a)
    limit = min(int(a.get("limit") or 50), MAX_ROWS)
    offset = max(int(a.get("offset") or 0), 0)

    order = {
        "date_desc": "publication_date DESC",
        "date_asc": "publication_date ASC",
        "sum_desc": f"{_amount()} DESC NULLS LAST",
        "sum_asc": f"{_amount()} ASC NULLS LAST",
    }.get(a.get("sort") or "date_desc", "publication_date DESC")

    total = (await _fetch(
        f"SELECT count(*) AS n FROM {corpus} c WHERE {where}", params))[0]["n"]
    summed = (await _fetch(
        f"SELECT COALESCE(sum({_amount()}), 0) AS s FROM {corpus} c WHERE {where}",
        params))[0]["s"]
    rows = await _fetch(
        f"SELECT {', '.join(_UNIFIED_COLUMNS)} FROM {corpus} c WHERE {where} "
        f"ORDER BY {order} LIMIT {limit} OFFSET {offset}", params)

    base = base_url(request)
    return {
        "total_matching": int(total),
        "total_sum": _num(summed),
        "returned": len(rows),
        "offset": offset,
        "results": rows,
        "_provenance": _provenance(entries),
        "links": _links(base, entries),
        "note": ("שם תורם אינו מזהה ייחודי — ייתכנו כמה אנשים באותו שם, ואותו אדם "
                 "בכמה איותים."),
    }, len(rows)


async def _tool_donor_profile(request, db, user, a) -> tuple[dict, int]:
    corpus, entries = await _corpus_sql(db, _norm_types(a.get("election_type")))
    name = (a.get("donor") or "").strip()
    if not name:
        raise ValueError("donor is required")

    params: list = [name if a.get("exact") else f"%{name}%"]
    match = "donor_name = $1" if a.get("exact") else "donor_name ILIKE $1"
    where = [match]
    pub = _norm_pub_type(a.get("publication_type"))
    if pub is not None:
        params.append(str(pub))
        where.append(f"publication_type_id = ${len(params)}")
    clause = " AND ".join(where)

    totals = (await _fetch(
        f"SELECT count(*) AS n, COALESCE(sum({_amount()}), 0) AS s, "
        f"min(NULLIF(publication_date, '')) AS first_date, "
        f"max(NULLIF(publication_date, '')) AS last_date, "
        f"count(DISTINCT donor_name) AS distinct_spellings "
        f"FROM {corpus} c WHERE {clause}", params))[0]

    by_type = await _fetch(
        f"SELECT publication_type, count(*) AS n, COALESCE(sum({_amount()}), 0) AS s "
        f"FROM {corpus} c WHERE {clause} GROUP BY publication_type ORDER BY s DESC", params)
    by_election = await _fetch(
        f"SELECT election_type, count(*) AS n, COALESCE(sum({_amount()}), 0) AS s "
        f"FROM {corpus} c WHERE {clause} GROUP BY election_type ORDER BY s DESC", params)
    by_recipient = await _fetch(
        f"SELECT recipient_name, recipient_place, recipient_party, election_type, "
        f"count(*) AS n, COALESCE(sum({_amount()}), 0) AS s "
        f"FROM {corpus} c WHERE {clause} "
        f"GROUP BY recipient_name, recipient_place, recipient_party, election_type "
        f"ORDER BY s DESC LIMIT 100", params)
    spellings = await _fetch(
        f"SELECT donor_name, donor_city, count(*) AS n, "
        f"COALESCE(sum({_amount()}), 0) AS s "
        f"FROM {corpus} c WHERE {clause} GROUP BY donor_name, donor_city "
        f"ORDER BY s DESC LIMIT 50", params)

    base = base_url(request)
    return {
        "query": {"donor": name, "exact": bool(a.get("exact"))},
        "publications": int(totals["n"]),
        "total_sum": _num(totals["s"]),
        "first_publication_date": totals["first_date"],
        "last_publication_date": totals["last_date"],
        "by_publication_type": [_grp(r) for r in by_type],
        "by_election_type": [_grp(r) for r in by_election],
        "by_recipient": [_grp(r) for r in by_recipient],
        "name_variants_matched": [_grp(r) for r in spellings],
        "_provenance": _provenance(entries),
        "links": _links(base, entries),
        "note": ("השם אינו מזהה ייחודי. 'name_variants_matched' מראה בדיוק אילו "
                 "כתיבים נכללו — בדקו אותו לפני שתייחסו את הסכום לאדם אחד."),
    }, int(totals["n"])


async def _tool_recipient_profile(request, db, user, a) -> tuple[dict, int]:
    corpus, entries = await _corpus_sql(db, _norm_types(a.get("election_type")))
    name = (a.get("recipient") or "").strip()
    if not name:
        raise ValueError("recipient is required")

    params: list = [name if a.get("exact") else f"%{name}%"]
    match = ("recipient_name = $1" if a.get("exact")
             else "(recipient_name ILIKE $1 OR recipient_party ILIKE $1)")
    where = [match]
    pub = _norm_pub_type(a.get("publication_type"))
    if pub is not None:
        params.append(str(pub))
        where.append(f"publication_type_id = ${len(params)}")
    clause = " AND ".join(where)
    top_n = min(int(a.get("top_donors") or 25), MAX_ROWS)

    totals = (await _fetch(
        f"SELECT count(*) AS n, COALESCE(sum({_amount()}), 0) AS s, "
        f"count(DISTINCT donor_name) AS donors, "
        f"min(NULLIF(publication_date, '')) AS first_date, "
        f"max(NULLIF(publication_date, '')) AS last_date "
        f"FROM {corpus} c WHERE {clause}", params))[0]
    by_type = await _fetch(
        f"SELECT publication_type, count(*) AS n, COALESCE(sum({_amount()}), 0) AS s "
        f"FROM {corpus} c WHERE {clause} GROUP BY publication_type ORDER BY s DESC", params)
    donors = await _fetch(
        f"SELECT donor_name, donor_city, count(*) AS n, "
        f"COALESCE(sum({_amount()}), 0) AS s "
        f"FROM {corpus} c WHERE {clause} GROUP BY donor_name, donor_city "
        f"ORDER BY s DESC LIMIT {top_n}", params)
    matched = await _fetch(
        f"SELECT recipient_name, recipient_place, recipient_party, election_type, "
        f"count(*) AS n, COALESCE(sum({_amount()}), 0) AS s "
        f"FROM {corpus} c WHERE {clause} "
        f"GROUP BY recipient_name, recipient_place, recipient_party, election_type "
        f"ORDER BY s DESC LIMIT 100", params)

    base = base_url(request)
    return {
        "query": {"recipient": name, "exact": bool(a.get("exact"))},
        "publications": int(totals["n"]),
        "total_sum": _num(totals["s"]),
        "distinct_donors": int(totals["donors"]),
        "first_publication_date": totals["first_date"],
        "last_publication_date": totals["last_date"],
        "by_publication_type": [_grp(r) for r in by_type],
        "top_donors": [_grp(r) for r in donors],
        "recipients_matched": [_grp(r) for r in matched],
        "_provenance": _provenance(entries),
        "links": _links(base, entries),
    }, int(totals["n"])


async def _tool_top_donors(request, db, user, a) -> tuple[dict, int]:
    corpus, entries = await _corpus_sql(db, _norm_types(a.get("election_type")))
    where, params = _filters(a)
    limit = min(int(a.get("limit") or 25), MAX_ROWS)
    rows = await _fetch(
        f"SELECT donor_name, donor_city, count(*) AS n, "
        f"COALESCE(sum({_amount()}), 0) AS s "
        f"FROM {corpus} c WHERE {where} "
        f"GROUP BY donor_name, donor_city ORDER BY s DESC LIMIT {limit}", params)
    base = base_url(request)
    return {
        "results": [_grp(r) for r in rows],
        "_provenance": _provenance(entries),
        "links": _links(base, entries),
        "note": ("מקובץ לפי מחרוזת השם — אותו אדם בשני איותים יופיע כשתי שורות, "
                 "ושני אנשים באותו שם ימוזגו לאחת."),
    }, len(rows)


async def _tool_stats(request, db, user, a) -> tuple[dict, int]:
    corpus, entries = await _corpus_sql(db, _norm_types(a.get("election_type")))
    by_election = await _fetch(
        f"SELECT election_type, count(*) AS n, COALESCE(sum({_amount()}), 0) AS s, "
        f"count(DISTINCT donor_name) AS donors, "
        f"min(NULLIF(publication_date, '')) AS first_date, "
        f"max(NULLIF(publication_date, '')) AS last_date "
        f"FROM {corpus} c GROUP BY election_type ORDER BY n DESC", [])
    by_type = await _fetch(
        f"SELECT publication_type, count(*) AS n, COALESCE(sum({_amount()}), 0) AS s "
        f"FROM {corpus} c GROUP BY publication_type ORDER BY n DESC", [])
    overall = (await _fetch(
        f"SELECT count(*) AS n, COALESCE(sum({_amount()}), 0) AS s, "
        f"count(DISTINCT donor_name) AS donors FROM {corpus} c", []))[0]

    base = base_url(request)
    return {
        "publications": int(overall["n"]),
        "total_sum": _num(overall["s"]),
        "distinct_donor_names": int(overall["donors"]),
        "by_election_type": [
            {**_grp(r), "label_he": ELECTION_TYPES.get(r["election_type"], {}).get("label_he")}
            for r in by_election],
        "by_publication_type": [_grp(r) for r in by_type],
        "_provenance": _provenance(entries),
        "links": _links(base, entries),
    }, len(by_election)


async def _tool_list_election_types(request, db, user, a) -> tuple[dict, int]:
    entries = {e["election_type"]: e for e in await _tables(db)}
    base = base_url(request)
    out = []
    for key, spec in ELECTION_TYPES.items():
        entry = entries.get(key)
        out.append({
            "election_type": key,
            "label_he": spec["label_he"],
            "recipient_columns": spec["recipient"],
            "collected": bool(entry),
            "sql_table": entry["table"] if entry else None,
            "versions_url": f"{base}/versions/{entry['dataset_id']}" if entry else None,
            "source_url": f"{SITE_URL}publisher?electionType={key}",
        })
    return {
        "election_types": out,
        "note": ("סוג שסומן collected=false טרם נאסף — אין לו טבלה, ולכן היעדר "
                 "תוצאות עבורו אינו אומר שאין תרומות."),
        "links": {"source_site": SITE_URL, "sql_console": f"{base}/data"},
    }, len(out)


# ---------------------------------------------------------------------------
# Shared filter builder
# ---------------------------------------------------------------------------

def _filters(a: dict) -> tuple[str, list]:
    """WHERE clause + asyncpg params from the common filter arguments.

    Every caller-supplied value is a $n parameter — never interpolated — so a
    donor called O'Brien is a search, not a syntax error.
    """
    where: list[str] = ["TRUE"]
    params: list = []

    def add(clause_fmt: str, value):
        params.append(value)
        where.append(clause_fmt.format(n=len(params)))

    if (a.get("donor") or "").strip():
        add("donor_name ILIKE ${n}", f"%{a['donor'].strip()}%")
    if (a.get("recipient") or "").strip():
        params.append(f"%{a['recipient'].strip()}%")
        where.append(f"(recipient_name ILIKE ${len(params)} "
                     f"OR recipient_party ILIKE ${len(params)})")
    if (a.get("donor_city") or "").strip():
        add("donor_city ILIKE ${n}", f"%{a['donor_city'].strip()}%")

    pub = _norm_pub_type(a.get("publication_type"))
    if pub is not None:
        add("publication_type_id = ${n}", str(pub))

    # Dates are text in the register's own ISO shape. Compare as text against a
    # normalized bound — correct because the format is fixed-width ISO, and it
    # avoids to_date blowing up on the first row that is empty.
    if (a.get("from_date") or "").strip():
        add("(publication_date <> '' AND publication_date >= ${n})",
            _date_bound(a["from_date"], "T00:00:00"))
    if (a.get("to_date") or "").strip():
        add("(publication_date <> '' AND publication_date <= ${n})",
            _date_bound(a["to_date"], "T23:59:59"))

    if a.get("min_sum") not in (None, ""):
        add(f"{_amount()} >= ${{n}}", float(a["min_sum"]))
    if a.get("max_sum") not in (None, ""):
        add(f"{_amount()} <= ${{n}}", float(a["max_sum"]))

    return " AND ".join(where), params


def _date_bound(value: str, suffix: str) -> str:
    v = str(value).strip()
    if len(v) == 10 and v[4] == "-" and v[7] == "-":
        return v + suffix
    return v


def _num(value):
    """Numeric out of Postgres as a float, so JSON carries a number."""
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _grp(row: dict) -> dict:
    """A GROUP BY row with its count/sum given readable names."""
    out = {k: v for k, v in row.items() if k not in ("n", "s")}
    if "n" in row:
        out["publications"] = int(row["n"])
    if "s" in row:
        out["total_sum"] = _num(row["s"])
    return out


_IMPL = {
    "search_donations": _tool_search_donations,
    "donor_profile": _tool_donor_profile,
    "recipient_profile": _tool_recipient_profile,
    "top_donors": _tool_top_donors,
    "stats": _tool_stats,
    "list_election_types": _tool_list_election_types,
}


# ---------------------------------------------------------------------------
# JSON-RPC plumbing (mirrors app/mcp/ocal_server.py)
# ---------------------------------------------------------------------------

def _rpc_result(mid, result):
    return {"jsonrpc": "2.0", "id": mid, "result": result}


def _rpc_error(mid, code, message):
    return {"jsonrpc": "2.0", "id": mid, "error": {"code": code, "message": message}}


def _uuid(s):
    try:
        return uuid.UUID(str(s)) if s else None
    except (ValueError, TypeError):
        return None


async def _run_tool(request: Request, db: AsyncSession, user: McpUser, session_id: str | None,
                    name: str, args: dict) -> dict:
    impl = _IMPL.get(name)
    started = time.time()
    if not impl:
        await log_usage(api_user_id=user.id, client_id=_uuid(user.client_id), session_id=session_id,
                        tool_name=name, request_params=args, result_count=None, result_bytes=None,
                        latency_ms=int((time.time() - started) * 1000), status="error",
                        error_message="unknown tool")
        return {"content": [{"type": "text", "text": f"Unknown tool: {name}"}], "isError": True}
    try:
        data, count = await impl(request, db, user, args or {})
        out = json.dumps(data, ensure_ascii=False, indent=2, default=str)
        await log_usage(api_user_id=user.id, client_id=_uuid(user.client_id), session_id=session_id,
                        tool_name=name, request_params=args, result_count=count,
                        result_bytes=len(out.encode("utf-8")),
                        latency_ms=int((time.time() - started) * 1000), status="ok", error_message=None)
        return {"content": [{"type": "text", "text": out}]}
    except Exception as e:  # noqa: BLE001
        await log_usage(api_user_id=user.id, client_id=_uuid(user.client_id), session_id=session_id,
                        tool_name=name, request_params=args, result_count=None, result_bytes=None,
                        latency_ms=int((time.time() - started) * 1000), status="error",
                        error_message=str(e)[:1000])
        return {"content": [{"type": "text", "text": f"Error: {e}"}], "isError": True}


async def handle_message(request: Request, db: AsyncSession, user: McpUser,
                         session_id: str | None, msg: dict):
    """Handle one JSON-RPC message. Returns a response dict, or None for notifications."""
    method = msg.get("method")
    mid = msg.get("id")
    is_notification = "id" not in msg

    if method == "initialize":
        client_proto = (msg.get("params") or {}).get("protocolVersion") or DEFAULT_PROTOCOL
        return _rpc_result(mid, {
            "protocolVersion": client_proto,
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
            "instructions": SERVER_INSTRUCTIONS,
        })
    if method in ("notifications/initialized", "notifications/cancelled"):
        return None
    if method == "ping":
        return _rpc_result(mid, {})
    if method == "tools/list":
        return _rpc_result(mid, {"tools": TOOLS})
    if method == "tools/call":
        params = msg.get("params") or {}
        result = await _run_tool(request, db, user, session_id,
                                 params.get("name"), params.get("arguments") or {})
        return _rpc_result(mid, result)

    if is_notification:
        return None
    return _rpc_error(mid, -32601, f"Method not found: {method}")
