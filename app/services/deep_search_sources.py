"""The source registry for "שאלות לעם" — the cross-source deep search.

Single source of truth for the חיפוש רוחבי gateway. Each entry describes ONE
searchable corpus: which MCP server holds it, which tool searches it, how to
build that tool's arguments from a free-text query, WHERE in the tool payload
the result array lives, and how to normalize each raw row into the one card
shape the page renders.

Ported from the ``cross-source-search`` package of zomer-g/shomrim-dashboard,
with one structural change: there, every source was a remote MCP server reached
over HTTP. Here the v1 sources are all servers of THIS process, so they are
dispatched in-process (see deep_search.LocalTransport) — no network hop, no
loopback through Cloudflare, no service token, no cold start. The remote
transport still exists so adding TAG-IT / OCOI / BudgetKey later is an entry in
this file plus one env var.

``results_path`` is per-source ON PURPOSE — OVER's five MCP servers do not agree
on the key holding the rows. All four spellings are live:

    search_datasets / search / search_protocols / search_mmm →  "items"
    search_events                                            →  "events"
    list_tables                                              →  "tables"
    query_dataset_rows                                       →  "rows"

Hardcoding "items" would leave the יומנים and טבלאות columns silently empty —
they would look like "no results" rather than like a bug.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

# ── shared helpers ──────────────────────────────────────────────────────────

_WS = re.compile(r"\s+")


def truncate(s: Any, n: int = 220) -> str:
    """Collapse whitespace and cut to n chars with an ellipsis."""
    if s is None:
        return ""
    t = _WS.sub(" ", str(s)).strip()
    return t if len(t) <= n else t[: n - 1].rstrip() + "…"


def join_parts(*parts: Any, sep: str = " · ") -> str:
    """Join the non-empty parts — the standard way we build a card snippet."""
    return sep.join(str(p).strip() for p in parts if p is not None and str(p).strip())


def _balance_marks(s: str) -> str:
    """Drop a highlight marker left dangling by truncation.

    Snippets carry «…» around the match. Cutting one to length can leave an
    opener with no closer, which the client would render as an unterminated
    highlight swallowing the rest of the line.
    """
    if not s or "«" not in s:
        return s
    if s.count("«") > s.count("»"):
        return s[: s.rfind("«")].rstrip() + "…"
    return s


def first_of(row: dict, *keys: str) -> Any:
    """First present, non-empty value among keys. Corpora disagree on column
    names (the corporate registries alone use three different conventions), so
    normalizers name candidates rather than assume one."""
    for k in keys:
        v = row.get(k)
        if v is not None and str(v).strip() != "":
            return v
    return None


def iso_date(v: Any) -> str | None:
    """Best-effort YYYY-MM-DD out of the several date spellings in the corpora.

    Handles ISO strings/timestamps, DD/MM/YYYY (the CKAN registries) and
    M/D/YYYY h:mm:ss AM (the cooperatives registry). Returns None rather than
    guessing — a wrong date on a card is worse than no date.
    """
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return m.group(0)
    # DOTS are unambiguous here: the מבקר library writes DD.MM.YYYY, so
    # 08.05.2018 is 8 May, never 5 August.
    m = re.match(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})", s)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{y:04d}-{mo:02d}-{d:02d}" if 1 <= mo <= 12 else None
    # SLASHES are genuinely mixed across these corpora: the cooperatives feed
    # writes US M/D/YYYY ("3/1/2020 12:00:00 AM"). Read month-first, unless the
    # first number cannot be a month.
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        a, b, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        mo, d = (b, a) if a > 12 else (a, b)
        return f"{y:04d}-{mo:02d}-{d:02d}" if 1 <= mo <= 12 else None
    return None


def date_args(f: dict, from_key: str = "date_from", to_key: str = "date_to") -> dict:
    """Map the shared date-range filter onto a tool's own argument names."""
    out: dict = {}
    if f.get("date_from"):
        out[from_key] = f["date_from"]
    if f.get("date_to"):
        out[to_key] = f["date_to"]
    return out


def year_args(f: dict) -> dict:
    out: dict = {}
    for k in ("year_from", "year_to"):
        if f.get(k):
            try:
                out[k] = int(f[k])
            except (TypeError, ValueError):
                pass
    return out


# ── types ───────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Filter:
    """One user-facing control, declared by the backend and rendered generically."""
    id: str
    label: str
    type: str = "text"                       # text | date | number | select
    options: tuple[dict, ...] = ()           # [{"value","label"}] for select

    def as_dict(self) -> dict:
        d = {"id": self.id, "label": self.label, "type": self.type}
        if self.options:
            d["options"] = [dict(o) for o in self.options]
        return d


@dataclass(frozen=True)
class Card:
    """The one shape every source normalizes to.

    Length is capped HERE rather than in each normalizer. These corpora contain
    free-text fields with no size discipline — one Ocal event's `participants`
    is a 1,200-character attendee list — and a single such row would otherwise
    blow out the column it lands in. Capping centrally means a new source cannot
    reintroduce the problem by forgetting to truncate.
    """
    title: str
    snippet: str = ""
    url: str | None = None
    date: str | None = None
    badges: tuple[str, ...] = ()

    MAX_TITLE = 180
    # Roomier than the title because this is where the match context lives —
    # the point of a hit inside a 300-page protocol is the sentence around it.
    # The extra headroom over the ~220-char window also absorbs the highlight
    # markers without eating into the text.
    MAX_SNIPPET = 320
    MAX_BADGE = 40

    def __post_init__(self):
        object.__setattr__(self, "title", truncate(self.title, self.MAX_TITLE))
        object.__setattr__(self, "snippet", _balance_marks(
            truncate(self.snippet, self.MAX_SNIPPET)))
        object.__setattr__(
            self, "badges",
            tuple(truncate(b, self.MAX_BADGE) for b in self.badges if b))

    def as_dict(self) -> dict:
        return {"title": self.title, "snippet": self.snippet, "url": self.url,
                "date": self.date, "badges": list(self.badges)}


DATE_RANGE = (
    Filter("date_from", "מתאריך", "date"),
    Filter("date_to", "עד תאריך", "date"),
)
YEAR_RANGE = (
    Filter("year_from", "משנה", "number"),
    Filter("year_to", "עד שנה", "number"),
)
SOURCE_TYPE = Filter("source_type", "סוג מקור", "select", (
    {"value": "", "label": "הכול"},
    {"value": "ckan", "label": "CKAN (data.gov.il)"},
    {"value": "scraper", "label": "סקרייפר"},
    {"value": "govmap", "label": "GovMap"},
))


@dataclass(frozen=True)
class Source:
    """One column on the page.

    Transport is exactly one of ``local`` (a server in this process) or
    ``mcp_url`` (a remote MCP endpoint). ``run`` is the escape hatch for a
    source that needs several tool calls merged into one column; when it is set,
    ``tool``/``results_path``/``normalize`` are unused.
    """
    id: str
    name: str
    color: str
    attribution: dict
    build_args: Callable[[str, int, dict], dict]
    normalize: Callable[[dict], Card | None] | None = None
    tool: str = ""
    results_path: str = "items"
    # transport
    local: str | None = None                 # over | sql | cbs | knesset | ocal
    mcp_url: str | None = None
    token_env: str | None = None
    public: bool = False
    # Session-style MCP server: needs initialize → Mcp-Session-Id before any
    # tools/call. Ours and TAG-IT's are stateless; מפתח התקציב is not.
    handshake: bool = False
    # Held by someone else. Surfaced in the UI as a "מקור חיצוני" marker so a
    # reader is never left thinking OVER produced the row.
    external: bool = False
    # The backend parses "phrases", -exclusions and OR itself (TAG-IT does —
    # measured, see deep_search_query). Such a source gets the user's query
    # verbatim and its results are NOT re-filtered here: it searched full
    # document bodies, so a match may legitimately sit outside the snippet we
    # can see, and filtering on what we can see would drop real hits.
    native_operators: bool = False
    run: Callable[..., Awaitable[dict]] | None = None
    filters: tuple[Filter, ...] = ()
    active: bool = True
    hint: str = ""                           # one line under the column head

    @property
    def server(self) -> str:
        """Grouping key for the client: sources on the SAME server are run one
        after another, different servers concurrently. For local sources that is
        the module key, so פרוטוקולים and ממ״מ (both on the knesset server) do
        not hit the same append-DB pool at once."""
        if self.local:
            return self.local
        if self.mcp_url:
            from urllib.parse import urlparse
            return urlparse(self.mcp_url).netloc or self.id
        return self.id

    def as_dict(self, *, configured: bool) -> dict:
        return {
            "id": self.id, "name": self.name, "color": self.color,
            "attribution": dict(self.attribution), "server": self.server,
            "local": self.local, "public": self.public, "configured": configured,
            "external": self.external, "hint": self.hint,
            "filters": [f.as_dict() for f in self.filters] or None,
        }


# ── normalizers ─────────────────────────────────────────────────────────────

def _n_dataset(r: dict) -> Card | None:
    title = r.get("title")
    if not title:
        return None
    badges = [b for b in (r.get("source_type"),) if b]
    vc = r.get("version_count")
    if vc:
        badges.append(f"{vc} גרסאות")
    return Card(
        title=truncate(title, 160),
        snippet=join_parts(r.get("organization"), *(r.get("tags") or [])[:3]),
        url=r.get("page_url") or r.get("source_url"),
        badges=tuple(truncate(b, 40) for b in badges),
    )


def _n_table(r: dict) -> Card | None:
    tbl = r.get("table")
    if not tbl:
        return None
    badges = []
    if r.get("schema"):
        badges.append(r["schema"])
    if r.get("est_rows"):
        badges.append(f"~{int(r['est_rows']):,} שורות".replace(",", ","))
    matched = r.get("matched_columns") or []
    return Card(
        # The table name is the actionable identifier here (it is what you type
        # into /data), so it leads; the human title is the subtitle.
        title=truncate(tbl, 160),
        snippet=join_parts(
            r.get("title"), r.get("organization"),
            ("עמודות תואמות: " + ", ".join(matched[:4])) if matched else None,
        ),
        url=r.get("page_url") or r.get("source_url"),
        badges=tuple(truncate(b, 40) for b in badges),
    )


def _n_cbs(r: dict) -> Card | None:
    title = r.get("title") or r.get("title_en")
    if not title:
        return None
    years = join_parts(r.get("year_start"), r.get("year_end"), sep="–")
    badges = [b for b in (r.get("section"), r.get("item_type")) if b]
    if years:
        badges.append(years)
    return Card(
        title=truncate(title, 160),
        snippet=truncate(r.get("summary") or join_parts(*(r.get("subject_tags") or [])[:4])),
        url=r.get("page_url") or r.get("over_url"),
        badges=tuple(truncate(b, 40) for b in badges),
    )


def _n_protocol(r: dict) -> Card | None:
    committee = r.get("committee_name")
    knesset = r.get("knessetnum")
    title = committee or (f"ועדה {r.get('committee_id')}" if r.get("committee_id") else None)
    if not title:
        return None
    badges = []
    if knesset:
        badges.append(f"כנסת {knesset}")
    if r.get("file_format"):
        badges.append(str(r["file_format"]))
    return Card(
        title=truncate(title, 160),
        snippet=join_parts("פרוטוקול ועדה", r.get("startdate")),
        # file_url is the protocol itself on the Knesset server; session_url is
        # the sitting's page. Prefer the document.
        url=r.get("file_url") or r.get("session_url") or r.get("over_url"),
        date=iso_date(r.get("startdate")),
        badges=tuple(truncate(b, 40) for b in badges),
    )


def _n_mmm(r: dict) -> Card | None:
    title = r.get("title")
    if not title:
        return None
    badges = [b for b in (r.get("doc_type"), r.get("author")) if b]
    return Card(
        title=truncate(title, 160),
        snippet=truncate(r.get("abstract") or r.get("keywords")),
        url=r.get("pdf_url") or r.get("incident_url"),
        date=iso_date(r.get("date")),
        badges=tuple(truncate(b, 40) for b in badges),
    )


def _n_event(r: dict) -> Card | None:
    title = r.get("title")
    if not title:
        return None
    links = r.get("links") or {}
    badges = [b for b in (r.get("source_name"),) if b]
    return Card(
        title=truncate(title, 160),
        snippet=join_parts(r.get("location"), r.get("participants")),
        url=links.get("ocal_view") or links.get("ckan_dataset") or r.get("dataset_link"),
        date=iso_date(r.get("event_date") or r.get("start_time")),
        badges=tuple(truncate(b, 40) for b in badges),
    )


# ── the תאגידים column: three registries merged ─────────────────────────────
# Each corporate registry uses its OWN column names — the companies register is
# Hebrew, the cooperatives register is English, and the amutot register is
# Hebrew but spelled differently again. Verified against live rows; do not
# "simplify" to one set of names.
CORPORATE_REGISTRIES: tuple[dict, ...] = (
    {"dataset_id": "cc6286ac-5bd5-4930-a7a2-44df22863e77", "label": "רשם החברות",
     "name": ("שם חברה", "שם באנגלית"), "number": ("מספר חברה",),
     "status": ("סטטוס חברה",), "kind": ("סוג תאגיד",), "place": ("שם עיר",)},
    {"dataset_id": "73f3cd78-ef41-4f2e-90c6-64ecbfc6e9a9", "label": "עמותות",
     "name": ("שם עמותה בעברית", "שם עמותה באנגלית"), "number": ("מספר עמותה",),
     "status": ("סטטוס עמותה",), "kind": ("סיווג פעילות ענפי",), "place": ("כתובת - ישוב",)},
    {"dataset_id": "59360419-13ac-4a8e-8dce-f6f6b89a3beb", "label": "אגודות שיתופיות",
     "name": ("Name",), "number": ("Identity",),
     "status": ("StatusDesc",), "kind": ("PrimaryType",), "place": ("TownName",)},
)

CORPORATE_FILTER = Filter("dataset", "מרשם", "select", tuple(
    [{"value": "", "label": "כל המרשמים"}]
    + [{"value": d["dataset_id"], "label": d["label"]} for d in CORPORATE_REGISTRIES]
))


def _corporate_row(row: dict, spec: dict, page_url: str | None) -> Card | None:
    name = first_of(row, *spec["name"])
    if not name:
        return None
    number = first_of(row, *spec["number"])
    badges = [spec["label"]]
    for key in ("status", "kind"):
        v = first_of(row, *spec[key])
        if v:
            badges.append(str(v))
    return Card(
        title=truncate(name, 160),
        snippet=join_parts(
            (f"מספר תאגיד {number}" if number else None),
            first_of(row, *spec["place"]),
        ),
        # Every row links to the dataset's page on OVER rather than to an
        # invented per-entity URL: these registries publish no stable public
        # entity page, and a fabricated link is worse than a correct general one.
        url=page_url,
        badges=tuple(truncate(b, 40) for b in badges),
    )


async def _run_corporate(call, q: str, limit: int, filters: dict) -> dict:
    """Query the three corporate registries and merge them into one column.

    Ported from the reference's ``over-entities`` entry, including its rule that
    a total failure must RAISE rather than return an empty column — otherwise a
    dead append-DB reads to the user as "no such company".
    """
    import asyncio

    wanted = (filters or {}).get("dataset") or ""
    specs = [d for d in CORPORATE_REGISTRIES if not wanted or d["dataset_id"] == wanted]
    if not specs:
        specs = list(CORPORATE_REGISTRIES)
    per = max(3, -(-limit // len(specs)))

    async def one(spec: dict) -> list[Card]:
        payload = await call("query_dataset_rows",
                             {"dataset_id": spec["dataset_id"], "q": q, "limit": per})
        rows = payload.get("rows") or []
        page_url = payload.get("page_url")
        out = []
        for r in rows[:per]:
            try:
                c = _corporate_row(r, spec, page_url)
            except Exception:  # noqa: BLE001 — one bad row must not empty the column
                c = None
            if c:
                out.append(c)
        return out

    settled = await asyncio.gather(*(one(s) for s in specs), return_exceptions=True)
    ok = [r for r in settled if not isinstance(r, BaseException)]
    if not ok:
        raise next(r for r in settled if isinstance(r, BaseException))
    results = [c for sub in ok for c in sub]
    return {"results": results, "total": len(results)}


# ── TAG-IT corpora: מבקר המדינה + החלטות הממשלה ─────────────────────────────
# Both live on the SAME tag-it.biz workspace as ממ״מ, differing only by scope,
# and reuse the tagit_mcp_token already in the environment. We go through
# tagit_mcp's own _rpc rather than the generic remote transport so this inherits
# its proven cold-start retry and its shape-agnostic normalizer — TAG-IT's
# document rows nest differently per corpus, which is exactly what _normalize
# was written to absorb.

def _n_tagit(d: dict) -> Card | None:
    title = d.get("title")
    if not title:
        return None
    badges = [b for b in (d.get("doc_type"),) if b]
    # The SNIPPET first, not the abstract: TAG-IT returns the text around the
    # match with the matched words already wrapped in «…», and that is the
    # whole reason to search document bodies. An abstract describes the
    # document; the snippet shows why THIS document answered THIS query.
    return Card(
        title=truncate(title, 160),
        snippet=d.get("snippet") or truncate(d.get("abstract") or ""),
        url=d.get("link"),
        date=iso_date(d.get("date")),
        badges=tuple(badges),
    )


def _tagit_runner(scope_setting: str):
    async def run(call, q: str, limit: int, filters: dict) -> dict:
        from app.config import settings
        from app.services import tagit_mcp

        args = {"scope": int(getattr(settings, scope_setting)),
                "text_query": q, "page": 1, "size": max(1, min(limit, 50))}
        args.update(date_args(filters))
        result = await tagit_mcp._rpc(
            "tools/call", {"name": "search_documents", "arguments": args})
        items, total = tagit_mcp._as_items_and_total(tagit_mcp._tool_payload(result))
        cards: list[Card] = []
        for it in items:
            if not isinstance(it, dict):
                continue
            try:
                c = _n_tagit(tagit_mcp._normalize(it))
            except Exception:  # noqa: BLE001
                continue
            if c:
                cards.append(c)
        return {"results": cards, "total": total if total is not None else len(cards)}

    return run


# ── מבקר המדינה: OVER's own catalog of the reports ──────────────────────────
# The TAG-IT column above searches document BODIES but holds only the 2018-19
# local-government slice. This one is OVER's own scrape of the State
# Comptroller's library: metadata + a link to the PDF rather than full text,
# but every report type back to 1989 — and it extends itself as the scraper
# runs. (Its coverage currently stops at 2019 because the scraper's page walk
# gave up on a 192-page gap; fixed in govil-scraper ed6db15, so this column
# grows on its own once that lands.)

# Addressed by their idx-mirror TABLE, not by dataset_id: these corpora have no
# ``append_`` table, so query_dataset_rows resolves a name that does not exist
# and answers zero rows without erroring. See deep_search.idx_text_search.
MEVAKER_DATASETS: tuple[dict, ...] = (
    {"table": "mevaker_annual_12077e64_930bcb01", "label": "דוחות שנתיים",
     "ds": "930bcb01-26b4-4d3e-a179-eb1a7d7e8e0e"},
    {"table": "mevaker_local_government_55aa6126_7484e444", "label": "ביקורת על השלטון המקומי",
     "ds": "7484e444-d33c-4ecb-8f0b-bb14db2b3b92"},
    {"table": "mevaker_special_0dafb68d_2e052782", "label": "דוחות מיוחדים",
     "ds": "2e052782-7170-4c35-a095-913f92241460"},
    {"table": "mevaker_local_elections_funding_224da9e6_e1caf35f", "label": "מימון בחירות ברשויות",
     "ds": "e1caf35f-24da-440e-8741-432f8201f829"},
    {"table": "mevaker_studies_a22d3742_cc50a099", "label": "עיונים, מאמרים, ספרים",
     "ds": "cc50a099-013a-4639-9531-eaa87fa5dc66"},
    {"table": "mevaker_party_funding_b3a5060d_3c2671aa", "label": "מימון מפלגות",
     "ds": "3c2671aa-1fcc-42bf-afe0-bbb0e3cc1e1b"},
    {"table": "mevaker_ombudsman_25059dda_ace3e717", "label": "דוחות נציב תלונות הציבור",
     "ds": "ace3e717-1924-408c-8fef-311211647099"},
    {"table": "mevaker_unions_eb5d1524_e808abed", "label": "ביקורת על האיגודים",
     "ds": "e808abed-db33-41f9-8423-0557b6c91d9d"},
    {"table": "mevaker_primaries_funding_683675f3_424b6993", "label": "מימון בחירות מקדימות",
     "ds": "424b6993-3ce8-4368-b89b-b1882760ad5c"},
    # Re-listed 2026-08-11 after the enumeration fix proved the type is not
    # empty (it was de-listed on a truncated scan). Small but current: 3 rows,
    # both 2026, including a multi-national audit of government AI readiness.
    {"table": "mevaker_international_9546ba7e_1d7401a7", "label": "דוחות בינלאומיים",
     "ds": "1d7401a7-b91f-4825-ba12-3d79e00e4588"},
)

_MEVAKER_COLUMNS = ("title", "group_name", "main_audit_obj", "publication_name",
                    "publish_date", "report_url", "pdf_url")
_MEVAKER_SEARCH_IN = ("title", "group_name", "publication_name")
# publish_date is TEXT in DD.MM.YYYY, so a plain sort would order by day first.
_MEVAKER_ORDER = ('right("publish_date", 4) DESC NULLS LAST, '
                  'substr("publish_date", 4, 2) DESC, substr("publish_date", 1, 2) DESC')

MEVAKER_FILTER = Filter("dataset", "סוג הדוח", "select", tuple(
    [{"value": "", "label": "כל סוגי הדוחות"}]
    + [{"value": d["table"], "label": d["label"]} for d in MEVAKER_DATASETS]
))


def _mevaker_row(r: dict, spec: dict) -> Card | None:
    title = r.get("title")
    if not title:
        return None
    badges = [spec["label"]]
    pub = r.get("publication_name")
    if pub and pub != spec["label"]:
        badges.append(str(pub))
    return Card(
        title=truncate(title, 160),
        snippet=join_parts(r.get("group_name"), r.get("main_audit_obj")),
        # report_url is the report's page in the library; the PDF is the
        # fallback when a task has no page of its own.
        url=r.get("report_url") or r.get("pdf_url"),
        date=iso_date(r.get("publish_date")),
        badges=tuple(badges),
    )


async def _run_mevaker_catalog(call, q: str, limit: int, filters: dict) -> dict:
    """Search OVER's nine מבקר mirror tables and merge them into one column.

    Ignores ``call``: these corpora live in the idx schema with no append_
    table, so there is no MCP tool that can read them (see
    deep_search.idx_text_search for why, and for the safety argument).
    """
    import asyncio

    from app.services.deep_search import idx_text_search

    wanted = (filters or {}).get("dataset") or ""
    specs = [d for d in MEVAKER_DATASETS if not wanted or d["table"] == wanted] \
        or list(MEVAKER_DATASETS)
    per = max(2, -(-limit // len(specs)))

    async def one(spec: dict) -> list[Card]:
        rows = await idx_text_search(spec["table"], _MEVAKER_COLUMNS,
                                     _MEVAKER_SEARCH_IN, _MEVAKER_ORDER, q, per)
        out = []
        for r in rows:
            try:
                c = _mevaker_row(r, spec)
            except Exception:  # noqa: BLE001
                c = None
            if c:
                out.append(c)
        return out

    settled = await asyncio.gather(*(one(s) for s in specs), return_exceptions=True)
    ok = [r for r in settled if not isinstance(r, BaseException)]
    if not ok:
        raise next(r for r in settled if isinstance(r, BaseException))
    results = [c for sub in ok for c in sub]
    return {"results": results, "total": len(results)}


# ── מפתח התקציב (BudgetKey) — external, public, session-style MCP ───────────
# Four datasets merged into one column. Field names below are verified against
# live DatasetFullTextSearch responses, not inferred.

def _ils(v) -> str | None:
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    if n >= 1e9:
        return f"₪{n / 1e9:.1f} מיליארד"
    if n >= 1e6:
        return f"₪{n / 1e6:.0f} מיליון"
    if n >= 1e3:
        return f"₪{n / 1e3:.0f} אלף"
    return f"₪{n:.0f}"


def _n_ob_entity(r: dict) -> Card | None:
    if not r.get("entity_name"):
        return None
    return Card(title=r["entity_name"],
                snippet=join_parts(r.get("entity_kind__he"),
                                   _ils(r.get("received_amount")) and
                                   f"קיבל {_ils(r.get('received_amount'))}"),
                url=r.get("item_url"), badges=("ישויות",))


def _n_ob_budget(r: dict) -> Card | None:
    if not r.get("title"):
        return None
    return Card(title=r["title"],
                snippet=join_parts(r.get("code"), r.get("year-range")),
                url=r.get("item_url"), badges=("סעיפי תקציב",))


def _n_ob_support(r: dict) -> Card | None:
    name = r.get("receiver_entity_name")
    if not name:
        return None
    return Card(title=name,
                snippet=join_parts(r.get("purpose"), r.get("supporting_ministry")),
                url=r.get("item_url"),
                date=(str(r["__approval_year"]) if r.get("__approval_year") else None),
                badges=("תמיכות",))


def _n_ob_contract(r: dict) -> Card | None:
    name = r.get("supplier_entity_name")
    if not name:
        return None
    return Card(title=name,
                snippet=join_parts(r.get("purpose"), r.get("purchasing_ministry"),
                                   _ils(r.get("executed") or r.get("volume"))),
                url=r.get("item_url"),
                date=(str(r["start_year"]) if r.get("start_year") else None),
                badges=("התקשרויות",))


OBUDGET_DATASETS: tuple[dict, ...] = (
    {"id": "entities_data", "label": "ישויות", "norm": _n_ob_entity},
    {"id": "budget_items_data", "label": "סעיפי תקציב", "norm": _n_ob_budget},
    {"id": "supports_transactions_data", "label": "תמיכות", "norm": _n_ob_support},
    {"id": "contracts_data", "label": "התקשרויות", "norm": _n_ob_contract},
)

OBUDGET_FILTER = Filter("dataset", "מאגר", "select", tuple(
    [{"value": "", "label": "כל המאגרים"}]
    + [{"value": d["id"], "label": d["label"]} for d in OBUDGET_DATASETS]
))


async def _run_obudget(call, q: str, limit: int, filters: dict) -> dict:
    """Query the four BudgetKey datasets and merge them into one column.

    SEQUENTIALLY, on purpose. This server serves one request at a time per MCP
    session: firing the four concurrently makes the whole column hang until the
    timeout, while one after another costs ~3s in total. Do not "optimise" this
    into a gather without re-measuring against the live endpoint.
    """
    wanted = (filters or {}).get("dataset") or ""
    specs = [d for d in OBUDGET_DATASETS if not wanted or d["id"] == wanted] \
        or list(OBUDGET_DATASETS)
    per = max(2, -(-limit // len(specs)))

    results: list[Card] = []
    failures: list[BaseException] = []
    for spec in specs:
        try:
            payload = await call("DatasetFullTextSearch", {"dataset": spec["id"], "q": q})
        except Exception as e:  # noqa: BLE001 — one dead dataset ≠ a dead column
            failures.append(e)
            continue
        for r in (payload.get("search_results") or [])[:per]:
            try:
                c = spec["norm"](r)
            except Exception:  # noqa: BLE001
                c = None
            if c:
                results.append(c)
    if failures and len(failures) == len(specs):
        # Everything failed — surface it as an error rather than as "no results".
        raise failures[0]
    return {"results": results, "total": len(results)}


# ── the registry ────────────────────────────────────────────────────────────

_OVER_ATTR = {"text": "נתוני מטא-דאטה מעובדים של גרסאות לעם — היסטוריית הגרסאות של מאגרים ממשלתיים.",
              "href": "https://www.over.org.il"}

SOURCES: tuple[Source, ...] = (
    Source(
        id="datasets", name="מאגרי מידע במעקב", color="#044E66",
        attribution=_OVER_ATTR, local="over", tool="search_datasets",
        results_path="items", hint="מאגרים שגרסאות לעם עוקבת אחריהם",
        filters=(SOURCE_TYPE, Filter("tag", "תגית")),
        build_args=lambda q, limit, f: {
            "query": q, "limit": limit,
            **({"source_type": f["source_type"]} if f.get("source_type") else {}),
            **({"tag": f["tag"]} if f.get("tag") else {}),
        },
        normalize=_n_dataset,
    ),
    Source(
        id="tables", name="טבלאות במסד הנתונים", color="#0A7A9A",
        attribution={"text": "קטלוג הטבלאות של קונסולת ה-SQL הציבורית של גרסאות לעם.",
                     "href": "https://www.over.org.il/data"},
        local="sql", tool="list_tables", results_path="tables",
        hint="טבלאות שאפשר לתשאל ב-/data",
        filters=(Filter("schema", "סכימה"), SOURCE_TYPE),
        build_args=lambda q, limit, f: {
            "q": q, "limit": limit,
            **({"schema": f["schema"]} if f.get("schema") else {}),
            **({"source_type": f["source_type"]} if f.get("source_type") else {}),
        },
        normalize=_n_table,
    ),
    Source(
        id="cbs", name="הלמ״ס", color="#1094B8",
        attribution={"text": "אינדקס תוכן מעובד של אתר הלמ״ס (cbs.gov.il) — מטא-דאטה בלבד.",
                     "href": "https://www.over.org.il/cbs"},
        local="cbs", tool="search", results_path="items",
        hint="פרסומים ולוחות של הלשכה המרכזית לסטטיסטיקה",
        filters=YEAR_RANGE + (Filter("file_type", "סוג קובץ"),),
        build_args=lambda q, limit, f: {
            "q": q, "limit": limit, **year_args(f),
            **({"file_type": f["file_type"]} if f.get("file_type") else {}),
        },
        normalize=_n_cbs,
    ),
    Source(
        id="protocols", name="פרוטוקולי ועדות הכנסת", color="#38AED0",
        attribution={"text": "מראה מעובדת של נתוני ODATA של הכנסת; תוכן הפרוטוקול נשאר בשרת הכנסת.",
                     "href": "https://www.over.org.il/knesset"},
        local="knesset", tool="search_protocols", results_path="items",
        hint="ישיבות ועדה — מטא-דאטה וקישור לקובץ",
        filters=(Filter("knesset_num", "מספר כנסת", "number"),) + DATE_RANGE,
        build_args=lambda q, limit, f: {
            "q": q, "limit": limit, **date_args(f),
            **({"knesset_num": int(f["knesset_num"])} if str(f.get("knesset_num") or "").isdigit() else {}),
        },
        normalize=_n_protocol,
    ),
    Source(
        id="mmm", name="מסמכי ממ״מ", color="#68C4DE",
        attribution={"text": "קטלוג מעובד של מרכז המחקר והמידע של הכנסת; המסמך המלא בשרת הכנסת.",
                     "href": "https://www.over.org.il/knesset?tab=mmm"},
        local="knesset", tool="search_mmm", results_path="items",
        hint="מחקרי מרכז המחקר והמידע של הכנסת",
        filters=YEAR_RANGE + (Filter("author", "מחבר"), Filter("doc_type", "סוג מסמך")),
        build_args=lambda q, limit, f: {
            "q": q, "limit": limit, **year_args(f),
            **({"author": f["author"]} if f.get("author") else {}),
            **({"doc_type": f["doc_type"]} if f.get("doc_type") else {}),
        },
        normalize=_n_mmm,
    ),
    Source(
        id="ocal", name="יומני בעלי תפקידים", color="#06607C",
        attribution={"text": "נתונים מעובדים (זיהוי ישויות והצלבות) של יומן לעם — יומני פגישות של בעלי תפקידים.",
                     "href": "https://www.over.org.il/projects/ocal"},
        local="ocal", tool="search_events", results_path="events",
        hint="פגישות מיומני נבחרי ציבור",
        filters=DATE_RANGE + (Filter("location", "מיקום"), Filter("participants", "משתתפים")),
        build_args=lambda q, limit, f: {
            "q": q, "limit": limit, **date_args(f, "from_date", "to_date"),
            **({"location": f["location"]} if f.get("location") else {}),
            **({"participants": f["participants"]} if f.get("participants") else {}),
        },
        normalize=_n_event,
    ),
    Source(
        id="mevaker_reports", name="מבקר המדינה — קטלוג הדוחות", color="#0369a1",
        attribution={"text": "סריקה של גרסאות לעם לספריית מבקר המדינה — מטא-דאטה "
                             "וקישור לדוח המלא באתר המבקר.",
                     "href": "https://www.mevaker.gov.il/subjects"},
        # Grouped with the SQL server because that is the store it actually
        # reads (the idx mirror), not the app DB.
        local="sql", hint="כל סוגי הדוחות · מטא-דאטה וקישור ל-PDF",
        filters=(MEVAKER_FILTER,),
        build_args=lambda q, limit, f: {},   # unused — run() drives this source
        run=_run_mevaker_catalog,
    ),
    Source(
        # Deliberately separate from the catalog column: this one searches the
        # document TEXT, but the TAG-IT workspace holds only the 2018-19
        # local-government slice. The hint says so — a user who searches 2023
        # here and gets nothing must know why.
        id="mevaker", name="מבקר המדינה — טקסט מלא", color="#075985",
        attribution={"text": "מקור חיצוני: מסמכי מבקר המדינה כפי שנסרקו ונותחו ב-TAG-IT; "
                             "המסמך המלא באתר המבקר. הקורפוס כאן מכסה דוחות ביקורת "
                             "על השלטון המקומי בלבד, 2018–2019.",
                     "href": "https://tag-it.biz"},
        mcp_url="https://tag-it.biz/mcp", token_env="TAGIT_MCP_TOKEN", external=True,
        native_operators=True,
        hint="חיפוש בגוף המסמך · שלטון מקומי 2018–2019 בלבד",
        filters=DATE_RANGE,
        build_args=lambda q, limit, f: {},   # unused — run() drives this source
        run=_tagit_runner("tagit_mevaker_scope"),
    ),
    Source(
        id="protocols_text", name="פרוטוקולי ועדות — טקסט מלא", color="#38AED0",
        attribution={"text": "מקור חיצוני: פרוטוקולי ועדות הכנסת כפי שנסרקו ונותחו ב-TAG-IT; "
                             "הפרוטוקול המלא בשרת הכנסת. הכיסוי חלקי ואינו כולל את כל הכנסות.",
                     "href": "https://tag-it.biz"},
        mcp_url="https://tag-it.biz/mcp", token_env="TAGIT_MCP_TOKEN", external=True,
        native_operators=True,
        hint="חיפוש בתוך דברי הדיון · כיסוי חלקי",
        filters=DATE_RANGE,
        build_args=lambda q, limit, f: {},   # unused — run() drives this source
        run=_tagit_runner("tagit_protocols_scope"),
    ),
    Source(
        id="mmm_text", name="מסמכי ממ״מ — טקסט מלא", color="#68C4DE",
        attribution={"text": "מקור חיצוני: מסמכי מרכז המחקר והמידע של הכנסת כפי שנסרקו "
                             "ונותחו ב-TAG-IT; המסמך המלא בשרת הכנסת.",
                     "href": "https://tag-it.biz"},
        mcp_url="https://tag-it.biz/mcp", token_env="TAGIT_MCP_TOKEN", external=True,
        native_operators=True,
        hint="חיפוש בתוך גוף המחקר",
        # NO date filter, on purpose. Measured 2026-08-11: 59 of 60 ממ״מ hits
        # come back with an empty `date`, so a date range here would silently
        # return nothing — the exact failure mode this page keeps having to
        # design against. Restore it once TAG-IT populates the field.
        filters=(),
        build_args=lambda q, limit, f: {},   # unused — run() drives this source
        run=_tagit_runner("tagit_mmm_scope"),
    ),
    Source(
        id="gov_decisions", name="החלטות הממשלה", color="#7c3aed",
        attribution={"text": "מקור חיצוני: מאגר החלטות הממשלה כפי שנסרק ונותח ב-TAG-IT; "
                             "ההחלטה המלאה באתר gov.il.",
                     "href": "https://tag-it.biz"},
        mcp_url="https://tag-it.biz/mcp", token_env="TAGIT_MCP_TOKEN", external=True,
        native_operators=True,
        hint="חיפוש בתוך גוף ההחלטות",
        filters=DATE_RANGE,
        build_args=lambda q, limit, f: {},   # unused — run() drives this source
        run=_tagit_runner("tagit_gov_decisions_scope"),
    ),
    Source(
        id="obudget", name="מפתח התקציב", color="#b45309",
        attribution={
            "text": "מקור חיצוני ומידע מעובד: הנתונים נאספים ומעובדים על-ידי מפתח "
                    "התקציב (obudget.org) — לא על-ידי גרסאות לעם. ישויות, סעיפי "
                    "תקציב, תמיכות והתקשרויות.",
            "href": "https://next.obudget.org",
        },
        mcp_url="https://next.obudget.org/mcp", public=True, handshake=True,
        external=True,
        hint="ישויות · סעיפי תקציב · תמיכות · התקשרויות",
        filters=(OBUDGET_FILTER,),
        build_args=lambda q, limit, f: {},   # unused — run() drives this source
        run=_run_obudget,
    ),
    Source(
        id="entities", name="תאגידים", color="#003647",
        attribution={"text": "שורות מתוך מרשמי התאגידים הרשמיים כפי שנשמרו בגרסאות לעם.",
                     "href": "https://www.over.org.il/data"},
        local="over", hint="רשם החברות · עמותות · אגודות שיתופיות",
        filters=(CORPORATE_FILTER,),
        build_args=lambda q, limit, f: {},   # unused — run() drives this source
        run=_run_corporate,
    ),
)

_BY_ID = {s.id: s for s in SOURCES}


def active_sources() -> list[Source]:
    return [s for s in SOURCES if s.active]


def source_by_id(sid: str) -> Source | None:
    s = _BY_ID.get(sid)
    return s if s and s.active else None


def resolve(ids: list[str] | None) -> list[Source]:
    """The requested subset, in registry order; empty/None ⇒ every active source."""
    if not ids:
        return active_sources()
    wanted = {i.strip() for i in ids if i.strip()}
    return [s for s in active_sources() if s.id in wanted]
