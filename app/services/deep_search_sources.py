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
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        # DD/MM/YYYY is the Israeli registries' format; a day > 12 disambiguates
        # it from the US M/D/YYYY the cooperatives feed uses. When both readings
        # are valid the two agree closely enough not to matter for sorting.
        if d > 12:
            return f"{y:04d}-{mo:02d}-{d:02d}"
        return f"{y:04d}-{d:02d}-{mo:02d}"
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
    MAX_SNIPPET = 240
    MAX_BADGE = 40

    def __post_init__(self):
        object.__setattr__(self, "title", truncate(self.title, self.MAX_TITLE))
        object.__setattr__(self, "snippet", truncate(self.snippet, self.MAX_SNIPPET))
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
            "hint": self.hint,
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
