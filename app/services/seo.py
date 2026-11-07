"""Server-rendered head tags for the SPA.

The problem this solves: over.org.il is a client-rendered React app, so every
URL — all ~1,250 dataset pages included — served the same shell to a crawler:

    <title>גרסאות לעם</title>
    <div id="root"></div>

Zero characters of body text, one title for the whole site, no description, no
canonical, no structured data. The per-route titles the app sets are written by
JavaScript after hydration, which a crawler's first pass never sees. There was
also no sitemap and no robots.txt, so nothing pointed at the deep pages either.

Full server-side rendering would fix it and is a rewrite. This does the part
that actually matters for discovery: the ``<head>``. FastAPI already intercepts
every SPA route to serve ``index.html`` (see ``spa_fallback`` in main.py), so
the head can be composed there — title, description, canonical, Open Graph, and
JSON-LD — before the file goes out. The body stays client-rendered.

Three rules this module holds to:

* **It must never break the page.** Every lookup is wrapped; on any failure the
  generic head is used and the SPA loads exactly as before. SEO is not worth a
  500.
* **It must not cost a database round trip per view.** Route metadata is
  memoised with a TTL, so a crawler walking 1,250 pages does not walk the
  database 1,250 times per minute.
* **It is served to everyone, not just bots.** Cloaking is a ranking risk, and
  the same tags are what make a link pasted into WhatsApp or Twitter render a
  title and a description instead of a bare URL.
"""
from __future__ import annotations

import asyncio
import html
import json
import logging
import re
import time
import uuid
from dataclasses import dataclass, field

from sqlalchemy import select

logger = logging.getLogger(__name__)

SITE_NAME = "גרסאות לעם"
SITE_URL = "https://www.over.org.il"
DEFAULT_DESCRIPTION = (
    "גרסאות לעם עוקב אחרי מאגרי המידע הממשלתיים הפתוחים של ישראל ושומר כל גרסה "
    "שלהם — כדי שאפשר יהיה לראות מה השתנה, מתי, ומה נמחק."
)
# The markers index.html carries so the server knows what to overwrite. They
# ENCLOSE the build's fallback <title>: replacing a single marker beside it
# would emit two <title> elements. A build without them still serves fine — see
# render() — which keeps a stale dist/ from taking the site down.
MARKER_START = "<!--SEO:START-->"
MARKER_END = "<!--SEO:END-->"

_TTL_SECONDS = 600.0


@dataclass
class PageMeta:
    title: str
    description: str = DEFAULT_DESCRIPTION
    canonical_path: str = "/"
    #: Extra JSON-LD objects to emit beside the sitewide WebSite node.
    jsonld: list[dict] = field(default_factory=list)
    #: Sent as `noindex` for pages that must never rank (admin, login).
    noindex: bool = False
    #: With noindex: also tell crawlers not to follow the page's links (admin).
    nofollow: bool = False
    #: Server-rendered content placed inside <div id="root"> — see render().
    body_html: str = ""
    #: HTTP status. 404 for a route the SPA does not have, or a detail page
    #: whose record does not exist — never for a lookup that merely failed.
    status: int = 200


# ── static routes ───────────────────────────────────────────────────────────
# Written out rather than derived from the router: these strings are the search
# result, so they are copy, not configuration. Each description says what the
# page does for someone who has not heard of the site.
_STATIC: dict[str, PageMeta] = {
    "/": PageMeta(
        title="מאגרי מידע ממשלתיים במעקב",
        description=DEFAULT_DESCRIPTION,
        canonical_path="/",
    ),
    "/about": PageMeta(
        title="אודות",
        description="מה גרסאות לעם עושה, למה, ואיך המעקב אחרי מאגרי המידע הממשלתיים עובד.",
        canonical_path="/about",
    ),
    # פרויקט 1933: three tabs. The rationale is the first; /rationale redirects
    # to it in the SPA.
    "/projects/1933": PageMeta(
        title="פרויקט 1933 — הרציונל",
        description="למה שמירת גרסאות של מאגרי מידע ממשלתיים היא תנאי לשקיפות — הנימוק המלא, בעקבות החלטת ממשלה 1933.",
        canonical_path="/projects/1933",
    ),
    "/projects/1933/decision": PageMeta(
        title="פרויקט 1933 — הנוסח המלא, המשימות והחשבון",
        description="החלטת ממשלה 1933 סעיף אחר סעיף: המשימות שנגזרו ממנה, מה היה הפוטנציאל ומה קרה בפועל.",
        canonical_path="/projects/1933/decision",
    ),
    "/projects/abroad": PageMeta(
        title='חו"ל — פריטי GovMap מחוץ לגבולות המדינה',
        description='פריטים בשכבות GovMap שנמצאים מחוץ לגבולות ישראל, איו"ש והמרחב הימי: לפי גוף מפרסם ושכבה, עם רשימה מלאה של הפריטים להורדה.',
        canonical_path="/projects/abroad",
    ),
    "/projects/1933/request": PageMeta(
        title="פרויקט 1933 — הבקשה הרוחבית",
        description="בקשת חופש מידע של עמותת הצלחה לכל משרדי הממשלה: מה עשיתם כדי ליישם את החלטה 1933? התשובות, משרד אחר משרד.",
        canonical_path="/projects/1933/request",
    ),
    "/api": PageMeta(
        title="API ציבורי",
        description="ממשק פתוח לכל מה שגרסאות לעם אוסף: מאגרים, גרסאות, טבלאות ושכבות מפה. בלי הרשמה ובלי מפתח.",
        canonical_path="/api",
    ),
    "/data": PageMeta(
        title="מאגר הנתונים — ממשק SQL מרכזי",
        description="תשאול SQL חופשי, לקריאה בלבד, מעל כל הטבלאות של האתר — מאגרי data.gov.il, מסד הכנסת, שכבות ממ״ג ועוד.",
        canonical_path="/data",
    ),
    "/data/explore": PageMeta(
        title="מצא נתונים",
        description="תארו מה אתם מחפשים, וגרסאות לעם יציע את הטבלאות והמאגרים הרלוונטיים.",
        canonical_path="/data/explore",
    ),
    "/data/guide": PageMeta(
        title="מדריך — הצלבה מתוקנת בין מאגרים",
        description="איך מחברים שני מאגרים ממשלתיים לפי יישוב או רשות מקומית בלי לאבד שורות לכתיב שונה.",
        canonical_path="/data/guide",
    ),
    "/data/normalize": PageMeta(
        title="נרמול רשימת שמות יישובים",
        description="הדביקו רשימת שמות יישובים וקבלו את הכתיב הרשמי והקוד הקנוני של הלמ״ס.",
        canonical_path="/data/normalize",
    ),
    "/knesset": PageMeta(
        title="מסד הנתונים של הכנסת",
        description="עותק מלא ומתעדכן של הנתונים הפתוחים של הכנסת — חברי כנסת, הצעות חוק, הצבעות, ועדות ופרוטוקולים — עם חיפוש ותשאול SQL.",
        canonical_path="/knesset",
    ),
    "/organizations": PageMeta(
        title="ארגונים",
        description="כל משרדי הממשלה והגופים הציבוריים שמאגרי המידע שלהם במעקב.",
        canonical_path="/organizations",
    ),
    "/tags": PageMeta(
        title="תגיות",
        description="עיון במאגרי המידע הממשלתיים לפי נושא.",
        canonical_path="/tags",
    ),
    "/sources": PageMeta(
        title="מקורות",
        description="האתרים והפורטלים הממשלתיים שגרסאות לעם אוסף מהם, ואיך כל אחד נאסף.",
        canonical_path="/sources",
    ),
    "/cbs": PageMeta(
        title="חיפוש בנתוני הלמ״ס",
        description="חיפוש בשפה חופשית בקטלוג הפרסומים והנתונים של הלשכה המרכזית לסטטיסטיקה.",
        canonical_path="/cbs",
    ),
    "/lookup": PageMeta(
        title="איתור מאגר לפי כתובת",
        description="הדביקו קישור למאגר ממשלתי וגלו אם הוא במעקב ומה ההיסטוריה שלו.",
        canonical_path="/lookup",
    ),
    "/projects/questions": PageMeta(
        title="שאלות לעם — חיפוש רוחבי",
        description="שאלה אחת, חיפוש בו־זמנית ב־13 מאגרי מידע ציבוריים: מאגרי ממשלה, הכנסת, מבקר המדינה, החלטות ממשלה ועוד.",
        canonical_path="/projects/questions",
    ),
    "/projects/ocal": PageMeta(
        title="יומן לעם",
        description="יומני הפגישות של בעלי תפקידים בשירות הציבורי, במקום אחד וניתנים לחיפוש.",
        canonical_path="/projects/ocal",
    ),
    "/projects/ocoi": PageMeta(
        title="ניגוד עניינים לעם",
        description="הצהרות ניגוד העניינים של נושאי משרה ציבורית, מחולצות ומקושרות לגרף ישויות.",
        canonical_path="/projects/ocoi",
    ),
    "/projects/nadlan": PageMeta(
        title="נדל״ן לעם",
        description="הצלבה בין חלקות, גושים, כתובות ומיקוד — מיליון ומאה אלף חלקות מקושרות לכתובות.",
        canonical_path="/projects/nadlan",
    ),
    "/projects/deals": PageMeta(
        title="עסקאות נדל\"ן",
        description=("מאגר עסקאות הנדל\"ן של מיסוי מקרקעין — 3.8 מיליון עסקאות "
                     "מדווחות מ-1998, לחיפוש ולהשוואה, לצד דוח הפערים מול "
                     "אתר הנדל\"ן הממשלתי."),
        canonical_path="/projects/deals",
    ),
    "/projects/odata": PageMeta(
        title="מידע לעם",
        description="מאגרי מידע שהתקבלו בבקשות חופש מידע, מיובאים ופתוחים לתשאול.",
        canonical_path="/projects/odata",
    ),
    "/growth": PageMeta(
        title="גידולים חקלאיים",
        description="מפת הגידולים החקלאיים בישראל לאורך זמן.",
        canonical_path="/growth",
    ),
}

# Pages that must never appear in a search result.
_NOINDEX_PREFIXES = ("/admin", "/cbs/feedback")

# Every path the SPA can render (frontend/src/App.tsx). Anything else is a 404:
# serving the shell with a 200 told Google that every typo and every dead link
# was a real, indexable page ("soft 404"). The shell still goes out with the
# 404, so a person who mistypes a URL sees the app, not a bare error.
# KEEP IN SYNC with App.tsx — a route missing here is served with a 404 status
# (it still renders in a browser, but will not be indexed).
_SPA_ROUTES = tuple(re.compile(p) for p in (
    r"^/$",
    r"^/(about|lookup|organizations|tags|sources|cbs|data|knesset|api|growth|security|rationale)$",
    r"^/direct(/.*)?$",
    r"^/(versions|archive|organizations|tags|sources)/[^/]+$",
    r"^/cbs/feedback$",
    r"^/data/(explore|guide|normalize)$",
    r"^/knesset/guide$",
    r"^/projects/(questions|odata|ocal|ocoi|nadlan|deals|prices|abroad|1933)$",
    r"^/projects/1933/(decision|request)$",
    r"^/rationale/[^/]+$",
    r"^/admin(/.*)?$",
))

#: Returned by _lookup_dynamic for a detail route whose record does not exist.
_NOT_FOUND = object()

#: A tag with fewer public datasets than this is a near-duplicate of the one
#: dataset page it lists: kept out of the sitemap and served noindex,follow.
#: 2,159 of 2,377 tags had exactly one dataset when this was set.
MIN_TAG_DATASETS = 3


def is_spa_route(path: str) -> bool:
    return any(p.match(path) for p in _SPA_ROUTES)


# ── per-route metadata ──────────────────────────────────────────────────────
_cache: dict[str, tuple[float, PageMeta]] = {}
_lock = asyncio.Lock()

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I
)


def _clean(value: str | None, limit: int = 300) -> str:
    """Collapse whitespace and clip — meta descriptions are one line, not prose."""
    text = " ".join((value or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


# ── server-rendered body ────────────────────────────────────────────────────
# The head alone was not enough: the body a crawler received was an empty
# <div id="root">, with no words and no links. What follows renders real, plain
# HTML for the pages that carry the catalogue — dataset, organization, tag and
# the hubs that link to them — inside #root. React's createRoot replaces it on
# mount, so a person sees it only for the moment before the bundle runs, and
# everyone (crawler or not) gets the same bytes: no cloaking.

_ID_CARD_TIMEOUT = 3.0  # seconds; the append DB being slow costs the card, not the page
_MAX_LIST = 500

_BODY_STYLE = (
    "<style>"
    "#ssr{max-width:60rem;margin:1.5rem auto;padding:0 1rem;"
    "font-family:Heebo,Rubik,system-ui,sans-serif;line-height:1.6}"
    "#ssr nav a{margin-inline-end:1rem}"
    "#ssr .ai-card{border:2px dashed #b45309;background:#fffbeb;color:#422006;"
    "border-radius:8px;padding:1rem 1.25rem;margin:1.5rem 0}"
    "#ssr .ai-badge{display:block;background:#b45309;color:#fff;font-weight:700;"
    "padding:.4rem .75rem;border-radius:6px;margin-bottom:.75rem}"
    "</style>"
)

#: The label every LLM-written block carries. Prominent and explicit on purpose:
#: the ID card must never read as part of the dataset or the publisher's words.
AI_LABEL_HE = (
    "🤖 תוכן שנוצר אוטומטית על ידי מודל שפה (AI) — אינו חלק מהמאגר ואינו טקסט "
    "של הגוף המפרסם. נכתב מתוך דגימה של שורות המאגר ועלול להכיל טעויות."
)


def _e(text) -> str:
    return html.escape(str(text if text is not None else ""), quote=True)


def _a(href: str, text: str) -> str:
    return f'<a href="{_e(href)}">{_e(text)}</a>'


def _nav() -> str:
    links = [
        ("/", "דף הבית"), ("/organizations", "ארגונים"), ("/tags", "תגיות"),
        ("/sources", "מקורות"), ("/data", "ממשק SQL"), ("/knesset", "הכנסת"),
        ("/projects/questions", "חיפוש רוחבי"), ("/api", "API"), ("/about", "אודות"),
    ]
    return '<nav aria-label="ניווט ראשי">' + "".join(_a(h, t) for h, t in links) + "</nav>"


def _wrap(inner: str) -> str:
    return f'{_BODY_STYLE}<div id="ssr" dir="rtl" lang="he">{_nav()}<main>{inner}</main></div>'


def _link_list(items: list[tuple[str, str]]) -> str:
    if not items:
        return ""
    return "<ul>" + "".join(f"<li>{_a(h, t)}</li>" for h, t in items) + "</ul>"


def id_card_html(cards: list[dict]) -> str:
    """The LLM-written ID card, inside an unmistakable AI frame."""
    if not cards:
        return ""
    out = (
        '<section class="ai-card" aria-label="תוכן שנוצר על ידי מודל שפה" '
        'data-generated-by="llm">'
        f'<strong class="ai-badge">{_e(AI_LABEL_HE)}</strong>'
        "<h2>תעודת זהות של המאגר (נוצרה על ידי מודל שפה)</h2>"
    )
    for card in cards:
        if len(cards) > 1 and card.get("title"):
            out += f"<h3>{_e(card['title'])}</h3>"
        out += f"<p>{_e(card['summary_he'])}</p>"
        if card.get("tags"):
            out += "<p>נושאים (לפי המודל): " + _e(" · ".join(card["tags"])) + "</p>"
        if card.get("columns"):
            out += "<dl>" + "".join(
                f"<dt>{_e(c['name'])}</dt><dd>{_e(c['description_he'])}</dd>"
                for c in card["columns"]
            ) + "</dl>"
    return out + "</section>"


def _date(d) -> str:
    return d.date().isoformat() if d else ""


async def _lookup_dynamic(path: str):
    """Resolve a detail route (or a hub page) against the database.

    Returns a PageMeta, ``_NOT_FOUND`` for a detail route whose record does not
    exist, or None for a path this function does not handle.
    """
    from sqlalchemy import desc, func

    from app.database import async_session
    from app.models.organization import Organization
    from app.models.tag import Tag, dataset_tags
    from app.models.tracked_dataset import TrackedDataset
    from app.models.version_index import VersionIndex

    parts = [p for p in path.split("/") if p]
    public = (
        TrackedDataset.status.in_(("active", "pending")),
        TrackedDataset.ckan_name.notlike("knesset-committee-single-%"),
    )

    async with async_session() as db:
        # /versions/<uuid> and /archive/<uuid> — a tracked dataset
        if len(parts) == 2 and parts[0] in ("versions", "archive"):
            if not _UUID_RE.match(parts[1]):
                return _NOT_FOUND
            ds = (
                await db.execute(
                    select(TrackedDataset).where(TrackedDataset.id == uuid.UUID(parts[1]))
                )
            ).scalar_one_or_none()
            if ds is None or ds.status not in ("active", "pending"):
                return _NOT_FOUND
            # TrackedDataset.organization holds the SOURCE's slug — CKAN gives
            # us "interior_affairs", which is what a search result would have
            # shown. The Organization row carries the name a person recognises,
            # so prefer it and keep the slug only as a fallback.
            org = ds.organization or ""
            if ds.organization_id:
                row = (
                    await db.execute(
                        select(Organization.title, Organization.name).where(
                            Organization.id == ds.organization_id
                        )
                    )
                ).first()
                if row and (row[0] or row[1]):
                    org = row[0] or row[1]

            versions = (
                await db.execute(
                    select(VersionIndex.version_number, VersionIndex.detected_at)
                    .where(VersionIndex.tracked_dataset_id == ds.id)
                    .order_by(desc(VersionIndex.version_number))
                )
            ).all()
            tags = (
                await db.execute(
                    select(Tag.id, Tag.name)
                    .join(dataset_tags, dataset_tags.c.tag_id == Tag.id)
                    .where(dataset_tags.c.dataset_id == ds.id)
                    .order_by(Tag.name)
                )
            ).all()
            siblings = []
            if ds.organization_id:
                siblings = (
                    await db.execute(
                        select(TrackedDataset.id, TrackedDataset.title)
                        .where(
                            TrackedDataset.organization_id == ds.organization_id,
                            TrackedDataset.id != ds.id,
                            *public,
                        )
                        .order_by(TrackedDataset.title)
                        .limit(30)
                    )
                ).all()

            cards: list[dict] = []
            try:
                from app.services import table_profiler
                cards = await asyncio.wait_for(
                    table_profiler.dataset_id_cards(db, ds), _ID_CARD_TIMEOUT
                )
            except Exception:  # noqa: BLE001 — the card is optional
                logger.info("SEO: no ID card for %s", ds.id, exc_info=True)

            what = "ארכיון מצטבר" if parts[0] == "archive" else "היסטוריית גרסאות"
            n_versions = len(versions)
            desc_text = (
                f"{what} של המאגר «{_clean(ds.title, 120)}»"
                + (f" מאת {_clean(org, 80)}" if org else "")
                + (f" — {n_versions} גרסאות שמורות" if n_versions > 1 else "")
                + ". גרסאות לעם שומר כל שינוי במאגר כדי שאפשר יהיה להשוות בין גרסאות ולראות מה נמחק."
            )

            inner = f"<h1>{_e(ds.title)}</h1><p>{_e(desc_text)}</p><ul>"
            if org:
                label = _a(f"/organizations/{ds.organization_id}", org) if ds.organization_id else _e(org)
                inner += f"<li>גוף מפרסם: {label}</li>"
            source = _source_url(ds)
            if source:
                inner += f'<li>המקור: <a href="{_e(source)}" rel="nofollow">{_e(source)}</a></li>'
            if ds.last_polled_at:
                inner += f"<li>נבדק לאחרונה: {_e(_date(ds.last_polled_at))}</li>"
            inner += f"<li>גרסאות שמורות: {n_versions}</li></ul>"
            if versions:
                inner += "<h2>גרסאות אחרונות</h2><ul>" + "".join(
                    f"<li>גרסה {v} — {_e(_date(at))}</li>" for v, at in versions[:10]
                ) + "</ul>"
            if tags:
                inner += "<h2>תגיות</h2>" + _link_list([(f"/tags/{i}", n) for i, n in tags])
            inner += id_card_html(cards)
            if siblings:
                inner += f"<h2>מאגרים נוספים של {_e(org)}</h2>" + _link_list(
                    [(f"/versions/{i}", t) for i, t in siblings]
                )

            return PageMeta(
                title=_clean(ds.title, 110),
                description=_clean(desc_text),
                canonical_path=f"/{parts[0]}/{ds.id}",
                jsonld=[_dataset_jsonld(ds, org)],
                body_html=_wrap(inner),
            )

        # /organizations/<id>
        if len(parts) == 2 and parts[0] == "organizations":
            org = (
                await db.execute(
                    select(Organization).where(Organization.id == uuid.UUID(parts[1]))
                )
            ).scalar_one_or_none() if _UUID_RE.match(parts[1]) else None
            if org is None:
                return _NOT_FOUND
            name = org.title or org.name
            rows = (
                await db.execute(
                    select(TrackedDataset.id, TrackedDataset.title)
                    .where(TrackedDataset.organization_id == org.id, *public)
                    .order_by(TrackedDataset.title)
                    .limit(_MAX_LIST)
                )
            ).all()
            inner = f"<h1>{_e(name)}</h1>"
            if org.description:
                inner += f"<p>{_e(_clean(org.description, 1000))}</p>"
            inner += f"<h2>מאגרי המידע במעקב ({len(rows)})</h2>" + _link_list(
                [(f"/versions/{i}", t) for i, t in rows]
            )
            return PageMeta(
                title=_clean(name, 110),
                description=_clean(
                    f"{len(rows)} מאגרי המידע של {_clean(name, 90)} שנמצאים במעקב "
                    "גרסאות לעם, עם היסטוריית הגרסאות של כל אחד."
                ),
                canonical_path=f"/organizations/{org.id}",
                # An organization with nothing tracked is an empty page.
                noindex=not rows,
                body_html=_wrap(inner),
            )

        # /tags/<id>
        if len(parts) == 2 and parts[0] == "tags":
            tag = (
                await db.execute(select(Tag).where(Tag.id == uuid.UUID(parts[1])))
            ).scalar_one_or_none() if _UUID_RE.match(parts[1]) else None
            if tag is None:
                return _NOT_FOUND
            rows = (
                await db.execute(
                    select(TrackedDataset.id, TrackedDataset.title)
                    .join(dataset_tags, dataset_tags.c.dataset_id == TrackedDataset.id)
                    .where(dataset_tags.c.tag_id == tag.id, *public)
                    .order_by(TrackedDataset.title)
                    .limit(_MAX_LIST)
                )
            ).all()
            inner = f"<h1>{_e(tag.name)}</h1>"
            if tag.description:
                inner += f"<p>{_e(_clean(tag.description, 1000))}</p>"
            inner += f"<h2>מאגרי מידע בנושא ({len(rows)})</h2>" + _link_list(
                [(f"/versions/{i}", t) for i, t in rows]
            )
            return PageMeta(
                title=_clean(tag.name, 110),
                description=_clean(
                    f"מאגרי מידע ממשלתיים בנושא {_clean(tag.name, 90)} — במעקב גרסאות לעם."
                ),
                canonical_path=f"/tags/{tag.id}",
                # A one- or two-dataset tag duplicates the dataset page it
                # lists. Keep it out of the index, but let the links be followed.
                noindex=len(rows) < MIN_TAG_DATASETS,
                body_html=_wrap(inner),
            )

        # Hubs: the static pages that link into the catalogue. Their head is
        # in _STATIC; only the body is composed here.
        if path in ("/", "/organizations", "/tags"):
            base = _STATIC[path]
            count = func.count(TrackedDataset.id)
            if path == "/tags":
                rows = (
                    await db.execute(
                        select(Tag.id, Tag.name, count)
                        .join(dataset_tags, dataset_tags.c.tag_id == Tag.id)
                        .join(TrackedDataset, TrackedDataset.id == dataset_tags.c.dataset_id)
                        .where(*public)
                        .group_by(Tag.id, Tag.name)
                        .having(count >= MIN_TAG_DATASETS)
                        .order_by(Tag.name)
                    )
                ).all()
                inner = f"<h1>{_e(base.title)}</h1><p>{_e(base.description)}</p>" + _link_list(
                    [(f"/tags/{i}", f"{n} ({c})") for i, n, c in rows]
                )
            else:
                orgs = (
                    await db.execute(
                        select(Organization.id, Organization.title, count)
                        .join(TrackedDataset, TrackedDataset.organization_id == Organization.id)
                        .where(*public)
                        .group_by(Organization.id, Organization.title)
                        .order_by(count.desc())
                    )
                ).all()
                org_links = [(f"/organizations/{i}", f"{t} ({c})") for i, t, c in orgs]
                if path == "/organizations":
                    inner = f"<h1>{_e(base.title)}</h1><p>{_e(base.description)}</p>" + _link_list(org_links)
                else:
                    recent = (
                        await db.execute(
                            select(TrackedDataset.id, TrackedDataset.title)
                            .where(*public)
                            .order_by(desc(TrackedDataset.created_at))
                            .limit(60)
                        )
                    ).all()
                    inner = (
                        f"<h1>{_e(SITE_NAME)} — {_e(base.title)}</h1><p>{_e(base.description)}</p>"
                        "<h2>מאגרים שנוספו לאחרונה</h2>"
                        + _link_list([(f"/versions/{i}", t) for i, t in recent])
                        + "<h2>לפי גוף מפרסם</h2>" + _link_list(org_links)
                    )
            from dataclasses import replace
            return replace(base, body_html=_wrap(inner))

    # /sources/<slug> — the registry lives in the worker manifest, not the DB,
    # so the slug itself is the best title available here without a round trip.
    if len(parts) == 2 and parts[0] == "sources":
        slug = _clean(parts[1], 60)
        return PageMeta(
            title=f"מקור: {slug}",
            description=_clean(
                f"כל מאגרי המידע שגרסאות לעם אוסף מהמקור {slug}, ואיך הם נאספים."
            ),
            canonical_path=f"/sources/{slug}",
        )

    return None


def _source_url(ds) -> str | None:
    """Where this dataset lives at its publisher.

    ``sameAs`` is how Dataset markup says "this is the same thing as that", and
    leaving it out on a mirror invites the mirror being read as the origin.
    Scraped sources carry an explicit URL; a CKAN dataset does not store one
    because it is derivable from the slugs — the same construction the dataset
    card uses (HomePage.tsx).
    """
    if ds.source_url:
        return ds.source_url
    if ds.source_type in ("scraper", "govmap"):
        return None
    if ds.organization and ds.ckan_name:
        return f"https://data.gov.il/he/datasets/{ds.organization}/{ds.ckan_name}"
    return None


def _dataset_jsonld(ds, creator_name: str = "") -> dict:
    """schema.org/Dataset — the reason this whole module earns its keep.

    Google Dataset Search is a separate index that reads exactly this markup,
    and a catalogue of ~1,250 government datasets is precisely what it is for.
    Emitting it is the difference between competing for Hebrew keywords on the
    open web and appearing in the vertical built for this content.
    """
    node: dict = {
        "@context": "https://schema.org",
        "@type": "Dataset",
        "name": _clean(ds.title, 200),
        "description": _clean(
            f"{_clean(ds.title, 150)} — מאגר מידע ממשלתי במעקב גרסאות לעם, "
            "עם היסטוריית הגרסאות שנשמרו.",
            450,
        ),
        "url": f"{SITE_URL}/versions/{ds.id}",
        "isAccessibleForFree": True,
        "includedInDataCatalog": {
            "@type": "DataCatalog",
            "name": SITE_NAME,
            "url": SITE_URL,
        },
    }
    creator = creator_name or ds.organization or ""
    if creator:
        node["creator"] = {"@type": "Organization", "name": _clean(creator, 150)}
    source = _source_url(ds)
    if source:
        node["sameAs"] = source
    if getattr(ds, "last_polled_at", None):
        node["dateModified"] = ds.last_polled_at.date().isoformat()
    return node


def _website_jsonld() -> dict:
    return {
        "@context": "https://schema.org",
        "@type": "WebSite",
        "name": SITE_NAME,
        "url": SITE_URL,
        "inLanguage": "he",
        "potentialAction": {
            "@type": "SearchAction",
            "target": {
                "@type": "EntryPoint",
                "urlTemplate": f"{SITE_URL}/?q={{search_term_string}}",
            },
            "query-input": "required name=search_term_string",
        },
    }


async def meta_for(path: str) -> PageMeta:
    """Metadata (and, where there is any, the body) for one route. Never raises."""
    path = (path or "/").rstrip("/") or "/"

    if any(path.startswith(p) for p in _NOINDEX_PREFIXES):
        return PageMeta(title="ניהול", canonical_path=path, noindex=True, nofollow=True)

    if not is_spa_route(path):
        return _not_found(path)

    static = _STATIC.get(path)
    if static is not None and path not in _HUBS:
        return static

    now = time.monotonic()
    hit = _cache.get(path)
    if hit and now - hit[0] < _TTL_SECONDS:
        return hit[1]

    try:
        found = await _lookup_dynamic(path)
    except Exception:  # noqa: BLE001 — a lookup failure must not cost the page
        # Not cached and not a 404: the database being away says nothing about
        # whether the page exists, so serve the generic head and retry next time.
        logger.warning("SEO lookup failed for %s", path, exc_info=True)
        return static or PageMeta(title=SITE_NAME, canonical_path=path)

    if found is _NOT_FOUND:
        resolved = _not_found(path)
    else:
        resolved = found or static or PageMeta(title=SITE_NAME, canonical_path=path)
    async with _lock:
        # Bounded: a crawler walking every URL, or a scanner inventing them,
        # must not grow this without limit.
        if len(_cache) > 5000:
            _cache.clear()
        _cache[path] = (now, resolved)
    return resolved


#: Static routes whose body lists the catalogue, so they go through the lookup.
_HUBS = ("/", "/organizations", "/tags")


def _not_found(path: str) -> PageMeta:
    return PageMeta(
        title="הדף לא נמצא",
        description="הדף המבוקש לא קיים בגרסאות לעם.",
        canonical_path=path,
        noindex=True,
        status=404,
        body_html=_wrap("<h1>הדף לא נמצא</h1><p>הכתובת הזו לא קיימת באתר.</p>"),
    )


# ── rendering ───────────────────────────────────────────────────────────────
def _tag(name: str, content: str, attr: str = "name") -> str:
    return f'    <meta {attr}="{name}" content="{html.escape(content, quote=True)}">\n'


def head_html(meta: PageMeta) -> str:
    """The block that replaces the marker in index.html."""
    full_title = meta.title if meta.title == SITE_NAME else f"{meta.title} — {SITE_NAME}"
    canonical = f"{SITE_URL}{meta.canonical_path}"

    out = f"    <title>{html.escape(full_title)}</title>\n"
    out += _tag("description", meta.description)
    out += f'    <link rel="canonical" href="{html.escape(canonical, quote=True)}">\n'
    if meta.noindex:
        out += _tag("robots", "noindex, nofollow" if meta.nofollow else "noindex, follow")
    else:
        out += _tag("robots", "index, follow, max-image-preview:large, max-snippet:-1")

    out += _tag("og:site_name", SITE_NAME, attr="property")
    out += _tag("og:type", "website", attr="property")
    out += _tag("og:locale", "he_IL", attr="property")
    out += _tag("og:title", full_title, attr="property")
    out += _tag("og:description", meta.description, attr="property")
    out += _tag("og:url", canonical, attr="property")
    out += _tag("twitter:card", "summary")
    out += _tag("twitter:title", full_title)
    out += _tag("twitter:description", meta.description)

    for node in [_website_jsonld(), *meta.jsonld]:
        payload = json.dumps(node, ensure_ascii=False, separators=(",", ":"))
        # "</" cannot appear raw inside a script element.
        payload = payload.replace("</", "<\\/")
        out += f'    <script type="application/ld+json">{payload}</script>\n'
    return out


_TITLE_RE = re.compile(r"[ \t]*<title>.*?</title>\s*", re.S | re.I)


_ROOT = '<div id="root"></div>'


def render(template: str, meta: PageMeta) -> str:
    """Write the head block, and the body if there is one, into index.html.

    Prefers the explicit marker. Falls back to replacing the build's own
    <title>, so a dist/ built before the marker existed still gets the tags
    rather than silently getting none. The body goes inside the empty
    <div id="root">, which React's createRoot clears when it mounts.
    """
    block = head_html(meta)
    start = template.find(MARKER_START)
    end = template.find(MARKER_END)
    if start != -1 and end > start:
        out = template[:start] + block.strip() + template[end + len(MARKER_END):]
    elif _TITLE_RE.search(template):
        out = _TITLE_RE.sub(block, template, count=1)
    else:
        out = template
    if meta.body_html and _ROOT in out:
        out = out.replace(_ROOT, f'<div id="root">{meta.body_html}</div>', 1)
    return out
