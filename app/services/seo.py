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
    "/rationale": PageMeta(
        title="הרציונל",
        description="למה שמירת גרסאות של מאגרי מידע ממשלתיים היא תנאי לשקיפות — הנימוק המלא.",
        canonical_path="/rationale",
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


async def _lookup_dynamic(path: str) -> PageMeta | None:
    """Resolve a detail route against the database.

    Returns None for anything unrecognised, which the caller treats as "use the
    site default" rather than as an error.
    """
    from app.database import async_session
    from app.models.organization import Organization
    from app.models.tag import Tag
    from app.models.tracked_dataset import TrackedDataset

    parts = [p for p in path.split("/") if p]

    async with async_session() as db:
        # /versions/<uuid> and /archive/<uuid> — a tracked dataset
        if len(parts) == 2 and parts[0] in ("versions", "archive") and _UUID_RE.match(parts[1]):
            ds = (
                await db.execute(
                    select(TrackedDataset).where(TrackedDataset.id == uuid.UUID(parts[1]))
                )
            ).scalar_one_or_none()
            if ds is None or ds.status not in ("active", "pending"):
                return None
            org = ds.organization or ""
            what = "ארכיון מצטבר" if parts[0] == "archive" else "היסטוריית גרסאות"
            desc = (
                f"{what} של המאגר «{_clean(ds.title, 120)}»"
                + (f" מאת {_clean(org, 80)}" if org else "")
                + ". גרסאות לעם שומר כל שינוי במאגר כדי שאפשר יהיה להשוות בין גרסאות ולראות מה נמחק."
            )
            return PageMeta(
                title=_clean(ds.title, 110),
                description=_clean(desc),
                canonical_path=f"/{parts[0]}/{ds.id}",
                jsonld=[_dataset_jsonld(ds)],
            )

        # /organizations/<id>
        if len(parts) == 2 and parts[0] == "organizations":
            org = (
                await db.execute(
                    select(Organization).where(Organization.id == uuid.UUID(parts[1]))
                )
            ).scalar_one_or_none() if _UUID_RE.match(parts[1]) else None
            if org is None:
                return None
            return PageMeta(
                title=_clean(org.title or org.name, 110),
                description=_clean(
                    f"מאגרי המידע של {_clean(org.title or org.name, 90)} שנמצאים במעקב "
                    "גרסאות לעם, עם היסטוריית הגרסאות של כל אחד."
                ),
                canonical_path=f"/organizations/{org.id}",
            )

        # /tags/<id>
        if len(parts) == 2 and parts[0] == "tags":
            tag = (
                await db.execute(select(Tag).where(Tag.id == uuid.UUID(parts[1])))
            ).scalar_one_or_none() if _UUID_RE.match(parts[1]) else None
            if tag is None:
                return None
            return PageMeta(
                title=_clean(tag.name, 110),
                description=_clean(
                    f"מאגרי מידע ממשלתיים בנושא {_clean(tag.name, 90)} — במעקב גרסאות לעם."
                ),
                canonical_path=f"/tags/{tag.id}",
            )

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


def _dataset_jsonld(ds) -> dict:
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
    if ds.organization:
        node["creator"] = {"@type": "Organization", "name": _clean(ds.organization, 150)}
    if ds.source_url:
        node["sameAs"] = ds.source_url
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
    """Metadata for one SPA route. Never raises."""
    path = (path or "/").rstrip("/") or "/"

    if any(path.startswith(p) for p in _NOINDEX_PREFIXES):
        return PageMeta(title="ניהול", canonical_path=path, noindex=True)

    static = _STATIC.get(path)
    if static is not None:
        return static

    now = time.monotonic()
    hit = _cache.get(path)
    if hit and now - hit[0] < _TTL_SECONDS:
        return hit[1]

    try:
        found = await _lookup_dynamic(path)
    except Exception:  # noqa: BLE001 — a lookup failure must not cost the page
        logger.warning("SEO lookup failed for %s", path, exc_info=True)
        found = None

    resolved = found or PageMeta(title=SITE_NAME, canonical_path=path)
    async with _lock:
        # Bounded: a crawler walking every URL, or a scanner inventing them,
        # must not grow this without limit.
        if len(_cache) > 5000:
            _cache.clear()
        _cache[path] = (now, resolved)
    return resolved


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
        out += _tag("robots", "noindex, nofollow")
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


def render(template: str, meta: PageMeta) -> str:
    """Write the head block into index.html.

    Prefers the explicit marker. Falls back to replacing the build's own
    <title>, so a dist/ built before the marker existed still gets the tags
    rather than silently getting none.
    """
    block = head_html(meta)
    start = template.find(MARKER_START)
    end = template.find(MARKER_END)
    if start != -1 and end > start:
        return template[:start] + block.strip() + template[end + len(MARKER_END):]
    if _TITLE_RE.search(template):
        return _TITLE_RE.sub(block, template, count=1)
    return template
