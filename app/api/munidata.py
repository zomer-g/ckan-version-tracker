"""municipal-data.org URL validation endpoint — "דוח מצב שלטון מקומי".

Mirror of ``app/api/avodata.py`` / ``app/api/health.py``.
municipal-data.org is the Ministry of Interior's "מצב השלטון המקומי"
dashboard (an Azure static site). It has no per-item HTML pages — all
data lives in four content-hash-versioned per-screen JSON files, each
holding several metrics. Every metric carries a per-authority × per-year
table (exactly what the site's "הורדת כל הנתונים של המדד" button exports).

We track ONE OVER dataset per metric (38 metrics across 4 screens). The
trackable URL is::

    https://municipal-data.org/<slug>?metric=<metric_id>

where ``<slug>`` is the clean screen slug (``demographics`` | ``budget`` |
``governance`` | ``human-capital``) and ``<metric_id>`` is the metric id.
A bare screen page (no ``?metric=``) is rejected — each metric is its own
dataset.

This module recognises that URL shape and surfaces a Hebrew title for the
request form from a static catalog (``MUNIDATA_METRICS``), so ``/validate``
needs no live fetch (the screen JSON files are 1–6 MB). The GOVSCRAPER
worker re-derives everything from the URL and fetches the live JSON.
"""

import logging
from urllib.parse import parse_qs, quote, unquote, urlparse

from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.rate_limit import limiter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/munidata", tags=["munidata"])


MUNIDATA_HOSTS = {"municipal-data.org", "www.municipal-data.org"}

# Clean-URL slug ↔ screen id (mirrors the site's data-loader.js ID_TO_SLUG,
# and the GOVSCRAPER engine's SLUG_TO_ID). Both forms — and ``?screen=`` —
# are accepted in the trackable URL.
SLUG_TO_ID = {
    "demographics": "demographics",
    "budget": "budget_economy",
    "governance": "gov_mechanisms",
    "human-capital": "human_capital",
}
ID_TO_SLUG = {v: k for k, v in SLUG_TO_ID.items()}

# Hebrew label per screen (the top category), for building dataset titles.
SCREEN_LABELS = {
    "demographics": "דמוגרפיה",
    "budget_economy": "ביקורת וכלכלה",
    "gov_mechanisms": "מנגנונים ממשלתיים",
    "human_capital": "הון אנושי",
}

# Static catalog of the 38 metrics (screen_id, metric_id, Hebrew title,
# topic/subcategory, upstream data source). Generated from the live screen
# JSON at build time. Used only for instant /validate titles + to confirm a
# pasted metric id is real; the scraper always reads the live JSON, so a
# stale catalog degrades gracefully (the engine still resolves by id).
MUNIDATA_METRICS: list[dict] = [
    {"screen_id": "demographics", "metric_id": "population", "title": "מספר תושבים", "topic": "אוכלוסייה", "data_source": "הלמ\"ס"},
    {"screen_id": "demographics", "metric_id": "ages", "title": "שיעור תושבים לפי קבוצות גיל", "topic": "גילאים", "data_source": "הלמ\"ס"},
    {"screen_id": "demographics", "metric_id": "natural", "title": "ריבוי טבעי", "topic": "ריבוי טבעי", "data_source": "הלמ\"ס"},
    {"screen_id": "demographics", "metric_id": "migration", "title": "הגירה פנימית נטו", "topic": "הגירה פנימית", "data_source": "הלמ\"ס"},
    {"screen_id": "demographics", "metric_id": "wage", "title": "שכר ממוצע", "topic": "מצב כלכלי", "data_source": "הלמ\"ס"},
    {"screen_id": "demographics", "metric_id": "התחלות_בנייה_למגורים_2024", "title": "התחלות בנייה למגורים", "topic": "בנייה ודיור", "data_source": "הלמ\"ס"},
    {"screen_id": "budget_economy", "metric_id": "self_income_share", "title": "שיעור הכנסות עצמיות מסך הכנסות", "topic": "תמהיל הכנסות", "data_source": "משרד הפנים"},
    {"screen_id": "budget_economy", "metric_id": "arnona_other_share", "title": "שיעור ארנונה אחרת מסך הכנסות", "topic": "תמהיל ארנונה", "data_source": "משרד הפנים"},
    {"screen_id": "budget_economy", "metric_id": "arnona_collection_rate", "title": "שיעור גבייה ארנונה נטו", "topic": "גביית ארנונה", "data_source": "משרד הפנים"},
    {"screen_id": "budget_economy", "metric_id": "arnona_residential", "title": "סה\"כ חיוב, שטחים, וחיוב משוקלל למ\"ר", "topic": "ארנונה למגורים", "data_source": "משרד הפנים"},
    {"screen_id": "budget_economy", "metric_id": "current_deficit_rate", "title": "שיעור גרעון שוטף מסך הכנסות", "topic": "איזון שוטף", "data_source": "משרד הפנים"},
    {"screen_id": "budget_economy", "metric_id": "debt_per_household", "title": "עומס חוב ממוצע למשק בית", "topic": "עומס חוב", "data_source": "משרד הפנים"},
    {"screen_id": "budget_economy", "metric_id": "debt_repayment_rate", "title": "שיעור פרעון מלוות ביחס לסך הכנסות", "topic": "פרעון מלוות שנתי", "data_source": "משרד הפנים"},
    {"screen_id": "budget_economy", "metric_id": "edu_subsidy_share", "title": "שיעור סבסוד חינוך", "topic": "סבסוד חינוך", "data_source": "משרד הפנים"},
    {"screen_id": "budget_economy", "metric_id": "welfare_subsidy_rate", "title": "שיעור סבסוד רווחה", "topic": "סבסוד רווחה", "data_source": "משרד הפנים"},
    {"screen_id": "budget_economy", "metric_id": "municipal_corporations", "title": "שיעור תאגידים בסיכון, שיעור תאגידים בגרעון, מספר תאגידים", "topic": "תאגידים עירוניים", "data_source": "משרד הפנים"},
    {"screen_id": "budget_economy", "metric_id": "audit_deficiencies_count", "title": "סה\"כ מספר ליקויים", "topic": "ביקורת ברשויות המקומיות", "data_source": "משרד הפנים"},
    {"screen_id": "budget_economy", "metric_id": "deficit_authorities_share", "title": "שיעור רשויות בגרעון שוטף", "topic": "ביקורת ברשויות המקומיות", "data_source": "משרד הפנים"},
    {"screen_id": "budget_economy", "metric_id": "net_accum_deficit", "title": "גרעון מצטבר נטו (גרעון מצטבר בניכוי עודף מצטבר)", "topic": "ביקורת ברשויות המקומיות", "data_source": "משרד הפנים"},
    {"screen_id": "budget_economy", "metric_id": "loan_burden_ratio", "title": "שיעור עומס מלוות מההכנסות", "topic": "ביקורת ברשויות המקומיות", "data_source": "משרד הפנים"},
    {"screen_id": "budget_economy", "metric_id": "debt_concentration", "title": "סה\"כ ריכוזיות החוב (סה\"כ עומס מלוות וגרעון מצטבר בניכוי עודף מצטבר)", "topic": "ביקורת ברשויות המקומיות", "data_source": "משרד הפנים"},
    {"screen_id": "budget_economy", "metric_id": "total_income", "title": "סה\"כ הכנסות", "topic": "ביקורת ברשויות המקומיות", "data_source": "משרד הפנים"},
    {"screen_id": "budget_economy", "metric_id": "unbudgeted_funds", "title": "סה\"כ קרנות בלתי מתוקצבות", "topic": "ביקורת ברשויות המקומיות", "data_source": "משרד הפנים"},
    {"screen_id": "budget_economy", "metric_id": "dev_funds_balance", "title": "סה\"כ יתרת קרנות הפיתוח", "topic": "ביקורת ברשויות המקומיות", "data_source": "משרד הפנים"},
    {"screen_id": "budget_economy", "metric_id": "extraordinary_budget_income", "title": "סה\"כ הכנסות בתקציב הבלתי רגיל", "topic": "ביקורת ברשויות המקומיות", "data_source": "משרד הפנים"},
    {"screen_id": "budget_economy", "metric_id": "extraordinary_budget_expenses", "title": "סה\"כ הוצאות בתקציב הבלתי רגיל", "topic": "ביקורת ברשויות המקומיות", "data_source": "משרד הפנים"},
    {"screen_id": "budget_economy", "metric_id": "funds_dev_projects", "title": "יחסי גומלים בין סה\"כ תקבולים בקרנות, סה\"כ השקעה בפיתוח וסה\"כ פרויקטים הסתיימו", "topic": "ביקורת ברשויות המקומיות", "data_source": "משרד הפנים"},
    {"screen_id": "gov_mechanisms", "metric_id": "areas_budget", "title": "תקציב שהוקצה לרשויות ומספר פעילויות", "topic": "שירותים אזוריים", "data_source": "משרד הפנים"},
    {"screen_id": "gov_mechanisms", "metric_id": "kolot_korim", "title": "תמיכות ממשלתיות בקולות קוראים - סכום מאושר ותשלום בפועל", "topic": "קולות קוראים", "data_source": "אתר התמיכות הממשלתי, החשב הכללי"},
    {"screen_id": "gov_mechanisms", "metric_id": "maanak_pituah", "title": "סה\"כ תקציב ושיעור תקציב ששולם", "topic": "מענקי משרד הפנים - מענקי פיתוח", "data_source": "משרד הפנים"},
    {"screen_id": "gov_mechanisms", "metric_id": "maanak_izun", "title": "סה\"כ מענק ושיעור מענק שמומש", "topic": "מענקי משרד הפנים - מענקי איזון", "data_source": "משרד הפנים"},
    {"screen_id": "gov_mechanisms", "metric_id": "keren_pearim", "title": "סה\"כ הקצאה והקצאת עליה", "topic": "מענקי משרד הפנים - קרן לצמצום פערים", "data_source": "משרד הפנים"},
    {"screen_id": "human_capital", "metric_id": "888", "title": "שיעור נבחרי ציבור וצוערים לפי מגדר", "topic": "ייצוג מגדרי", "data_source": "משרד הפנים"},
    {"screen_id": "human_capital", "metric_id": "cadet_count", "title": "מספר צוערים", "topic": "צוערים לשלטון מקומי", "data_source": "היחידה להכשרת צוערים"},
    {"screen_id": "human_capital", "metric_id": "manager_seniority", "title": "וותק ממוצע של מנהלים בכירים", "topic": "רציפות ניהולית", "data_source": "משרד הפנים"},
    {"screen_id": "human_capital", "metric_id": "hr_manager_gap", "title": "פער ממוצע ברמת המשרה — מנהל הון אנושי מול תפקידי מפתח", "topic": "מעמד ההון האנושי", "data_source": "משרד הפנים"},
    {"screen_id": "human_capital", "metric_id": "statutory_roles", "title": "שיעור איוש תפקידים סטטוטוריים", "topic": "תפקוד רשותי רציף", "data_source": "משרד הפנים"},
    {"screen_id": "human_capital", "metric_id": "org_dev_plans", "title": "סכום התקציב + שיעור מימוש תקציב ממוצע", "topic": "פיתוח ארגוני", "data_source": "משרד הפנים"},
]

# Fast lookup: (screen_id, metric_id) -> catalog entry.
_CATALOG_BY_KEY = {(m["screen_id"], m["metric_id"]): m for m in MUNIDATA_METRICS}


class ValidateRequest(BaseModel):
    url: str


class ValidateResponse(BaseModel):
    valid: bool
    page_type: str | None = None
    collector_name: str | None = None
    title: str | None = None
    url: str | None = None
    error: str | None = None


def _resolve_screen_id(seg: str, qs: dict) -> str | None:
    screen_q = (qs.get("screen") or [""])[0].strip()
    if screen_q:
        if screen_q in ID_TO_SLUG:
            return screen_q
        if screen_q in SLUG_TO_ID:
            return SLUG_TO_ID[screen_q]
    seg = (seg or "").replace(".html", "")
    if seg in SLUG_TO_ID:
        return SLUG_TO_ID[seg]
    if seg in ID_TO_SLUG:
        return seg
    return None


def _target_of(url: str) -> tuple[str | None, str | None]:
    """(screen_id, metric_id) from a municipal-data.org per-metric URL, or
    ``(None, None)`` for a wrong host / unknown screen / missing metric."""
    s = (url or "").strip()
    parsed = urlparse(s)
    if (parsed.hostname or "").lower() not in MUNIDATA_HOSTS:
        return None, None
    qs = parse_qs(parsed.query or "")
    metric_id = unquote((qs.get("metric") or [""])[0].strip())
    if not metric_id:
        return None, None
    last_seg = (parsed.path or "").rstrip("/").split("/")[-1]
    screen_id = _resolve_screen_id(last_seg, qs)
    if not screen_id:
        return None, None
    return screen_id, metric_id


def _collector_name(screen_id: str, metric_id: str) -> str:
    """ASCII-safe, stable slug fragment for this metric (the URL-hash in
    ``scraper_url_slug`` guarantees final uniqueness, but a readable base
    keeps the ckan_id legible). Non-ASCII / numeric ids fall back to a
    sanitised fragment."""
    ascii_frag = "".join(
        c if (c.isascii() and (c.isalnum() or c == "_")) else "-" for c in metric_id
    ).strip("-")
    frag = ascii_frag or "metric"
    return f"munidata-{screen_id}-{frag}"


def _parse_munidata_url(url: str) -> tuple[str | None, str | None]:
    """Parse a municipal-data.org per-metric URL.

    Returns ``(page_type, collector_name)`` where
    ``page_type = "munidata_metric:{screen_id}:{metric_id}"`` (matches
    ``startswith("munidata_")`` for the dispatch switch in ``datasets.py``),
    or ``(None, None)`` for anything else.
    """
    screen_id, metric_id = _target_of(url)
    if not (screen_id and metric_id):
        return None, None
    page_type = f"munidata_metric:{screen_id}:{metric_id}"
    return page_type, _collector_name(screen_id, metric_id)


def target_of_page_type(page_type: str) -> tuple[str | None, str | None]:
    """(screen_id, metric_id) from a munidata page_type, for datasets.py."""
    if not page_type or not page_type.startswith("munidata_metric:"):
        return None, None
    rest = page_type.split(":", 1)[1]
    screen_id, _, metric_id = rest.partition(":")
    return (screen_id or None), (metric_id or None)


def title_for(screen_id: str, metric_id: str) -> str:
    """Human dataset title: מצב שלטון מקומי — <screen> — <metric title>."""
    entry = _CATALOG_BY_KEY.get((screen_id, metric_id))
    screen_label = SCREEN_LABELS.get(screen_id, screen_id)
    metric_title = entry["title"] if entry else metric_id
    return f"מצב שלטון מקומי — {screen_label} — {metric_title}"


def canonical_url(screen_id: str, metric_id: str) -> str:
    slug = ID_TO_SLUG.get(screen_id, screen_id)
    return f"https://municipal-data.org/{slug}?metric={quote(metric_id, safe='')}"


# (max_depth, max_docs). No recursion — one screen JSON per metric. Each
# metric is a long-format fact table (authority × year × internal dims); the
# largest today is kolot_korim (~59k rows). Cap well above that so nothing
# truncates (rows are only a few columns, so 200k is cheap).
MUNIDATA_DEFAULT_LIMITS: tuple[int, int] = (1, 200000)


def get_munidata_limits(page_type: str) -> tuple[int, int]:
    return MUNIDATA_DEFAULT_LIMITS


@router.post("/validate", response_model=ValidateResponse)
@limiter.limit("10/minute")
async def validate_munidata_url(request: Request, body: ValidateRequest):
    """Validate a municipal-data.org per-metric URL. No live fetch — the
    metric is recognised by URL shape + the static catalog."""
    url = body.url.strip()
    page_type, collector_name = _parse_munidata_url(url)
    if not page_type or not collector_name:
        return ValidateResponse(
            valid=False,
            error=(
                "URL is not a supported municipal-data.org metric page. "
                "Expected a per-metric URL like "
                "https://municipal-data.org/demographics?metric=population "
                "(slugs: demographics | budget | governance | human-capital; "
                "each metric is tracked as its own dataset). A bare screen "
                "page without ?metric= can't be tracked on its own."
            ),
        )
    screen_id, metric_id = target_of_page_type(page_type)
    return ValidateResponse(
        valid=True,
        page_type=page_type,
        collector_name=collector_name,
        title=title_for(screen_id, metric_id),
        url=url,
    )
