"""What is on one cbs.gov.il publication page — read live, for the picker.

A CBS publication page is a page of text with a table of Excel files hanging
off it, and the tables it holds vary from a handful to 110. Tracking one means
choosing which of them to mirror, and that choice has to be made BEFORE the
dataset exists — so this runs here, in the request path, rather than in the
worker. It is read-only: it lists, it never downloads a file.

**This is a deliberate duplicate.** The scraper worker reads the same three
SharePoint calls in ``govscraper/scrapers/cbs_pub/_sharepoint.py``, and OVER
cannot import that repo. The two must agree about what is on a page — if they
drift, the picker offers files the scrape then ignores. The shared contract is
the row shape returned by ``list_page_files`` there and by ``preview`` here:
``path``, ``name``, ``title``, ``chapter``, ``subject``, ``order``, ``ext``,
``size``, ``modified``, ``on_page``, ``tabular``. Change one side and change
the other.

**Why not parse the page's HTML.** cbs.gov.il is SharePoint behind a WAF, and
the .aspx returns a 40 KB client-rendered shell with no file link in it. The
WAF allowlists exact REST shapes: the folder-scoped ``GetFolderByServerRelative
Url(…)/Files`` works, while ``lists/getbytitle('DocLib')/items?$filter=…`` and
``/_api/search/query`` are refused with an HTML "Oops, Something is wrong" body
under HTTP 200. A non-JSON response here means that happened; it is not a
transient error and retrying it will not help.
"""
import asyncio
import html as _html
import logging
import re
from urllib.parse import quote, unquote, urlparse

logger = logging.getLogger(__name__)

CBS = "https://www.cbs.gov.il"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

PAGE_RE = re.compile(
    r"^/(?:he|en|ar|ru)/.+?/Pages/\d{4}/[^/]+\.aspx$", re.I,
)
FILE_EXT_RE = re.compile(r"\.(xlsx?|csv|pdf|zip|docx?|pptx?|xml|json|txt)$", re.I)
ANCHOR_RE = re.compile(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', re.I | re.S)
TAG_RE = re.compile(r"<[^>]+>")
TABLE_EXTS = ("xlsx", "xls", "csv")
MAX_FOLDER_DEPTH = 1

# The person is waiting on this with a form open, so it is bounded hard. A
# publication folder is one call plus at most a handful of sub-folder calls.
REQUEST_TIMEOUT = 20.0
TOTAL_TIMEOUT = 45.0
MAX_FILES = 1000


class CbsPreviewError(RuntimeError):
    """The page could not be read. The message is shown to the user."""


def is_cbs_pub_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except (ValueError, AttributeError):
        return False
    if parsed.scheme not in ("http", "https"):
        return False
    if (parsed.hostname or "").lower() not in ("cbs.gov.il", "www.cbs.gov.il"):
        return False
    return bool(PAGE_RE.match(unquote(parsed.path or "")))


def _web_of(path: str) -> str:
    """Everything before ``/Pages/`` — sub-webs exist, and the wrong web 404s."""
    return path.split("/Pages/", 1)[0].strip("/")


def _text(value) -> str:
    return value.strip() if isinstance(value, str) else ""


def _ext_of(name: str) -> str:
    match = FILE_EXT_RE.search(name)
    return match.group(1).lower() if match else ""


async def _api(client, web: str, tail: str):
    """GET a SharePoint REST call → parsed JSON, or None when the WAF refused."""
    try:
        resp = await client.get(f"{CBS}/{web}/_api/{tail}", timeout=REQUEST_TIMEOUT)
    except Exception as exc:  # noqa: BLE001
        raise CbsPreviewError(f"cbs.gov.il did not answer ({exc.__class__.__name__})")
    if resp.status_code >= 400:
        raise CbsPreviewError(f"cbs.gov.il answered {resp.status_code}")
    if "json" not in (resp.headers.get("content-type") or "").lower():
        return None
    try:
        return resp.json()
    except ValueError:
        return None


def _file_row(entry: dict, folder: str) -> dict | None:
    item = entry.get("ListItemAllFields") or {}
    name = _text(entry.get("Name"))
    rel = _text(entry.get("ServerRelativeUrl"))
    ext = _ext_of(name)
    if not name or not rel or not ext or item.get("CbsHide") is True:
        return None
    chapter = _text(item.get("CbsPublishingDocChapter"))
    subject = _text(item.get("CbsPublishingDocSubject"))
    try:
        size = int(entry.get("Length") or 0)
    except (TypeError, ValueError):
        size = 0
    order = item.get("CbsOrderField")
    return {
        "path": rel,
        "name": name,
        "title": _text(item.get("Title")) or name,
        "chapter": chapter,
        "subject": subject,
        "order": float(order) if isinstance(order, (int, float)) else None,
        "ext": ext,
        "size": size,
        "modified": _text(entry.get("TimeLastModified")),
        "url": f"{CBS}{quote(rel)}",
        # A file with no heading is not on the page's own table — a publication
        # folder also accumulates files belonging to other pages. Shown, but
        # not ticked by default. See the worker's _sharepoint.py.
        "on_page": bool(chapter or subject),
        "tabular": ext in TABLE_EXTS,
    }


def _inline_links(item: dict) -> list[tuple[str, str]]:
    blob = f"{item.get('PublishingPageContent') or ''} {item.get('CbsPageSummary') or ''}"
    out = []
    for href, label in ANCHOR_RE.findall(blob):
        href = _html.unescape(href).split("?")[0]
        if href.startswith("http"):
            parts = urlparse(href)
            if (parts.hostname or "").lower() not in ("cbs.gov.il", "www.cbs.gov.il"):
                continue
            href = parts.path
        if not href.startswith("/"):
            continue
        out.append((" ".join(TAG_RE.sub("", label).split())[:200], unquote(href)))
    return out


async def _preview(url: str) -> dict:
    import httpx

    path = unquote(urlparse(url).path or "")
    web = _web_of(path)
    headers = {"User-Agent": UA, "Accept": "application/json;odata=nometadata",
               "Accept-Language": "he-IL,he;q=0.9,en;q=0.8"}
    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
        item = await _api(
            client, web,
            f"web/GetFileByServerRelativeUrl('{quote(path)}')/ListItemAllFields",
        )
        if not item or not item.get("Title"):
            raise CbsPreviewError(
                "לא נמצא עמוד פרסום בכתובת הזו באתר הלמ\"ס"
            )
        level1 = _text(str(item.get("CbsPublishingFolderLevel1") or ""))
        level2 = _text(str(item.get("CbsPublishingFolderLevel2") or ""))

        files: list[dict] = []
        seen: set[str] = set()
        if level1 and level2:
            folders = [f"/{web}/DocLib/{level1}/{level2}"]
            for depth in range(MAX_FOLDER_DEPTH + 1):
                next_level: list[str] = []
                for folder in folders:
                    data = await _api(
                        client, web,
                        f"web/GetFolderByServerRelativeUrl('{quote(folder)}')"
                        f"/Files?$expand=ListItemAllFields",
                    )
                    for entry in (data or {}).get("value", []) or []:
                        row = _file_row(entry, folder)
                        if row and row["path"] not in seen and len(files) < MAX_FILES:
                            seen.add(row["path"])
                            files.append(row)
                    if depth < MAX_FOLDER_DEPTH:
                        subs = await _api(
                            client, web,
                            f"web/GetFolderByServerRelativeUrl('{quote(folder)}')"
                            f"/Folders?$select=Name,ServerRelativeUrl",
                        )
                        next_level.extend(
                            f["ServerRelativeUrl"]
                            for f in (subs or {}).get("value", []) or []
                            if f.get("ServerRelativeUrl")
                            and (f.get("Name") or "").lower() != "forms"
                        )
                folders = next_level
                if not folders:
                    break

    for label, href in _inline_links(item):
        if href in seen:
            for row in files:
                if row["path"] == href:
                    row["on_page"] = True
            continue
        ext = _ext_of(href)
        if not ext or len(files) >= MAX_FILES:
            continue
        seen.add(href)
        files.append({
            "path": href, "name": href.rsplit("/", 1)[-1],
            "title": label or href.rsplit("/", 1)[-1], "chapter": "", "subject": "",
            "order": None, "ext": ext, "size": 0, "modified": "",
            "url": f"{CBS}{quote(href)}", "on_page": True,
            "tabular": ext in TABLE_EXTS,
        })

    files.sort(key=lambda f: (
        not f["on_page"], f["chapter"] or "￿", f["subject"] or "￿",
        f["order"] if f["order"] is not None else 1e9, f["title"],
    ))
    return {"title": _text(item.get("Title")), "url": url, "files": files}


async def preview(url: str) -> dict:
    """``{"title", "url", "files": [...]}`` for one publication page.

    Raises ``CbsPreviewError`` with a Hebrew message the form can show. The
    whole read is capped so a slow CBS cannot hold a request open — a picker
    that never arrives should say so rather than hang.
    """
    if not is_cbs_pub_url(url):
        raise CbsPreviewError("הכתובת אינה עמוד פרסום של הלמ\"ס")
    try:
        return await asyncio.wait_for(_preview(url), timeout=TOTAL_TIMEOUT)
    except asyncio.TimeoutError:
        raise CbsPreviewError("אתר הלמ\"ס לא השיב בזמן — אפשר לנסות שוב") from None
