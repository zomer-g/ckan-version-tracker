"""When GovMap's search service refuses us — told apart from "no such address".

Since 2026-09-18 the worker fleet's weekly health check has reported
``govmap.geocode: HTTP 403`` on the autocomplete endpoint. A 403 is GovMap
saying "not you", and so is a 429, a 5xx, or an HTML page where JSON should be
(a block page or a maintenance page answers with 200 + HTML). None of those
says anything about the address that was asked. The geocoding ledger
(``over_re_geocode``) already records a ``not_found`` as a miss that counts
towards retiring the address for good (``geocode_queue.MAX_ATTEMPTS``), so
letting a refusal look like an empty answer would retire addresses GovMap
never actually looked up — the same damage the 2026-08 "soft block" night did,
which ``geocode_queue.reset_misses`` exists to undo.

So every caller classifies a response here FIRST, and a refusal:

* raises :class:`GovMapRefused` (a distinct exception, not ``None``/empty),
* is remembered in :func:`last_refusal`, which the admin geocode state
  endpoint shows, and which gates the next geocoding batch
  (``geocode_queue.enqueue_next_batch``),
* is logged ONCE per episode, not once per address — a refusing service
  answers every address the same way, and 10,000 identical warning lines
  would bury the one that matters.

Deliberately NOT here: anything that tries to get past the refusal. A 403 is
an answer we respect; the reaction is to stop asking and say so, not to look
different and ask again.

State is in-process memory. A restart forgets it, and the cost of that is
bounded: at most one batch (or one request) is asked again, and it is refused
again and recorded again. A table would make the state survive restarts at the
price of a migration for one timestamp; not worth it.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

#: How long one refusal keeps us from asking again. A block is not lifted in
#: minutes, and a batch queued while it holds would only collect 10,000 more
#: refusals. Long enough to stop that, short enough that a lifted block is
#: noticed the same day.
BACKOFF = timedelta(hours=6)

#: How long a refusal episode lasts for logging purposes: a refusal within
#: this window of the previous one, with the same status, is the same episode
#: and logs nothing new.
_EPISODE = timedelta(hours=1)

_state: dict | None = None


class GovMapRefused(Exception):
    """GovMap refused to answer — the request said nothing about the address."""

    def __init__(self, status: str, source: str = "") -> None:
        super().__init__(f"GovMap refused ({status})")
        self.status = status
        self.source = source


def classify(status_code: int, content_type: str | None = None,
             body: bytes | str | None = None) -> str | None:
    """Why this response is a refusal, as a short label, or None if it is an answer.

    An answer is a 200 carrying JSON. Anything else from this endpoint is a
    refusal: 403/429 explicitly, 5xx because a broken service is not evidence
    of absence, and a 200 with HTML (or anything unparseable as JSON) because
    that is what a block or maintenance page looks like. Other 4xx (400 = we
    sent a malformed request) are our bug, not their refusal — still not a
    miss, so they are labelled too."""
    if status_code != 200:
        return f"HTTP {int(status_code)}"
    ct = (content_type or "").lower()
    if "json" not in ct:
        head = body[:64] if body is not None else b""
        if isinstance(head, bytes):
            head = head.decode("utf-8", "replace")
        if not head.lstrip().startswith(("{", "[")):
            return "non-JSON answer"
    return None


def classify_response(response) -> str | None:
    """:func:`classify` for an ``httpx.Response``."""
    return classify(response.status_code, response.headers.get("content-type"),
                    response.content)


#: A worker's failure reason or abort reason that names a refusal. The worker
#: reports failures as free text ("HTTP 500", "HTTP 403 Forbidden", "non-JSON
#: answer"), so this is the one place that text is read.
_REFUSAL_TEXT = re.compile(
    r"HTTP\s*(?P<code>403|429|5\d\d)\b|non-JSON|html|forbidden|too many requests",
    re.IGNORECASE)


def refusal_in_text(text) -> str | None:
    """The refusal a free-text reason names, or None."""
    if not text:
        return None
    m = _REFUSAL_TEXT.search(str(text))
    if not m:
        return None
    return f"HTTP {m.group('code')}" if m.group("code") else m.group(0)


def note(status: str, source: str, *, now: datetime | None = None) -> None:
    """Remember a refusal; log it only when a new episode begins."""
    global _state
    now = now or datetime.now(timezone.utc)
    prev = _state
    _state = {"at": now.isoformat(), "status": status, "source": source,
              "_at": now}
    if prev is None or prev["status"] != status or now - prev["_at"] > _EPISODE:
        logger.warning("govmap: search service refused us (%s, via %s) — not "
                       "recording misses; pausing geocoding for %s",
                       status, source, BACKOFF)


def last_refusal() -> dict | None:
    """``{"at", "status", "source"}`` of the latest refusal, or None."""
    if _state is None:
        return None
    return {k: v for k, v in _state.items() if not k.startswith("_")}


def active(*, now: datetime | None = None) -> bool:
    """Whether a refusal is recent enough that we should not ask yet."""
    if _state is None:
        return False
    now = now or datetime.now(timezone.utc)
    return now - _state["_at"] < BACKOFF


def clear() -> None:
    """Forget the refusal state (an admin forcing a batch, and tests)."""
    global _state
    _state = None
