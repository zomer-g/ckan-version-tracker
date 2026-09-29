"""Notes on the deal register — OVER's reading, beside the rows, never in them.

The register is served as published (``processed: false``), and some of what it
publishes misleads on its own: a settlement split across a pre-merger name and
its current one, a name that is a regional council rather than a place, a deal
type whose name says "residential" while 99% of its rows carry no room count.
Correcting the rows would break the promise that they are the publisher's.
Saying nothing leaves a user to draw the wrong conclusion. This module is the
third option: a note attached to the value the user filtered on, derived from
the data where it can be measured and from a curated list where it cannot.

Two sources, both cached upstream:

* ``deals_query.settlements()`` — every published name with OVER's resolution
  of it (``resolved_code`` / ``resolved_name`` via the settlement index, and
  ``authority`` for regional councils). Two names that resolve to one code are
  one settlement published twice; that is measured, not listed.
* ``deals_query.natures()`` — every deal type with the share of its rows that
  carry no area and no room count.

The curated part is small on purpose: NATURE_NOTES says what a measured gap
MEANS where the numbers alone do not, and the historical settlement names live
in data/settlement_aliases_manual.json (``historical: true``), where they also
teach ``over_settlement_code()`` to resolve them for every other consumer.
"""
from __future__ import annotations

import json
import logging
import os
import re

from app.services import deals_query

logger = logging.getLogger(__name__)

_MANUAL = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                       "data", "settlement_aliases_manual.json")

# What a measured gap means, where the numbers do not say it themselves.
NATURE_NOTES = {
    "מגורים": ("למרות השם, רוב העסקאות מהסוג הזה אינן דירות: כמעט לאף אחת אין מספר "
               "חדרים ולרובן אין שטח, והן נראות כעסקאות בקרקע או בזכויות. לדירות "
               "סננו \"דירה בבית קומות\" (ולצדה \"דירת גן\", \"דירת גג\" וסוגי "
               "הקוטג'ים)."),
}

# A deal type for which a room count is expected, so its absence is news. For a
# shop or a plot, "no rooms" is the normal state and saying so would be noise.
_DWELLING = re.compile(r"מגורים|דיר|קוטג|בית בודד|דיור")
# Below this, a gap is ordinary missing data rather than a property of the type.
_GAP_PCT = 25


def _historical() -> dict[str, str]:
    """Published name → the settlement it is part of today (curated)."""
    try:
        with open(_MANUAL, encoding="utf-8") as fh:
            return {m["variant"]: m["name"] for m in json.load(fh) if m.get("historical")}
    except (OSError, ValueError, KeyError):
        logger.warning("deals notes: historical names unavailable", exc_info=True)
        return {}


def _n(v: int) -> str:
    return f"{int(v):,}"


def settlement_notes(name: str, listing: list[dict]) -> list[str]:
    """Notes for a settlement filter, from the published-name listing."""
    entry = next((s for s in listing if s.get("settlement") == name), None)
    if not entry:
        return []
    notes: list[str] = []
    code = entry.get("resolved_code")
    resolved = entry.get("resolved_name")
    hist = _historical()

    if name in hist:
        notes.append(f"\"{name}\" אינו יישוב נפרד היום: הוא חלק מ\"{hist[name]}\". "
                     f"כאן מוצגות רק העסקאות שפורסמו תחת השם \"{name}\".")
    if code is None and entry.get("authority"):
        notes.append(f"\"{name}\" הוא שם של מועצה אזורית ({entry['authority']}) ולא של "
                     "יישוב: המקור לא מציין באיזה יישוב בתוכה נעשתה העסקה, ולכן אי "
                     "אפשר לשייך את העסקאות האלה ליישוב מסוים.")
    elif code is None:
        notes.append(f"השם \"{name}\" לא מזוהה באינדקס היישובים של הלמ\"ס, ולכן "
                     "העסקאות שתחתיו לא ישויכו ליישוב בהצלבה עם מאגרים אחרים.")

    # One settlement, published under several names: measured, not listed.
    if code is not None:
        siblings = [s for s in listing
                    if s.get("resolved_code") == code and s.get("settlement") != name]
        if siblings:
            others = ", ".join(f"\"{s['settlement']}\" ({_n(s['deals'])})"
                               for s in sorted(siblings, key=lambda s: -s["deals"])[:5])
            notes.append(f"אותו יישוב ({resolved}) מופיע במאגר גם בשם אחר: {others} "
                         "עסקאות. הן אינן כלולות בתוצאה הזו; בחרו את השם השני כדי "
                         "לראות אותן.")

    if not entry.get("settlement_code"):
        tail = (f" לפי אינדקס היישובים של האתר זה {resolved} (סמל {code})." if code
                else "")
        notes.append("לעסקאות שפורסמו בשם הזה אין קוד יישוב במקור, ולכן מי שמחבר את "
                     "המאגר לנתונים אחרים לפי קוד יישוב לא ימצא אותן." + tail)
    return notes


def nature_note(entry: dict) -> str | None:
    """The note for one deal type, or None when there is nothing to say."""
    nature = entry.get("nature") or ""
    parts: list[str] = []
    area, rooms = entry.get("pct_no_area"), entry.get("pct_no_rooms")
    gaps = []
    if area is not None and area >= _GAP_PCT:
        gaps.append(f"ב-{area}% אין שטח")
    if _DWELLING.search(nature) and rooms is not None and rooms >= _GAP_PCT:
        gaps.append(f"ב-{rooms}% אין מספר חדרים")
    if gaps:
        parts.append(f"מתוך העסקאות מסוג \"{nature}\": " + " ו".join(gaps) + ".")
    if nature in NATURE_NOTES:
        parts.append(NATURE_NOTES[nature])
    return " ".join(parts) or None


def annotate_natures(rows: list[dict]) -> list[dict]:
    return [r | {"note": nature_note(r)} for r in rows]


async def for_filters(f: dict) -> list[str]:
    """Every note that applies to this filter. Best-effort: a note that cannot
    be computed is left out rather than failing the query it annotates."""
    notes: list[str] = []
    try:
        if f.get("settlement"):
            notes += settlement_notes(f["settlement"], await deals_query.settlements())
        if f.get("nature"):
            entry = next((n for n in await deals_query.natures()
                          if n.get("nature") == f["nature"]), None)
            note = nature_note(entry) if entry else None
            if note:
                notes.append(note)
    except Exception:  # noqa: BLE001
        logger.warning("deals notes failed for %s", f, exc_info=True)
    return notes
