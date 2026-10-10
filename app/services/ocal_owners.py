"""Diary owners for יומן לעם (Ocal) — who a diary belongs to, and that person's
Knesset "קשר עם הציבור" (contact-with-the-public) expenses.

A diary source's title names its owner(s) in free text ("יומן השר לביטחון
לאומי, איתמר בן גביר ומנכ"ל המשרד, ..."). ``diary_sources.person_id`` holds at
most one person and is missing or stale on many sources, so this module derives
a separate many-to-many layer:

    diary_source_owners (source_id, owner_key, owner_label, person_id, kind, role, method)

``owner_key`` groups every spelling of one owner (``מאי גולן`` and the Knesset
form ``בדרה גולן פלורה מאי`` share a key), and is what the public owner picker
filters on. ``kind`` is ``person`` when a name was found, else ``subject`` — the
diary's office ("ראש עיריית כפר סבא") so that a diary without a named owner can
still be grouped and selected.

The extraction is a pure function (``extract_owners``) over the title and the
known people names, so it is unit-testable; ``rebuild_owners`` applies it to the
whole corpus. It never touches ``diary_sources.person_id`` (admin-curated).

Name matching is shared with the MK-expenses importer (ocal_mk_expenses.py):
``name_key`` / ``names_match`` compare names order-insensitively, so the
Knesset's "family-first, all given names" spelling still links to the short
name the diaries use.
"""
from __future__ import annotations

import logging
import re
import unicodedata
import uuid
from collections import Counter

from app.services import ocal_db

logger = logging.getLogger(__name__)

# ── name normalisation ───────────────────────────────────────────────────────

_NIKUD_RE = re.compile("[֑-ׇ]")
_INVIS_RE = re.compile("[​-‏﻿­‪-‮⁠]")
_QUOTES_RE = re.compile("[\"'`׳״‘’“”]")
_PAREN_RE = re.compile(r"\([^)]*\)")
_HONORIFIC_RE = re.compile(
    r"^(?:פרופ(?:סור)?|ד\"?ר|דוקטור|עו\"?ד|רו\"?ח|הרב|הגב|גב|מר|ח\"?כ|תא\"?ל|אלוף|השר|השרה|ניצב)\.?\s+")
# Final letters fold to their medial form so "כץ"/"כצ" and "סמוטריץ'"/"סמוטריץ"
# compare equal after the geresh is dropped.
_FINALS = str.maketrans({"ך": "כ", "ם": "מ", "ן": "נ", "ף": "פ", "ץ": "צ"})


def normalize_name(name: str | None) -> str:
    """Display form: NFC, no nikud/invisibles/parentheticals, single spaces."""
    s = unicodedata.normalize("NFC", name or "")
    s = _INVIS_RE.sub("", _NIKUD_RE.sub("", s))
    s = _PAREN_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip(" ,.-–—")
    while True:
        t = _HONORIFIC_RE.sub("", s)
        if t == s:
            break
        s = t
    return s


def name_tokens(name: str | None) -> list[str]:
    """Comparable tokens: quotes dropped, hyphens split, final letters folded,
    and the Arabic article joined to its word ("אל הואשלה" = "אלהואשלה")."""
    s = _QUOTES_RE.sub("", normalize_name(name))
    s = re.sub(r"[-־,;.]", " ", s)
    s = re.sub(r"(?:^|(?<=\s))אל\s+(?=\S)", "אל", s)
    return [t.translate(_FINALS) for t in s.split() if t]


def name_key(name: str | None) -> str:
    """Order-insensitive key: "גולן מאי" and "מאי גולן" share one."""
    return " ".join(sorted(name_tokens(name)))


# Nicknames and the given names they stand for, as the Knesset's expense files
# and the diary titles write the same person ("אבי דיכטר" / "דיכטר אברהם משה",
# "טלי גוטליב" / "גוטליב רויטל"). Each group is one name.
_NICKNAMES = [
    ("אבי", "אברהם", "אביגדור"), ("אלי", "אליהו", "אליעזר"), ("מיקי", "מכלוף", "מיכאל"),
    ("מירי", "מרים"), ("דבי", "דבורה"), ("טלי", "רויטל"), ("טניה", "טטיאנה"),
    ("קטי", "קטרין"), ("דודי", "דוד"), ("צביקה", "צבי"), ("בני", "בנימין"),
    ("שלומי", "שלמה"), ("קובי", "יעקב"), ("מוטי", "מרדכי"), ("רפי", "רפאל"),
    ("גבי", "גבריאל"), ("איציק", "יצחק"), ("יוסי", "יוסף"), ("משה", "מושיק"),
    ("גדי", "גד"), ("חילי", "יחיאל"),
    # spelling variants the vowel-letter rule below does not cover
    ("עמאר", "עמר"),
]
_NICK_GROUPS: dict[str, set[str]] = {}
for _grp in _NICKNAMES:
    _folded = {g.translate(_FINALS) for g in _grp}
    for _g in _folded:
        _NICK_GROUPS.setdefault(_g, set()).update(_folded)


def _skeleton(t: str) -> str:
    """The token without inner ו/י and doubled letters — "שיטרית"/"שטרית",
    "סולומון"/"סלומון", "בועז"/"בעז" are spelling variants of one name."""
    if len(t) < 3:
        return t
    body = re.sub("[וי]", "", t[1:-1])
    return re.sub(r"(.)\1+", r"\1", t[0] + body + t[-1])


def tokens_equivalent(a: str, b: str) -> bool:
    if a == b:
        return True
    if b in _NICK_GROUPS.get(a, ()):
        return True
    return max(len(a), len(b)) >= 4 and _skeleton(a) == _skeleton(b)


def names_match(a: str | None, b: str | None) -> bool:
    """Same person under a different spelling? Every token of the shorter name
    (≥2 tokens) has its own equivalent token in the longer one — the Knesset
    lists every given name ("בדרה גולן פלורה מאי") where a diary says "מאי
    גולן", and tokens may differ by nickname or spelling (see tokens_equivalent)."""
    ta, tb = list(dict.fromkeys(name_tokens(a))), list(dict.fromkeys(name_tokens(b)))
    if not ta or not tb:
        return False
    small, big = (ta, tb) if len(ta) <= len(tb) else (tb, ta)
    if len(small) < 2 and len(big) > 1:
        return False
    used: set[int] = set()
    for t in small:
        # Exact matches first, so a nickname never takes a token another word needs.
        j = next((i for i, u in enumerate(big) if i not in used and u == t), None)
        if j is None:
            j = next((i for i, u in enumerate(big) if i not in used and tokens_equivalent(t, u)), None)
        if j is None:
            return False
        used.add(j)
    return True


# ── title parsing ────────────────────────────────────────────────────────────

# Words that mark an office/role or a period, never part of a person's name.
_ROLE_WORDS = {
    "יומן", "יומני", "יומנו", "יומנה", "לוז", "לו\"ז", "שר", "שרת", "השר", "השרה", "שרי", "שרה",
    "סגן", "סגנית", "וסגן", "וסגנית", "מנכ\"ל", "מנכ\"לית", "מנכל", "מנכלית", "המנכ\"ל", "המנכ\"לית",
    "ומנכ\"ל", "ומנכ\"לית", "ומנכל", "מ\"מ", "ממלא", "ממלאת", "ראש", "יו\"ר", "יושב", "יושבת",
    "משרד", "המשרד", "במשרד", "למשרד", "לשנת", "לשנים", "לחודשים", "לחודש", "רבעון", "חציון",
    "מחצית", "שנת", "נציב", "נציבת", "הממונה", "החשב", "הכלכלן", "היועץ", "היועצת", "המשפטי",
    "המשפטית", "נשיא", "נשיאת", "מבקר", "מבקרת", "שגריר", "שגרירת", "עיריית", "מועצה", "המועצה",
    "רשות", "הרשות", "מנהל", "מנהלת", "המנהל", "פרקליט", "פרקליטת", "ועדת", "הוועדה", "עמותת",
    "הצלחה", "עת\"מ", "בעקבות", "שהתקבל", "כפי", "התקבל", "עותק", "של", "עבור", "למסירה",
    "סופי", "חופש", "המידע", "מידע", "מכניסתה", "מכניסתו", "לתפקיד", "ועד", "עד", "מיום",
    "ראשון", "שני", "שלישי", "רביעי", "ואחרון", "ראשונה", "שנייה", "שניה", "חודש", "ינואר",
    "פברואר", "מרץ", "אפריל", "מאי_", "יוני", "יולי", "אוגוסט", "ספטמבר", "אוקטובר", "נובמבר",
    "דצמבר", "החברה", "דירקטוריון", "בית", "המשפט", "העליון", "הכנסת", "הממשלה", "ממשלת",
    "ישראל", "המדינה", "לביטחון", "לאומי", "הפנים", "האוצר", "החינוך", "הבריאות", "הביטחון",
    "וטכנולוגיה", "פגישות", "נציגי", "מענה", "לבקשות", "שונות", "תשובה", "משלימה", "יועץ",
    "בכיר", "לשר", "הרשות", "המקומית", "האיזורית", "האזורית", "ו/או", "כלכלה", "לכלכלה",
    "הלאומית", "מנהלת", "החטופים", "והנעדרים", "ראשי", "יועמ\"ש", "התחרות", "התקציבים",
    "קובץ", "מתוקן", "מתוקנת", "חלקי", "אקסל", "ירושלים", "מורשת", "כבוד", "השופט", "השופטת",
    "בדימוס", "בפועל", "מקום", "ניגוד", "עניינים", "והסדר", "הסדר", "שנים", "רבעונים",
}
# Hebrew words opening with the definite article are topics ("המדע", "והספורט"),
# not names — except these surnames/given names.
_HE_NAMES = {"הילה", "הדס", "הלל", "הראל", "הלוי", "הנדל", "הרצוג", "הייזלר", "הדר", "הודיה",
             "הגר", "הנגבי", "הורוביץ", "הכהן", "הלפרין", "הרשקוביץ", "הלר", "הבר", "הס",
             "הרטשטיין", "הורן", "הדסה", "הוד", "הלנה", "הרא\"ש", "הרמן", "היימן", "הנדלר"}
# A role word may carry a one-letter prefix (ו/ה/ב/ל/מ/ש/כ) — "ומנכ"ל", "לשר".
_PREFIXES = "והבלמשכ"
_NAME_TOKEN_RE = re.compile(r"^[א-ת][א-ת\"'׳״\-]*$")
_DIGIT_RE = re.compile(r"\d")


def _is_role_word(w: str) -> bool:
    w = w.strip(" ,.():;")
    if w in _ROLE_WORDS:
        return True
    if len(w) > 2 and w[0] in _PREFIXES and w[1:] in _ROLE_WORDS:
        return True
    base = w[1:] if w.startswith("וה") else w
    return len(base) >= 4 and base.startswith("ה") and base not in _HE_NAMES


# Given/family names that really start with ו; any other ו-word is a conjunction
# ("ביטוח וחיסכון", "דביר כהנא וציונה ...").
_VAV_NAME_RE = re.compile(r"^(?:וו|וי|וא|ולדימיר|ורד|ולך|וליד|וקנין)")


def _looks_like_name(seg: str) -> bool:
    words = seg.split()
    if not 2 <= len(words) <= 4 or _DIGIT_RE.search(seg):
        return False
    if not all(_NAME_TOKEN_RE.match(w) for w in words):
        return False
    if any(w.startswith("ו") and not _VAV_NAME_RE.match(w) for w in words):
        return False
    return not any(_is_role_word(w) for w in words)


# Split the title into clauses: commas, parentheses, dashes, and the "ו" that
# joins a second office ("... מאי גולן ומנכ"ל המשרד ...").
_CLAUSE_SPLIT_RE = re.compile(
    r"\s*[,;()]\s*|\s+[-–—]\s+|\s+(?=ו(?:מנכ|סגנ|יומן|יו\"ר|ראש|השר|שר|המנכ|מ\"מ|ממלא))")
_ROLE_HEAD_RE = re.compile(
    r"^(?:יומנ[יוה]?|יומן|לו\"?ז)\s+|^ו(?=\S)")
# "מאי" is also a given name (מאי גולן), so the bare month list leaves it out;
# it only counts with a ל/מ prefix ("למאי", "ממאי").
_MONTHS = "ינואר|פברואר|מרץ|מרס|אפריל|יוני|יולי|אוגוסט|ספטמבר|אוקטובר|נובמבר|דצמבר"
_PERIOD_TAIL_RE = re.compile(
    r"[\s.]+(?:ו?ל?(?:שנ(?:ת|ים)|חודש(?:ים)?|תאריכים|רבעון|רבעונים|חציון|מחצית)|מיום|מראשית|"
    r"ל(?=\s+\d)|מכניסת|בעקבות|כפי ש|שהתקבל|(?:" + _MONTHS + r")\b|[למ](?:" + _MONTHS + r"|מאי)\b|\d).*$")


def _clean_role(seg: str) -> str:
    s = re.sub(r"^(?:FW|Fwd|RE)\s*:\s*", "", seg.strip(), flags=re.IGNORECASE)
    prev = None
    while prev != s:
        prev = s
        s = _ROLE_HEAD_RE.sub("", s)
        s = re.sub(r"^(?:של|ולו\"?ז|לו\"?ז)\s+", "", s)
    s = _PERIOD_TAIL_RE.sub("", s)
    s = re.sub(r"\s+(?:פרופ['׳]?|ד[\"״]ר|עו[\"״]ד|הגב['׳]?|מר|כבוד השופטת?)$", "", s)
    s = re.sub(r"\s+", " ", s).strip(" ,.-–—")
    if not s or all(w in ("יומן", "יומני", "לו\"ז", "ולו\"ז", "של") for w in s.split()):
        return ""
    return s


def _strip_period(seg: str) -> str:
    return _PERIOD_TAIL_RE.sub("", seg).strip(" ,.-–—")


def _name_variants(name: str, reorder: bool = True) -> list[str]:
    """Spellings a stored person name may take inside a title. The Knesset form
    is family-name first ("סמוטריץ בצלאל יואל"); diaries say "בצלאל סמוטריץ'".
    ``reorder=False`` for names already in title order (learned from titles)."""
    toks = normalize_name(name).split()
    out = {" ".join(toks)}
    n = len(toks)
    for k in range(1, n if reorder else 1):
        fam, given = toks[:k], toks[k:]
        if given[0] in _SURNAME_PARTICLES:
            continue
        out.add(" ".join(given + fam))
        out.add(" ".join(given[:1] + fam))
    return [v for v in out if len(v.split()) >= 2]


_SURNAME_PARTICLES = {"בן", "בר", "אבו", "אל", "דה", "ון", "פון", "בית"}


def _comparable(text: str) -> str:
    s = _QUOTES_RE.sub("", normalize_name(text)).replace("-", " ").replace("־", " ")
    s = re.sub(r"[,;()\[\]–—]", " ", s)
    return " " + " ".join(t.translate(_FINALS) for t in s.split()) + " "


class PeopleIndex:
    """Known people, searchable inside a title by any of their spellings.

    ``people`` is ``[(id or None, name)]``; an id of ``None`` is a name learned
    from another title (see ``learn_names``) that has no ``people`` row yet.
    """

    def __init__(self, people: list[tuple[str | None, str]]):
        self._variants: list[tuple[str, str | None, str, str]] = []  # (cmp, id, stored, display)
        seen = set()
        for pid, nm in people:
            for v in _name_variants(nm, reorder=pid is not None):
                cv = _comparable(v)
                if (cv, nm) in seen or len(cv.split()) < 2:
                    continue
                seen.add((cv, nm))
                self._variants.append((cv, pid, nm, v))
        # Longest variant first so "אבי כהן סקלי" wins over "אבי כהן".
        self._variants.sort(key=lambda t: -len(t[0]))

    def find_in(self, text: str) -> list[tuple[str | None, str, int]]:
        """(person_id, name as spelled in the title, position) for each person
        named in ``text``, one entry per overlapping span."""
        hay = _comparable(text)
        taken: list[tuple[int, int]] = []
        out: list[tuple[str | None, str, int]] = []
        for cv, pid, _stored, display in self._variants:
            start = hay.find(cv)
            while start != -1:
                end = start + len(cv)
                if not any(s < end and start < e for s, e in taken):
                    taken.append((start, end))
                    out.append((pid, display, start))
                    break
                start = hay.find(cv, start + 1)
        out.sort(key=lambda t: t[2])
        return out


_HONORIFIC_NAME_RE = re.compile(
    r"(?:פרופ['׳]|פרופסור|ד[\"״]ר|עו[\"״]ד|רו[\"״]ח|הרב|כבוד השופט(?:ת)?)\s+"
    r"((?:[א-ת][א-ת\"'׳״\-]*\s*){2,3})")


def _clause_names(clause: str) -> list[str]:
    """Name-shaped text in one clause: the whole clause ("איתמר בן גביר"),
    two names joined by ו ("דביר כהנא וציונה קניג-יאיר"), or a name after an
    honorific ("המשנה למנכ"ל ... פרופ' איתמר גרוטו")."""
    c = _strip_period(normalize_name(clause))
    if _looks_like_name(c):
        return [c]
    out: list[str] = []
    parts = re.split(r"\s+ו(?=[א-ת])", c)
    if len(parts) == 2 and all(_looks_like_name(p) for p in parts):
        return parts
    for m in _HONORIFIC_NAME_RE.finditer(clause):
        words = []
        for w in m.group(1).split():
            if _is_role_word(w) or _DIGIT_RE.search(w):
                break
            words.append(w)
        cand = " ".join(words)
        if _looks_like_name(cand):
            out.append(cand)
    return out


def _subject_label(title: str) -> str:
    """The diary's office when no person is named: "עיריית חדרה - יומן ראש
    הרשות ... - מחצית ראשונה 2022" → "עיריית חדרה – ראש הרשות"."""
    t = normalize_name(re.sub(r"^(?:FW|Fwd|RE)\s*:\s*", "", title.split(" — ")[0], flags=re.I))
    org = ""
    m = re.match(r"^(.{3,80}?)\s+[-–]\s+(.*)$", t)
    if m and not re.match(r"^(?:יומנ?[יוה]?|לו\"?ז)\b", m.group(1)):
        org, t = m.group(1).strip(), m.group(2)
    role = ""
    for part in re.split(r"\s+[-–]\s+|[,(]", t):
        role = _clean_role(part)
        if role:
            break
    role = re.sub(r"\s+ו?/?או\s+.*$|\s+המקומית.*$", "", role).strip()
    if org and role:
        return f"{org} – {role}"
    return role or org or t


def _role_before(head: str, pos: int) -> str | None:
    """The office phrase just before the name at comparable offset ``pos``
    ("יומן השרה לשוויון חברתי, מאי גולן" → "השרה לשוויון חברתי")."""
    n_words = len(_comparable(head)[:pos].split())
    if n_words == 0:
        return None
    # Re-cut the original text at the same comparable-word count, keeping its
    # punctuation (a hyphenated or punctuation-only word counts as _comparable does).
    words = re.sub(r"\s+", " ", head).strip().split(" ")
    acc, cut = 0, 0
    for i, w in enumerate(words):
        if acc >= n_words:
            break
        acc += len(_comparable(w).split())
        cut = i + 1
    before = " ".join(words[:cut])
    clauses = [c for c in _CLAUSE_SPLIT_RE.split(before) if c and c.strip(" ,")]
    picked: list[str] = []
    for c in reversed(clauses):
        if _clause_names(c):
            break  # the previous owner's name — the role belongs to it, not us
        picked.insert(0, c.strip())
        if any(_is_role_word(w) and w.strip(" ,") not in ("יומן", "יומני", "ויומן", "של")
               for w in c.split()):
            break
    else:
        if not picked:
            return None
    r = _clean_role(", ".join(picked))
    return r or None


_DIARY_WORD_RE = re.compile(r"(?:^|\s|\()ו?(?:יומן|יומני|יומנו|יומנה|יומנים|לו\"?ז)(?:\s|$|,)")


def extract_owners(title: str | None, resource_name: str | None,
                   people: PeopleIndex | None) -> list[dict]:
    """Owners named by a diary title, in title order.

    Returns ``[{name, person_id, role, kind, method}]``; ``kind`` is ``person``
    (a known person matched, or a name-shaped clause) or ``subject`` (no name:
    the office itself). ``resource_name`` is consulted for names only when the
    title names nobody (e.g. "יומן מנכ"ל משרד החקלאות — לוז אורן לביא").
    """
    title = normalize_name(title)
    if not title:
        return []
    # The OVER source name is "<dataset title> — <resource name>"; the dataset
    # title is the authoritative part.
    head = title.split(" — ")[0]
    found: list[tuple[int, dict]] = []

    def add(pos, name, pid, kind, method):
        name = normalize_name(name)
        for _, o in found:
            if kind == "person" and o["kind"] == "person" and names_match(o["name"], name):
                o["person_id"] = o["person_id"] or pid
                return
        found.append((pos, {"name": name, "person_id": pid, "role": None,
                            "kind": kind, "method": method}))

    hay = _comparable(head)
    # 1. Known people anywhere in the dataset title.
    for pid, nm, pos in (people.find_in(head) if people else []):
        add(pos, nm, pid, "person", "known_name" if pid else "learned_name")
    # 2. Name-shaped clauses ("יומן השר ..., <שם פרטי ושם משפחה>, ...") — only in
    # a diary-shaped title; "פגישות עם נציגי דלק, שברון, ..." lists companies.
    clauses = _CLAUSE_SPLIT_RE.split(head) if _DIARY_WORD_RE.search(head) else []
    for c in clauses:
        if not c or not c.strip():
            continue
        for nm in _clause_names(c):
            pos = hay.find(_comparable(nm))
            add(pos if pos >= 0 else len(hay), nm, None, "person", "title_clause")

    found.sort(key=lambda t: t[0])
    owners = []
    for pos, o in found:
        o["role"] = _role_before(head, pos) if pos > 0 else None
        owners.append(o)

    # 3. Nobody in the title → try the resource name, then fall back to the office.
    if not owners and resource_name and people:
        for pid, nm, _ in people.find_in(resource_name):
            if pid:
                owners.append({"name": normalize_name(nm), "person_id": pid, "role": None,
                               "kind": "person", "method": "resource_name"})
    if not owners:
        label = _subject_label(title)
        if label:
            owners.append({"name": label, "person_id": None, "role": None,
                           "kind": "subject", "method": "subject"})
    return owners


def learn_names(titles: list[str], people: PeopleIndex | None) -> list[str]:
    """Names that some title states plainly (", עמיר פרץ,"), so a pass over
    the corpus can also find them where another title runs them into the role
    ("יומן שר הכלכלה עמיר פרץ לשנת 2020")."""
    names: dict[str, str] = {}
    for t in titles:
        for o in extract_owners(t, None, people):
            if o["kind"] == "person" and o["method"] == "title_clause":
                names.setdefault(name_key(o["name"]), o["name"])
    return list(names.values())


# ── storage ──────────────────────────────────────────────────────────────────

_DDL = [
    """CREATE TABLE IF NOT EXISTS diary_source_owners (
        source_id uuid NOT NULL REFERENCES diary_sources(id) ON DELETE CASCADE,
        owner_key text NOT NULL,
        owner_label text NOT NULL,
        person_id uuid REFERENCES people(id) ON DELETE SET NULL,
        kind text NOT NULL DEFAULT 'person',
        role text,
        method text,
        position int NOT NULL DEFAULT 0,
        created_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (source_id, owner_key))""",
    "CREATE INDEX IF NOT EXISTS diary_source_owners_key_idx ON diary_source_owners (owner_key)",
    "CREATE INDEX IF NOT EXISTS diary_source_owners_person_idx ON diary_source_owners (person_id)",
]
_ready = False


async def ensure_tables() -> None:
    global _ready
    if _ready or not ocal_db.is_configured():
        return
    for stmt in _DDL:
        await ocal_db.execute(stmt)
    _ready = True


def owner_key_for(owner: dict) -> str:
    if owner["kind"] == "person":
        return "p:" + name_key(owner["name"])
    return "s:" + re.sub(r"\s+", " ", owner["name"]).strip()


def canonical_keys(owners: list[dict]) -> dict[str, str]:
    """Collapse person keys that ``names_match`` (מאי גולן / בדרה גולן פלורה מאי)
    onto the shortest spelling's key, so the picker shows one entry per person."""
    persons = sorted({owner_key_for(o) for o in owners if o["kind"] == "person"},
                     key=lambda k: (len(k.split()), k))
    canon: dict[str, str] = {}
    reps: list[str] = []
    for k in persons:
        rep = next((r for r in reps if names_match(r[2:], k[2:])
                    and len(set(r[2:].split())) >= 2), None)
        if rep is None:
            reps.append(k)
            rep = k
        canon[k] = rep
    return canon


async def rebuild_owners() -> dict:
    """(Re)derive diary_source_owners for the whole corpus (~1k sources — one
    pass is cheap, and owner keys must be canonicalised across all of it)."""
    await ensure_tables()
    people_rows = await ocal_db.fetch("SELECT id, name FROM people WHERE length(name) > 3")
    people = [(str(r["id"]), r["name"]) for r in people_rows]
    srcs = await ocal_db.fetch(
        "SELECT s.id, s.name, s.person_id, p.name AS person_name, "
        "s.ckan_metadata->>'datasetTitle' AS ds_title, "
        "s.ckan_metadata->>'resourceName' AS res_name "
        "FROM diary_sources s LEFT JOIN people p ON p.id = s.person_id")
    titles = [s["ds_title"] or s["name"] for s in srcs]
    learned = learn_names(titles, PeopleIndex(people))
    index = PeopleIndex(people + [(None, n) for n in learned])

    per_source: list[tuple] = []
    all_owners: list[dict] = []
    for s, title in zip(srcs, titles):
        owners = extract_owners(title, s["res_name"], index)
        # The admin-curated person_id always counts as an owner, and replaces a
        # subject placeholder.
        if s["person_id"] and s["person_name"]:
            if not any(o["kind"] == "person" and names_match(o["name"], s["person_name"])
                       for o in owners):
                owners = [o for o in owners if o["kind"] == "person"]
                owners.append({"name": normalize_name(s["person_name"]),
                               "person_id": str(s["person_id"]), "role": None,
                               "kind": "person", "method": "source_person"})
        per_source.append((s["id"], owners))
        all_owners.extend(owners)

    canon = canonical_keys(all_owners)
    # Display label per canonical key: the most common spelling in the titles.
    labels: dict[str, Counter] = {}
    pid_for: dict[str, str] = {}
    for o in all_owners:
        k = canon.get(owner_key_for(o), owner_key_for(o))
        labels.setdefault(k, Counter())[o["name"]] += 1
        if o.get("person_id") and k not in pid_for:
            pid_for[k] = o["person_id"]

    rows = []
    for sid, owners in per_source:
        seen = set()
        for pos, o in enumerate(owners):
            k = canon.get(owner_key_for(o), owner_key_for(o))
            if k in seen:
                continue
            seen.add(k)
            label = labels[k].most_common(1)[0][0]
            pid = o.get("person_id") or pid_for.get(k)
            rows.append((sid, k, label, uuid.UUID(str(pid)) if pid else None, o["kind"], o.get("role"), o["method"], pos))

    pool = await ocal_db.get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DELETE FROM diary_source_owners")
            await conn.executemany(
                "INSERT INTO diary_source_owners "
                "(source_id, owner_key, owner_label, person_id, kind, role, method, position) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING",
                rows)
    result = {"sources": len(per_source), "owner_links": len(rows),
              "person_links": sum(1 for r in rows if r[4] == "person"),
              "distinct_owners": len({r[1] for r in rows}),
              "learned_names": len(learned)}
    logger.info("ocal_owners: rebuilt %s", result)
    return result
