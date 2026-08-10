"""נדל"ן לעם — the pure text/key layer.

Everything here is deterministic, DB-free and offline-testable. It exists as its
own module for the same reason ``settlement_index.aliases_for()`` is pure: the
matching rules are where a property crosswalk silently goes wrong, so they have
to be assertable without a database.

Three jobs:

1. **Keys.** ``parcel_key()`` / ``gp_key()`` turn the parcels layer's text
   columns into the canonical identifiers the whole project joins on. They are
   deterministic strings derived from source values — NOT surrogate ids — so a
   TRUNCATE+INSERT rebuild reproduces them exactly and nothing downstream
   (including the transactions table that lands later) breaks.
2. **Address parsing.** ``parse_house_number()`` copes with the two very
   different conventions the sources use: the postal file zero-pads
   (``'00043'``) while the address list is dirty free text
   (``'דוד אבידן 10'`` instead of ``'10'``).
3. **Street aliasing.** ``street_aliases_for()`` generates the weighted variants
   that close the one measured naming seam — the gazetteer's street spellings
   against the postal/address-list spellings (raw-name overlap is only 56%,
   while postal↔address-list is 100%).

The normalization itself is imported from :mod:`settlement_index` rather than
re-implemented, so a street key and a locality key can never drift apart in how
they treat geresh, maqaf and spacing.
"""
from __future__ import annotations

import re

from app.services.settlement_index import norm, strip_paren

__all__ = [
    "norm", "strip_paren", "as_int", "parcel_key", "gp_key", "split_parcel_key",
    "parse_house_number", "strip_street_type", "street_aliases_for", "sniff_mode",
]

# ── numbers ───────────────────────────────────────────────────────────────────
# The parcels layer stores integers as floats-in-text ('3287.0'), the postal file
# zero-pads ('00043'). Both must land on the same int.
_INT_RE = re.compile(r"^\s*([0-9]+)(?:\.0*)?\s*$")


def as_int(v) -> int | None:
    """'3287.0' → 3287, '00043' → 43, '' / None / 'א' → None."""
    if v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v) if v == int(v) else None
    m = _INT_RE.match(str(v))
    return int(m.group(1)) if m else None


# ── parcel keys ───────────────────────────────────────────────────────────────
def parcel_key(gush, suffix, parcel) -> str | None:
    """The canonical חלקה key: ``'{gush}-{suffix}-{parcel}'`` e.g. ``'6390-0-308'``.

    A missing/blank suffix means 0 — the overwhelming majority of the register.
    Returns None when gush or parcel is not a plain integer, so a malformed row
    is dropped loudly at build time rather than creating a key nothing matches."""
    g, p = as_int(gush), as_int(parcel)
    if g is None or p is None:
        return None
    s = as_int(suffix)
    return f"{g}-{0 if s is None else s}-{p}"


def gp_key(gush, parcel) -> str | None:
    """The suffix-LESS key: ``'{gush}-{parcel}'``.

    The gazetteer publishes GushNum/ParcelNum but no gush suffix, so it can only
    attach at this grain. Measured on three gush ranges (173,405 pairs), 0.63% of
    them cover more than one real parcel — those are flagged ``gp_ambiguous`` on
    the spine rather than silently collapsed onto one of them."""
    g, p = as_int(gush), as_int(parcel)
    return None if g is None or p is None else f"{g}-{p}"


def split_parcel_key(key: str) -> tuple[int, int, int] | None:
    """'6390-0-308' → (6390, 0, 308); anything else → None."""
    parts = (key or "").split("-")
    if len(parts) != 3:
        return None
    vals = [as_int(x) for x in parts]
    return None if any(v is None for v in vals) else (vals[0], vals[1], vals[2])


# ── house numbers ─────────────────────────────────────────────────────────────
_DIGITS_RE = re.compile(r"[0-9]+")


def parse_house_number(v) -> tuple[int | None, str | None]:
    """Pull (number, letter-suffix) out of a house-number field.

    The two sources disagree completely on what this field holds, so the rule is
    positional: if the value STARTS with a digit the number is the first run
    ('00043' → 43, '12א' → (12,'א'), '5-7' → 5); otherwise the number is the last
    run, which is what rescues the address list's dirty
    ``'דוד אבידן 10'`` → 10. A letter immediately following the chosen run is
    kept as the suffix (בית 12א and 12ב are different doorways)."""
    if v is None:
        return None, None
    s = str(v).strip()
    if not s:
        return None, None
    matches = list(_DIGITS_RE.finditer(s))
    if not matches:
        return None, None
    m = matches[0] if s[0].isdigit() else matches[-1]
    num = int(m.group(0))
    tail = s[m.end():].lstrip()
    suffix = tail[0] if tail and (("א" <= tail[0] <= "ת") or tail[0].isalpha()) else None
    return num, suffix


# ── street names ──────────────────────────────────────────────────────────────
# Generic street-type words that one source writes and another omits. Stored
# NORMALIZED (norm() has already dropped the geresh), so "רח׳" arrives as "רח".
_STREET_TYPES = [
    "רחוב", "רח", "שדרות", "שדרה", "שד", "סמטה", "סמטת", "דרך", "כיכר", "ככר",
    "כיכרת", "משעול", "מעלה", "מורד", "נתיב", "מבוא", "מעבר", "טיילת", "גשר",
    "רחבת", "רחבה", "שכונת", "שכונה", "כביש", "street", "st", "road", "rd",
]
_STREET_TYPES_SORTED = sorted(_STREET_TYPES, key=len, reverse=True)


def strip_street_type(key: str) -> str:
    """Drop a leading generic street-type word from an ALREADY-normalized key.

    Longest-first so 'שדרות' is not shortened by the 'שד' rule. Refuses to strip
    when little would be left, so a street actually named 'הדרך' survives."""
    for t in _STREET_TYPES_SORTED:
        if key.startswith(t) and len(key) - len(t) >= 3:
            return key[len(t):]
    return key


def street_aliases_for(name: str) -> list[tuple[str, str, str, int]]:
    """All (variant_key, surface, kind, weight) variants for one street name.

    The ladder is ordered so an exact hit always beats a guess:

    ==========  ======  =================================================
    kind        weight  rule
    ==========  ======  =================================================
    official    100     the normalized name itself
    no_paren     90     parentheticals dropped
    no_type      85     leading רחוב/שד׳/סמטת… dropped
    no_he        70     leading definite ה dropped
    token_set    60     same words, any order
    last_token   40     the distinctive last word ('שמואל יבניאלי'→'יבניאלי')
    ==========  ======  =================================================

    ``last_token`` is the lever that actually closes the gazetteer gap — it is
    also the one that can mis-fire ('דוד המלך' and 'שלמה המלך' both end in
    'המלך'), which is why it is emitted at the bottom weight and why the builder
    DROPS any last_token variant that is ambiguous inside its settlement. This
    function only proposes; :mod:`nadlan_index` disambiguates."""
    out: list[tuple[str, str, str, int]] = []
    seen: set[str] = set()

    def add(key: str, kind: str, weight: int):
        if key and len(key) >= 2 and key not in seen:
            seen.add(key)
            out.append((key, name, kind, weight))

    if not name or not name.strip():
        return out

    base = norm(name)
    add(base, "official", 100)

    # The two strippings COMPOSE: "רח' הרצל (הישנה)" has to reach "הרצל", not
    # merely "רחהרצל". Emitting only the un-composed pair left the useful form
    # reachable solely through the weakest rule.
    bare = norm(strip_paren(name))
    add(bare, "no_paren", 90)
    no_type = strip_street_type(base)
    add(no_type, "no_type", 85)
    add(strip_street_type(bare), "no_type", 85)

    for k in (base, bare, no_type, strip_street_type(bare)):
        if k.startswith("ה") and len(k) >= 4:
            add(k[1:], "no_he", 70)

    # Word-order-insensitive variant, built from the RAW words (norm() would have
    # already glued them together and lost the boundaries).
    words = [w for w in re.split(r"[^0-9A-Za-zא-ת]+", strip_paren(name)) if w]
    if len(words) > 1:
        add("".join(sorted(norm(w) for w in words)), "token_set", 60)
        last = norm(words[-1])
        if len(last) >= 4 and last != base:
            add(last, "last_token", 40)

    return out


# ── free-text mode sniffing ───────────────────────────────────────────────────
_GUSH_HELKA_WORDS = re.compile(r"גוש\D{0,4}([0-9]+)\D{0,12}?חלקה\D{0,4}([0-9]+)")
_GUSH_HELKA_SLASH = re.compile(r"^\s*([0-9]{1,6})\s*[/\\]\s*([0-9]{1,6})\s*$")
_LATLON = re.compile(r"^\s*(-?[0-9]{1,2}\.[0-9]+)\s*,\s*(-?[0-9]{1,3}\.[0-9]+)\s*$")
_DIGITS_ONLY = re.compile(r"^\s*([0-9]+)\s*$")


def sniff_mode(q: str) -> dict:
    """Map one free-text box onto a lookup mode.

    Returns ``{"mode", "parsed", "alternatives"}``. ``alternatives`` matters: a
    bare 5-digit number is a legitimate ZIP5 *and* a legitimate gush, so both
    readings come back and the caller shows both rather than guessing wrong."""
    s = (q or "").strip()
    if not s:
        return {"mode": "empty", "parsed": {}, "alternatives": []}

    m = _LATLON.match(s)
    if m:
        lat, lon = float(m.group(1)), float(m.group(2))
        # Israel is ~29–34 N, 34–36 E; accept a swapped pair rather than
        # returning an empty map for what is obviously a coordinate.
        if lon < lat:
            lat, lon = lon, lat
        return {"mode": "point", "parsed": {"lat": lat, "lon": lon}, "alternatives": []}

    m = _GUSH_HELKA_WORDS.search(s) or _GUSH_HELKA_SLASH.match(s)
    if m:
        return {"mode": "gush_helka",
                "parsed": {"gush": int(m.group(1)), "helka": int(m.group(2))},
                "alternatives": []}

    m = _DIGITS_ONLY.match(s)
    if m:
        digits = m.group(1)
        if len(digits) == 7:
            return {"mode": "zip", "parsed": {"zip": digits}, "alternatives": []}
        if len(digits) == 5:
            return {"mode": "zip", "parsed": {"zip": digits},
                    "alternatives": [{"mode": "gush", "parsed": {"gush": int(digits)}}]}
        return {"mode": "gush", "parsed": {"gush": int(digits)},
                "alternatives": []}

    return {"mode": "address", "parsed": {"text": s}, "alternatives": []}
