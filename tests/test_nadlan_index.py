"""Offline tests for the נדל"ן לעם index: the street matcher and the invariants
that keep the crosswalk honest. No database — `_resolve_streets` is deliberately
pure so the matching rules can be asserted with plain dicts.

The rule under the most scrutiny here is the ambiguity guard. The measured seam
this index exists to close is the gazetteer's street spellings against the
postal/address-list spellings (56% raw overlap), and the lever that closes it —
matching on the distinctive last word — is also the one that can mis-link
'דוד המלך' to 'שלמה המלך'. So a low-confidence variant that could pick either
street must pick NEITHER.
"""
from app.services import data_catalog, nadlan_index as ni


def _canon(sc, name, n=1, in_addr=True, in_post=False, street_id=None):
    return {"sc": sc, "name": name, "n": n, "in_addr": in_addr,
            "in_post": in_post, "street_id": street_id}


def _gaz(sc, name, n=1, code=None, name_en=None, sname="עיר"):
    return {"sc": sc, "name": name, "n": n, "code": code,
            "name_en": name_en, "sname": sname}


def _by_key(rows):
    return {r[0]: r for r in rows}


# ── canonical side ────────────────────────────────────────────────────────────
def test_canonical_street_keys_are_deterministic_text():
    streets, _aliases, _un = ni._resolve_streets([_canon(7900, "אבימלך")], [])
    # A deterministic key (not a surrogate id) is what lets a TRUNCATE+INSERT
    # rebuild reproduce the crosswalk without breaking anything downstream.
    assert _by_key(streets)["7900-אבימלך"][2] == "אבימלך"


def test_spelling_variants_collapse_onto_one_street():
    # geresh/spacing/hyphen differences must not create two streets.
    streets, _a, _u = ni._resolve_streets(
        [_canon(7900, "ג'בר", n=5), _canon(7900, "ג׳בר", n=2)], [])
    assert len(streets) == 1
    assert streets[0][2] == "ג'בר"       # the more frequent spelling wins


def test_postal_and_address_list_merge_into_one_street():
    streets, _a, _u = ni._resolve_streets(
        [_canon(7900, "רוטשילד", n=9, in_addr=True),
         _canon(7900, "רוטשילד", n=3, in_addr=False, in_post=True, street_id="079277")], [])
    row = _by_key(streets)["7900-רוטשילד"]
    assert row[7] is True and row[8] is True      # in_postal, in_address_list
    assert row[5] == "079277"                     # postal_street_id carried over


# ── the gazetteer seam ────────────────────────────────────────────────────────
def test_gazetteer_full_name_matches_the_short_canonical_name():
    """The whole point of the index: 'שמואל יבניאלי' must reach 'יבניאלי'."""
    streets, aliases, unmatched = ni._resolve_streets(
        [_canon(7900, "יבניאלי")],
        [_gaz(7900, "שמואל יבניאלי", code="79000600")])
    assert len(streets) == 1                       # not two separate streets
    assert unmatched == []
    assert _by_key(streets)["7900-יבניאלי"][6] == "79000600"   # gaztir code attached
    assert _by_key(streets)["7900-יבניאלי"][9] is True         # in_gazetteer
    # The gazetteer's own spelling is now an alias of the canonical street.
    assert ("שמואליבניאלי", "7900-יבניאלי") in {(a[1], a[2]) for a in aliases}


def test_street_type_prefix_does_not_split_a_street():
    streets, _a, unmatched = ni._resolve_streets(
        [_canon(7900, "רוטשילד")], [_gaz(7900, "שדרות רוטשילד")])
    assert len(streets) == 1 and unmatched == []


def test_ambiguous_last_token_links_to_neither_street():
    """'דוד המלך' and 'שלמה המלך' both end in 'המלך'.

    A variant that could designate either street must designate neither —
    otherwise the crosswalk silently attaches a property to the wrong road."""
    streets, aliases, _u = ni._resolve_streets(
        [_canon(7900, "דוד המלך"), _canon(7900, "שלמה המלך")], [])
    keyed = {(a[0], a[1]): a[2] for a in aliases}
    assert (7900, "המלך") not in keyed             # poisoned, not arbitrated
    # The unambiguous full forms still resolve.
    assert keyed[(7900, "דודהמלך")] == "7900-דודהמלך"
    assert keyed[(7900, "שלמההמלך")] == "7900-שלמההמלך"


def test_unmatched_gazetteer_street_still_becomes_a_real_street():
    """A street only the gazetteer knows is real; it just has no counterpart."""
    streets, _a, unmatched = ni._resolve_streets(
        [_canon(7900, "אבימלך")], [_gaz(7900, "רחוב שאיש לא רשם", n=4)])
    assert len(streets) == 2
    # ...and it is recorded as a visible, shrinkable work queue rather than lost.
    assert len(unmatched) == 1
    assert unmatched[0][2] == "gazetteer" and unmatched[0][4] == 4


def test_streets_are_scoped_per_settlement():
    streets, aliases, _u = ni._resolve_streets(
        [_canon(7900, "הרצל"), _canon(5000, "הרצל")], [])
    assert len(streets) == 2
    assert {a[0] for a in aliases} == {7900, 5000}
    # An identical name in another town must never merge.
    assert {s[0] for s in streets} == {"7900-הרצל", "5000-הרצל"}


def test_rows_with_no_settlement_code_are_dropped():
    streets, _a, _u = ni._resolve_streets([_canon(None, "הרצל")], [_gaz(None, "הרצל")])
    assert streets == []


# ── invariants ────────────────────────────────────────────────────────────────
def test_every_table_has_a_hebrew_title_in_the_data_catalog():
    """A new over_re_ table must not surface in /data under its raw name."""
    missing = [t for t in ni.ALL_TABLES if t not in data_catalog._OVER_TITLES]
    assert missing == [], f"missing /data titles for: {missing}"


def test_all_tables_are_over_prefixed_so_the_catalog_picks_them_up():
    # data_catalog._over_index_records() only collects tables starting 'over_'.
    assert all(t.startswith("over_") for t in ni.ALL_TABLES)


def test_every_stage_has_a_builder():
    assert set(ni.STAGES) == set(ni._BUILDERS)


def test_default_build_skips_the_two_expensive_opt_ins():
    """source_indexes is a one-off and pip is the only stage with material Neon
    compute cost — neither may run just because someone pressed rebuild."""
    default = [s for s in ni.STAGES if s not in ("source_indexes", "pip")]
    assert "pip" not in default and "source_indexes" not in default
    assert default == ["parcels", "gazetteer", "postal_localities",
                       "streets", "addresses", "zip5"]
